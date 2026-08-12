"""End-to-end proof of the real-time scoring path: an event goes in, a fraud decision comes out.

The streaming scorer (`src/streamlake/stream/scorer.py`) is the production shape: Kafka -> Spark
Structured Streaming -> the model -> an Iceberg decision table. That needs a running Kafka broker
and a Spark session. This script proves the same scoring *logic* without either, by replaying real
transactions from the curated export as event dicts (the exact payload the Kafka producer emits,
built by the same `EVENT_COLUMNS`) and pushing each one through `FraudScorer.score_one`, the same
call the streaming job runs per row inside `foreachBatch`.

So what is real here and what is stubbed: the model is real (trained on the curated silver export by
`scripts/train_scorer.py`), the events are real transactions with their real amounts, categories,
distances and fraud labels, and the decision logic is the identical code the streaming job uses.
What is stubbed is only the transport: this reads Parquet and calls the scorer in-process instead of
producing to Kafka and consuming with Spark. The streaming module is what wires the same scorer to a
live broker.

Run: `.venv/bin/python scripts/demo_realtime_scoring.py [--n 8]` (needs a trained model and the
curated export; `make batch` then `python scripts/train_scorer.py`).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from streamlake.scoring import DEFAULT_MODEL_PATH, FraudScorer  # noqa: E402

CURATED = REPO_ROOT / "data" / "curated" / "transactions"
# The same fields the Kafka producer puts on an event (streamlake.stream.producer.EVENT_COLUMNS),
# plus trans_time so the demo can derive the hour exactly as the live event's event_ts would.
EVENT_COLUMNS = [
    "trans_num",
    "amt",
    "category",
    "merchant",
    "state",
    "cc_num_hash",
    "cc_num_last4",
    "is_fraud",
    "distance_km",
    "trans_time",
]


def _sample_events(n: int) -> list[dict]:
    """Take some real transactions, biased so the demo shows both frauds and legit charges.

    A pure random sample of a 0.5%-fraud feed would almost never contain a fraud, and a demo that
    only ever prints APPROVE proves nothing about the decline path. So this pulls a few known frauds
    and a few known-legit rows and interleaves them. The rows themselves are unaltered real
    transactions; only which rows are shown is curated, not their values or their labels.
    """
    df = pd.read_parquet(CURATED, columns=EVENT_COLUMNS)
    frauds = df[df["is_fraud"] == 1].head(n // 2)
    legit = df[df["is_fraud"] == 0].head(n - len(frauds))
    mixed = pd.concat([frauds, legit]).sample(frac=1.0, random_state=7).reset_index(drop=True)
    events = mixed.to_dict("records")
    for e in events:
        # Mirror the producer: it stamps event_ts (an ISO string) rather than shipping trans_time.
        e["event_ts"] = pd.Timestamp(e.pop("trans_time")).isoformat()
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=8, help="how many events to score")
    parser.add_argument("--model", default=str(DEFAULT_MODEL_PATH))
    args = parser.parse_args()

    if not CURATED.exists():
        raise SystemExit(f"{CURATED} not found; run `make batch` first.")
    scorer = FraudScorer.load(args.model)
    print(
        f"loaded model (review>={scorer.review_threshold:.3f}, "
        f"decline>={scorer.decline_threshold:.3f}, "
        f"out-of-time ROC-AUC {scorer.metadata.get('roc_auc', 'n/a')})\n"
    )

    events = _sample_events(args.n)
    print(
        f"{'trans_num':<12}{'amt':>10} {'category':<16}"
        f"{'p(fraud)':>10} {'decision':<9}{'actual':>8}"
    )
    print("-" * 76)
    correct_flags = 0
    for event in events:
        decision = scorer.score_one(event)
        actual = "FRAUD" if event.get("is_fraud") == 1 else "legit"
        flagged = decision["decision"] in ("DECLINE", "REVIEW")
        if flagged and event.get("is_fraud") == 1:
            correct_flags += 1
        print(
            f"{str(decision['trans_num'])[:10]:<12}"
            f"{decision['amt']:>10.2f} "
            f"{str(decision['category']):<16}"
            f"{decision['fraud_probability']:>10.4f} "
            f"{decision['decision']:<9}"
            f"{actual:>8}"
        )

    n_fraud = sum(1 for e in events if e.get("is_fraud") == 1)
    print(
        f"\nof {n_fraud} real fraud(s) in this sample, {correct_flags} were flagged "
        "(declined or sent to review)"
    )
    print(
        "\nthis is the in-process path. the same FraudScorer.score_one runs per event inside the "
        "streaming job's foreachBatch (src/streamlake/stream/scorer.py) against a live Kafka topic."
    )


if __name__ == "__main__":
    main()
