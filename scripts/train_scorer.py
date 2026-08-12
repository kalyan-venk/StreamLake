"""Train the real-time fraud scorer from the curated silver export and report honest metrics.

Sparkov ships as a train file and a test file that are split in time (train is the earlier period,
test the later one), and the curated export keeps that split in a `source_split` column. This script
fits the logistic regression on the `train` rows only and evaluates it on the `test` rows, so the
reported ROC-AUC / PR-AUC are out-of-time (scored on transactions from a period the model never saw
during fitting), not an in-sample number that would flatter the model.

Everything runs on pandas over the curated Parquet, no Spark. Writes the fitted model to
`models/fraud_scorer.joblib`, which the streaming scorer, the demo, and the unit test all load.

Run: `.venv/bin/python scripts/train_scorer.py` (needs `data/curated/transactions`, i.e. `make
batch` must have run first).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from streamlake.scoring import DEFAULT_MODEL_PATH, LABEL, FraudScorer  # noqa: E402

CURATED = REPO_ROOT / "data" / "curated" / "transactions"
# Only the columns the scorer needs, so the read stays a few hundred MB, not the whole 27-column
# table. is_fraud is the label; source_split is how the train/test period boundary is recovered.
COLUMNS = ["amt", "distance_km", "category", "trans_hour", "is_fraud", "source_split"]


def _metrics(y_true, scores) -> dict[str, float]:
    from sklearn.metrics import average_precision_score, roc_auc_score

    return {
        "roc_auc": round(float(roc_auc_score(y_true, scores)), 4),
        "pr_auc": round(float(average_precision_score(y_true, scores)), 4),
    }


def _decision_breakdown(scorer: FraudScorer, feats_frame: pd.DataFrame, y_true) -> None:
    """Print what the two thresholds actually do on the holdout: how many transactions land in each
    band, and how many of the real frauds each band catches."""
    scores = scorer.score_frame(feats_frame)
    decisions = [scorer.decision_for(float(s))[0] for s in scores]
    frame = pd.DataFrame({"decision": decisions, "is_fraud": y_true})
    total = len(frame)
    total_fraud = int(frame["is_fraud"].sum())
    print(f"\nholdout decision breakdown ({total:,} transactions, {total_fraud:,} actually fraud):")
    print(f"{'decision':<9}{'txns':>12}{'share':>9}{'frauds caught':>16}{'of band':>10}")
    for decision in ("DECLINE", "REVIEW", "APPROVE"):
        band = frame[frame["decision"] == decision]
        n = len(band)
        caught = int(band["is_fraud"].sum())
        share = f"{n / total:.2%}" if total else "0%"
        precision = f"{caught / n:.2%}" if n else "n/a"
        print(f"{decision:<9}{n:>12,}{share:>9}{caught:>16,}{precision:>10}")
    flagged = frame[frame["decision"].isin(["DECLINE", "REVIEW"])]
    recall = int(flagged["is_fraud"].sum()) / total_fraud if total_fraud else 0.0
    print(
        f"\ndecline+review together flag {len(flagged):,} txns "
        f"({len(flagged) / total:.2%}) and catch {int(flagged['is_fraud'].sum()):,} of "
        f"{total_fraud:,} frauds (recall {recall:.1%})"
    )


def main() -> None:
    if not CURATED.exists():
        raise SystemExit(
            f"{CURATED} not found. Run the batch spine first (`make batch`); the scorer trains on "
            "the real curated silver export, it does not invent data."
        )

    df = pd.read_parquet(CURATED, columns=COLUMNS)
    train = df[df["source_split"] == "train"].reset_index(drop=True)
    test = df[df["source_split"] == "test"].reset_index(drop=True)
    print(
        f"train: {len(train):,} rows ({int(train[LABEL].sum()):,} fraud, "
        f"{train[LABEL].mean():.3%})   "
        f"test: {len(test):,} rows ({int(test[LABEL].sum()):,} fraud, {test[LABEL].mean():.3%})"
    )

    scorer = FraudScorer.train(train, metadata={"trained_from": str(CURATED)})

    from streamlake.scoring import event_to_features

    test_feats = pd.DataFrame([event_to_features(r) for r in test.to_dict("records")])
    test_scores = scorer.score_frame(test_feats)
    metrics = _metrics(test[LABEL].to_numpy(), test_scores)
    print(
        f"\nout-of-time (test split): ROC-AUC {metrics['roc_auc']}  PR-AUC {metrics['pr_auc']}"
    )
    print(
        f"thresholds set on train: review >= {scorer.review_threshold:.4f}, "
        f"decline >= {scorer.decline_threshold:.4f}"
    )

    _decision_breakdown(scorer, test_feats, test[LABEL].to_numpy())

    scorer.metadata.update(metrics)
    path = scorer.save(DEFAULT_MODEL_PATH)
    print(f"\nwrote model -> {path}")


if __name__ == "__main__":
    main()
