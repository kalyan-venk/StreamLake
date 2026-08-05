"""The event producer: replay real transactions onto Kafka as if they were happening now.

Event time gets rewritten, the payload does not: each transaction keeps its real amount,
category, merchant and fraud label but is stamped with a fresh ``event_ts``, so the windowed
aggregation downstream runs against a live clock while the numbers stay real. The payload is read
from the curated export, which is already past silver's PII handling, so nothing that identifies
a cardholder ever reaches the Kafka topic either.

The producer also misbehaves on purpose. A configurable share of events is sent twice
(``duplicate_rate``) and another share arrives a little late (``late_rate``, backdated 60-240s),
which is enough to exercise the consumer's dedup on every ordinary run. Proving the watermark's
*drop* path needs more than a little lateness, though: it needs events stamped well behind a
watermark that has already moved on, which a single continuous replay does not naturally produce
(nothing has advanced the watermark yet when the first event arrives). ``force_late_seconds``
exists for that: it deterministically backdates every event in the call by a fixed amount,
regardless of the configured ``late_rate``, so a second, separate producer call can be sent after
the consumer's watermark has genuinely moved past that point. See
``scripts/demo_late_arrivals.py`` and ``docs/RUNBOOK.md``'s late-arrival section for the full
two-phase demo this makes possible.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from streamlake import monitoring
from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger

log = get_logger(__name__)

# Columns pulled from the curated export to build events. Read with pyarrow so the producer
# starts in about a second and holds a few MB, instead of booting a JVM to read parquet.
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
]


def _source_rows(cfg: Config, limit: int, *, skip: int = 0) -> list[dict[str, Any]]:
    import pyarrow.dataset as ds

    curated = cfg.curated_dir("transactions")
    if not curated.exists():
        raise RuntimeError(
            f"{curated} not found, run the batch spine first (`make batch`); the producer "
            "replays real transactions rather than inventing them"
        )
    dataset = ds.dataset(str(curated), format="parquet")
    # head() only reads from the start, so a non-zero skip reads a larger head and slices it in
    # memory. Two producer calls sharing the same curated file (one demo's "on time" phase, the
    # next demo's "late" phase) use disjoint skip/limit ranges so their trans_num keys never
    # collide, which keeps the dedup and late-drop counts each call produces unambiguous.
    table = dataset.head(skip + limit, columns=EVENT_COLUMNS)
    if skip:
        table = table.slice(skip, limit)
    return table.to_pylist()


def _events(
    rows: list[dict[str, Any]],
    cfg: Config,
    *,
    force_late_seconds: int | None,
    duplicate_rate: float,
    late_rate: float,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (key, event) pairs, injecting duplicates and late arrivals as configured."""
    rng = random.Random(42)  # reproducible misbehaviour

    for row in rows:
        now = datetime.now(UTC)
        if force_late_seconds is not None:
            # Deterministic, not probabilistic: every event in this call lands exactly this far
            # behind wall clock, so the caller can guarantee it is older than a watermark that
            # has already advanced past a chosen point, rather than hoping a random jitter of
            # 60-240s happens to be enough.
            event_ts = now - timedelta(seconds=force_late_seconds)
            late = True
        else:
            late = rng.random() < late_rate
            event_ts = now - timedelta(seconds=rng.randint(60, 240)) if late else now

        event = {
            "trans_num": row["trans_num"],
            "event_ts": event_ts.isoformat(),
            "ingested_ts": now.isoformat(),
            "late": late,
            "amt": row["amt"],
            "category": row["category"],
            "merchant": row["merchant"],
            "state": row["state"],
            "cc_num_hash": row["cc_num_hash"],
            "cc_num_last4": row["cc_num_last4"],
            "is_fraud": row["is_fraud"],
            "distance_km": row["distance_km"],
        }
        yield row["trans_num"], event

        if rng.random() < duplicate_rate:
            # Same key, same event time, sent twice, exactly what an at-least-once upstream
            # does after a failed ack.
            yield row["trans_num"], dict(event, redelivery=True)


def run(
    cfg: Config | None = None,
    *,
    max_events: int | None = None,
    skip_events: int = 0,
    force_late_seconds: int | None = None,
    duplicate_rate: float | None = None,
    late_rate: float | None = None,
    label: str = "producer",
) -> dict[str, int]:
    from confluent_kafka import Producer

    cfg = cfg or get_config()
    topic = str(cfg.require("kafka.topic"))
    bootstrap = str(cfg.require("kafka.bootstrap_servers"))
    eps = int(cfg.get("kafka.producer.events_per_second", 200))
    limit = int(max_events or cfg.get("kafka.producer.max_events", 10000))
    default_dup_rate = float(cfg.get("kafka.producer.duplicate_rate", 0.0))
    default_late_rate = float(cfg.get("kafka.producer.late_rate", 0.0))
    dup_rate = default_dup_rate if duplicate_rate is None else duplicate_rate
    lat_rate = default_late_rate if late_rate is None else late_rate

    banner(
        log,
        f"PRODUCER[{label}] | topic={topic} broker={bootstrap} eps={eps} max={limit} "
        f"skip={skip_events} force_late_seconds={force_late_seconds}",
    )

    rows = _source_rows(cfg, limit, skip=skip_events)
    log.info("replaying %d transactions from the curated export (skip=%d)", len(rows), skip_events)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": f"streamlake-producer-{label}",
            "linger.ms": 50,
            "compression.type": "lz4",
            # acks=all: the producer is not done until the broker has the record. Anything less
            # and "sent" means "handed to a buffer that may still be lost".
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    delivered = {"ok": 0, "failed": 0}

    def on_delivery(err, _msg):
        delivered["failed" if err else "ok"] += 1
        if err:
            log.error("delivery failed: %s", err)

    sent = duplicates = late_sent = 0
    interval = 1.0 / eps if eps > 0 else 0.0
    started = time.monotonic()

    events = _events(
        rows,
        cfg,
        force_late_seconds=force_late_seconds,
        duplicate_rate=dup_rate,
        late_rate=lat_rate,
    )
    for key, event in events:
        producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(event).encode(),
            on_delivery=on_delivery,
        )
        sent += 1
        duplicates += int(bool(event.get("redelivery")))
        late_sent += int(bool(event.get("late")))
        producer.poll(0)
        if interval:
            # Pace by wall clock rather than sleeping a fixed amount per record, so the rate
            # holds even when a batch of sends is slow.
            target = started + sent * interval
            drift = target - time.monotonic()
            if drift > 0:
                time.sleep(drift)

    producer.flush(30)
    elapsed = time.monotonic() - started
    log.info(
        "produced[%s] %d events (%d duplicates, %d stamped late) in %.1fs, %.0f events/s, "
        "%d delivered, %d failed",
        label,
        sent,
        duplicates,
        late_sent,
        elapsed,
        sent / elapsed if elapsed else 0,
        delivered["ok"],
        delivered["failed"],
    )
    if delivered["failed"]:
        raise RuntimeError(f"{delivered['failed']} events failed to reach Kafka")

    stats = {
        "sent": sent,
        "duplicates": duplicates,
        "unique": sent - duplicates,
        "late_sent": late_sent,
    }
    _write_stats(cfg, stats, label=label)
    monitoring.emit_producer_stats(stats)
    return stats


def _write_stats(cfg: Config, stats: dict[str, int], *, label: str = "producer") -> None:
    # The consumer's dedup and late-drop counts are only checkable against what was actually
    # sent, so each labelled phase of a multi-phase demo gets its own file rather than the phases
    # overwriting each other.
    reports = cfg.path("reports") / "stream"
    reports.mkdir(parents=True, exist_ok=True)
    path = Path(reports / f"{label}_latest.json")
    path.write_text(json.dumps({"produced_at": datetime.now(UTC).isoformat(), **stats}, indent=2))


if __name__ == "__main__":  # pragma: no cover
    run()
