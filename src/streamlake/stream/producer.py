"""The event producer: replay real trips onto Kafka as if they were happening now.

Event time gets rewritten, the payload does not: each trip keeps its real fares, distances and
zones but is stamped with a fresh ``event_ts``, so the windowed aggregation downstream runs
against a live clock while the numbers stay real.

The producer also misbehaves on purpose. A configurable share of events is sent twice
(``duplicate_rate``) and another share arrives late (``late_rate``). The consumer's dedup and
watermark exist for exactly those two cases, so every run exercises them.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger

log = get_logger(__name__)

# Columns pulled from the curated export to build events. Read with pyarrow so the producer
# starts in about a second and holds a few MB, instead of booting a JVM to read parquet.
EVENT_COLUMNS = [
    "trip_id",
    "pickup_ts",
    "dropoff_ts",
    "passenger_count",
    "trip_distance_mi",
    "pu_location_id",
    "do_location_id",
    "pickup_borough",
    "pickup_zone",
    "payment_type_desc",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def _source_rows(cfg: Config, limit: int) -> list[dict[str, Any]]:
    import pyarrow.dataset as ds

    curated = cfg.curated_dir("trips")
    if not curated.exists():
        raise RuntimeError(
            f"{curated} not found — run the batch spine first (`make batch`); the producer "
            "replays real trips rather than inventing them"
        )
    dataset = ds.dataset(str(curated), format="parquet")
    table = dataset.head(limit, columns=EVENT_COLUMNS)
    return table.to_pylist()


def _events(rows: list[dict[str, Any]], cfg: Config) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (key, event) pairs, injecting duplicates and late arrivals as configured."""
    duplicate_rate = float(cfg.get("kafka.producer.duplicate_rate", 0.0))
    late_rate = float(cfg.get("kafka.producer.late_rate", 0.0))
    rng = random.Random(42)  # reproducible misbehaviour

    for row in rows:
        now = datetime.now(UTC)
        event_ts = now
        late = rng.random() < late_rate
        if late:
            # Backdate the event time without delaying the send: this is a record that took a
            # detour through a slow upstream and shows up after the window it belongs to.
            event_ts = now - timedelta(seconds=rng.randint(60, 240))

        event = {
            "trip_id": row["trip_id"],
            "event_ts": event_ts.isoformat(),
            "ingested_ts": now.isoformat(),
            "late": late,
            "pickup_ts": str(row["pickup_ts"]),
            "dropoff_ts": str(row["dropoff_ts"]),
            "passenger_count": row["passenger_count"],
            "trip_distance_mi": row["trip_distance_mi"],
            "pu_location_id": row["pu_location_id"],
            "do_location_id": row["do_location_id"],
            "pickup_borough": row["pickup_borough"],
            "pickup_zone": row["pickup_zone"],
            "payment_type_desc": row["payment_type_desc"],
            "fare_amount": row["fare_amount"],
            "tip_amount": row["tip_amount"],
            "total_amount": row["total_amount"],
        }
        yield row["trip_id"], event

        if rng.random() < duplicate_rate:
            # Same key, same event time, sent twice — exactly what an at-least-once upstream
            # does after a failed ack.
            yield row["trip_id"], dict(event, redelivery=True)


def run(cfg: Config | None = None, *, max_events: int | None = None) -> dict[str, int]:
    from confluent_kafka import Producer

    cfg = cfg or get_config()
    topic = str(cfg.require("kafka.topic"))
    bootstrap = str(cfg.require("kafka.bootstrap_servers"))
    eps = int(cfg.get("kafka.producer.events_per_second", 200))
    limit = int(max_events or cfg.get("kafka.producer.max_events", 10000))

    banner(log, f"PRODUCER | topic={topic} broker={bootstrap} eps={eps} max={limit}")

    rows = _source_rows(cfg, limit)
    log.info("replaying %d trips from the curated export", len(rows))

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "streamlake-producer",
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

    sent = duplicates = 0
    interval = 1.0 / eps if eps > 0 else 0.0
    started = time.monotonic()

    for key, event in _events(rows, cfg):
        producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(event).encode(),
            on_delivery=on_delivery,
        )
        sent += 1
        duplicates += int(bool(event.get("redelivery")))
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
        "produced %d events (%d duplicates) in %.1fs — %.0f events/s, %d delivered, %d failed",
        sent,
        duplicates,
        elapsed,
        sent / elapsed if elapsed else 0,
        delivered["ok"],
        delivered["failed"],
    )
    if delivered["failed"]:
        raise RuntimeError(f"{delivered['failed']} events failed to reach Kafka")

    stats = {"sent": sent, "duplicates": duplicates, "unique": sent - duplicates}
    _write_stats(cfg, stats)
    return stats


def _write_stats(cfg: Config, stats: dict[str, int]) -> None:
    # The consumer's dedup is only checkable against what was actually sent.
    reports = cfg.path("reports") / "stream"
    reports.mkdir(parents=True, exist_ok=True)
    path = Path(reports / "producer_latest.json")
    path.write_text(json.dumps({"produced_at": datetime.now(UTC).isoformat(), **stats}, indent=2))


if __name__ == "__main__":  # pragma: no cover
    run()
