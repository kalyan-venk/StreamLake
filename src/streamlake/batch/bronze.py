"""Step 2 — bronze: land the raw file in Iceberg, unchanged except for lineage.

Bronze is a faithful copy of the source with four columns bolted on (``trip_id``,
``source_file``, ``batch_id``, ``ingested_at``). Nothing is filtered here on purpose: bronze is
the layer you replay from when a silver rule turns out to be wrong, so it must still contain
the rows that rule would have thrown away.

Idempotency: the table is partitioned by pickup day, and a re-run uses Iceberg's dynamic
partition overwrite, so loading the same month twice replaces those partitions instead of
appending a second copy of every trip.
"""

from __future__ import annotations

from datetime import UTC, datetime

from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces
from streamlake.transforms import add_ingestion_metadata

log = get_logger(__name__)

TABLE = "trips_raw"
ZONES_TABLE = "zones_raw"


def period(cfg: Config) -> tuple[str, str]:
    """First instant of the configured month, and first instant of the next one (half-open)."""
    year, month = (int(x) for x in cfg.month.split("-"))
    start = datetime(year, month, 1, tzinfo=UTC)
    end = datetime(year + (month == 12), (month % 12) + 1, 1, tzinfo=UTC)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def as_of(cfg: Config) -> datetime:
    """Freshness reference for a historical backfill: the end of the month being loaded.

    Wall-clock freshness would mark a 2024 backfill stale the moment you run it in 2026, which
    tells you nothing. What you actually want to assert is "the data covers its own period".
    """
    _, end = period(cfg)
    return datetime.strptime(end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def run(cfg: Config | None = None, *, batch_id: str | None = None) -> dict[str, int]:
    from pyspark.sql.functions import partitioning as P

    cfg = cfg or get_config()
    batch_id = batch_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    banner(log, f"BRONZE | month={cfg.month} batch_id={batch_id}")

    spark = build_spark("bronze", cfg=cfg)
    ensure_namespaces(spark, cfg)

    trips_table = cfg.table("bronze", TABLE)
    zones_table = cfg.table("bronze", ZONES_TABLE)

    raw = spark.read.parquet(str(cfg.raw_trips_file()))
    log.info("read %s columns from %s", len(raw.columns), cfg.raw_trips_file().name)

    bronze = add_ingestion_metadata(raw, source=cfg.raw_trips_file().name, batch_id=batch_id)

    writer = bronze.writeTo(trips_table)
    if spark.catalog.tableExists(trips_table):
        log.info("table exists -> dynamic partition overwrite (idempotent re-run)")
        writer.overwritePartitions()
    else:
        (
            writer.partitionedBy(P.days("tpep_pickup_datetime"))
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .create()
        )

    zones = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(cfg.raw_zones_file()))
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
    )
    zones.writeTo(zones_table).createOrReplace()

    trips = spark.table(trips_table)
    enforce(trips, "bronze_trips", cfg=cfg, stage="bronze", as_of=as_of(cfg))
    enforce(spark.table(zones_table), "bronze_zones", cfg=cfg, stage="bronze")

    counts = {"trips": trips.count(), "zones": spark.table(zones_table).count()}
    log.info("bronze written: %s", counts)
    return counts


if __name__ == "__main__":  # pragma: no cover
    run()
