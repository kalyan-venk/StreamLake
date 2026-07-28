"""Step 3, silver: conform, enrich, deduplicate, and quarantine.

Order matters. Conforming renames the raw TLC columns and computes the derived metrics
(duration, speed, tip %) once here instead of in six dashboards. Rows that then break a validity
rule are *moved*, not dropped: they land in ``silver.trips_quarantine`` tagged with the rule
that rejected them, so "where did my 4,000 trips go?" has an answer you can query. Finally
silver keeps one row per ``trip_id``, newest ingestion wins, Iceberg tables are append-friendly
and Kafka is at-least-once, so duplicates arrive by design.

The contract runs on what survived. Quarantining is a row-level decision; the contract is a
dataset-level gate, if quarantine swallows more than the configured share of the month, the run
fails even though every surviving row is individually clean.
"""

from __future__ import annotations

from streamlake.batch.bronze import as_of, period
from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces
from streamlake.transforms import (
    SILVER_COLUMNS,
    derive_trip_metrics,
    enrich_with_zones,
    normalize_timestamps,
    reject_reason,
    rename_to_silver,
)

log = get_logger(__name__)

TABLE = "trips"
QUARANTINE_TABLE = "trips_quarantine"
ZONE_DIM_TABLE = "dim_zone"

# Share of the month's rows allowed to be quarantined before the run is considered broken.
MAX_REJECT_RATE = 0.10


def run(cfg: Config | None = None) -> dict[str, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.functions import partitioning as P

    cfg = cfg or get_config()
    banner(log, f"SILVER | month={cfg.month}")

    spark = build_spark("silver", cfg=cfg)
    ensure_namespaces(spark, cfg)

    start, end = period(cfg)
    bronze = spark.table(cfg.table("bronze", "trips_raw"))
    zones = spark.table(cfg.table("bronze", "zones_raw"))

    conformed = derive_trip_metrics(normalize_timestamps(rename_to_silver(bronze))).withColumn(
        "reject_reason", reject_reason(start, end)
    )
    conformed.cache()

    rejected = conformed.where(F.col("reject_reason").isNotNull())
    accepted = conformed.where(F.col("reject_reason").isNull())

    # Keep the most recently ingested row per trip; ties broken by batch_id so the result is
    # deterministic when a backfill and a normal run write in the same second.
    newest = Window.partitionBy("trip_id").orderBy(
        F.col("ingested_at").desc(), F.col("batch_id").desc()
    )
    deduped = (
        enrich_with_zones(accepted, zones)
        .withColumn("_rn", F.row_number().over(newest))
        .where(F.col("_rn") == 1)
        .select(*SILVER_COLUMNS)
    )

    silver_table = cfg.table("silver", TABLE)
    (
        deduped.writeTo(silver_table)
        .partitionedBy(P.days("pickup_ts"))
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .createOrReplace()
    )

    quarantine_table = cfg.table("silver", QUARANTINE_TABLE)
    (
        rejected.select(
            "trip_id",
            "reject_reason",
            "pickup_ts",
            "dropoff_ts",
            "trip_distance_mi",
            "fare_amount",
            "total_amount",
            "source_file",
            "batch_id",
            "ingested_at",
        )
        .writeTo(quarantine_table)
        .createOrReplace()
    )

    zone_dim = zones.select(
        F.col("location_id").cast("int").alias("location_id"),
        F.col("borough"),
        F.col("zone"),
        F.col("service_zone"),
    )
    zone_dim.writeTo(cfg.table("silver", ZONE_DIM_TABLE)).createOrReplace()

    total = conformed.count()
    kept = spark.table(silver_table).count()
    dropped = spark.table(quarantine_table).count()
    reject_rate = dropped / total if total else 0.0

    log.info(
        "silver: %d in -> %d kept, %d quarantined (%.2f%%), %d deduplicated",
        total,
        kept,
        dropped,
        reject_rate * 100,
        total - dropped - kept,
    )
    for row in (
        spark.table(quarantine_table)
        .groupBy("reject_reason")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    ):
        log.info("  quarantined %-24s %8d", row["reject_reason"], row["count"])

    conformed.unpersist()

    enforce(spark.table(silver_table), "silver_trips", cfg=cfg, stage="silver", as_of=as_of(cfg))
    enforce(zone_dim, "silver_dim_zone", cfg=cfg, stage="silver")

    if reject_rate > MAX_REJECT_RATE:
        raise RuntimeError(
            f"quarantine rate {reject_rate:.2%} exceeds the {MAX_REJECT_RATE:.0%} budget, "
            "the source changed shape or an upstream rule is wrong; not promoting to gold"
        )

    return {"input": total, "kept": kept, "quarantined": dropped}


if __name__ == "__main__":  # pragma: no cover
    run()
