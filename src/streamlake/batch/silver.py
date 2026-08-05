"""Step 3, silver: conform, mask PII, enrich, deduplicate, and quarantine.

Order matters. Conforming renames the raw Sparkov columns and parses the timestamp once here
instead of in six dashboards. PII handling comes next, before anything else touches the row: the
card number is masked, the name and street are dropped, the date of birth becomes an age, and the
cardholder's home coordinates are consumed into a distance and then dropped, so nothing that
identifies a person survives past this hop. Rows that then break a validity rule are *moved*, not
dropped: they land in ``silver.transactions_quarantine`` tagged with the rule that rejected them.
Finally silver keeps one row per ``trans_num``, newest ingestion wins, at-least-once delivery
(both Sparkov's train/test split boundary and the later Kafka arm) means duplicates arrive by
design.

The contract runs on what survived. Quarantining is a row-level decision; the contract is a
dataset-level gate, if quarantine swallows more than the configured share of the data, the run
fails even though every surviving row is individually clean.
"""

from __future__ import annotations

from streamlake import monitoring
from streamlake.batch.bronze import as_of
from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces
from streamlake.transforms import (
    SILVER_COLUMNS,
    derive_age,
    derive_transaction_fields,
    enrich_with_category,
    haversine_km,
    mask_card_number,
    normalize_timestamps,
    reject_reason,
    rename_to_silver,
    strip_home_coordinates,
)

log = get_logger(__name__)

TABLE = "transactions"
QUARANTINE_TABLE = "transactions_quarantine"
CATEGORY_DIM_TABLE = "dim_category"

# Share of rows allowed to be quarantined before the run is considered broken. Sparkov is a clean
# synthetic feed, so in practice this budget is never approached; it exists for the day the source
# is swapped for a real, messier one.
MAX_REJECT_RATE = 0.10


def run(cfg: Config | None = None) -> dict[str, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.functions import partitioning as P

    cfg = cfg or get_config()
    banner(log, "SILVER")

    spark = build_spark("silver", cfg=cfg)
    ensure_namespaces(spark, cfg)

    bronze = spark.table(cfg.table("bronze", "transactions_raw"))
    category_ref = spark.table(cfg.table("bronze", "category_ref_raw"))

    renamed = normalize_timestamps(rename_to_silver(bronze))
    with_distance = renamed.withColumn(
        "distance_km", haversine_km("lat", "long", "merch_lat", "merch_long")
    )
    conformed = (
        derive_transaction_fields(
            strip_home_coordinates(
                derive_age(mask_card_number(with_distance))
            )
        )
        .withColumn("reject_reason", reject_reason())
    )
    conformed.cache()

    rejected = conformed.where(F.col("reject_reason").isNotNull())
    accepted = conformed.where(F.col("reject_reason").isNull())

    # Keep the most recently ingested row per transaction; ties broken by batch_id so the result
    # is deterministic when a backfill and a normal run write in the same second.
    newest = Window.partitionBy("trans_num").orderBy(
        F.col("ingested_at").desc(), F.col("batch_id").desc()
    )
    deduped = (
        enrich_with_category(accepted, category_ref)
        .withColumn("_rn", F.row_number().over(newest))
        .where(F.col("_rn") == 1)
        .select(*SILVER_COLUMNS)
    )

    silver_table = cfg.table("silver", TABLE)
    (
        deduped.writeTo(silver_table)
        .partitionedBy(P.days("trans_time"))
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .createOrReplace()
    )

    quarantine_table = cfg.table("silver", QUARANTINE_TABLE)
    (
        rejected.select(
            "trans_num",
            "reject_reason",
            "trans_time",
            "amt",
            "category",
            "source_file",
            "batch_id",
            "ingested_at",
        )
        .writeTo(quarantine_table)
        .createOrReplace()
    )

    category_dim = category_ref.select(
        F.col("category"),
        F.col("channel"),
    )
    category_dim.writeTo(cfg.table("silver", CATEGORY_DIM_TABLE)).createOrReplace()

    total = conformed.count()
    kept = spark.table(silver_table).count()
    dropped = spark.table(quarantine_table).count()
    reject_rate = dropped / total if total else 0.0

    log.info(
        "silver: %d in -> %d kept, %d quarantined (%.4f%%), %d deduplicated",
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
        log.info("  quarantined %-28s %8d", row["reject_reason"], row["count"])

    conformed.unpersist()
    monitoring.emit_quarantine_count(dropped, stage="silver")

    enforce(
        spark.table(silver_table), "silver_transactions", cfg=cfg, stage="silver", as_of=as_of(cfg)
    )
    enforce(category_dim, "silver_dim_category", cfg=cfg, stage="silver")

    if reject_rate > MAX_REJECT_RATE:
        raise RuntimeError(
            f"quarantine rate {reject_rate:.2%} exceeds the {MAX_REJECT_RATE:.0%} budget, "
            "the source changed shape or an upstream rule is wrong; not promoting to gold"
        )

    return {"input": total, "kept": kept, "quarantined": dropped}


if __name__ == "__main__":  # pragma: no cover
    run()
