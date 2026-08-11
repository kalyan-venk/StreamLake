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

Publishing is write-audit-publish, not write-then-check. A ``createOrReplace()`` straight onto
``lakehouse.silver.transactions`` would commit before the contract ever runs, so a breach would
already have clobbered the previous good table by the time the run fails; only what reads the
table afterward would be protected, not the table itself. Instead each protected table is first
written to a ``_staging`` table under the same name, the contract runs against that staged
output, and only a passing contract triggers the copy into the real table. A failing contract (or
an over-budget quarantine rate) raises before either publish happens, so the previous good table
is never touched.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

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

    category_dim = category_ref.select(
        F.col("category"),
        F.col("channel"),
    )

    # Quarantine is diagnostic, not a table any contract protects, so it is written straight away
    # regardless of what happens to the two audited tables below: an operator debugging a bad run
    # needs to see exactly what this run rejected, not what a previous run rejected.
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

    # The reject-rate gate runs on in-memory counts, before either protected table is written at
    # all, not on counts read back from a published table. Gating on a post-publish count is the
    # same "clobber first, check later" bug this function exists to fix, just for a business rule
    # instead of a contract.
    total = conformed.count()
    dropped = rejected.count()
    reject_rate = dropped / total if total else 0.0
    if reject_rate > MAX_REJECT_RATE:
        conformed.unpersist()
        raise RuntimeError(
            f"quarantine rate {reject_rate:.2%} exceeds the {MAX_REJECT_RATE:.0%} budget, "
            "the source changed shape or an upstream rule is wrong; not promoting to gold"
        )

    silver_table = cfg.table("silver", TABLE)
    staged_transactions = _write_audited(
        deduped,
        silver_table,
        "silver_transactions",
        cfg=cfg,
        spark=spark,
        stage="silver",
        as_of=as_of(cfg),
        partition=P.days("trans_time"),
        properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
    )

    category_dim_table = cfg.table("silver", CATEGORY_DIM_TABLE)
    _write_audited(
        category_dim,
        category_dim_table,
        "silver_dim_category",
        cfg=cfg,
        spark=spark,
        stage="silver",
    )

    kept = staged_transactions.count()

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

    return {"input": total, "kept": kept, "quarantined": dropped}


def _write_audited(
    df: DataFrame,
    table: str,
    contract_name: str,
    *,
    cfg: Config,
    spark: SparkSession,
    stage: str,
    as_of=None,
    partition=None,
    properties: dict[str, str] | None = None,
) -> DataFrame:
    """Write-audit-publish: stage ``df``, run its contract against the staged output, and only
    copy the staged table into ``table`` once the contract holds.

    On a contract violation, ``enforce`` raises and this function never reaches the publish
    write, so ``table`` keeps whatever it held before this call, byte for byte. The staging table
    is left behind on a failing run on purpose: it is what the operator reads to see the exact
    rows that broke the contract, and the next successful run overwrites it anyway.
    """
    staging_table = f"{table}_staging"
    props = {"format-version": "2", **(properties or {})}

    def _write(source: DataFrame, target: str) -> None:
        writer = source.writeTo(target)
        for key, value in props.items():
            writer = writer.tableProperty(key, value)
        if partition is not None:
            writer = writer.partitionedBy(partition)
        writer.createOrReplace()

    _write(df, staging_table)
    staged = spark.table(staging_table)
    enforce(staged, contract_name, cfg=cfg, stage=stage, as_of=as_of)

    _write(staged, table)
    return spark.table(table)


if __name__ == "__main__":  # pragma: no cover
    run()
