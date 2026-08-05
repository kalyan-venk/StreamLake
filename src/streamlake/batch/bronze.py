"""Step 2, bronze: land the raw files in Iceberg, unchanged except for lineage.

Bronze is a faithful copy of the source with lineage columns bolted on (``source_file``,
``source_split``, ``batch_id``, ``ingested_at``). Nothing is filtered here on purpose: bronze is
the layer you replay from when a silver rule turns out to be wrong, so it must still contain the
rows that rule would have thrown away.

Idempotency: the table is partitioned by transaction day, and a re-run uses Iceberg's dynamic
partition overwrite, so loading the same source twice replaces those partitions instead of
appending a second copy of every transaction.
"""

from __future__ import annotations

from datetime import UTC, datetime

from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces
from streamlake.transforms import add_ingestion_metadata

log = get_logger(__name__)

TABLE = "transactions_raw"
CATEGORY_REF_TABLE = "category_ref_raw"


def period(cfg: Config) -> tuple[str, str]:
    """The half-open date range the loaded source claims to cover."""
    return cfg.period_start, cfg.period_end


def as_of(cfg: Config) -> datetime:
    """Freshness reference for a historical dataset: the end of the period it covers.

    Wall-clock freshness would mark 2019-2020 Sparkov data stale the moment you run it today,
    which tells you nothing. What you actually want to assert is "the data covers the period it
    claims to".
    """
    _, end = period(cfg)
    return datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=UTC)


def run(cfg: Config | None = None, *, batch_id: str | None = None) -> dict[str, int]:
    from pyspark.sql.functions import partitioning as P

    cfg = cfg or get_config()
    batch_id = batch_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    banner(log, f"BRONZE | batch_id={batch_id}")

    spark = build_spark("bronze", cfg=cfg)
    ensure_namespaces(spark, cfg)

    txn_table = cfg.table("bronze", TABLE)
    category_table = cfg.table("bronze", CATEGORY_REF_TABLE)

    def _read_split(path, split: str):
        raw = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(path))
            .drop("_c0", "Unnamed: 0")
        )
        log.info("read %s columns from %s (%s split)", len(raw.columns), path.name, split)
        return add_ingestion_metadata(raw, source=path.name, split=split, batch_id=batch_id)

    train = _read_split(cfg.raw_train_file(), "train")
    test = _read_split(cfg.raw_test_file(), "test")
    bronze = train.unionByName(test)

    writer = bronze.writeTo(txn_table)
    if spark.catalog.tableExists(txn_table):
        log.info("table exists -> dynamic partition overwrite (idempotent re-run)")
        writer.overwritePartitions()
    else:
        (
            writer.partitionedBy(P.days("trans_date_trans_time"))
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .create()
        )

    category_ref = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(cfg.category_ref_file()))
    )
    category_ref.writeTo(category_table).createOrReplace()

    txns = spark.table(txn_table)
    enforce(txns, "bronze_transactions", cfg=cfg, stage="bronze", as_of=as_of(cfg))
    enforce(spark.table(category_table), "bronze_category_ref", cfg=cfg, stage="bronze")

    counts = {
        "transactions": txns.count(),
        "category_ref": spark.table(category_table).count(),
        "train_rows": train.count(),
        "test_rows": test.count(),
    }
    log.info("bronze written: %s", counts)
    return counts


if __name__ == "__main__":  # pragma: no cover
    run()
