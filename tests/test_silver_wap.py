"""Proves the silver write-audit-publish path actually protects the published table.

Real local Spark, a real Iceberg catalog on a temp warehouse directory (the same ``hadoop``
catalog type production uses), and the repo's own ``silver.run()``, not a mock of it. The point
being proved: when the contract on the staged output fails, the previously published
``silver.transactions`` table is left exactly as it was, not partially overwritten.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from streamlake.config import Config
from streamlake.contracts.engine import DataContractViolation

CATALOG = "wap_test"
CATEGORY = "grocery_pos"
CHANNEL = "in_person"
BASE_TIME = datetime(2019, 6, 15, 12, 0, tzinfo=UTC)


def _cfg(tmp_path, contracts_dir) -> Config:
    return Config(
        data={
            "lakehouse": {
                "catalog": CATALOG,
                "type": "hadoop",
                "warehouse": str(tmp_path / "warehouse"),
                "namespaces": {"bronze": "bronze", "silver": "silver"},
            },
            "spark": {
                "packages": ["org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"],
                "conf": {"spark.sql.session.timeZone": "UTC"},
            },
            "contracts": {"dir": str(contracts_dir), "on_violation": "fail"},
            "paths": {"reports": str(tmp_path / "_reports")},
            "dataset": {"period_start": "2019-01-01", "period_end": "2021-01-01"},
        },
        root=tmp_path,
    )


def _write_contracts(contracts_dir, *, txn_max_rows: int) -> None:
    """A minimal pair of test-only contracts under the same two names silver.run() enforces.

    ``row_count`` is the check deliberately used to trigger the violation in the failing test: it
    is the simplest check that a batch of individually clean, unquarantinable rows can still
    trip, which is exactly the "surviving rows still break an assertion" case the real
    ``silver_transactions`` contract's docstring describes.
    """
    contracts_dir.mkdir(parents=True, exist_ok=True)
    (contracts_dir / "silver_transactions.yml").write_text(
        f"""\
name: silver_transactions
dataset: {CATALOG}.silver.transactions
layer: silver
checks:
  - type: unique
    columns: [trans_num]
  - type: row_count
    min: 1
    max: {txn_max_rows}
"""
    )
    (contracts_dir / "silver_dim_category.yml").write_text(
        f"""\
name: silver_dim_category
dataset: {CATALOG}.silver.dim_category
layer: silver
checks:
  - type: row_count
    min: 1
"""
    )


def _bronze_rows(n: int, *, batch_id: str, start: int = 0) -> list[tuple]:
    """``n`` rows, each individually valid under transforms.reject_reason(): a real trans_num
    shape, a real category, a positive amount, an in-range fraud flag and merchant coordinates.
    None of them get quarantined, so a run's total row count is exactly ``n``.
    """
    rows = []
    for i in range(start, start + n):
        ts = BASE_TIME + timedelta(minutes=i)
        rows.append(
            (
                ts,  # trans_date_trans_time
                4_000_000_000_000_000 + i,  # cc_num
                f"merchant_{i}",  # merchant
                CATEGORY,  # category
                25.0 + i,  # amt
                "F",  # gender
                "Springfield",  # city
                "IL",  # state
                62701,  # zip
                10_000,  # city_pop
                "Engineer",  # job
                date(1990, 1, 1),  # dob
                format(i + 1, "032x"),  # trans_num: 32 lowercase hex chars
                int(ts.timestamp()),  # unix_time
                40.0,  # merch_lat
                -89.6,  # merch_long
                0,  # is_fraud
                62701,  # merch_zipcode
                39.8,  # lat (cardholder home)
                -89.5,  # long (cardholder home)
                "fixture.csv",  # source_file
                "train",  # source_split
                batch_id,  # batch_id
                ts,  # ingested_at
            )
        )
    return rows


def _raw_schema():
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("trans_date_trans_time", TimestampType()),
            StructField("cc_num", LongType()),
            StructField("merchant", StringType()),
            StructField("category", StringType()),
            StructField("amt", DoubleType()),
            StructField("gender", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", IntegerType()),
            StructField("city_pop", IntegerType()),
            StructField("job", StringType()),
            StructField("dob", DateType()),
            StructField("trans_num", StringType()),
            StructField("unix_time", LongType()),
            StructField("merch_lat", DoubleType()),
            StructField("merch_long", DoubleType()),
            StructField("is_fraud", IntegerType()),
            StructField("merch_zipcode", IntegerType()),
            StructField("lat", DoubleType()),
            StructField("long", DoubleType()),
            StructField("source_file", StringType()),
            StructField("source_split", StringType()),
            StructField("batch_id", StringType()),
            StructField("ingested_at", TimestampType()),
        ]
    )


def _seed_bronze(spark, cfg: Config, *, n: int, batch_id: str, start: int = 0) -> None:
    from streamlake.spark import ensure_namespaces

    ensure_namespaces(spark, cfg)

    txns = spark.createDataFrame(_bronze_rows(n, batch_id=batch_id, start=start), _raw_schema())
    txns.writeTo(cfg.table("bronze", "transactions_raw")).createOrReplace()

    category_ref = spark.createDataFrame([(CATEGORY, CHANNEL)], ["category", "channel"])
    category_ref.writeTo(cfg.table("bronze", "category_ref_raw")).createOrReplace()


@pytest.fixture
def wap_cfg(tmp_path, spark):
    """A Config pointed at a fresh temp warehouse + temp contracts dir, with the Iceberg catalog
    already registered on the shared session (see conftest.spark for why the shared session is
    the one that carries Iceberg support, not this fixture).
    """
    from streamlake.spark import build_spark

    contracts_dir = tmp_path / "contracts"

    def _make(*, txn_max_rows: int) -> Config:
        _write_contracts(contracts_dir, txn_max_rows=txn_max_rows)
        cfg = _cfg(tmp_path, contracts_dir)
        build_spark("wap-test", cfg=cfg)  # registers the wap_test catalog on the shared session
        return cfg

    return _make


def test_publish_succeeds_when_contract_holds(spark, wap_cfg):
    from streamlake.batch import silver

    cfg = wap_cfg(txn_max_rows=10)
    _seed_bronze(spark, cfg, n=3, batch_id="batch-1")

    result = silver.run(cfg=cfg)

    assert result == {"input": 3, "kept": 3, "quarantined": 0}
    published = spark.table(cfg.table("silver", "transactions"))
    assert published.count() == 3
    assert {r["trans_num"] for r in published.select("trans_num").collect()} == {
        format(i, "032x") for i in (1, 2, 3)
    }


def test_contract_violation_leaves_previous_good_table_untouched(spark, wap_cfg):
    from streamlake.batch import silver

    cfg = wap_cfg(txn_max_rows=5)
    silver_table = cfg.table("silver", "transactions")

    # A good first run publishes a 3-row table.
    _seed_bronze(spark, cfg, n=3, batch_id="batch-1")
    good_result = silver.run(cfg=cfg)
    assert good_result == {"input": 3, "kept": 3, "quarantined": 0}
    good_rows = sorted(
        r["trans_num"] for r in spark.table(silver_table).select("trans_num").collect()
    )
    assert good_rows == [format(i, "032x") for i in (1, 2, 3)]

    # Reseed bronze with 8 rows, individually clean (none of them quarantined), but past the
    # contract's row_count max of 5. This must fail the contract, not sneak past quarantine.
    _seed_bronze(spark, cfg, n=8, batch_id="batch-2", start=100)
    with pytest.raises(DataContractViolation):
        silver.run(cfg=cfg)

    # The production table must still hold exactly what the first, good run published: not the
    # bad batch, not a mix of the two, not an empty table from a half-finished write.
    survivor_rows = sorted(
        r["trans_num"] for r in spark.table(silver_table).select("trans_num").collect()
    )
    assert survivor_rows == good_rows, (
        "a contract violation on the staged output must never reach the published table"
    )

    # The staging table is left behind with the bad batch, which is what proves the run actually
    # got as far as staging (and was stopped there) rather than never writing anything at all.
    staged_rows = spark.table(f"{silver_table}_staging").count()
    assert staged_rows == 8


def test_reject_rate_over_budget_also_leaves_previous_good_table_untouched(spark, wap_cfg):
    """The reject-rate budget is a business rule, not a data contract, but it gates the same
    publish and must leave the same guarantee: a run that trips it must not touch the table
    a previous good run already published.
    """
    from streamlake.batch import silver

    cfg = wap_cfg(txn_max_rows=100)
    silver_table = cfg.table("silver", "transactions")

    _seed_bronze(spark, cfg, n=3, batch_id="batch-1")
    good_result = silver.run(cfg=cfg)
    assert good_result == {"input": 3, "kept": 3, "quarantined": 0}
    good_rows = sorted(
        r["trans_num"] for r in spark.table(silver_table).select("trans_num").collect()
    )

    # 2 clean rows plus 9 rows with an invalid category (quarantined): a 9/11 reject rate, well
    # past the 10% budget in streamlake.batch.silver.MAX_REJECT_RATE.
    clean = _bronze_rows(2, batch_id="batch-2", start=200)
    dirty = _bronze_rows(9, batch_id="batch-2", start=300)
    dirty = [row[:3] + ("not_a_real_category",) + row[4:] for row in dirty]
    from streamlake.spark import ensure_namespaces

    ensure_namespaces(spark, cfg)
    txns = spark.createDataFrame(clean + dirty, _raw_schema())
    txns.writeTo(cfg.table("bronze", "transactions_raw")).createOrReplace()
    category_ref = spark.createDataFrame([(CATEGORY, CHANNEL)], ["category", "channel"])
    category_ref.writeTo(cfg.table("bronze", "category_ref_raw")).createOrReplace()

    with pytest.raises(RuntimeError, match="quarantine rate"):
        silver.run(cfg=cfg)

    survivor_rows = sorted(
        r["trans_num"] for r in spark.table(silver_table).select("trans_num").collect()
    )
    assert survivor_rows == good_rows
