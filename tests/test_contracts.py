"""Tests for the contract engine.

Real local SparkSession, three-row in-memory DataFrames. The engine's behaviour lives in the
aggregate expressions it compiles, which a mocked Spark would not exercise at all.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from streamlake.contracts.checks import ValidationContext
from streamlake.contracts.engine import DataContractViolation, validate
from streamlake.contracts.spec import CheckSpec, ColumnSpec, Contract, SchemaSpec, parse_duration


@pytest.fixture
def trips(spark):
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("trip_id", StringType()),
            StructField("pickup_ts", TimestampType()),
            StructField("fare_amount", DoubleType()),
            StructField("payment_type", StringType()),
        ]
    )
    base = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
    return spark.createDataFrame(
        [
            ("a", base, 10.0, "card"),
            ("b", base + timedelta(minutes=5), 20.0, "cash"),
            ("c", base + timedelta(minutes=10), 30.0, "card"),
        ],
        schema,
    )


def contract(*checks: CheckSpec, schema: SchemaSpec | None = None) -> Contract:
    return Contract(
        name="test",
        dataset="test.table",
        schema=schema or SchemaSpec(),
        checks=tuple(checks),
    )


def run(df, *checks: CheckSpec, mode: str = "warn", as_of: datetime | None = None):
    return validate(
        df,
        contract(*checks),
        mode=mode,
        ctx=ValidationContext(as_of=as_of or datetime(2024, 1, 15, 13, tzinfo=UTC)),
        collect_examples=False,
    )


# --- individual checks ---------------------------------------------------------------------


def test_row_count_within_bounds(trips):
    report = run(trips, CheckSpec(type="row_count", params={"min": 1, "max": 5}))
    assert report.ok
    assert report.row_count == 3


def test_row_count_below_minimum_fails(trips):
    report = run(trips, CheckSpec(type="row_count", params={"min": 10}))
    assert not report.ok
    assert report.results[0].observed == 3


def test_not_null_passes_and_reports_offending_column(spark, trips):
    from pyspark.sql import functions as F

    with_null = trips.withColumn(
        "fare_amount", F.when(F.col("trip_id") == "b", None).otherwise(F.col("fare_amount"))
    )
    report = run(with_null, CheckSpec(type="not_null", params={"columns": ["fare_amount"]}))
    assert not report.ok
    assert "fare_amount" in str(report.results[0].observed)
    assert report.results[0].failing_rows == 1


def test_unique_detects_duplicates(spark, trips):
    duplicated = trips.union(trips.limit(1))
    report = run(duplicated, CheckSpec(type="unique", params={"columns": ["trip_id"]}))
    assert not report.ok
    assert report.results[0].failing_rows == 1


def test_accepted_range_excludes_nulls(spark, trips):
    from pyspark.sql import functions as F

    # Fares are 10, 20, 30. Null out the 30 and require >= 15: only the 10 is out of range, and
    # the null must NOT be counted as a violation, that is the not_null check's job, and one
    # failed assertion should point at one root cause.
    with_null = trips.withColumn(
        "fare_amount", F.when(F.col("trip_id") == "c", None).otherwise(F.col("fare_amount"))
    )
    report = run(
        with_null, CheckSpec(type="accepted_range", params={"column": "fare_amount", "min": 15})
    )
    assert not report.ok
    assert report.results[0].failing_rows == 1, "the null must not be counted as out of range"


def test_accepted_values(trips):
    report = run(
        trips,
        CheckSpec(type="accepted_values", params={"column": "payment_type", "values": ["card"]}),
    )
    assert not report.ok
    assert report.results[0].failing_rows == 1


def test_expression_treats_null_as_violation(spark, trips):
    from pyspark.sql import functions as F

    with_null = trips.withColumn(
        "fare_amount", F.when(F.col("trip_id") == "a", None).otherwise(F.col("fare_amount"))
    )
    report = run(with_null, CheckSpec(type="expression", params={"expr": "fare_amount > 0"}))
    assert not report.ok, "an unknown truth value must not count as satisfied"
    assert report.results[0].failing_rows == 1


def test_null_rate_threshold(spark, trips):
    from pyspark.sql import functions as F

    with_null = trips.withColumn(
        "fare_amount", F.when(F.col("trip_id") == "a", None).otherwise(F.col("fare_amount"))
    )
    passing = run(
        with_null, CheckSpec(type="null_rate", params={"column": "fare_amount", "max": 0.5})
    )
    failing = run(
        with_null, CheckSpec(type="null_rate", params={"column": "fare_amount", "max": 0.1})
    )
    assert passing.ok and not failing.ok


def test_freshness_uses_logical_run_time(trips):
    """A 2024 backfill is not stale in 2026, freshness is measured against the run's own clock."""
    recent = run(
        trips,
        CheckSpec(type="freshness", params={"column": "pickup_ts", "max_age": "2 hours"}),
        as_of=datetime(2024, 1, 15, 13, tzinfo=UTC),
    )
    stale = run(
        trips,
        CheckSpec(type="freshness", params={"column": "pickup_ts", "max_age": "2 hours"}),
        as_of=datetime(2024, 1, 20, tzinfo=UTC),
    )
    assert recent.ok
    assert not stale.ok


def test_missing_column_fails_the_check_rather_than_crashing(trips):
    report = run(trips, CheckSpec(type="not_null", params={"columns": ["nope"]}))
    assert not report.ok
    assert "missing" in str(report.results[0].observed)


def test_schema_strict_rejects_undeclared_columns(trips):
    schema = SchemaSpec(
        columns=(ColumnSpec(name="trip_id", type="string", nullable=False),), strict=True
    )
    report = validate(
        trips,
        contract(schema=schema),
        mode="warn",
        collect_examples=False,
    )
    assert not report.ok
    assert "undeclared" in report.results[0].detail


# --- engine behaviour ----------------------------------------------------------------------


def test_error_severity_raises_in_fail_mode(trips):
    with pytest.raises(DataContractViolation) as excinfo:
        run(trips, CheckSpec(type="row_count", params={"min": 100}), mode="fail")
    assert "test" in str(excinfo.value)


def test_warn_severity_never_raises(trips):
    report = validate(
        trips,
        contract(CheckSpec(type="row_count", severity="warn", params={"min": 100})),
        mode="fail",
        collect_examples=False,
    )
    assert report.status == "PASSED_WITH_WARNINGS"
    assert report.ok, "a warning must not fail the run"


def test_all_checks_share_one_aggregate_pass(trips, monkeypatch):
    """The engine's central claim: N checks cost one scan, not N."""
    calls = {"agg": 0}
    original = type(trips).agg

    def counting_agg(self, *args, **kwargs):
        calls["agg"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(type(trips), "agg", counting_agg)
    run(
        trips,
        CheckSpec(type="row_count", params={"min": 1}),
        CheckSpec(type="not_null", params={"columns": ["trip_id"]}),
        CheckSpec(type="unique", params={"columns": ["trip_id"]}),
        CheckSpec(type="accepted_range", params={"column": "fare_amount", "min": 0}),
        CheckSpec(type="null_rate", params={"column": "fare_amount", "max": 1.0}),
    )
    assert calls["agg"] == 1


def test_report_serialises(trips, tmp_path):
    import json

    report = run(trips, CheckSpec(type="row_count", params={"min": 1}))
    payload = json.loads(json.dumps(report.to_dict(), default=str))
    assert payload["status"] == "PASSED"
    assert payload["checks_total"] == 1


# --- spec parsing --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("30 seconds", 30), ("2 minutes", 120), ("3 hours", 10800), ("1 day", 86400), (45, 45)],
)
def test_parse_duration(text, seconds):
    assert parse_duration(text) == seconds


def test_parse_duration_rejects_nonsense():
    with pytest.raises(ValueError):
        parse_duration("soon")


def test_unknown_check_type_is_an_error(trips):
    with pytest.raises(ValueError, match="unknown check type"):
        run(trips, CheckSpec(type="vibes", params={}))
