"""Tests for the transform logic shared by the batch and streaming arms."""

from __future__ import annotations

from datetime import datetime

import pytest

from streamlake.transforms import (
    PAYMENT_TYPES,
    SILVER_COLUMNS,
    derive_trip_metrics,
    enrich_with_zones,
    normalize_timestamps,
    reject_reason,
    rename_to_silver,
    trip_id_expr,
)


@pytest.fixture
def raw(spark):
    """Two rows shaped exactly like the TLC parquet, including its column names."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
    )

    schema = StructType(
        [
            StructField("VendorID", IntegerType()),
            StructField("tpep_pickup_datetime", TimestampNTZType()),
            StructField("tpep_dropoff_datetime", TimestampNTZType()),
            StructField("passenger_count", LongType()),
            StructField("trip_distance", DoubleType()),
            StructField("PULocationID", IntegerType()),
            StructField("DOLocationID", IntegerType()),
            StructField("payment_type", LongType()),
            StructField("fare_amount", DoubleType()),
            StructField("tip_amount", DoubleType()),
            StructField("total_amount", DoubleType()),
            StructField("RatecodeID", LongType()),
            StructField("store_and_fwd_flag", StringType()),
            StructField("extra", DoubleType()),
            StructField("mta_tax", DoubleType()),
            StructField("tolls_amount", DoubleType()),
            StructField("improvement_surcharge", DoubleType()),
            StructField("congestion_surcharge", DoubleType()),
            StructField("Airport_fee", DoubleType()),
        ]
    )
    return spark.createDataFrame(
        [
            (
                1,
                datetime(2024, 1, 15, 12, 0, 0),
                datetime(2024, 1, 15, 12, 30, 0),
                2,
                6.0,
                161,
                236,
                1,
                20.0,
                4.0,
                28.5,
                1,
                "N",
                0.0,
                0.5,
                0.0,
                1.0,
                2.5,
                0.0,
            ),
            (
                2,
                datetime(2024, 1, 15, 13, 0, 0),
                datetime(2024, 1, 15, 12, 50, 0),  # dropoff before pickup
                1,
                2.0,
                161,
                236,
                2,
                10.0,
                0.0,
                12.0,
                1,
                "N",
                0.0,
                0.5,
                0.0,
                1.0,
                2.5,
                0.0,
            ),
        ],
        schema,
    )


def test_trip_id_is_deterministic_across_runs(spark, raw):
    """The same trip must hash to the same id in the batch file and on the Kafka topic."""
    first = raw.withColumn("trip_id", trip_id_expr()).select("trip_id").collect()
    second = raw.withColumn("trip_id", trip_id_expr()).select("trip_id").collect()
    assert [r.trip_id for r in first] == [r.trip_id for r in second]
    assert len({r.trip_id for r in first}) == 2, "different trips must not collide"
    assert all(len(r.trip_id) == 64 for r in first), "sha2-256 hex digest"


def test_normalize_timestamps_preserves_the_wall_clock(spark, raw):
    """Casting NTZ to an instant in a UTC session must not shift the clock reading."""
    renamed = normalize_timestamps(rename_to_silver(raw))
    row = renamed.orderBy("pickup_ts").first()
    assert row.pickup_ts.hour == 12
    assert dict(renamed.dtypes)["pickup_ts"] == "timestamp"


def test_derive_trip_metrics(spark, raw):
    metrics = derive_trip_metrics(normalize_timestamps(rename_to_silver(raw)))
    row = metrics.where("vendor_id = 1").first()
    assert row.trip_duration_min == pytest.approx(30.0)
    assert row.avg_speed_mph == pytest.approx(12.0)  # 6 miles in half an hour
    assert row.tip_pct == pytest.approx(20.0)  # 4 on a 20 fare
    assert row.payment_type_desc == PAYMENT_TYPES[1]
    assert row.pickup_hour == 12
    assert str(row.pickup_date) == "2024-01-15"


def test_reject_reason_flags_backwards_trips(spark, raw):
    from pyspark.sql import functions as F

    conformed = derive_trip_metrics(normalize_timestamps(rename_to_silver(raw))).withColumn(
        "reject_reason", reject_reason("2024-01-01 00:00:00", "2024-02-01 00:00:00")
    )
    reasons = {r.vendor_id: r.reject_reason for r in conformed.collect()}
    assert reasons[1] is None
    assert reasons[2] == "non_positive_duration"
    assert conformed.where(F.col("reject_reason").isNull()).count() == 1


def test_reject_reason_flags_out_of_period(spark, raw):
    conformed = derive_trip_metrics(normalize_timestamps(rename_to_silver(raw))).withColumn(
        # Ask for February; every January row is then out of period.
        "reject_reason",
        reject_reason("2024-02-01 00:00:00", "2024-03-01 00:00:00"),
    )
    reasons = [r.reject_reason for r in conformed.collect()]
    assert "pickup_outside_period" in reasons


def test_enrich_with_zones_defaults_unknown_ids(spark, raw):
    zones = spark.createDataFrame(
        [(161, "Manhattan", "Midtown Center", "Yellow Zone")],
        "location_id int, borough string, zone string, service_zone string",
    )
    enriched = enrich_with_zones(
        derive_trip_metrics(normalize_timestamps(rename_to_silver(raw))), zones
    )
    row = enriched.where("vendor_id = 1").first()
    assert row.pickup_borough == "Manhattan"
    assert row.pickup_zone == "Midtown Center"
    # 236 is absent from this tiny lookup: the left join must not drop the trip, and the label
    # must not be null — a null borough would silently vanish from every grouped dashboard.
    assert row.dropoff_borough == "Unknown"
    assert row.dropoff_zone == "Unknown"


def test_silver_columns_are_all_produced(spark, raw):
    """Guards the strict schema contract: the transform and the contract must agree."""
    zones = spark.createDataFrame(
        [(161, "Manhattan", "Midtown Center", "Yellow Zone")],
        "location_id int, borough string, zone string, service_zone string",
    )
    enriched = enrich_with_zones(
        derive_trip_metrics(normalize_timestamps(rename_to_silver(raw))), zones
    )
    produced = set(enriched.columns) | {"trip_id", "source_file", "batch_id", "ingested_at"}
    missing = [c for c in SILVER_COLUMNS if c not in produced]
    assert not missing, f"silver contract declares columns the transform never builds: {missing}"
