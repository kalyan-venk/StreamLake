"""Tests for the transform logic shared by the batch and streaming arms."""

from __future__ import annotations

from datetime import datetime

import pytest

from streamlake.transforms import (
    CATEGORIES,
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


@pytest.fixture
def raw(spark):
    """Two rows shaped exactly like the Sparkov CSV, including its column names."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
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
            StructField("dob", StringType()),
            StructField("trans_num", StringType()),
            StructField("unix_time", IntegerType()),
            StructField("lat", DoubleType()),
            StructField("long", DoubleType()),
            StructField("merch_lat", DoubleType()),
            StructField("merch_long", DoubleType()),
            StructField("is_fraud", IntegerType()),
            StructField("merch_zipcode", IntegerType()),
        ]
    )
    return spark.createDataFrame(
        [
            (
                datetime(2019, 6, 15, 14, 30, 0),
                4111111111111111,
                "fraud_Example Co",
                "grocery_pos",
                42.50,
                "F",
                "Springfield",
                "NC",
                28654,
                3495,
                "Psychologist",
                "1988-03-09",
                "0b242abb623afc578575680df30655b9",
                1560609000,
                36.0788,
                -81.1781,
                36.0511,
                -82.0483,
                0,
                28705,
            ),
            (
                datetime(2019, 6, 15, 23, 0, 0),
                4222222222222222,
                "fraud_Other Co",
                "shopping_net",
                999.99,
                "M",
                "Malad City",
                "ID",
                83252,
                4154,
                "Officer",
                "1962-01-19",
                "a1a22d70485983eac12b5b88dad1cf95",
                1560640800,
                42.1808,
                -112.262,
                43.1507,
                -112.1545,
                1,
                None,
            ),
        ],
        schema,
    )


def test_normalize_timestamps_preserves_the_wall_clock(spark, raw):
    renamed = normalize_timestamps(rename_to_silver(raw))
    row = renamed.orderBy("trans_time").first()
    assert row.trans_time.hour == 14
    assert dict(renamed.dtypes)["trans_time"] == "timestamp"


def test_mask_card_number_drops_the_raw_pan(spark, raw):
    masked = mask_card_number(rename_to_silver(raw))
    assert "cc_num" not in masked.columns
    row = masked.where("trans_num = '0b242abb623afc578575680df30655b9'").first()
    assert row.cc_num_last4 == "1111"
    assert len(row.cc_num_hash) == 64, "sha2-256 hex digest"


def test_mask_card_number_is_deterministic_and_collision_free(spark, raw):
    masked = mask_card_number(rename_to_silver(raw))
    hashes = [r.cc_num_hash for r in masked.select("cc_num_hash").collect()]
    assert len(set(hashes)) == 2, "different cards must not collide"

    masked_again = mask_card_number(rename_to_silver(raw))
    again = [r.cc_num_hash for r in masked_again.select("cc_num_hash").collect()]
    assert hashes == again, "the same card must hash to the same value across runs"


def test_derive_age_drops_dob(spark, raw):
    renamed = rename_to_silver(raw)
    aged = derive_age(normalize_timestamps(renamed))
    assert "dob" not in aged.columns
    row = aged.where("trans_num = '0b242abb623afc578575680df30655b9'").first()
    # dob 1988-03-09, transaction 2019-06-15 -> 31 years old.
    assert row.cardholder_age == 31


def test_haversine_km_zero_for_same_point(spark):
    schema = "lat double, long double, merch_lat double, merch_long double"
    df = spark.createDataFrame([(40.0, -75.0, 40.0, -75.0)], schema)
    row = df.withColumn("d", haversine_km("lat", "long", "merch_lat", "merch_long")).first()
    assert row.d == pytest.approx(0.0, abs=0.001)


def test_haversine_km_matches_a_known_distance(spark):
    # New York (40.7128, -74.0060) to Philadelphia (39.9526, -75.1652): ~130 km great-circle.
    df = spark.createDataFrame(
        [(40.7128, -74.0060, 39.9526, -75.1652)],
        "lat double, long double, merch_lat double, merch_long double",
    )
    row = df.withColumn("d", haversine_km("lat", "long", "merch_lat", "merch_long")).first()
    assert row.d == pytest.approx(130.0, abs=5.0)


def test_strip_home_coordinates_removes_lat_long(spark, raw):
    stripped = strip_home_coordinates(rename_to_silver(raw))
    assert "lat" not in stripped.columns
    assert "long" not in stripped.columns
    assert "merch_lat" in stripped.columns, "the merchant's location is not PII and must survive"


def test_reject_reason_flags_non_positive_amount(spark, raw):
    from pyspark.sql import functions as F

    zeroed = F.col("trans_num") == "a1a22d70485983eac12b5b88dad1cf95"
    conformed = normalize_timestamps(rename_to_silver(raw)).withColumn(
        "amt", F.when(zeroed, 0.0).otherwise(F.col("amt"))
    )
    conformed = conformed.withColumn("reject_reason", reject_reason())
    reasons = {r.trans_num: r.reject_reason for r in conformed.collect()}
    assert reasons["0b242abb623afc578575680df30655b9"] is None
    assert reasons["a1a22d70485983eac12b5b88dad1cf95"] == "non_positive_amount"


def test_reject_reason_flags_invalid_category(spark, raw):
    from pyspark.sql import functions as F

    conformed = normalize_timestamps(rename_to_silver(raw)).withColumn(
        "category", F.lit("not_a_real_category")
    )
    conformed = conformed.withColumn("reject_reason", reject_reason())
    reasons = [r.reject_reason for r in conformed.collect()]
    assert all(r == "invalid_category" for r in reasons)


def test_reject_reason_flags_bad_merchant_coordinates(spark, raw):
    from pyspark.sql import functions as F

    conformed = normalize_timestamps(rename_to_silver(raw)).withColumn(
        "merch_lat",
        F.when(F.col("trans_num") == "0b242abb623afc578575680df30655b9", 200.0).otherwise(
            F.col("merch_lat")
        ),
    )
    conformed = conformed.withColumn("reject_reason", reject_reason())
    reasons = {r.trans_num: r.reject_reason for r in conformed.collect()}
    assert reasons["0b242abb623afc578575680df30655b9"] == "invalid_merchant_coordinates"
    assert reasons["a1a22d70485983eac12b5b88dad1cf95"] is None


def test_derive_transaction_fields(spark, raw):
    derived = derive_transaction_fields(normalize_timestamps(rename_to_silver(raw)))
    row = derived.where("trans_num = '0b242abb623afc578575680df30655b9'").first()
    assert str(row.trans_date) == "2019-06-15"
    assert row.trans_hour == 14


def test_enrich_with_category_joins_channel(spark, raw):
    category_ref = spark.createDataFrame(
        [("grocery_pos", "in_person"), ("shopping_net", "online")],
        "category string, channel string",
    )
    enriched = enrich_with_category(normalize_timestamps(rename_to_silver(raw)), category_ref)
    row = enriched.where("trans_num = '0b242abb623afc578575680df30655b9'").first()
    assert row.channel == "in_person"


def test_categories_constant_has_fourteen_entries():
    assert len(CATEGORIES) == 14


def test_silver_columns_are_all_produced(spark, raw):
    """Guards the strict schema contract: the transform and the contract must agree."""
    category_ref = spark.createDataFrame(
        [("grocery_pos", "in_person"), ("shopping_net", "online")],
        "category string, channel string",
    )
    renamed = normalize_timestamps(rename_to_silver(raw))
    with_distance = renamed.withColumn(
        "distance_km", haversine_km("lat", "long", "merch_lat", "merch_long")
    )
    pipeline = derive_transaction_fields(
        strip_home_coordinates(derive_age(mask_card_number(with_distance)))
    )
    enriched = enrich_with_category(pipeline, category_ref)
    produced = set(enriched.columns) | {"source_file", "source_split", "batch_id", "ingested_at"}
    missing = [c for c in SILVER_COLUMNS if c not in produced]
    assert not missing, f"silver contract declares columns the transform never builds: {missing}"
