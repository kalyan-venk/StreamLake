"""Transform logic shared by the batch spine and the streaming arm.

Batch/stream parity is a correctness requirement, not a nicety: if the nightly job and the
Kafka consumer derive trip keys or apply validity rules differently, the two arms disagree about
the same trip and the reconciliation test in dbt fails. Both arms import from this module, so
there is exactly one definition of "what a trip is".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame

# Columns as NYC TLC publishes them, mapped to the names the lakehouse uses downstream.
RAW_TO_SILVER = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance_mi",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}

PAYMENT_TYPES = {
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
    5: "unknown",
    6: "voided_trip",
}

# The source has no natural primary key, so we mint a deterministic surrogate from the
# immutable facts of the trip. Deterministic means: the same trip hashes to the same id whether
# it arrives in tonight's parquet file or on the Kafka topic, which is what makes both the
# batch re-run and the stream idempotent.
TRIP_ID_COLUMNS = (
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
)


def trip_id_expr(columns: tuple[str, ...] = TRIP_ID_COLUMNS) -> Column:
    """SHA-256 surrogate key over the trip's identifying columns."""
    from pyspark.sql import functions as F

    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def add_ingestion_metadata(df: DataFrame, *, source: str, batch_id: str) -> DataFrame:
    """Stamp lineage onto every bronze row: where it came from and which run wrote it."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("trip_id", trip_id_expr())
        .withColumn("source_file", F.lit(source))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )


def rename_to_silver(df: DataFrame) -> DataFrame:
    """Apply the raw -> silver column naming, tolerating columns the month happens to lack."""
    for raw, silver in RAW_TO_SILVER.items():
        if raw in df.columns and raw != silver:
            df = df.withColumnRenamed(raw, silver)
    return df


TIMESTAMP_COLUMNS = ("pickup_ts", "dropoff_ts")


def normalize_timestamps(df: DataFrame, columns: tuple[str, ...] = TIMESTAMP_COLUMNS) -> DataFrame:
    """Cast TLC's zone-less timestamps to real instants.

    The parquet files store TIMESTAMP without a zone, which Spark 4 reads as ``timestamp_ntz``.
    Under ANSI mode you cannot do arithmetic between an NTZ value and an instant, and comparing
    the two silently invites the kind of off-by-a-timezone bug that only shows up in a
    month-boundary partition. The session runs in UTC (see conf/streamlake.yml), so the cast is
    explicit and consistent everywhere.
    """
    from pyspark.sql import functions as F

    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("timestamp"))
    return df


def payment_type_desc(column: str = "payment_type") -> Column:
    from pyspark.sql import functions as F

    mapping = F.create_map(
        *[x for code, name in PAYMENT_TYPES.items() for x in (F.lit(code), F.lit(name))]
    )
    return F.coalesce(mapping[F.col(column)], F.lit("unknown"))


def reject_reason(period_start: str, period_end: str) -> Column:
    """First matching validity rule, or NULL when the row is clean.

    These are the rules that quarantine a row instead of failing the whole load: individual bad
    trips are a fact of life in TLC data (fares of -$300, dropoffs in 2098). The *contract* is
    what fails the run — if too many rows land here, or the surviving rows still break an
    assertion, the pipeline stops.
    """
    from pyspark.sql import functions as F

    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0)

    return (
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "missing_timestamp")
        .when(duration_min <= 0, "non_positive_duration")
        .when(duration_min > 24 * 60, "duration_over_24h")
        .when(
            (F.col("pickup_ts") < F.lit(period_start).cast("timestamp"))
            | (F.col("pickup_ts") >= F.lit(period_end).cast("timestamp")),
            "pickup_outside_period",
        )
        .when(F.col("trip_distance_mi") < 0, "negative_distance")
        .when(F.col("trip_distance_mi") > 300, "implausible_distance")
        .when(F.col("total_amount") < 0, "negative_total_amount")
        .when(F.col("fare_amount") < 0, "negative_fare_amount")
        .otherwise(None)
    )


def derive_trip_metrics(df: DataFrame) -> DataFrame:
    """Business-level columns every downstream consumer would otherwise recompute."""
    from pyspark.sql import functions as F

    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0)

    return (
        df.withColumn("trip_duration_min", F.round(duration_min, 3))
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.round(F.col("trip_distance_mi") / (F.col("trip_duration_min") / 60.0), 3),
            ),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.round(100 * F.col("tip_amount") / F.col("fare_amount"), 3),
            ),
        )
        .withColumn("payment_type_desc", payment_type_desc())
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
    )


def enrich_with_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Broadcast-join the 265-row zone lookup onto pickup and dropoff location ids."""
    from pyspark.sql import functions as F

    pickup = zones.select(
        F.col("location_id").alias("_pu_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    dropoff = zones.select(
        F.col("location_id").alias("_do_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )
    return (
        df.join(F.broadcast(pickup), df.pu_location_id == pickup._pu_id, "left")
        .join(F.broadcast(dropoff), df.do_location_id == dropoff._do_id, "left")
        .drop("_pu_id", "_do_id")
        .withColumn("pickup_zone", F.coalesce("pickup_zone", F.lit("Unknown")))
        .withColumn("pickup_borough", F.coalesce("pickup_borough", F.lit("Unknown")))
        .withColumn("dropoff_zone", F.coalesce("dropoff_zone", F.lit("Unknown")))
        .withColumn("dropoff_borough", F.coalesce("dropoff_borough", F.lit("Unknown")))
    )


SILVER_COLUMNS = [
    "trip_id",
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "pickup_hour",
    "passenger_count",
    "trip_distance_mi",
    "trip_duration_min",
    "avg_speed_mph",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "dropoff_borough",
    "dropoff_zone",
    "dropoff_service_zone",
    "payment_type",
    "payment_type_desc",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tip_pct",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
    "source_file",
    "batch_id",
    "ingested_at",
]
