"""Transform logic shared by the batch spine and the streaming arm.

If the nightly job and the Kafka consumer derive PII handling or validity rules differently, the
two arms disagree about the same transaction, and the reconciliation test in dbt fails. Both arms
import from this module, so there is exactly one definition of "what a clean transaction is".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame

# Columns as Sparkov publishes them, mapped to the names the lakehouse uses downstream. Sparkov's
# own `trans_num` is already a natural, globally unique event id (a 32-char hex string), so there
# is no surrogate key to mint here, unlike a source with no natural per-row identifier.
RAW_TO_SILVER = {
    "trans_date_trans_time": "trans_time",
    "cc_num": "cc_num",
    "merchant": "merchant",
    "category": "category",
    "amt": "amt",
    "gender": "gender",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "city_pop": "city_pop",
    "job": "job",
    "dob": "dob",
    "trans_num": "trans_num",
    "unix_time": "unix_time",
    "merch_lat": "merch_lat",
    "merch_long": "merch_long",
    "is_fraud": "is_fraud",
    "merch_zipcode": "merch_zipcode",
}

CATEGORIES = (
    "entertainment",
    "food_dining",
    "gas_transport",
    "grocery_net",
    "grocery_pos",
    "health_fitness",
    "home",
    "kids_pets",
    "misc_net",
    "misc_pos",
    "personal_care",
    "shopping_net",
    "shopping_pos",
    "travel",
)

# A per-row identity check for `trans_num`: 32 lowercase hex characters, the shape Sparkov mints
# for every row. Not a derivation (the id is already in the source), just a sanity bound so a
# truncated or re-encoded download fails loudly here rather than three hops later.
TRANS_NUM_PATTERN = r"^[0-9a-f]{32}$"


def add_ingestion_metadata(df: DataFrame, *, source: str, split: str, batch_id: str) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        df.withColumn("source_file", F.lit(source))
        .withColumn("source_split", F.lit(split))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )


def rename_to_silver(df: DataFrame) -> DataFrame:
    for raw, silver in RAW_TO_SILVER.items():
        if raw in df.columns and raw != silver:
            df = df.withColumnRenamed(raw, silver)
    return df


def normalize_timestamps(df: DataFrame, column: str = "trans_time") -> DataFrame:
    """Make sure Sparkov's `trans_date_trans_time` is a real timestamp, not a string.

    The CSV stores it as `YYYY-MM-DD HH:MM:SS` with no zone. Spark's own CSV schema inference
    already recognises that shape and reads the column straight in as `timestamp`, but a plain
    `.cast("timestamp")` is kept here rather than assumed, so the transform still does the right
    thing if a future run reads the source with inference off, or from a format that types it as
    a string. The session runs in UTC (see conf/streamlake.yml), so the cast treats the value as
    a UTC wall clock, consistent with `unix_time`, which Sparkov also publishes in UTC.
    """
    from pyspark.sql import functions as F

    if column in df.columns:
        df = df.withColumn(column, F.col(column).cast("timestamp"))
    return df


# --- PII handling -----------------------------------------------------------------------------
#
# Sparkov is Faker-generated, not real cardholders, but it is shaped exactly like a real card
# feed (16-digit PANs, real names, home addresses, dates of birth) and the pipeline treats it the
# way it would treat the real thing: nothing that identifies a person survives past silver.
#
#   cc_num        -> dropped. cc_num_last4 (display) and cc_num_hash (join/velocity key) replace
#                    it. The hash is salted so the raw PAN cannot be recovered by brute-forcing
#                    the hash space, and it is stable across ingestions, which is what makes
#                    "transactions per card per rolling window" possible without ever storing
#                    the card number itself.
#   first, last    -> dropped. Not needed by any KPI; carrying them forward only for a "just in
#                     case" column is exactly how a lakehouse ends up with a name in a fact table.
#   street         -> dropped, same reason.
#   dob            -> dropped. Replaced by `cardholder_age`, an integer that is useful for
#                     analysis and reveals far less than a birth date (which combined with zip
#                     and gender is a classic re-identification vector).
#   lat, long      -> dropped after being consumed to compute `distance_km`. The cardholder's
#                     home coordinates are the most sensitive field in the source; the derived
#                     distance is the only thing any KPI in this project actually needs from it.
#   merch_lat/long -> kept. A merchant's location is a business fact, not personal data.
#   city, state,   -> kept. Coarse geography is how the state-level KPI works, and on its own it
#   zip, city_pop     does not identify a person the way street+lat/long do.
#   job            -> kept. An occupation string is shared by many people and carries no direct
#                     identifier; Sparkov's own job field is deliberately non-identifying.
#   gender         -> kept, single-letter code, needed nowhere downstream yet but cheap to keep
#                     and common in fraud-model feature sets.

CC_HASH_SALT_ENV = "STREAMLAKE_PII_SALT"
_DEFAULT_SALT = "streamlake-local-dev-salt-not-for-production"


def _hash_salt() -> str:
    import os

    return os.environ.get(CC_HASH_SALT_ENV, _DEFAULT_SALT)


def mask_card_number(df: DataFrame, column: str = "cc_num") -> DataFrame:
    """Replace the raw PAN with a last-4 display column and a salted, non-reversible hash.

    The hash, not the raw number, is what `gold.card_velocity` groups by: it lets the pipeline
    answer "how many transactions did this card make in the last day" without a card number ever
    landing in a table past bronze.
    """
    from pyspark.sql import functions as F

    salt = _hash_salt()
    as_string = F.col(column).cast("string")
    return (
        df.withColumn(f"{column}_last4", F.substring(as_string, -4, 4))
        .withColumn(f"{column}_hash", F.sha2(F.concat(F.lit(salt), as_string), 256))
        .drop(column)
    )


def derive_age(df: DataFrame, dob_column: str = "dob", as_of: str | None = None) -> DataFrame:
    """Years between `dob` and the transaction's own event time, then drop `dob`."""
    from pyspark.sql import functions as F

    ref = F.col("trans_time") if as_of is None else F.lit(as_of).cast("timestamp")
    age = F.floor(F.months_between(ref, F.to_date(F.col(dob_column))) / F.lit(12.0))
    return df.withColumn("cardholder_age", age.cast("int")).drop(dob_column)


EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: str, lon1: str, lat2: str, lon2: str) -> Column:
    """Great-circle distance between the cardholder's home and the merchant, in kilometres.

    A large cardholder-to-merchant distance is one of the oldest fraud signals there is: a card
    used a thousand miles from where its owner lives, within the same day, is a pattern worth
    flagging even before a model sees it. Computed here so both the batch aggregate and the
    streaming event carry a consistent number.
    """
    from pyspark.sql import functions as F

    lat1_r, lon1_r = F.radians(F.col(lat1)), F.radians(F.col(lon1))
    lat2_r, lon2_r = F.radians(F.col(lat2)), F.radians(F.col(lon2))
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_r) * F.cos(lat2_r) * F.pow(F.sin(dlon / 2), 2)
    c = F.lit(2.0) * F.asin(F.sqrt(a))
    return F.round(F.lit(EARTH_RADIUS_KM) * c, 3)


def strip_home_coordinates(df: DataFrame) -> DataFrame:
    """Drop the cardholder's raw home coordinates once `distance_km` has been derived from them."""
    return df.drop("lat", "long")


def reject_reason() -> Column:
    """First matching validity rule, or NULL when the row is clean.

    These are the rules that quarantine a row instead of failing the whole load: a handful of
    malformed rows is a fact of life in any feed. The *contract* is what fails the run, if too
    many rows land here, or the surviving rows still break an assertion, the pipeline stops.

    Sparkov is a clean, Faker-generated dataset (no negative amounts, no null trans_num in the
    published files), so on this source the quarantine count is expected to be small or zero.
    That is reported honestly rather than manufactured: the rules exist and run on every row,
    they just do not find much to catch here, which is different from a real production card feed.
    """
    from pyspark.sql import functions as F

    bad_id = F.col("trans_num").isNull() | (~F.col("trans_num").rlike(TRANS_NUM_PATTERN))
    return (
        F.when(bad_id, "invalid_trans_num")
        .when(F.col("trans_time").isNull(), "missing_timestamp")
        .when(F.col("amt").isNull() | (F.col("amt") <= 0), "non_positive_amount")
        .when(F.col("amt") > 30000, "implausible_amount")
        .when(~F.col("category").isin(*CATEGORIES), "invalid_category")
        .when(F.col("is_fraud").isNull() | (~F.col("is_fraud").isin(0, 1)), "invalid_is_fraud_flag")
        .when(
            F.col("merch_lat").isNull()
            | (F.col("merch_lat") < -90)
            | (F.col("merch_lat") > 90)
            | F.col("merch_long").isNull()
            | (F.col("merch_long") < -180)
            | (F.col("merch_long") > 180),
            "invalid_merchant_coordinates",
        )
        .otherwise(None)
    )


def derive_transaction_fields(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        df.withColumn("trans_date", F.to_date("trans_time"))
        .withColumn("trans_hour", F.hour("trans_time"))
    )


def enrich_with_category(df: DataFrame, category_ref: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    ref = category_ref.select(
        F.col("category").alias("_cat"),
        F.col("channel"),
    )
    return df.join(F.broadcast(ref), df.category == ref._cat, "left").drop("_cat")


SILVER_COLUMNS = [
    "trans_num",
    "trans_time",
    "trans_date",
    "trans_hour",
    "unix_time",
    "cc_num_last4",
    "cc_num_hash",
    "merchant",
    "category",
    "channel",
    "amt",
    "gender",
    "city",
    "state",
    "zip",
    "city_pop",
    "job",
    "cardholder_age",
    "merch_lat",
    "merch_long",
    "distance_km",
    "is_fraud",
    "merch_zipcode",
    "source_file",
    "source_split",
    "batch_id",
    "ingested_at",
]
