"""Step 4, gold: the fraud KPI aggregates the lakehouse serves directly.

Wide aggregations over the full history. These tables are the lake-side serving layer and the
reference two of them get reconciled against in dbt. Each one is small, denormalised, and built
to answer a single fraud-analytics question.
"""

from __future__ import annotations

from streamlake.batch.bronze import as_of
from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces

log = get_logger(__name__)

CATEGORY_HOURLY_FRAUD = "category_hourly_fraud"
STATE_HOURLY_VOLUME = "state_hourly_volume"
CARD_VELOCITY = "card_velocity"
MERCHANT_RISK_LEADERBOARD = "merchant_risk_leaderboard"
GEO_DISTANCE_ANOMALY = "geo_distance_anomaly"

# A merchant needs at least this many transactions in the whole dataset before its fraud rate is
# meaningful. A merchant with one transaction that happens to be fraud has a 100% fraud rate and
# tells you nothing; the leaderboard is a ranking, and a ranking of noise is not a ranking.
MERCHANT_MIN_VOLUME = 20


def run(cfg: Config | None = None) -> dict[str, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.functions import partitioning as P

    cfg = cfg or get_config()
    banner(log, "GOLD")

    spark = build_spark("gold", cfg=cfg)
    ensure_namespaces(spark, cfg)

    txns = spark.table(cfg.table("silver", "transactions"))
    txns.cache()

    # 1. Fraud rate, fraud count and transaction count per category per hour.
    category_hourly = (
        txns.withColumn("trans_hour_ts", F.date_trunc("hour", F.col("trans_time")))
        .groupBy("trans_hour_ts", "category")
        .agg(
            F.count(F.lit(1)).alias("txns"),
            F.sum(F.col("is_fraud")).cast("long").alias("fraud_txns"),
            F.round(F.sum("amt"), 2).alias("total_amt"),
            F.round(F.avg("amt"), 3).alias("avg_amt"),
        )
        .withColumn("fraud_rate", F.round(F.col("fraud_txns") / F.col("txns"), 6))
    )
    _write(
        category_hourly,
        cfg.table("gold", CATEGORY_HOURLY_FRAUD),
        partition=P.days("trans_hour_ts"),
    )

    # 2. Transaction volume and total amount per state per hour.
    state_hourly = (
        txns.withColumn("trans_hour_ts", F.date_trunc("hour", F.col("trans_time")))
        .groupBy("trans_hour_ts", "state")
        .agg(
            F.count(F.lit(1)).alias("txns"),
            F.round(F.sum("amt"), 2).alias("total_amt"),
            F.round(F.avg("amt"), 3).alias("avg_amt"),
        )
    )
    _write(state_hourly, cfg.table("gold", STATE_HOURLY_VOLUME), partition=P.days("trans_hour_ts"))

    # 3. Card velocity: transactions per card per day, plus a trailing 7-day rolling count. A
    # card that suddenly jumps from its usual daily rate is a classic fraud signal on its own,
    # before any single transaction looks unusual.
    daily_per_card = txns.groupBy("cc_num_hash", "trans_date").agg(
        F.count(F.lit(1)).alias("txns_that_day"),
        F.round(F.sum("amt"), 2).alias("amt_that_day"),
        F.round(F.max("amt"), 2).alias("max_amt_that_day"),
    )
    # rangeBetween works on a numeric ordering, so the date becomes "days since epoch"; that
    # makes the window a true trailing 7 calendar days even for a card with a gap in activity,
    # which rowsBetween would get wrong.
    rolling = Window.partitionBy("cc_num_hash").orderBy(
        F.datediff(F.col("trans_date"), F.lit("1970-01-01")).cast("long")
    ).rangeBetween(-6, 0)
    card_velocity = daily_per_card.withColumn(
        "txns_trailing_7d", F.sum("txns_that_day").over(rolling)
    ).withColumn("amt_trailing_7d", F.round(F.sum("amt_that_day").over(rolling), 2))
    _write(card_velocity, cfg.table("gold", CARD_VELOCITY), partition=P.days("trans_date"))

    # 4. High-risk merchant leaderboard: fraud rate per merchant, gated on a minimum volume so a
    # single-transaction merchant cannot land a 100% fraud rate at the top.
    merchant_risk = (
        txns.groupBy("merchant", "category")
        .agg(
            F.count(F.lit(1)).alias("txns"),
            F.sum(F.col("is_fraud")).cast("long").alias("fraud_txns"),
            F.round(F.sum("amt"), 2).alias("total_amt"),
        )
        .withColumn("fraud_rate", F.round(F.col("fraud_txns") / F.col("txns"), 6))
        .where(F.col("txns") >= MERCHANT_MIN_VOLUME)
    )
    _write(merchant_risk, cfg.table("gold", MERCHANT_RISK_LEADERBOARD))

    # 5. Geo-distance anomaly: how far the transaction happened from the cardholder's home,
    # fraud vs legitimate, average and the tails (p50/p90/p99) rather than just the mean, because
    # a mean hides exactly the long-distance outliers this table exists to surface.
    geo_anomaly = txns.groupBy("is_fraud").agg(
        F.count(F.lit(1)).alias("txns"),
        F.round(F.avg("distance_km"), 3).alias("avg_distance_km"),
        F.round(F.expr("percentile_approx(distance_km, 0.5)"), 3).alias("p50_distance_km"),
        F.round(F.expr("percentile_approx(distance_km, 0.9)"), 3).alias("p90_distance_km"),
        F.round(F.expr("percentile_approx(distance_km, 0.99)"), 3).alias("p99_distance_km"),
    )
    _write(geo_anomaly, cfg.table("gold", GEO_DISTANCE_ANOMALY))

    txns.unpersist()

    enforce(
        spark.table(cfg.table("gold", CATEGORY_HOURLY_FRAUD)),
        "gold_category_hourly_fraud",
        cfg=cfg,
        stage="gold",
        as_of=as_of(cfg),
    )
    enforce(
        spark.table(cfg.table("gold", STATE_HOURLY_VOLUME)),
        "gold_state_hourly_volume",
        cfg=cfg,
        stage="gold",
    )

    counts = {
        CATEGORY_HOURLY_FRAUD: spark.table(cfg.table("gold", CATEGORY_HOURLY_FRAUD)).count(),
        STATE_HOURLY_VOLUME: spark.table(cfg.table("gold", STATE_HOURLY_VOLUME)).count(),
        CARD_VELOCITY: spark.table(cfg.table("gold", CARD_VELOCITY)).count(),
        MERCHANT_RISK_LEADERBOARD: spark.table(
            cfg.table("gold", MERCHANT_RISK_LEADERBOARD)
        ).count(),
        GEO_DISTANCE_ANOMALY: spark.table(cfg.table("gold", GEO_DISTANCE_ANOMALY)).count(),
    }
    log.info("gold written: %s", counts)
    return counts


def _write(df, table: str, partition=None) -> None:
    writer = df.writeTo(table).tableProperty("format-version", "2")
    if partition is not None:
        writer = writer.partitionedBy(partition)
    writer.createOrReplace()
    log.info("wrote %s", table)


if __name__ == "__main__":  # pragma: no cover
    run()
