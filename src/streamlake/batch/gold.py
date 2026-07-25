"""Step 4 — gold: the aggregates the lakehouse serves directly.

Gold is where Spark earns its keep: wide aggregations over the full month. These tables are the
lake-side serving layer (and the reference the dbt marts are reconciled against), so they are
small, denormalised, and answer one question each.
"""

from __future__ import annotations

from streamlake.batch.bronze import as_of
from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces

log = get_logger(__name__)

DAILY_ZONE_KPIS = "daily_zone_kpis"
HOURLY_DEMAND = "hourly_demand"
PAYMENT_MIX = "payment_mix"


def run(cfg: Config | None = None) -> dict[str, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.functions import partitioning as P

    cfg = cfg or get_config()
    banner(log, f"GOLD | month={cfg.month}")

    spark = build_spark("gold", cfg=cfg)
    ensure_namespaces(spark, cfg)

    trips = spark.table(cfg.table("silver", "trips"))
    trips.cache()

    daily_zone = (
        trips.groupBy("pickup_date", "pickup_borough", "pickup_zone")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum(F.coalesce("passenger_count", F.lit(0))).cast("long").alias("passengers"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("fare_amount"), 3).alias("avg_fare"),
            F.round(F.avg("trip_distance_mi"), 3).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 3).alias("avg_duration_min"),
            F.round(F.avg("avg_speed_mph"), 3).alias("avg_speed_mph"),
            F.round(F.avg("tip_pct"), 3).alias("avg_tip_pct"),
        )
        .withColumn("revenue_per_trip", F.round(F.col("revenue") / F.col("trips"), 3))
    )
    _write(daily_zone, cfg.table("gold", DAILY_ZONE_KPIS), partition=P.months("pickup_date"))

    hourly = (
        trips.withColumn("pickup_hour_ts", F.date_trunc("hour", F.col("pickup_ts")))
        .groupBy("pickup_hour_ts", "pickup_borough")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("trip_duration_min"), 3).alias("avg_duration_min"),
        )
    )
    _write(hourly, cfg.table("gold", HOURLY_DEMAND), partition=P.days("pickup_hour_ts"))

    payment = (
        trips.groupBy("pickup_date", "payment_type", "payment_type_desc")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("tip_pct"), 3).alias("avg_tip_pct"),
        )
        .withColumn(
            "trip_share",
            F.round(F.col("trips") / F.sum("trips").over(Window.partitionBy("pickup_date")), 4),
        )
    )
    _write(payment, cfg.table("gold", PAYMENT_MIX))

    trips.unpersist()

    enforce(
        spark.table(cfg.table("gold", DAILY_ZONE_KPIS)),
        "gold_daily_zone_kpis",
        cfg=cfg,
        stage="gold",
        as_of=as_of(cfg),
    )
    enforce(
        spark.table(cfg.table("gold", HOURLY_DEMAND)), "gold_hourly_demand", cfg=cfg, stage="gold"
    )

    counts = {
        DAILY_ZONE_KPIS: spark.table(cfg.table("gold", DAILY_ZONE_KPIS)).count(),
        HOURLY_DEMAND: spark.table(cfg.table("gold", HOURLY_DEMAND)).count(),
        PAYMENT_MIX: spark.table(cfg.table("gold", PAYMENT_MIX)).count(),
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
