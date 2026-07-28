"""Spark Structured Streaming: Kafka -> windowed aggregation -> Iceberg upsert.

Four things here are load-bearing and none of them are obvious from the code alone.

``withWatermark("event_ts", "2 minutes")`` sets how long Spark waits for stragglers. Without it
state for every window ever seen is kept forever; with it a window is finalised two minutes
after its event time passes, and anything later is dropped rather than silently rewriting
history.

``dropDuplicatesWithinWatermark(["trip_id"])`` removes the redeliveries the producer injects.
Kafka is at-least-once, and a redelivered trip counted twice makes the revenue number wrong. The
watermark-scoped variant is the right one because a duplicate can carry a *different* event time
than the original, which plain ``dropDuplicates`` on (id, event_ts) would miss.

Windowed aggregates in update mode re-emit a window every time it changes, so the sink is a
MERGE keyed on the window rather than an append. Append would leave several rows for one window
and double-count on read.

Each micro-batch is validated *before* it is merged. A breach raises, the batch is not
committed, and the offsets are not advanced, the streaming equivalent of failing the DAG.
"""

from __future__ import annotations

from datetime import UTC, datetime

from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces

log = get_logger(__name__)

TABLE = "trip_metrics_1m"


def event_schema():
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    # An explicit schema, never schema inference: a stream cannot re-read history to work out
    # its own shape, and a producer that starts omitting a field should surface as nulls in a
    # known column rather than as a silently different DataFrame.
    return StructType(
        [
            StructField("trip_id", StringType()),
            StructField("event_ts", StringType()),
            StructField("ingested_ts", StringType()),
            StructField("late", BooleanType()),
            StructField("pickup_ts", StringType()),
            StructField("dropoff_ts", StringType()),
            StructField("passenger_count", LongType()),
            StructField("trip_distance_mi", DoubleType()),
            StructField("pu_location_id", IntegerType()),
            StructField("do_location_id", IntegerType()),
            StructField("pickup_borough", StringType()),
            StructField("pickup_zone", StringType()),
            StructField("payment_type_desc", StringType()),
            StructField("fare_amount", DoubleType()),
            StructField("tip_amount", DoubleType()),
            StructField("total_amount", DoubleType()),
            StructField("redelivery", BooleanType()),
        ]
    )


def ensure_target_table(spark, cfg: Config) -> str:
    # The sink has to exist before the first micro-batch, or its MERGE has nothing to target.
    table = cfg.table("stream", TABLE)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            window_start     timestamp,
            window_end       timestamp,
            pickup_borough   string,
            trips            bigint,
            passengers       bigint,
            revenue          double,
            avg_fare         double,
            avg_distance_mi  double,
            updated_at       timestamp
        )
        USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    return table


def build_stream(spark, cfg: Config):
    from pyspark.sql import functions as F

    window_duration = str(cfg.get("stream.window_duration", "1 minute"))
    watermark_delay = str(cfg.get("stream.watermark_delay", "2 minutes"))

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", str(cfg.require("kafka.bootstrap_servers")))
        .option("subscribe", str(cfg.require("kafka.topic")))
        .option("startingOffsets", "earliest")
        # Bound how much a restart after downtime tries to swallow in one batch; without it the
        # first batch after an outage can be big enough to OOM the executor.
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .where(F.col("trip_id").isNotNull() & F.col("event_ts").isNotNull())
    )

    deduped = events.withWatermark("event_ts", watermark_delay).dropDuplicatesWithinWatermark(
        ["trip_id"]
    )

    return (
        deduped.groupBy(
            F.window(F.col("event_ts"), window_duration).alias("w"),
            F.col("pickup_borough"),
        )
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum(F.coalesce("passenger_count", F.lit(0))).cast("bigint").alias("passengers"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("fare_amount"), 3).alias("avg_fare"),
            F.round(F.avg("trip_distance_mi"), 3).alias("avg_distance_mi"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("pickup_borough"),
            "trips",
            "passengers",
            "revenue",
            "avg_fare",
            "avg_distance_mi",
        )
    )


def make_batch_writer(cfg: Config, table: str):
    """foreachBatch handler: validate, then MERGE. In that order, always."""
    from pyspark.sql import functions as F

    state = {"batches": 0, "rows": 0}

    def write_batch(batch_df, batch_id: int) -> None:
        stamped = batch_df.withColumn("updated_at", F.current_timestamp())
        # Cache: the contract pass and the MERGE both consume this batch, and recomputing it
        # would mean re-reading the same Kafka offsets twice.
        stamped.persist()
        try:
            count = stamped.count()
            if count == 0:
                log.info("batch %d: empty, nothing to merge", batch_id)
                return

            enforce(
                stamped,
                "stream_trip_metrics_1m",
                cfg=cfg,
                stage=f"stream/batch-{batch_id}",
                as_of=datetime.now(UTC),
            )

            stamped.createOrReplaceTempView("streamlake_micro_batch")
            stamped.sparkSession.sql(
                f"""
                MERGE INTO {table} AS target
                USING (SELECT * FROM streamlake_micro_batch) AS source
                    ON  target.window_start   = source.window_start
                    AND target.pickup_borough = source.pickup_borough
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            state["batches"] += 1
            state["rows"] += count
            log.info("batch %d: merged %d window rows", batch_id, count)
        finally:
            stamped.unpersist()

    return write_batch, state


def run(cfg: Config | None = None, *, run_seconds: int | None = None) -> dict[str, int]:
    cfg = cfg or get_config()
    seconds = int(run_seconds or cfg.get("stream.run_seconds", 120))
    trigger = str(cfg.get("stream.trigger_interval", "10 seconds"))

    banner(
        log,
        f"CONSUMER | topic={cfg.get('kafka.topic')} window={cfg.get('stream.window_duration')} "
        f"watermark={cfg.get('stream.watermark_delay')} run={seconds}s",
    )

    spark = build_spark("stream", streaming=True, cfg=cfg)
    ensure_namespaces(spark, cfg)
    table = ensure_target_table(spark, cfg)

    aggregated = build_stream(spark, cfg)
    write_batch, state = make_batch_writer(cfg, table)

    checkpoint = cfg.path("checkpoints") / TABLE
    checkpoint.mkdir(parents=True, exist_ok=True)

    query = (
        aggregated.writeStream
        # update mode: emit only the windows that changed in this batch. The MERGE makes that
        # safe; with an append sink the same window would arrive as several rows.
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime=trigger)
        .queryName("streamlake-trip-metrics-1m")
        .start()
    )

    # A bounded run keeps the same code usable from a Makefile, a test, and an Airflow task.
    # In Kubernetes STREAM_RUN_SECONDS is 0, which means "run until something stops you", the
    # Deployment is the supervisor there, not a timer.
    if seconds > 0:
        query.awaitTermination(seconds)
    else:
        log.info("run_seconds=0, running until terminated")
        query.awaitTermination()
    progress = query.lastProgress
    query.stop()

    if progress:
        log.info(
            "stream stopped | batches=%d rows=%d | last batch: input=%s rows/s processed=%s rows/s",
            state["batches"],
            state["rows"],
            round(progress.get("inputRowsPerSecond") or 0, 1),
            round(progress.get("processedRowsPerSecond") or 0, 1),
        )

    result = spark.table(table)
    total = result.count()
    log.info("%s now holds %d window rows", table, total)
    return {"batches": state["batches"], "merged_rows": state["rows"], "table_rows": total}


if __name__ == "__main__":  # pragma: no cover
    run()
