"""Spark Structured Streaming: Kafka -> windowed aggregation -> Iceberg upsert.

Four things here are load-bearing and none of them are obvious from the code alone.

``withWatermark("event_ts", "2 minutes")`` sets how long Spark waits for stragglers. Without it
state for every window ever seen is kept forever; with it a window is finalised two minutes after
its event time passes, and anything later is dropped rather than silently rewriting history.

``dropDuplicatesWithinWatermark(["trans_num"])`` removes the redeliveries the producer injects.
Kafka is at-least-once, and a redelivered transaction counted twice makes the fraud-rate and
volume numbers wrong. The dedup key is ``trans_num`` alone, deliberately not the compound
``(trans_num, event_ts)``: a genuine Kafka redelivery can carry a *different* event time than the
original (a retry after a slow ack does not necessarily replay the original timestamp), and
matching on the pair would let exactly that redelivery slip past as "new". Keying on the natural
event id and letting the watermark bound how long it is remembered is the right one.

Windowed aggregates in update mode re-emit a window every time it changes, so the sink is a MERGE
keyed on the window rather than an append. Append would leave several rows for one window and
double-count on read.

Each micro-batch is validated *before* it is merged. A breach raises, the batch is not committed,
and the offsets are not advanced, the streaming equivalent of failing the DAG.
"""

from __future__ import annotations

from datetime import UTC, datetime

from streamlake import monitoring
from streamlake.config import Config, get_config
from streamlake.contracts import enforce
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark, ensure_namespaces

log = get_logger(__name__)

TABLE = "txn_metrics_1m"


def event_schema():
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
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
            StructField("trans_num", StringType()),
            StructField("event_ts", StringType()),
            StructField("ingested_ts", StringType()),
            StructField("late", BooleanType()),
            StructField("amt", DoubleType()),
            StructField("category", StringType()),
            StructField("merchant", StringType()),
            StructField("state", StringType()),
            StructField("cc_num_hash", StringType()),
            StructField("cc_num_last4", StringType()),
            StructField("is_fraud", LongType()),
            StructField("distance_km", DoubleType()),
            StructField("redelivery", BooleanType()),
        ]
    )


def ensure_target_table(spark, cfg: Config) -> str:
    # The sink has to exist before the first micro-batch, or its MERGE has nothing to target.
    table = cfg.table("stream", TABLE)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            window_start   timestamp,
            window_end     timestamp,
            category       string,
            txns           bigint,
            fraud_txns     bigint,
            total_amt      double,
            avg_amt        double,
            updated_at     timestamp
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
        .where(F.col("trans_num").isNotNull() & F.col("event_ts").isNotNull())
    )

    deduped = events.withWatermark("event_ts", watermark_delay).dropDuplicatesWithinWatermark(
        ["trans_num"]
    )

    return (
        deduped.groupBy(
            F.window(F.col("event_ts"), window_duration).alias("w"),
            F.col("category"),
        )
        .agg(
            F.count(F.lit(1)).alias("txns"),
            F.sum(F.coalesce("is_fraud", F.lit(0))).cast("bigint").alias("fraud_txns"),
            F.round(F.sum("amt"), 2).alias("total_amt"),
            F.round(F.avg("amt"), 3).alias("avg_amt"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("category"),
            "txns",
            "fraud_txns",
            "total_amt",
            "avg_amt",
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
                "stream_txn_metrics_1m",
                cfg=cfg,
                stage=f"stream/batch-{batch_id}",
                as_of=datetime.now(UTC),
            )

            stamped.createOrReplaceTempView("streamlake_micro_batch")
            stamped.sparkSession.sql(
                f"""
                MERGE INTO {table} AS target
                USING (SELECT * FROM streamlake_micro_batch) AS source
                    ON  target.window_start = source.window_start
                    AND target.category     = source.category
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            state["batches"] += 1
            state["rows"] += count
            monitoring.emit_consumer_batch(batch_id=batch_id, rows=count)
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
        .queryName("streamlake-txn-metrics-1m")
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
    # recentProgress, not lastProgress: the batch that actually contains the dropped-duplicate
    # or dropped-by-watermark rows is not necessarily the final micro-batch of the run, so a
    # reconciliation across the whole run has to sum every batch's state-operator metrics, not
    # just read the last one.
    all_progress = list(query.recentProgress)
    progress = query.lastProgress
    query.stop()

    reconciliation = summarize_state_ops(all_progress)
    monitoring.emit_consumer_state_ops_totals(reconciliation)
    _write_reconciliation(cfg, reconciliation, state)

    log.info(
        "reconciliation | input_rows=%d dedup_removed=%d late_dropped=%d "
        "(summed across %d micro-batches)",
        reconciliation["input_rows"],
        reconciliation["dedup_removed"],
        reconciliation["late_dropped"],
        len(all_progress),
    )

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
    return {
        "batches": state["batches"],
        "merged_rows": state["rows"],
        "table_rows": total,
        **reconciliation,
    }


def summarize_state_ops(all_progress: list[dict]) -> dict[str, int]:
    """Sum the dedup and watermark-drop counters Spark itself tracks, across every micro-batch
    of the run, rather than trusting any single batch's snapshot.

    Verified against a real run's raw progress JSON (``streamlake.transactions.diag3``, three
    micro-batches: an on-time batch with 6 injected duplicates, then a batch forced 600s behind
    wall clock), because the two counters this function reads turned out to live in a different
    shape than a first guess assumed:

    * ``numRowsDroppedByWatermark`` is a **top-level field on every state operator**, not nested
      under ``customMetrics``, and it means "rows this operator discarded for arriving after the
      watermark had already passed them", i.e. the late-arrival drop. In this query's plan that
      is overwhelmingly the ``dedupeWithinWatermark`` operator (rows past the watermark cannot be
      safely deduplicated against evicted state, so the dedup operator is the one that drops
      them): the verifying run showed exactly 20/20 forced-late rows counted there, 0 on the
      aggregation (``stateStoreSave``) operator. Summing the field across *every* operator,
      rather than picking one by name, is still the correct general rule, a row that somehow
      reached the aggregation operator late would be counted there instead and must not be lost.
    * ``customMetrics.numDroppedDuplicateRows``, only present on the dedup operator, is the
      actual duplicate-removal count. The verifying run's on-time batch showed exactly 6/6
      injected duplicates counted there.

    Getting this wrong the first time (guessing the split was by operator *name* rather than by
    which field on which operator) would have silently mislabelled every late-arrival drop as a
    dedup removal, since both showed up as a ``numRowsDroppedByWatermark``-shaped number on
    *some* operator. Caught only by dumping the raw JSON from a real run instead of trusting the
    Spark docs' description of the metric from memory.
    """
    input_rows = dedup_removed = late_dropped = 0
    for batch in all_progress:
        input_rows += int(batch.get("numInputRows") or 0)
        for op in batch.get("stateOperators") or []:
            late_dropped += int(op.get("numRowsDroppedByWatermark") or 0)
            custom = op.get("customMetrics") or {}
            dedup_removed += int(custom.get("numDroppedDuplicateRows") or 0)
    return {"input_rows": input_rows, "dedup_removed": dedup_removed, "late_dropped": late_dropped}


def _write_reconciliation(cfg: Config, reconciliation: dict[str, int], state: dict) -> None:
    import json
    from datetime import UTC, datetime

    reports = cfg.path("reports") / "stream"
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / "consumer_reconciliation.json"
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "batches": state["batches"],
                "merged_rows": state["rows"],
                **reconciliation,
            },
            indent=2,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    run()
