"""Spark Structured Streaming: Kafka -> fraud model -> per-transaction decision sink.

The windowed consumer in `consumer.py` answers "what is the fraud *rate* this minute". This job
answers the real-time question a payment switch actually asks: for *this* transaction, right now,
approve it, hold it for review, or decline it. It reads the same Kafka topic and event schema the
consumer does, scores every event through the logistic-regression model in `streamlake.scoring`,
and writes one decision row per transaction to an Iceberg table (`stream.txn_decisions`).

Three things are deliberate and worth knowing:

The model is loaded once on the driver and scoring happens in `foreachBatch` on the collected
micro-batch, not in a UDF. A micro-batch is bounded (`maxOffsetsPerTrigger`), so pulling it to
pandas, scoring it in one vectorised call, and writing the result back is simpler and faster than
broadcasting a scikit-learn pipeline into a Python UDF, and it keeps one code path (`score_frame`)
shared with the offline evaluation.

Redeliveries are deduplicated on `trans_num` inside the watermark, the same guard the metrics
consumer uses, so an at-least-once Kafka redelivery does not produce two decision rows for one
transaction. The sink is a `MERGE` keyed on `trans_num`, so even a redelivery that slips past the
watermark overwrites its earlier decision rather than duplicating it.

`is_fraud` is carried through to the sink for evaluation only. It is never a model input (see
`scoring.event_to_features`); a live event would not have it, and the decision must be identical
whether the label is present or not.
"""

from __future__ import annotations

from datetime import UTC, datetime

from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger
from streamlake.scoring import DEFAULT_MODEL_PATH, FraudScorer, event_to_features
from streamlake.spark import build_spark, ensure_namespaces
from streamlake.stream.consumer import event_schema

log = get_logger(__name__)

TABLE = "txn_decisions"


def ensure_decision_table(spark, cfg: Config) -> str:
    table = cfg.table("stream", TABLE)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            trans_num          string,
            event_ts           timestamp,
            amt                double,
            category           string,
            state              string,
            distance_km        double,
            fraud_probability  double,
            decision           string,
            is_fraud           bigint,
            scored_at          timestamp
        )
        USING iceberg
        PARTITIONED BY (decision)
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    return table


def _score_pandas(scorer: FraudScorer, pdf):
    """Score a collected micro-batch (pandas) and return the decision rows to write."""
    import pandas as pd

    if pdf.empty:
        return pdf
    feats = pd.DataFrame([event_to_features(row) for row in pdf.to_dict("records")])
    probs = scorer.score_frame(feats)
    out = pd.DataFrame(
        {
            "trans_num": pdf["trans_num"].to_numpy(),
            "event_ts": pd.to_datetime(pdf["event_ts"]),
            "amt": pdf["amt"].to_numpy(),
            "category": pdf["category"].to_numpy(),
            "state": pdf["state"].to_numpy(),
            "distance_km": pdf["distance_km"].to_numpy(),
            "fraud_probability": probs.round(6),
            "decision": [scorer.decision_for(float(p))[0] for p in probs],
            "is_fraud": pdf["is_fraud"].fillna(-1).astype("int64").to_numpy(),
        }
    )
    return out


def make_batch_writer(cfg: Config, table: str, scorer: FraudScorer):
    from pyspark.sql import functions as F

    state = {"batches": 0, "scored": 0, "decline": 0, "review": 0, "approve": 0}

    def write_batch(batch_df, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            log.info("batch %d: empty, nothing to score", batch_id)
            return
        scored = _score_pandas(scorer, pdf)
        counts = scored["decision"].value_counts().to_dict()
        spark = batch_df.sparkSession
        out_df = spark.createDataFrame(scored).withColumn("scored_at", F.current_timestamp())
        out_df.createOrReplaceTempView("streamlake_decisions")
        spark.sql(
            f"""
            MERGE INTO {table} AS target
            USING (SELECT * FROM streamlake_decisions) AS source
                ON target.trans_num = source.trans_num
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        state["batches"] += 1
        state["scored"] += len(scored)
        state["decline"] += int(counts.get("DECLINE", 0))
        state["review"] += int(counts.get("REVIEW", 0))
        state["approve"] += int(counts.get("APPROVE", 0))
        log.info(
            "batch %d: scored %d txns -> %d decline, %d review, %d approve",
            batch_id,
            len(scored),
            int(counts.get("DECLINE", 0)),
            int(counts.get("REVIEW", 0)),
            int(counts.get("APPROVE", 0)),
        )

    return write_batch, state


def build_stream(spark, cfg: Config):
    from pyspark.sql import functions as F

    watermark_delay = str(cfg.get("stream.watermark_delay", "2 minutes"))
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", str(cfg.require("kafka.bootstrap_servers")))
        .option("subscribe", str(cfg.require("kafka.topic")))
        .option("startingOffsets", "earliest")
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
    # Same dedup guard as the metrics consumer: one decision per transaction, redeliveries dropped.
    return events.withWatermark("event_ts", watermark_delay).dropDuplicatesWithinWatermark(
        ["trans_num"]
    )


def run(
    cfg: Config | None = None,
    *,
    run_seconds: int | None = None,
    model_path=DEFAULT_MODEL_PATH,
) -> dict[str, int]:
    cfg = cfg or get_config()
    seconds = int(run_seconds or cfg.get("stream.run_seconds", 120))
    trigger = str(cfg.get("stream.trigger_interval", "10 seconds"))

    scorer = FraudScorer.load(model_path)
    banner(
        log,
        f"SCORER | topic={cfg.get('kafka.topic')} model={model_path} "
        f"thresholds(review={scorer.review_threshold:.3f},decline={scorer.decline_threshold:.3f}) "
        f"run={seconds}s",
    )

    spark = build_spark("scorer", streaming=True, cfg=cfg)
    ensure_namespaces(spark, cfg)
    table = ensure_decision_table(spark, cfg)

    events = build_stream(spark, cfg)
    write_batch, state = make_batch_writer(cfg, table, scorer)

    checkpoint = cfg.path("checkpoints") / TABLE
    checkpoint.mkdir(parents=True, exist_ok=True)

    query = (
        events.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime=trigger)
        .queryName("streamlake-txn-decisions")
        .start()
    )
    if seconds > 0:
        query.awaitTermination(seconds)
    else:
        log.info("run_seconds=0, running until terminated")
        query.awaitTermination()
    query.stop()

    total = spark.table(table).count()
    log.info(
        "scoring stopped | batches=%d scored=%d (decline=%d review=%d approve=%d) | table holds %d",
        state["batches"],
        state["scored"],
        state["decline"],
        state["review"],
        state["approve"],
        total,
    )
    _write_at = datetime.now(UTC).isoformat()
    return {**state, "table_rows": total, "generated_at": _write_at}


if __name__ == "__main__":  # pragma: no cover
    run()
