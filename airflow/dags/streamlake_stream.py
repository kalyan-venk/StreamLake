"""The streaming DAG.

A long-running Structured Streaming query is an odd fit for a batch scheduler, so this DAG does
not pretend otherwise. It runs the consumer for a bounded window on a short schedule — a
supervisor loop, not an ETL job. The alternative (and what the Kubernetes manifests in
infra/k8s do) is to run the consumer as a Deployment and let the cluster restart it; this DAG
exists so the streaming arm is exercised and monitored from the same place as the batch arm.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))


@dag(
    dag_id="streamlake_stream",
    description="Bounded run of the Kafka -> Iceberg streaming consumer, with a lag check",
    schedule=timedelta(minutes=15),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Never two consumers on the same checkpoint: Structured Streaming takes a lock on the
    # checkpoint directory, and the second one would simply crash.
    max_active_runs=1,
    default_args={"owner": "kalyan", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["streamlake", "streaming"],
    doc_md=__doc__,
)
def streamlake_stream():
    @task
    def produce() -> dict:
        """Replay a slice of trips onto the topic so the demo has something to consume."""
        from streamlake.stream import producer

        return producer.run(max_events=5000)

    @task(execution_timeout=timedelta(minutes=10))
    def consume() -> dict:
        """Run the consumer for a bounded window; contracts run per micro-batch inside it."""
        from streamlake.stream import consumer

        return consumer.run(run_seconds=180)

    @task
    def check_stream_freshness() -> dict:
        """Fail if the newest window in the stream table is older than the alert threshold.

        This is the check that catches the failure mode a green DAG hides: the consumer ran,
        exited cleanly, and processed nothing at all.
        """
        from datetime import timezone

        from streamlake.config import get_config
        from streamlake.spark import build_spark

        cfg = get_config()
        spark = build_spark("stream-freshness", cfg=cfg)
        table = cfg.table("stream", "trip_metrics_1m")
        latest = spark.sql(f"SELECT max(window_end) AS w FROM {table}").collect()[0]["w"]
        if latest is None:
            raise RuntimeError(f"{table} is empty — the stream has never produced a window")

        latest = latest if latest.tzinfo else latest.replace(tzinfo=timezone.utc)
        lag_minutes = (datetime.now(timezone.utc) - latest).total_seconds() / 60
        if lag_minutes > 30:
            raise RuntimeError(
                f"stream lag is {lag_minutes:.1f} minutes — the consumer is not keeping up"
            )
        return {"latest_window_end": latest.isoformat(), "lag_minutes": round(lag_minutes, 2)}

    produce() >> consume() >> check_stream_freshness()


streamlake_stream()
