"""The streaming DAG.

A long-running Structured Streaming query is an awkward fit for a batch scheduler. This runs the
consumer for a bounded window on a short schedule, which makes it a supervisor loop with
monitoring attached rather than an ETL job. The production shape is the Kubernetes Deployment in
``infra/k8s`` (``STREAM_RUN_SECONDS=0``, the cluster restarts it); this DAG exists so the
streaming arm is exercised and alerted on from the same place as the batch arm.

``stream-check`` is the task that matters. A consumer that starts, processes nothing and exits
cleanly leaves a green DAG behind it.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"

ENV = (
    f'cd "{REPO_ROOT}" && '
    'export JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || echo "$JAVA_HOME")" && '
    f'export PYTHONPATH="{REPO_ROOT}/src" && export TZ=UTC && '
)


def streamlake(command: str) -> str:
    return f"{ENV} {PIPELINE_PYTHON} -m streamlake {command}"


@dag(
    dag_id="streamlake_stream",
    description="Bounded run of the Kafka -> Iceberg streaming consumer, with a lag check",
    schedule=timedelta(minutes=15),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Never two consumers on one checkpoint: Structured Streaming locks the checkpoint directory
    # and the second query simply dies.
    max_active_runs=1,
    default_args={"owner": "kalyan", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["streamlake", "streaming"],
    doc_md=__doc__,
)
def streamlake_stream():
    @task.bash
    def produce() -> str:
        """Replay a slice of curated transactions onto the topic so the demo has something to consume."""
        return streamlake("produce --max-events 5000")

    @task.bash(execution_timeout=timedelta(minutes=10))
    def consume() -> str:
        """Run the consumer for a bounded window; contracts run per micro-batch inside it."""
        return streamlake("consume --run-seconds 180")

    @task.bash
    def check_stream_freshness() -> str:
        """Fail if the newest window is further behind than the alert threshold."""
        return streamlake("stream-check --max-lag-minutes 30")

    produce() >> consume() >> check_stream_freshness()


streamlake_stream()
