"""The nightly batch DAG.

Every task calls the same function the CLI calls — there is no scheduler-only code path, so
what you debug by hand is what runs at 03:00. The DAG's job is scheduling, retries, and
dependency order; the pipeline logic lives in ``src/streamlake``.

Failure semantics are the point of the whole project: a contract breach raises
``DataContractViolation`` inside a task, the task fails, downstream tasks are never scheduled,
and the stale-but-correct warehouse keeps serving yesterday's data instead of today's bad data.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

# Spark needs a JDK it supports and a UTC driver; the scheduler's environment is not the shell
# environment, so both are set explicitly rather than assumed.
os.environ.setdefault("TZ", "UTC")

DEFAULT_ARGS = {
    "owner": "kalyan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # A contract breach is not a transient error — retrying it just fails three times slower.
    # Retries here are for the flaky parts: the download and the object-store write.
    "retry_exponential_backoff": True,
    "depends_on_past": False,
}


@dag(
    dag_id="streamlake_batch",
    description="NYC taxi -> Iceberg lakehouse -> warehouse -> dbt marts, contract-gated",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["streamlake", "batch", "lakehouse"],
    doc_md=__doc__,
)
def streamlake_batch():
    @task(retries=3)
    def ingest() -> dict:
        """Download and checksum the source files. The only task allowed to touch the network."""
        from streamlake.batch import ingest as job

        return job.run()

    @task
    def bronze() -> dict:
        """Land the raw file in Iceberg, unchanged. Fails if the file is not the month we asked for."""
        from streamlake.batch import bronze as job

        return job.run()

    @task
    def silver() -> dict:
        """Conform, quarantine, dedup. Fails if the quarantine rate blows past its budget."""
        from streamlake.batch import silver as job

        return job.run()

    @task
    def gold() -> dict:
        """Build the lake-side aggregates."""
        from streamlake.batch import gold as job

        return job.run()

    @task
    def export() -> dict:
        """Write the curated Parquet the warehouse loads, plus a manifest to reconcile against."""
        from streamlake.batch import export as job

        return job.run()

    @task
    def warehouse_load() -> dict:
        """Load DuckDB or Snowflake and reconcile row counts against the export manifest."""
        from streamlake.warehouse import load as job

        return job.run()

    @task.bash
    def dbt_build() -> str:
        """Run every dbt model and test. `dbt build` interleaves them, so a model whose test
        fails does not get used by the models downstream of it."""
        return (
            f"cd {REPO_ROOT} && "
            f"{REPO_ROOT}/.venv/bin/dbt build "
            f"--project-dir dbt/streamlake --profiles-dir dbt/streamlake"
        )

    @task
    def dashboard() -> dict:
        """Render the static BI dashboard from the marts."""
        from streamlake.dashboard import build as job

        return job.run()

    @task(trigger_rule="all_done")
    def contract_summary() -> dict:
        """Always runs. Collects every contract report from this run into one summary.

        trigger_rule="all_done" is deliberate: the run you most want a contract summary for is
        the one that just failed.
        """
        from streamlake.contracts.summary import summarise

        return summarise()

    chain = ingest() >> bronze() >> silver() >> gold() >> export() >> warehouse_load()
    chain >> dbt_build() >> dashboard() >> contract_summary()


streamlake_batch()
