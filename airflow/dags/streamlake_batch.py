"""The nightly batch DAG.

Every task shells out to the same ``streamlake`` CLI you run by hand. That is deliberate, and it
is the one design decision in this file worth explaining:

Airflow's dependency pins and Spark's do not agree, so Airflow lives in its own virtualenv
(``.venv-airflow``) and the pipeline lives in ``.venv``. If the tasks were PythonOperators they
would import PySpark into the scheduler's interpreter, which does not have it — and the failure
would arrive at run time, in a worker log, looking like a data problem. Shelling out keeps the
two environments honestly separated and means there is no scheduler-only code path: what you
debug in a terminal is byte-for-byte what runs at 03:00.

Failure semantics are the point of the whole project: a contract breach raises inside the CLI,
the process exits non-zero, the task fails, downstream tasks never start, and the warehouse
keeps serving yesterday's correct data instead of today's bad data.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
DBT = REPO_ROOT / ".venv" / "bin" / "dbt"

# Spark 4 supports JDK 17 and 21 but not 25, and the scheduler's environment is not your shell's,
# so the JDK is resolved explicitly rather than inherited.
ENV = (
    f'cd "{REPO_ROOT}" && '
    'export JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || echo "$JAVA_HOME")" && '
    f'export PYTHONPATH="{REPO_ROOT}/src" && export TZ=UTC && '
)


def streamlake(command: str) -> str:
    """Build the shell command for one pipeline step."""
    return f"{ENV} {PIPELINE_PYTHON} -m streamlake {command}"


DEFAULT_ARGS = {
    "owner": "kalyan",
    # Retries are for the flaky parts — a download, an object-store write. A contract breach is
    # not transient: retrying it just fails three times more slowly.
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
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
    @task.bash(retries=3)
    def ingest() -> str:
        """Download and checksum the source files. The only task allowed to touch the network."""
        return streamlake("ingest")

    @task.bash
    def bronze() -> str:
        """Land the raw file in Iceberg unchanged; fail if it is not the month we asked for."""
        return streamlake("bronze")

    @task.bash
    def silver() -> str:
        """Conform, quarantine, dedup. Fails if the quarantine rate blows past its budget."""
        return streamlake("silver")

    @task.bash
    def gold() -> str:
        """Build the lake-side aggregates."""
        return streamlake("gold")

    @task.bash
    def export() -> str:
        """Write the curated Parquet the warehouse loads, plus a manifest to reconcile against."""
        return streamlake("export")

    @task.bash
    def warehouse_load() -> str:
        """Load DuckDB or Snowflake, reconciling row counts against the export manifest."""
        return streamlake("warehouse-load")

    @task.bash
    def dbt_build() -> str:
        """Run every dbt model and test.

        `dbt build` interleaves models and their tests, so a model whose test fails does not get
        consumed by the models downstream of it.
        """
        return (
            f'{ENV} {DBT} build --project-dir dbt/streamlake --profiles-dir dbt/streamlake'
        )

    @task.bash
    def dashboard() -> str:
        """Render the static BI dashboard from the marts."""
        return streamlake("dashboard")

    @task.bash(trigger_rule="all_done")
    def contract_summary() -> str:
        """Always runs.

        trigger_rule="all_done" is deliberate: the run you most want a contract summary for is
        the one that just failed.
        """
        return streamlake("summary")

    (
        ingest()
        >> bronze()
        >> silver()
        >> gold()
        >> export()
        >> warehouse_load()
        >> dbt_build()
        >> dashboard()
        >> contract_summary()
    )


streamlake_batch()
