"""Step 6: load the curated Parquet into the serving warehouse.

Two backends, one code path:

* **duckdb** (default): the whole warehouse is a single file, so the project runs end to end
  with no account, no credentials, and no bill.
* **snowflake**: the same tables via an internal stage and ``COPY INTO``. Enabled by setting
  ``WAREHOUSE_TARGET=snowflake`` plus the ``SNOWFLAKE_*`` variables in ``.env``.

Either way the loader finishes by reconciling what landed against the export manifest. A load
that inserts 90% of the rows goes unnoticed otherwise, every query still returns something.
"""

from __future__ import annotations

import json
from pathlib import Path

from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger

log = get_logger(__name__)


class WarehouseLoadError(RuntimeError):
    """Raised when the warehouse ends up with a different row count than the lake exported."""


def _manifest(cfg: Config) -> dict:
    path = cfg.path("curated") / "_export_manifest.json"
    if not path.exists():
        raise RuntimeError(f"no export manifest at {path}, run `streamlake export` first")
    return json.loads(path.read_text())


def _reconcile(name: str, expected: int, actual: int) -> None:
    if expected != actual:
        raise WarehouseLoadError(
            f"{name}: lake exported {expected} rows, warehouse has {actual} "
            f"({expected - actual:+d}), the load is incomplete, refusing to mark it good"
        )
    log.info("  reconciled %-16s %9d rows", name, actual)


# ---------------------------------------------------------------------------------------
# duckdb
# ---------------------------------------------------------------------------------------


def load_duckdb(cfg: Config, manifest: dict) -> dict[str, int]:
    import duckdb

    db_path = Path(str(cfg.require("paths.warehouse_db")))
    if not db_path.is_absolute():
        db_path = cfg.root / db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    schema = str(cfg.require("warehouse.schema_raw"))
    counts: dict[str, int] = {}

    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for name, entry in manifest["tables"].items():
            source = f"{entry['path']}/*.parquet"
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{name} AS "
                f"SELECT * FROM read_parquet('{source}')"
            )
            actual = con.execute(f"SELECT count(*) FROM {schema}.{name}").fetchone()[0]
            _reconcile(name, int(entry["row_count"]), int(actual))
            counts[name] = int(actual)
    finally:
        con.close()

    log.info("duckdb warehouse: %s", db_path)
    return counts


# ---------------------------------------------------------------------------------------
# snowflake
# ---------------------------------------------------------------------------------------


def load_snowflake(cfg: Config, manifest: dict) -> dict[str, int]:
    """Stage the curated Parquet and COPY INTO Snowflake.

    Uses MATCH_BY_COLUMN_NAME so the table is defined by the Parquet schema rather than a
    hand-maintained DDL that drifts from the lake.
    """
    import snowflake.connector  # imported lazily: only needed when this target is selected

    settings = cfg.get("warehouse.snowflake", {}) or {}
    missing = [k for k in ("account", "user", "password") if not settings.get(k)]
    if missing:
        raise RuntimeError(
            f"snowflake target selected but {missing} not set, see .env.example"
        )

    database, schema = settings["database"], settings["schema"]
    counts: dict[str, int] = {}

    con = snowflake.connector.connect(
        account=settings["account"],
        user=settings["user"],
        password=settings["password"],
        role=settings.get("role"),
        warehouse=settings.get("warehouse"),
    )
    try:
        cur = con.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
        cur.execute(f"USE SCHEMA {database}.{schema}")
        cur.execute(
            "CREATE FILE FORMAT IF NOT EXISTS streamlake_parquet TYPE = PARQUET"
        )

        for name, entry in manifest["tables"].items():
            stage = f"streamlake_stage_{name}"
            cur.execute(f"CREATE OR REPLACE STAGE {stage} FILE_FORMAT = streamlake_parquet")
            for parquet in sorted(Path(entry["path"]).glob("*.parquet")):
                cur.execute(f"PUT file://{parquet} @{stage} OVERWRITE = TRUE AUTO_COMPRESS = FALSE")

            # INFER_SCHEMA preserves the Parquet field case (lowercase), which makes Snowflake
            # store quoted lowercase columns. dbt models are shared with the DuckDB target and
            # reference bare identifiers that Snowflake folds to uppercase, so lowercase quoted
            # columns never resolve (DuckDB is case-insensitive and hides this). Uppercasing
            # COLUMN_NAME in the template makes the landed columns match the folded dbt refs;
            # COPY INTO below still matches by name case-insensitively.
            cur.execute(
                f"CREATE OR REPLACE TABLE {name} USING TEMPLATE ("
                f"  SELECT array_agg(object_construct("
                f"    'COLUMN_NAME', upper(\"COLUMN_NAME\"),"
                f"    'TYPE', \"TYPE\","
                f"    'NULLABLE', \"NULLABLE\""
                f"  )) FROM TABLE("
                f"    INFER_SCHEMA(LOCATION => '@{stage}', FILE_FORMAT => 'streamlake_parquet')"
                f"  ))"
            )
            cur.execute(
                f"COPY INTO {name} FROM @{stage} FILE_FORMAT = (FORMAT_NAME = streamlake_parquet) "
                f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
            )
            actual = cur.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            _reconcile(name, int(entry["row_count"]), int(actual))
            counts[name] = int(actual)
    finally:
        con.close()

    log.info("snowflake warehouse: %s.%s", database, schema)
    return counts


def run(cfg: Config | None = None, *, target: str | None = None) -> dict[str, int]:
    cfg = cfg or get_config()
    target = (target or str(cfg.require("warehouse.target"))).lower()
    banner(log, f"WAREHOUSE LOAD | target={target}")

    manifest = _manifest(cfg)
    if target == "duckdb":
        return load_duckdb(cfg, manifest)
    if target == "snowflake":
        return load_snowflake(cfg, manifest)
    raise ValueError(f"unknown warehouse target: {target}")


if __name__ == "__main__":  # pragma: no cover
    run()
