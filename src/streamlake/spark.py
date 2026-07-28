"""SparkSession factory wired for Iceberg (and optionally Kafka).

Two things make Spark "know about" Iceberg: the SQL extension that adds Iceberg's DDL/DML
grammar (MERGE INTO, ALTER TABLE ... WRITE ORDERED BY, CALL <catalog>.system.*), and a named
catalog whose tables are Iceberg tables rather than Hive/Parquet directories.

Catalog type is config-driven. ``hadoop`` keeps the metadata pointer file next to the data, so
it needs no external service and works the same on a laptop and on S3; ``rest`` talks to an
Iceberg REST catalog (Polaris, Nessie, Glue-via-REST) in a real deployment.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

from streamlake.config import Config, get_config
from streamlake.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover - import cost only paid inside jobs
    from pyspark.sql import SparkSession

log = get_logger(__name__)

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def _catalog_conf(cfg: Config) -> dict[str, str]:
    catalog = cfg.catalog
    prefix = f"spark.sql.catalog.{catalog}"
    catalog_type = str(cfg.get("lakehouse.type", "hadoop")).lower()

    conf: dict[str, str] = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.warehouse": cfg.warehouse_uri,
    }

    if catalog_type == "rest":
        conf[f"{prefix}.type"] = "rest"
        conf[f"{prefix}.uri"] = str(cfg.require("lakehouse.uri"))
    else:
        conf[f"{prefix}.type"] = "hadoop"

    # Object-store mode: only applied when an S3 endpoint/warehouse is configured, so the
    # local path stays dependency-free.
    endpoint = str(cfg.get("lakehouse.s3.endpoint", "") or "")
    if cfg.warehouse_uri.startswith("s3"):
        conf[f"{prefix}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
        if endpoint:
            conf[f"{prefix}.s3.endpoint"] = endpoint
            conf[f"{prefix}.s3.path-style-access"] = str(
                cfg.get("lakehouse.s3.path_style_access", "true")
            ).lower()
    return conf


def _pin_python_interpreter() -> None:
    """Make Spark's Python workers use the same interpreter as the driver.

    Without this, Spark launches workers with whatever `python3` resolves to on PATH. On a
    machine with a newer system Python than the virtualenv, every job that needs a Python worker
    dies with PYTHON_VERSION_MISMATCH, and it dies at execution time, so it looks like a data
    problem rather than an environment one.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _pin_driver_timezone() -> None:
    """Make the driver process agree with ``spark.sql.session.timeZone``.

    Spark computes in the session timezone (UTC here), but ``collect()`` converts timestamps to
    Python datetimes using the *driver machine's* local zone. On a laptop in New York that means
    a row whose ``pickup_hour`` column says 17 prints its ``pickup_ts`` as 12:26, the data is
    right and the report is a lie. Pinning TZ before the JVM starts removes the discrepancy.
    """
    if os.environ.get("TZ") != "UTC":
        os.environ["TZ"] = "UTC"
        if hasattr(time, "tzset"):
            time.tzset()


def build_spark(
    app_suffix: str = "job",
    *,
    streaming: bool = False,
    cfg: Config | None = None,
) -> SparkSession:
    from pyspark.sql import SparkSession

    cfg = cfg or get_config()
    _pin_python_interpreter()
    _pin_driver_timezone()

    packages = list(cfg.get("spark.packages", []) or [])
    if streaming:
        packages += list(cfg.get("spark.streaming_packages", []) or [])

    builder = (
        SparkSession.builder.appName(f"{cfg.get('spark.app_name', 'streamlake')}-{app_suffix}")
        .master(str(cfg.get("spark.master", "local[*]")))
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
    )

    # In a container the jars are baked into the image (scripts/fetch_jars.sh), so a pod start
    # never depends on Maven Central being reachable from the cluster. Locally, Ivy resolution
    # is fine and cached in ~/.ivy2 after the first run.
    jars_dir = os.environ.get("STREAMLAKE_JARS")
    if jars_dir and Path(jars_dir).is_dir():
        jars = sorted(str(p) for p in Path(jars_dir).glob("*.jar"))
        log.info("using %d pre-fetched jars from %s", len(jars), jars_dir)
        builder = builder.config("spark.jars", ",".join(jars))
    else:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    for key, value in (cfg.get("spark.conf", {}) or {}).items():
        builder = builder.config(key, str(value))
    for key, value in _catalog_conf(cfg).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("spark %s | catalog=%s | warehouse=%s", spark.version, cfg.catalog, cfg.warehouse_uri)
    return spark


def ensure_namespaces(spark: SparkSession, cfg: Config | None = None) -> None:
    cfg = cfg or get_config()
    for namespace in (cfg.get("lakehouse.namespaces", {}) or {}).values():
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.{namespace}")
