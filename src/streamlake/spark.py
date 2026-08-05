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
            # An explicit endpoint means an S3-compatible store that is not real AWS (MinIO
            # locally). Real AWS S3 leaves this unset and Iceberg's S3FileIO falls back to the
            # default AWS SDK credential chain, which picks up ~/.aws/credentials on its own.
            # MinIO has no IAM to hand out session credentials from, so its static
            # access/secret key pair has to be given to the client explicitly rather than
            # relying on a chain that assumes a real AWS account is behind it.
            conf[f"{prefix}.s3.endpoint"] = endpoint
            conf[f"{prefix}.s3.path-style-access"] = str(
                cfg.get("lakehouse.s3.path_style_access", "true")
            ).lower()
            access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
            if access_key and secret_key:
                conf[f"{prefix}.s3.access-key-id"] = access_key
                conf[f"{prefix}.s3.secret-access-key"] = secret_key
            # MinIO has no region concept but the AWS SDK v2 client still requires one to be
            # set; any value works since MinIO ignores it.
            conf[f"{prefix}.client.region"] = os.environ.get("AWS_REGION", "us-east-1")
    return conf


def _hadoop_s3a_conf(cfg: Config) -> dict[str, str]:
    """Hadoop-level S3A configuration, separate from the Iceberg-catalog-scoped one above.

    A ``hadoop`` Iceberg catalog uses Iceberg's own ``S3FileIO`` (configured in
    ``_catalog_conf``) for table *data* files, but the catalog itself still goes through
    Hadoop's generic ``FileSystem`` for namespace and table *directory* operations
    (``getFileStatus``, listing). That path is a different client with its own credential and
    endpoint configuration, ``spark.hadoop.fs.s3a.*``, and setting only the Iceberg-side
    properties leaves this half unauthenticated against a non-AWS endpoint like MinIO, a 403
    at namespace-creation time even though the actual file writes would have worked.
    """
    endpoint = str(cfg.get("lakehouse.s3.endpoint", "") or "")
    if not (cfg.warehouse_uri.startswith("s3") and endpoint):
        return {}

    ssl_enabled = "false" if endpoint.startswith("http://") else "true"
    conf = {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": str(
            cfg.get("lakehouse.s3.path_style_access", "true")
        ).lower(),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": ssl_enabled,
    }
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if access_key and secret_key:
        conf["spark.hadoop.fs.s3a.access.key"] = access_key
        conf["spark.hadoop.fs.s3a.secret.key"] = secret_key
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
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
    a row whose ``trans_hour`` column says 17 prints its ``trans_time`` as 12:26, the data is
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

    # Extra packages for S3-backed runs (the Iceberg AWS bundle), supplied out of band so the
    # default local-FS and CI unit-test runs stay lean. scripts/localstack_env.sh sets this.
    extra = str(cfg.get("spark.extra_packages", "") or "")
    packages += [p.strip() for p in extra.split(",") if p.strip()]

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
    for key, value in _hadoop_s3a_conf(cfg).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("spark %s | catalog=%s | warehouse=%s", spark.version, cfg.catalog, cfg.warehouse_uri)
    return spark


def ensure_namespaces(spark: SparkSession, cfg: Config | None = None) -> None:
    cfg = cfg or get_config()
    for namespace in (cfg.get("lakehouse.namespaces", {}) or {}).values():
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.{namespace}")
