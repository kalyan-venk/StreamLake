from __future__ import annotations

import os
import sys

import pytest

# Must happen before any SparkSession is created: Spark launches Python workers with whatever
# `python3` is on PATH, which on this machine is a different minor version than the venv.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("TZ", "UTC")


# Iceberg extensions and the runtime jar have to be present on the very first SparkSession
# created in the process: both are static, classpath-level settings that cannot be added to an
# already-running session. Registered here, once, so any test module that needs a real Iceberg
# catalog (see test_silver_wap.py) can register its own named catalog at runtime against this
# already-Iceberg-capable session, instead of racing to be the first to build one.
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("streamlake-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config("spark.jars.packages", ICEBERG_RUNTIME_PACKAGE)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
