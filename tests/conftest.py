"""Shared test fixtures.

The Spark session here is real, not mocked: the contract engine's behaviour lives in the
aggregate expressions it compiles, and a mock would assert that the Python around them runs.
"""

from __future__ import annotations

import os
import sys

import pytest

# Must happen before any SparkSession is created: Spark launches Python workers with whatever
# `python3` is on PATH, which on this machine is a different minor version than the venv.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("TZ", "UTC")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("streamlake-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
