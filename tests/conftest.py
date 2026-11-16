import os
import sys
from datetime import datetime

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

RUN_TS = datetime(2026, 1, 15, 12, 0, 0)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("dq-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def demo_tables(spark):
    from dq.demo import create_demo_tables

    return create_demo_tables(spark)


def make_suite(rules, **extra):
    """Build a suite dict with sensible defaults for tests."""
    from dq.config import load_suite

    return load_suite({"suite": "test_suite", "rules": rules, **extra})
