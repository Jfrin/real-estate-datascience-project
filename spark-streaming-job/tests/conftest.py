"""Shared fixtures for the streaming-job tests.

streaming_job.py reads required environment variables and builds its
per-source config at import time, so we set harmless defaults *before*
importing it. No Kafka, Cassandra or network access is needed: the tests only
exercise the pure DataFrame ``clean`` transformations on a local Spark session.
"""
import os
import sys
import time
from pathlib import Path

import pytest

os.environ.setdefault("KAFKA_TOPIC", "test.topic")
os.environ.setdefault("CASSANDRA_TABLE", "test_table")
os.environ.setdefault("SOURCE_TYPE", "acris")
# Don't let a Dockerfile-style --packages arg trigger a Maven download in tests.
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
# Workers must use the same interpreter as the driver (avoids version mismatch).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Timestamps are collected back into Python in the local timezone; pin it so the
# assertions pass on any machine.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("streaming-job-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def job():
    import streaming_job

    return streaming_job


@pytest.fixture
def make_raw(spark, job):
    """Build a raw all-string DataFrame for a source from a list of dicts.

    Fields not supplied become NULL, mirroring what from_json produces when a
    Socrata record omits a column.
    """

    def _make(source, records):
        fields = job.CONFIGS[source]["raw_fields"]
        rows = [tuple(rec.get(f) for f in fields) for rec in records]
        schema = ", ".join(f"{f} string" for f in fields)
        return spark.createDataFrame(rows, schema)

    return _make
