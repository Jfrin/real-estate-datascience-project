"""
DOB Permits Streaming Job
-------------------------
Spark Structured Streaming job that:
  1. Reads raw DOB permit JSON messages from the Kafka topic `permits.dob`
  2. Does light cleaning: selects relevant fields, parses date strings,
     casts lat/long to numeric, drops rows missing a primary key
  3. Writes each micro-batch into a Cassandra table (upsert by permit_si_no)

This runs Spark in LOCAL mode inside a single container rather than a
separate master/worker cluster. For a single-machine dev pipeline this is
functionally equivalent for proving the Kafka -> Spark -> Cassandra path,
and avoids an entire class of cluster-networking/version-matching issues.
"""

import logging
import os
import time

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider  # noqa: F401 (not used, no auth by default)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("dob_permits_streaming_job")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "permits.dob")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
CASSANDRA_TABLE = os.environ.get("CASSANDRA_TABLE", "permits_current")

# Raw JSON coming off Kafka is all-string fields (Socrata's format), so the
# read schema is intentionally all StringType. Casting to real types happens
# after parsing.
RAW_SCHEMA = StructType(
    [
        StructField("permit_si_no", StringType(), True),
        StructField("job__", StringType(), True),
        StructField("bin__", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("house__", StringType(), True),
        StructField("street_name", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("permit_type", StringType(), True),
        StructField("permit_status", StringType(), True),
        StructField("filing_status", StringType(), True),
        StructField("filing_date", StringType(), True),
        StructField("job_start_date", StringType(), True),
        StructField("owner_s_business_name", StringType(), True),
        StructField("gis_latitude", StringType(), True),
        StructField("gis_longitude", StringType(), True),
        StructField("gis_nta_name", StringType(), True),
        StructField("dobrundate", StringType(), True),
    ]
)


def wait_for_cassandra(max_attempts: int = 20, delay_seconds: int = 5) -> Cluster:
    """Connect to Cassandra, retrying since it may still be booting."""
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            session.shutdown()
            log.info("Cassandra is reachable at %s:%s", CASSANDRA_HOST, CASSANDRA_PORT)
            return cluster
        except Exception as e:  # noqa: BLE001
            last_err = e
            log.warning(
                "Cassandra not ready yet (attempt %d/%d): %s. Retrying in %ds...",
                attempt,
                max_attempts,
                e,
                delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not connect to Cassandra after retries: {last_err}")


def ensure_keyspace_and_table():
    """Create the keyspace/table if they don't exist yet. Idempotent."""
    cluster = wait_for_cassandra()
    session = cluster.connect()

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(CASSANDRA_KEYSPACE)

    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
            permit_si_no text PRIMARY KEY,
            job_number text,
            bin text,
            borough text,
            house_number text,
            street_name text,
            zip_code text,
            permit_type text,
            permit_status text,
            filing_status text,
            filing_date date,
            job_start_date date,
            owner_business_name text,
            latitude double,
            longitude double,
            neighborhood text,
            dob_run_date timestamp,
            ingested_at timestamp
        )
        """
    )
    log.info(
        "Keyspace '%s' and table '%s' are ready.", CASSANDRA_KEYSPACE, CASSANDRA_TABLE
    )
    cluster.shutdown()


def clean_batch(raw_df):
    """Apply light cleaning/casting to a raw micro-batch DataFrame."""
    return (
        raw_df.filter(col("permit_si_no").isNotNull())
        .select(
            col("permit_si_no"),
            col("job__").alias("job_number"),
            col("bin__").alias("bin"),
            col("borough"),
            col("house__").alias("house_number"),
            col("street_name"),
            col("zip_code"),
            col("permit_type"),
            col("permit_status"),
            col("filing_status"),
            to_date(col("filing_date"), "MM/dd/yyyy").alias("filing_date"),
            to_date(col("job_start_date"), "MM/dd/yyyy").alias("job_start_date"),
            col("owner_s_business_name").alias("owner_business_name"),
            col("gis_latitude").cast("double").alias("latitude"),
            col("gis_longitude").cast("double").alias("longitude"),
            col("gis_nta_name").alias("neighborhood"),
            to_timestamp(col("dobrundate")).alias("dob_run_date"),
            current_timestamp().alias("ingested_at"),
        )
    )


def write_batch_to_cassandra(batch_df, batch_id):
    """foreachBatch sink: write one micro-batch into Cassandra via the
    Python driver directly. This avoids needing the Scala
    spark-cassandra-connector jar and its Spark/Scala version matching."""
    cleaned = clean_batch(batch_df)
    rows = cleaned.collect()  # fine at this small dev-scale volume

    if not rows:
        log.info("Batch %d: no rows to write.", batch_id)
        return

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    insert_stmt = session.prepare(
        f"""
        INSERT INTO {CASSANDRA_TABLE} (
            permit_si_no, job_number, bin, borough, house_number, street_name,
            zip_code, permit_type, permit_status, filing_status, filing_date,
            job_start_date, owner_business_name, latitude, longitude,
            neighborhood, dob_run_date, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    written = 0
    for row in rows:
        try:
            session.execute(
                insert_stmt,
                (
                    row.permit_si_no,
                    row.job_number,
                    row.bin,
                    row.borough,
                    row.house_number,
                    row.street_name,
                    row.zip_code,
                    row.permit_type,
                    row.permit_status,
                    row.filing_status,
                    row.filing_date,
                    row.job_start_date,
                    row.owner_business_name,
                    row.latitude,
                    row.longitude,
                    row.neighborhood,
                    row.dob_run_date,
                    row.ingested_at,
                ),
            )
            written += 1
        except Exception as e:  # noqa: BLE001
            log.error("Failed to write row (permit_si_no=%s): %s", row.permit_si_no, e)

    log.info("Batch %d: wrote %d/%d rows to Cassandra.", batch_id, written, len(rows))
    session.shutdown()
    cluster.shutdown()


def main():
    ensure_keyspace_and_table()

    spark = (
        SparkSession.builder.appName("dob_permits_streaming_job")
        .config("spark.sql.shuffle.partitions", "2")  # small dev-scale workload
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "Starting stream: Kafka topic '%s' @ %s -> Cassandra '%s.%s'",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        CASSANDRA_KEYSPACE,
        CASSANDRA_TABLE,
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw_stream.selectExpr("CAST(value AS STRING) as json_str").select(
        "json_str"
    )

    from pyspark.sql.functions import from_json

    parsed = parsed.select(from_json(col("json_str"), RAW_SCHEMA).alias("data")).select(
        "data.*"
    )

    query = (
        parsed.writeStream.foreachBatch(write_batch_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark-checkpoints/permits_dob")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
