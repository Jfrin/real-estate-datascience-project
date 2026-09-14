import logging, os, time
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("streaming_job")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
CASSANDRA_TABLE = os.environ["CASSANDRA_TABLE"]
SOURCE_TYPE = os.environ["SOURCE_TYPE"]  # "dob" | "acris" | "pluto"

# ---------------------------------------------------------------------------
# Per-source configuration: raw field list, cleaning logic, table DDL
# ---------------------------------------------------------------------------
CONFIGS = {
    "dob": {
        "raw_fields": [
            "permit_si_no", "job__", "bin__", "borough", "house__", "street_name",
            "zip_code", "permit_type", "permit_status", "filing_status",
            "filing_date", "job_start_date", "owner_s_business_name",
            "gis_latitude", "gis_longitude", "gis_nta_name", "dobrundate",
        ],
        "key": "permit_si_no",
        "ddl": """
            permit_si_no text PRIMARY KEY, job_number text, bin text, borough text,
            house_number text, street_name text, zip_code text, permit_type text,
            permit_status text, filing_status text, filing_date date,
            job_start_date date, owner_business_name text, latitude double,
            longitude double, neighborhood text, dob_run_date timestamp,
            ingested_at timestamp
        """,
        "clean": lambda df: df.filter(col("permit_si_no").isNotNull()).select(
            col("permit_si_no"), col("job__").alias("job_number"), col("bin__").alias("bin"),
            col("borough"), col("house__").alias("house_number"), col("street_name"),
            col("zip_code"), col("permit_type"), col("permit_status"), col("filing_status"),
            to_date(col("filing_date"), "MM/dd/yyyy").alias("filing_date"),
            to_date(col("job_start_date"), "MM/dd/yyyy").alias("job_start_date"),
            col("owner_s_business_name").alias("owner_business_name"),
            col("gis_latitude").cast("double").alias("latitude"),
            col("gis_longitude").cast("double").alias("longitude"),
            col("gis_nta_name").alias("neighborhood"),
            to_timestamp(col("dobrundate")).alias("dob_run_date"),
            current_timestamp().alias("ingested_at"),
        ),
    },
    "acris": {
        "raw_fields": [
            "document_id", "recorded_borough", "doc_type", "document_date",
            "document_amt", "recorded_datetime", "good_through_date",
        ],
        "key": "document_id",
        "ddl": """
            document_id text PRIMARY KEY, borough text, doc_type text,
            document_date date, document_amt double, recorded_datetime timestamp,
            ingested_at timestamp
        """,
        "clean": lambda df: df.filter(col("document_id").isNotNull()).select(
            col("document_id"), col("recorded_borough").alias("borough"), col("doc_type"),
            to_date(col("document_date")).alias("document_date"),
            col("document_amt").cast("double").alias("document_amt"),
            to_timestamp(col("recorded_datetime")).alias("recorded_datetime"),
            current_timestamp().alias("ingested_at"),
        ),
    },
    "pluto": {
        "raw_fields": [
            "bbl", "borough", "block", "lot", "address", "zipcode", "bldgclass",
            "landuse", "ownername", "lotarea", "bldgarea", "numfloors", "yearbuilt",
            "assessland", "assesstot", "latitude", "longitude",
        ],
        "key": "bbl",
        "ddl": """
            bbl text PRIMARY KEY, borough text, block text, lot text, address text,
            zip_code text, bldg_class text, land_use text, owner_name text,
            lot_area double, bldg_area double, num_floors double, year_built int,
            assess_land double, assess_total double, latitude double, longitude double,
            ingested_at timestamp
        """,
        "clean": lambda df: df.filter(col("bbl").isNotNull()).select(
            col("bbl"), col("borough"), col("block"), col("lot"), col("address"),
            col("zipcode").alias("zip_code"), col("bldgclass").alias("bldg_class"),
            col("landuse").alias("land_use"), col("ownername").alias("owner_name"),
            col("lotarea").cast("double").alias("lot_area"),
            col("bldgarea").cast("double").alias("bldg_area"),
            col("numfloors").cast("double").alias("num_floors"),
            col("yearbuilt").cast("int").alias("year_built"),
            col("assessland").cast("double").alias("assess_land"),
            col("assesstot").cast("double").alias("assess_total"),
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
            current_timestamp().alias("ingested_at"),
        ),
    },
}

CFG = CONFIGS[SOURCE_TYPE]
RAW_SCHEMA = StructType([StructField(f, StringType(), True) for f in CFG["raw_fields"]])


def wait_for_cassandra(max_attempts=20, delay=5):
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            s = cluster.connect()
            s.shutdown()
            return cluster
        except Exception as e:
            last_err = e
            log.warning("Cassandra not ready (attempt %d/%d): %s", attempt, max_attempts, e)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Cassandra: {last_err}")


def ensure_keyspace_and_table():
    cluster = wait_for_cassandra()
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} "
        f"WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(CASSANDRA_KEYSPACE)
    session.execute(f"CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} ({CFG['ddl']})")
    log.info("Keyspace/table ready: %s.%s", CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
    cluster.shutdown()


def write_batch(batch_df, batch_id):
    cleaned = CFG["clean"](batch_df)
    rows = cleaned.collect()
    if not rows:
        log.info("Batch %d: no rows.", batch_id)
        return

    cols = cleaned.columns
    placeholders = ", ".join(["?"] * len(cols))
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    stmt = session.prepare(
        f"INSERT INTO {CASSANDRA_TABLE} ({', '.join(cols)}) VALUES ({placeholders})"
    )

    written = 0
    for row in rows:
        try:
            session.execute(stmt, tuple(row[c] for c in cols))
            written += 1
        except Exception as e:
            log.error("Write failed for key=%s: %s", row[CFG["key"]], e)

    log.info("Batch %d: wrote %d/%d rows to %s.", batch_id, written, len(rows), CASSANDRA_TABLE)
    session.shutdown()
    cluster.shutdown()


def main():
    ensure_keyspace_and_table()
    spark = (
        SparkSession.builder.appName(f"{SOURCE_TYPE}_streaming_job")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Starting stream: %s -> %s.%s", KAFKA_TOPIC, CASSANDRA_KEYSPACE, CASSANDRA_TABLE)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), RAW_SCHEMA).alias("data")).select("data.*")

    query = (
        parsed.writeStream.foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", f"/tmp/spark-checkpoints/{SOURCE_TYPE}")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
