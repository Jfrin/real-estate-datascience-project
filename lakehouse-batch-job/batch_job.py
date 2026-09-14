import io, logging, os
import boto3
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.util import Date as CassandraDate

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("lakehouse_batch")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")
RUN_DATE = os.environ.get("RUN_DATE") or pd.Timestamp.utcnow().strftime("%Y-%m-%d")

TABLES = ["permits_current", "sales_acris", "parcels_pluto"]

s3 = boto3.client(
    "s3", endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
)


def ensure_bucket():
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        log.info("Created bucket '%s'", BUCKET)


def write_parquet(df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    log.info("Wrote %d rows -> s3://%s/%s", len(df), BUCKET, key)


def main():
    ensure_bucket()
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    silver_frames = {}
    for table in TABLES:
        rows = list(session.execute(f"SELECT * FROM {table}"))
        if not rows:
            log.info("%s: no rows, skipping.", table)
            continue
        records = [dict(r._asdict()) for r in rows]
        for rec in records:
            for k, v in rec.items():
                if isinstance(v, CassandraDate):
                    rec[k] = v.date()
        df = pd.DataFrame(records)

        # bronze: raw 1:1 landing
        write_parquet(df, f"bronze/{table}/dt={RUN_DATE}/data.parquet")
        # silver: same data here (streaming job already cleaned it on the way in)
        write_parquet(df, f"silver/{table}/dt={RUN_DATE}/data.parquet")
        silver_frames[table] = df

    # gold: one simple example rollup - permit counts by borough
    if "permits_current" in silver_frames:
        gold = (
            silver_frames["permits_current"]
            .groupby("borough").size().reset_index(name="permit_count")
        )
        write_parquet(gold, f"gold/permits_by_borough/dt={RUN_DATE}/data.parquet")

    session.shutdown()
    cluster.shutdown()
    log.info("Batch run complete for %s.", RUN_DATE)


if __name__ == "__main__":
    main()
