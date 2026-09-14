"""
DOB Permits Poller
------------------
Pulls DOB (Department of Buildings) permit records from NYC Open Data's
Socrata API and publishes each record as a JSON message to a Kafka topic.

This is intentionally simple: one source, one topic, no batching logic,
no dedup, no schema enforcement. The point of Step 2 is proving the loop
works end-to-end (API -> Kafka), not building a production connector.

Dataset: DOB Permit Issuance
Socrata endpoint: https://data.cityofnewyork.us/resource/ipu4-2q9a.json

NOTE: NYC Open Data occasionally changes dataset IDs when datasets are
republished. If the URL below returns a 404 or empty results, search
"DOB Permit Issuance" at https://data.cityofnewyork.us and grab the
current API endpoint from the dataset's "API" tab, then update
SOCRATA_URL below.
"""

import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("dob_permits_poller")

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment variables so this can run
# the same way locally or inside Docker)
# ---------------------------------------------------------------------------
SOCRATA_URL = os.environ.get(
    "SOCRATA_URL", "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
)
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")  # optional, raises rate limit
FETCH_LIMIT = int(os.environ.get("FETCH_LIMIT", "50"))  # how many records per poll
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "permits.dob")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "0"))
# POLL_INTERVAL_SECONDS = 0 means "run once and exit" (good for testing).
# Set it to e.g. 900 (15 min) to run continuously.


def fetch_permits(limit: int = FETCH_LIMIT):
    """Fetch the most recent DOB permit records from the Socrata API."""
    params = {
        "$limit": limit,
        "$order": "issuance_date DESC",
    }
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    log.info("Fetching up to %d records from %s", limit, SOCRATA_URL)
    response = requests.get(SOCRATA_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    records = response.json()
    log.info("Fetched %d records", len(records))
    return records


def build_producer() -> KafkaProducer:
    """Create a Kafka producer, retrying briefly in case Kafka isn't ready yet."""
    last_err = None
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaError as e:
            last_err = e
            log.warning(
                "Kafka not ready yet (attempt %d/10): %s. Retrying in 5s...",
                attempt,
                e,
            )
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka after retries: {last_err}")


def publish_records(producer: KafkaProducer, records: list):
    """Publish each permit record to Kafka, keyed by building ID (bin) when present."""
    sent = 0
    for record in records:
        key = record.get("bin__") or record.get("bin") or None
        try:
            producer.send(KAFKA_TOPIC, key=key, value=record)
            sent += 1
        except KafkaError as e:
            log.error("Failed to send record (bin=%s): %s", key, e)
    producer.flush()
    log.info("Published %d/%d records to topic '%s'", sent, len(records), KAFKA_TOPIC)


def run_once(producer: KafkaProducer):
    records = fetch_permits()
    if not records:
        log.warning("No records returned from Socrata — nothing to publish this cycle.")
        return
    publish_records(producer, records)


def main():
    producer = build_producer()

    if POLL_INTERVAL_SECONDS <= 0:
        log.info("POLL_INTERVAL_SECONDS not set — running once and exiting.")
        run_once(producer)
        producer.close()
        return

    log.info("Running continuously, polling every %d seconds.", POLL_INTERVAL_SECONDS)
    while True:
        try:
            run_once(producer)
        except requests.RequestException as e:
            log.error("HTTP error fetching from Socrata: %s", e)
        except Exception as e:  # noqa: BLE001 - keep the poller alive on unexpected errors
            log.exception("Unexpected error during poll cycle: %s", e)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
