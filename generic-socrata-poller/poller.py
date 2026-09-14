import json, logging, os, time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("socrata_poller")

SOCRATA_URL = os.environ["SOCRATA_URL"]
ORDER_FIELD = os.environ.get("ORDER_FIELD", "")
KEY_FIELD = os.environ.get("KEY_FIELD", "")
FETCH_LIMIT = int(os.environ.get("FETCH_LIMIT", "50"))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "0"))


def fetch():
    params = {"$limit": FETCH_LIMIT}
    if ORDER_FIELD:
        params["$order"] = f"{ORDER_FIELD} DESC"
    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}
    r = requests.get(SOCRATA_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def build_producer():
    for attempt in range(1, 11):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return p
        except KafkaError as e:
            log.warning("Kafka not ready (attempt %d/10): %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka")


def run_once(producer):
    records = fetch()
    log.info("Fetched %d records from %s", len(records), SOCRATA_URL)
    sent = 0
    for r in records:
        key = r.get(KEY_FIELD) if KEY_FIELD else None
        try:
            producer.send(KAFKA_TOPIC, key=key, value=r)
            sent += 1
        except KafkaError as e:
            log.error("Send failed: %s", e)
    producer.flush()
    log.info("Published %d/%d to '%s'", sent, len(records), KAFKA_TOPIC)


def main():
    producer = build_producer()
    if POLL_INTERVAL_SECONDS <= 0:
        run_once(producer)
        producer.close()
        return
    while True:
        try:
            run_once(producer)
        except Exception as e:
            log.exception("Poll cycle failed: %s", e)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
