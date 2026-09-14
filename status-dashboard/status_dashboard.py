import os
from flask import Flask
from cassandra.cluster import Cluster
from kafka import KafkaConsumer

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

CASSANDRA_TABLES = ["permits_current", "sales_acris", "parcels_pluto"]
KAFKA_TOPICS = ["permits.dob", "sales.acris", "parcels.pluto"]

app = Flask(__name__)


def get_cassandra_counts():
    counts = {}
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)
        for t in CASSANDRA_TABLES:
            try:
                counts[t] = session.execute(f"SELECT COUNT(*) FROM {t}").one()[0]
            except Exception as e:
                counts[t] = f"error: {e}"
        session.shutdown()
        cluster.shutdown()
    except Exception as e:
        for t in CASSANDRA_TABLES:
            counts[t] = f"cassandra unreachable: {e}"
    return counts


def get_kafka_offsets():
    offsets = {}
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, consumer_timeout_ms=5000)
        for topic in KAFKA_TOPICS:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                offsets[topic] = "topic not found"
                continue
            from kafka import TopicPartition
            tps = [TopicPartition(topic, p) for p in partitions]
            end = consumer.end_offsets(tps)
            offsets[topic] = sum(end.values())
        consumer.close()
    except Exception as e:
        for topic in KAFKA_TOPICS:
            offsets[topic] = f"kafka unreachable: {e}"
    return offsets


@app.route("/")
def status():
    cass = get_cassandra_counts()
    kafka = get_kafka_offsets()

    rows_cass = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in cass.items())
    rows_kafka = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in kafka.items())

    return f"""
    <html><head><title>Pipeline Status</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body {{ font-family: sans-serif; margin: 40px; }}
        table {{ border-collapse: collapse; margin-bottom: 30px; }}
        td, th {{ border: 1px solid #ccc; padding: 8px 16px; text-align: left; }}
        h2 {{ margin-top: 0; }}
    </style></head>
    <body>
        <h1>NYC Real-Estate Pipeline Status</h1>
        <p>Auto-refreshes every 10 seconds.</p>

        <h2>Kafka topics (total messages ever published)</h2>
        <table><tr><th>Topic</th><th>Total Messages</th></tr>{rows_kafka}</table>

        <h2>Cassandra tables (current row counts)</h2>
        <table><tr><th>Table</th><th>Row Count</th></tr>{rows_cass}</table>
    </body></html>
    """


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
