FROM python:3.11-slim

# Spark needs a JVM. openjdk-17-jre-headless is the lightest option that works.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming_job.py .

# Tells PySpark to fetch the Kafka connector (and its transitive deps) from
# Maven Central at startup. Requires the container to have internet access
# the first time it runs; Ivy caches the jars afterward inside the image
# layer's filesystem for the life of the container (not persisted across
# rebuilds unless you add a volume for ~/.ivy2 — fine for dev use).
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"

CMD ["python", "streaming_job.py"]
