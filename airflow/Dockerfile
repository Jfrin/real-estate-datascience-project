FROM apache/airflow:2.9.3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends docker.io && rm -rf /var/lib/apt/lists/*
USER airflow
