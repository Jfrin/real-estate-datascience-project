# NYC Real Estate Data Platform

An end-to-end, Docker-based data engineering project that ingests New York City real-estate open data, streams it through Kafka and Spark into Cassandra, lands it in a Parquet lakehouse, and publishes heatmaps, a live dashboard, and a daily emailed PDF report. Airflow schedules the recurring jobs.

## What it does

Three public NYC Open Data (Socrata) datasets are pulled on a schedule:

| Source | Dataset | Kafka topic | Cassandra table | Key |
|---|---|---|---|---|
| **DOB Permits** | DOB Permit Issuance (`ipu4-2q9a`) | `permits.dob` | `permits_current` | `permit_si_no` |
| **ACRIS** | Property sales/transfer documents (`bnx9-e6tj`) | `sales.acris` | `sales_acris` | `document_id` |
| **PLUTO** | Parcel-level land-use data (`64uk-42ks`) | `parcels.pluto` | `parcels_pluto` | `bbl` |

From there the data is cleaned, stored, aggregated, and visualized: permit volume by borough and neighborhood, deed volume and median sale prices, assessed value per square foot, building age, top property owners, and an interactive permit geo-heatmap.

## Architecture

```
 NYC Open Data (Socrata API)
          │
          ▼
 ┌─────────────────┐     ┌─────────┐     ┌───────────────────┐     ┌───────────┐
 │ Pollers (x3)    │ ──▶ │  Kafka  │ ──▶ │ Spark Structured  │ ──▶ │ Cassandra │
 │ requests → JSON │     │ 3 topics│     │ Streaming (x3)    │     │ nyc_      │
 └─────────────────┘     └─────────┘     │ parse + clean     │     │ realestate│
                                          └───────────────────┘     └─────┬─────┘
                                                                          │
                        ┌─────────────────────────────┬───────────────────┤
                        ▼                             ▼                   ▼
              ┌──────────────────┐         ┌──────────────────┐  ┌─────────────────┐
              │ Lakehouse batch  │         │ Heatmap job      │  │ Report job      │
              │ bronze/silver/   │ ──────▶ │ charts + Leaflet │  │ PDF + email     │
              │ gold (Parquet)   │ (opt.)  │ map + index.html │  │ (SMTP)          │
              └──────────────────┘         └────────┬─────────┘  └─────────────────┘
                                                    ▼
                                           nginx dashboard :8088

              Airflow (LocalExecutor + Postgres) triggers the pollers, lakehouse
              batch, and heatmap job by running `docker start -a <container>`.
```

## Components

| Directory | Service(s) | Role |
|---|---|---|
| `dob_permits_poller.py`, `Dockerfile` | `dob-permits-poller` | Fetches DOB permits (newest first) and publishes JSON to Kafka, keyed by building ID (BIN). |
| `generic-socrata-poller/` | `acris-poller`, `pluto-poller` | Config-driven poller for any Socrata dataset (`SOCRATA_URL`, `KEY_FIELD`, `KAFKA_TOPIC`, ...). |
| `spark-streaming-job/` | `spark-streaming-job` (DOB), `acris-streaming-job`, `pluto-streaming-job` | One PySpark job, three behaviors selected by `SOURCE_TYPE` (`dob` / `acris` / `pluto`). Reads Kafka, applies the per-source schema and cleaning (type casts, date parsing, column renames, `ingested_at`), creates the keyspace/table if missing, and upserts into Cassandra every 30s. |
| `lakehouse-batch-job/` | `lakehouse-batch-job` | Snapshots each Cassandra table into date-partitioned Parquet files. |
| `heatmap-job/` | `heatmap-job` | Builds the PNG charts, the interactive permit heatmap (`permits_geo_heatmap.html`), and the dashboard `index.html` into `./heatmaps`. |
| `heatmaps/` | `dashboard` (nginx) | Generated output, served at http://localhost:8088. Auto-refreshes every 5 minutes. |
| `report-job/` | `report-job` | Queries Cassandra, builds charts and a PDF ("Daily Market Pulse"), and emails it with heatmap snapshots attached. |
| `airflow/` | `airflow-postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler` | Scheduling (see DAGs below). |
| `cadvisor`, `kafka-exporter` | | Container and Kafka metrics exporters (ports 8085 and the exporter's default). |

### Lakehouse layout

Stored in the `lakehouse_data` Docker volume (plain Parquet on a volume; MinIO/S3 was removed from the design):

```
/data/lakehouse/
├── bronze/<table>/dt=YYYY-MM-DD/data.parquet   # raw 1:1 snapshot of the Cassandra table
├── silver/<table>/dt=YYYY-MM-DD/data.parquet   # currently identical to bronze (cleaning happens in the streaming job)
└── gold/permits_by_borough/dt=YYYY-MM-DD/data.parquet   # permit counts per borough
```

`<table>` is one of `permits_current`, `sales_acris`, `parcels_pluto`. Set `RUN_DATE=YYYY-MM-DD` to write a specific partition.

### Airflow DAGs (`airflow/dags/`)

| DAG | Schedule | What it runs |
|---|---|---|
| `daily_ingest` | `@hourly` (despite the name) | Starts the three poller containers in turn. |
| `nightly_lakehouse` | `@daily` | Runs the lakehouse batch job. |
| `dashboard_hourly` | `@hourly` | Runs the heatmap job to refresh the dashboard. |

The DAGs call `docker start -a realestate-<service>-1`, so they rely on the Docker socket being mounted into Airflow (already done in `docker-compose.yml`) and on the Compose project name being `realestate` (set via `COMPOSE_PROJECT_NAME` in `.env`). The containers must already exist, so run `docker compose up` once first.

## Tech stack

Python 3.11 · Apache Kafka (KRaft, Confluent 7.6) · Apache Spark 3.5.1 Structured Streaming (PySpark) · Apache Cassandra 5.0 · Parquet / pandas / PyArrow · Apache Airflow 2.9.3 · matplotlib, seaborn, Leaflet (via CARTO basemap tiles) · ReportLab · nginx · Docker Compose

## Getting started

### Prerequisites

- Docker and Docker Compose v2
- Several GB of free RAM (Kafka, Cassandra, and three Spark jobs run together) and internet access (Socrata APIs, plus Maven Central on the first Spark start to download the Kafka connector)

### 1. Configure environment

Create a `.env` file in the project root (it is git-ignored):

```env
COMPOSE_PROJECT_NAME=realestate

# Airflow admin login
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=change-me

# Daily report email (Gmail SMTP; use an app password)
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your-app-password
REPORT_FROM_EMAIL=you@gmail.com
REPORT_TO_EMAIL=recipient@example.com

# Optional: CARTO basemap key (blank = free tiles without a key)
CARTO_API_KEY=
```

### 2. Start the platform

```bash
docker compose up -d --build
```

Kafka and Cassandra start first (both have health checks). The pollers then run once, publish their records, and exit; the Spark jobs consume from the earliest offset and write into Cassandra.

### 3. Open the UIs

| Service | URL |
|---|---|
| Dashboard (nginx) | http://localhost:8088 |
| Airflow | http://localhost:8080 (login from `.env`) |
| cAdvisor | http://localhost:8085 |
| Kafka (host listener) | `localhost:9092` |
| Cassandra (CQL) | `localhost:9042` |

Unpause the three DAGs in Airflow to start the schedule.

### 4. Run jobs manually

```bash
# Re-run ingestion
docker compose run --rm dob-permits-poller
docker compose run --rm acris-poller
docker compose run --rm pluto-poller

# Snapshot Cassandra to the lakehouse
docker compose run --rm lakehouse-batch-job
docker compose run --rm -e RUN_DATE=2026-09-28 lakehouse-batch-job

# Rebuild the dashboard / charts
docker compose run --rm heatmap-job

# Generate and email the daily PDF report
docker compose run --rm report-job
```

`report-job` is not on an Airflow schedule yet; run it manually or add a DAG.

### Handy inspection commands

```bash
# Kafka
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic permits.dob --from-beginning

# Cassandra
docker compose exec cassandra cqlsh
#   USE nyc_realestate;  DESCRIBE TABLES;  SELECT * FROM permits_current LIMIT 10;

# Streaming logs
docker compose logs -f acris-streaming-job

# Lakehouse contents
docker compose run --rm --entrypoint find lakehouse-batch-job /data/lakehouse -maxdepth 3
```

## Configuration reference

| Variable | Used by | Default | Notes |
|---|---|---|---|
| `SOCRATA_URL` | pollers | per dataset | Socrata JSON endpoint |
| `FETCH_LIMIT` | pollers | DOB 100,000 / ACRIS 1,000,000 / PLUTO 100,000 (compose) | Records per run |
| `POLL_INTERVAL_SECONDS` | pollers | `0` | `0` = run once and exit; `>0` = loop forever |
| `SOCRATA_APP_TOKEN` | pollers | empty | Optional; raises Socrata rate limits |
| `SOURCE_TYPE` | Spark job | required | `dob`, `acris`, or `pluto` |
| `RUN_DATE` | lakehouse job | today (UTC) | Partition date |
| `PERIOD` | heatmap job | `all` | `all`, `today`, `yesterday`, `week`, `month`, `30d`, or `YYYY-MM-DD` |
| `PREFER_CASSANDRA` | heatmap job | `true` | `false` reads permits from the lakehouse instead |
| `SALES_LIMIT` | report job | `300000` | Max ACRIS rows used in the report |

## Outputs

**Dashboard / heatmaps** (`heatmaps/`): permit trend, permit types, permits by borough × month, borough × type and neighborhood × type, top permit owners, an interactive geo heatmap, ACRIS deed volume, document types, median price and price distribution, PLUTO assessed value per sq ft, year-built distribution, and top parcel owners.

**Daily report**: a PDF titled "NYC Real-Estate Daily Market Pulse" with KPIs, tables, and charts, emailed via SMTP with heatmap snapshots.

## Known limitations
- Ingestion is a periodic snapshot (latest N records per run), not true incremental or CDC. Cassandra upserts by primary key keep it idempotent.
- The `silver` layer is a copy of `bronze`; the only `gold` table is `permits_by_borough`.
- ACRIS records only carry document-level fields (type, date, amount, borough). They are not joined to PLUTO parcels.
- The Spark job writes rows to Cassandra one at a time inside `foreachBatch`, which is fine at this scale but is the first thing to optimize.
- `dashboard/` is an earlier Plotly-based dashboard experiment. It targets a different keyspace and tables (`esra`, `neighborhood_daily_agg`, ...) that this pipeline does not create, and it is not wired into the current `docker-compose.yml`. The live dashboard is `heatmap-job` + nginx.
- Single-node Kafka and Cassandra with replication factor 1: development setup, not production.

## Security notes
- Postgres credentials for Airflow (`airflow/airflow`) are hard-coded in the compose file. Change them for anything beyond local use.

## Data sources and attribution

All data comes from [NYC Open Data](https://opendata.cityofnewyork.us): DOB Permit Issuance, ACRIS, and MapPLUTO/PLUTO. Map tiles © CARTO, © OpenStreetMap contributors.
