# Step 1 — Core Infrastructure

Kafka (KRaft mode, no Zookeeper) + Cassandra + MinIO.
This is plumbing only — no pipeline logic yet. The goal is to confirm all
three services start cleanly and are reachable before building anything on
top of them.

## Requirements

- Docker Desktop (or Docker Engine + Docker Compose v2)
- ~4 GB of free RAM for these three containers
- Ports free on your host: `9092`, `9093`, `9042`, `9000`, `9001`

## 1. Start everything

```bash
docker compose up -d
```

First startup will take a minute or two while images download and
Cassandra initializes. Check status with:

```bash
docker compose ps
```

All three should show `healthy` within about a minute (Cassandra takes the
longest — its healthcheck has a 60s start period).

## 2. Verify Kafka

Check your Kafka container's actual name first (it may not be exactly `kafka` depending on your Compose project name):

```powershell
docker ps
```

Create a test topic (replace `realestate-kafka-1` with what you saw above, e.g. `realestate-kafka-1`):

```powershell
docker exec -it realestate-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic test-topic --partitions 1 --replication-factor 1
```

List topics to confirm it exists:

```powershell
docker exec -it realestate-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

You should see `test-topic` in the output.

Optional — send and read a message to confirm the full loop:

```powershell
# In one terminal, start a producer and type a message, then Ctrl+C
docker exec -it realestate-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic

# In another terminal, start a consumer to read it back
docker exec -it realestate-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

## 3. Verify Cassandra

Open a CQL shell:

```bash
docker exec -it cassandra cqlsh
```

Inside the shell, confirm it responds and create a test keyspace:

```sql
DESCRIBE KEYSPACES;

CREATE KEYSPACE IF NOT EXISTS test_ks
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_ks;

CREATE TABLE ping (id int PRIMARY KEY, note text);
INSERT INTO ping (id, note) VALUES (1, 'cassandra is alive');
SELECT * FROM ping;

EXIT;
```

You should see the row come back from `SELECT`.

## 4. Verify MinIO

Open the web console in your browser:

```
http://localhost:9001
```

Log in with:
- **Username:** `minioadmin`
- **Password:** `minioadmin123`

Once logged in, create a bucket (e.g. `raw-data`) through the UI to confirm
writes work. You can also check the API health endpoint directly:

```bash
curl http://localhost:9000/minio/health/live
```

A `200 OK` with no error means the S3 API is up.

## 5. Tear down (when done testing)

```bash
# Stop containers, keep data
docker compose down

# Stop containers AND wipe all data volumes (clean slate)
docker compose down -v
```

## Running multiple copies of this stack

This compose file has no hardcoded `container_name` values, and a `.env`
file sets `COMPOSE_PROJECT_NAME=realestate`. That means:

- Containers, networks, and volumes are namespaced under `realestate`
  instead of colliding with anything else on your machine.
- If you previously hit a `container name "/kafka" already in use` error,
  that won't happen anymore — names are now auto-generated per project.

**Important caveat:** the project name only isolates *names*. The **ports**
(`9092`, `9093`, `9042`, `9000`, `9001`) are still fixed in
`docker-compose.yml`. That means:

- You can safely run this stack, tear it down, and bring it back up
  repeatedly with no conflicts (this solves the original error).
- You can also copy this folder and run a second, differently-named
  project (change `COMPOSE_PROJECT_NAME` in the copy's `.env`) as long as
  it's not running **at the same time** as the first — they'd fight over
  the same host ports.
- To run two copies **simultaneously**, edit the port mappings in the
  second copy's `docker-compose.yml` (left side of the `:` only, e.g.
  `9092` → `9192`) so each stack uses a different set of host ports.

For most workflows (one dev environment, stopped and restarted as needed)
you won't need to touch ports at all — the `.env` project name alone is
enough to avoid the conflict you ran into.

## Kafka image history (why it changed twice)

If you're wondering why this differs from earlier instructions:

1. **`apache/kafka`** — hit a known file-locking bug on some Docker Desktop
   for Windows volume backends (`Error while writing meta.properties file`).
2. **`bitnami/kafka`** — Bitnami moved this image behind a paid "Bitnami
   Secure Images" subscription; the free tag no longer resolves.
3. **`confluentinc/cp-kafka`** (current) — Confluent's official image,
   configured per their documented KRaft (no-Zookeeper) setup. This is
   also the same image your machine already had cached from another
   project, so it should pull instantly.

If you have leftover volumes from the earlier attempts, wipe them first:

```powershell
docker compose down -v
docker volume rm realestate_kafka_data
docker compose up -d
```

## Notes

- Credentials here (`minioadmin` / `minioadmin123`, no auth on Kafka/Cassandra)
  are fine for local development only — never use these defaults if you
  expose any of these ports beyond your own machine.
- The Kafka `CLUSTER_ID` is a fixed, arbitrary value so the same compose
  file behaves consistently across restarts. It doesn't need to be changed.
- Once all three checks above pass, you're ready to move to Step 2: writing
  a source poller that publishes real records into a Kafka topic.
