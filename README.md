# Step 3 — Spark Streaming Job (Kafka → Spark → Cassandra)

Adds a Spark Structured Streaming job that:
1. Reads raw DOB permit JSON messages from the `permits.dob` Kafka topic
2. Cleans them (parses dates, casts lat/long to numbers, drops rows
   missing a primary key)
3. Writes the cleaned rows into a Cassandra table, `nyc_realestate.permits_current`

**Design note:** this runs Spark in local mode inside one container,
not a separate master + worker cluster. Functionally it proves the same
thing (Kafka → Spark → Cassandra works), with far less to go wrong on a
single dev machine. You can move to a real multi-node cluster later if
you ever need to scale beyond one machine.

## 1. Where these files go

You should now have, in your **project root** (`D:\Projects\real estate
datascience project`):

```
docker-compose.yml          <-- OVERWRITE with the one in this zip
Dockerfile                  <-- unchanged, keep as-is
requirements.txt            <-- unchanged, keep as-is
dob_permits_poller.py       <-- unchanged, keep as-is
spark-streaming-job/        <-- NEW folder, extract this in
    Dockerfile
    requirements.txt
    streaming_job.py
```

**Extract this zip directly into your project root** — say yes to
overwriting `docker-compose.yml` (it's a superset of what you had: same
Kafka/Cassandra/MinIO/poller config, plus the new `spark-streaming-job`
service). Nothing else needs to change.

## 2. Bring it up

```powershell
docker compose up -d --build
```

This builds the new Spark job image (first build will take a few minutes
— it's installing a JVM plus PySpark) and starts it. It will wait for
Kafka and Cassandra to both be healthy before starting, thanks to
`depends_on: condition: service_healthy`.

The **first time it starts**, it also needs to download the Kafka
connector jar from Maven Central — expect an extra 30-60 seconds on
first boot while that happens. Watch it happen:

```powershell
docker compose logs -f spark-streaming-job
```

## 3. What success looks like in the logs

You should see, in order:
- Java/Spark startup noise (safe to ignore)
- Ivy resolving and downloading `spark-sql-kafka-0-10_2.12` and its
  dependencies
- `Cassandra is reachable at cassandra:9042`
- `Keyspace 'nyc_realestate' and table 'permits_current' are ready.`
- `Starting stream: Kafka topic 'permits.dob' @ kafka:29092 -> Cassandra 'nyc_realestate.permits_current'`
- Every ~30 seconds: `Batch N: wrote X/X rows to Cassandra.`

The job reads with `startingOffsets: earliest`, so on first run it will
pick up and process the 50 records your Step 2 poller already published.

## 4. Verify the data actually landed in Cassandra

```powershell
docker exec -it realestate-cassandra-1 cqlsh
```

*(use whatever your actual Cassandra container name is — check with `docker ps`)*

```sql
USE nyc_realestate;

SELECT permit_si_no, borough, street_name, permit_type, filing_date, latitude, longitude
FROM permits_current
LIMIT 10;

SELECT COUNT(*) FROM permits_current;

EXIT;
```

You should see real, cleaned rows — proper `date` type for `filing_date`
(not a string), numeric `latitude`/`longitude`, and readable column names.
That's the proof this step is working: raw JSON from Kafka became a
structured, queryable Cassandra table.

## 5. Feed it more data

The Step 2 poller runs once and exits by default. Re-run it to publish a
fresh batch, and the streaming job (which keeps running continuously)
will pick up the new messages within its 30-second trigger interval:

```powershell
docker compose up -d --build dob-permits-poller
```

Then re-check the Cassandra row count — it should grow.

## Troubleshooting

**"Could not connect to Cassandra after retries"**
Check `docker ps` — Cassandra needs to show `(healthy)`, not
`(health: starting)`. If Cassandra is slow to boot, the job will keep
retrying every 5 seconds for up to 20 attempts (~100 seconds) before
giving up. If it still fails after that, restart just this service:
```powershell
docker compose restart spark-streaming-job
```

**Ivy/Maven download fails or times out on first start**
This needs outbound internet access from the container to Maven Central.
If your network blocks this, try again — sometimes it's transient. If it
persists, check that your Docker Desktop network/proxy settings allow
outbound HTTPS.

**No rows showing up in Cassandra after a few minutes**
Check the job logs for errors:
```powershell
docker compose logs spark-streaming-job
```
Common cause: the Kafka topic is genuinely empty because the poller
hasn't been run yet, or ran but failed. Re-check Step 2's verification
steps first.

**Job container keeps restarting**
```powershell
docker compose logs spark-streaming-job
```
Since this service has `restart: unless-stopped`, a crash loop will keep
retrying automatically — check the logs to see the actual Python/Spark
error rather than just watching it restart.

## What's next

Once you've confirmed real, cleaned permit data is queryable in
Cassandra, you're ready for **Step 4: add the remaining source pollers**
— repeating the Step 2 pattern for ACRIS (sales), PLUTO (parcels),
Census, and mortgage rate data, each with its own Kafka topic and (later)
its own Spark streaming job or an extension of this one.
