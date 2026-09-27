#Showcase Time Baby
**Kafka**
- Topic list + message counts:
```powershell
docker exec -it realestate-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```
```
docker exec -it realestate-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic sales.acris
```
```
docker exec -it realestate-kafka-exporter-1 wget -qO- http://localhost:9308/metrics | findstr lag
```

**Spark**

`http://localhost:4040`

**Airflow**

`http://localhost:8080`


**Cassandra**
```powershell
docker exec -it realestate-cassandra-1 cqlsh
```
```sql
DESCRIBE KEYSPACE realestate;
DESCRIBE TABLE realestate.listings_current;
SELECT * FROM realestate.listings_current LIMIT 5;
SELECT * FROM realestate.price_history LIMIT 5;
```

**MinIO**

`http://localhost:9001`

