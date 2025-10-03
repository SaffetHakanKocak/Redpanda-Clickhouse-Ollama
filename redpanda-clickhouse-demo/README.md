# Redpanda + ClickHouse Demo (with Tabix UI)

End-to-end streaming demo for your scenario:
- Synthetic metrics/logs -> **Redpanda** (Kafka API)
- **ClickHouse** consumes from Kafka engine and stores in MergeTree
- A minimal web UI: **Tabix** (SQL console & table view)
- **Redpanda Console** to visualize topic messages

## 0) Prereqs
- Docker & Docker Compose
- Python 3.9+ (for the local producer)

## 1) Start the stack
```bash
cd redpanda-clickhouse-demo
docker compose up -d
```

Services:
- Redpanda Console: http://localhost:8080
- Tabix (ClickHouse UI): http://localhost:8081
- ClickHouse HTTP: http://localhost:8123
- Redpanda (Kafka): localhost:19092 (external), redpanda:9092 (internal)

## 2) Create topic (optional)
Redpanda Console provides a UI to create the topic `metrics`. Or use rpk:
```bash
docker exec -it $(docker ps -qf name=redpanda) rpk topic create metrics -p 3 -r 1 --brokers redpanda:9092
```
> If it already exists, you'll see a message and can ignore it.

## 3) Initialize ClickHouse objects
The SQL is auto-loaded via `clickhouse/init.sql`. If needed, you can re-run it:
```bash
docker exec -it $(docker ps -qf name=clickhouse) bash -lc "clickhouse-client --multiquery < /docker-entrypoint-initdb.d/init.sql"
```

Key objects:
- `raw_events_kafka` (ENGINE=Kafka) — streaming reader from Redpanda
- `events` (ENGINE=MergeTree) — durable storage
- `mv_consume_metrics` — materialized view to pump Kafka -> events
- `agg_minute` and `mv_agg_minute` — simple rollup for charts

## 4) Start the Python producer
Install deps:
```bash
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install kafka-python
python producer/producer.py --brokers localhost:19092 --topic metrics --rate 10
```
This generates ~10 JSONEachRow messages/sec like:
```json
{"timestamp":"2025-01-01T12:00:00.123456+00:00","service":"orders","host":"srv-1a2b3c","cpu_usage":17.5,"mem_usage":63.1,"level":"INFO","message":"orders processed request"}
```

## 5) See data in the UI
- Open **Redpanda Console** (http://localhost:8080) → **Topics** → `metrics` to see live messages.
- Open **Tabix** (http://localhost:8081)
  - Run: `SELECT * FROM events ORDER BY timestamp DESC LIMIT 50;`
  - Aggregates:
    ```sql
    SELECT ts_min, service, avg_cpu, p95_cpu, avg_mem, p95_mem, err_count
    FROM agg_minute
    ORDER BY ts_min DESC, service
    LIMIT 50;
    ```
  - Quick analysis:
    ```sql
    -- Top 5 busiest services by 95th percentile CPU in last 15 min
    SELECT service, max(p95_cpu) AS p95_max
    FROM agg_minute
    WHERE ts_min > now() - INTERVAL 15 MINUTE
    GROUP BY service
    ORDER BY p95_max DESC
    LIMIT 5;
    ```

## 6) (Optional) Simple alert-ish query in ClickHouse
```sql
SELECT *
FROM events
WHERE timestamp > now() - INTERVAL 5 MINUTE
  AND (cpu_usage > 85 OR mem_usage > 85)
ORDER BY timestamp DESC
LIMIT 100;
```

## 7) Stop
```bash
docker compose down -v
```

## Notes
- This setup maps closely to your project's real-time processing and anomaly-detection goals; you can extend it by wiring your detector to publish enriched events to another topic and ingest into ClickHouse for dashboards/forensics.