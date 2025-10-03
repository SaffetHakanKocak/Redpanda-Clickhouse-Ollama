-- Create target table (storage)
CREATE TABLE IF NOT EXISTS events (
  timestamp    DateTime64(3, 'UTC'),
  service      LowCardinality(String),
  host         String,
  cpu_usage    Float32,
  mem_usage    Float32,
  level        LowCardinality(String),
  message      String
) ENGINE = MergeTree
ORDER BY (service, timestamp)
TTL timestamp + INTERVAL 7 DAY;

-- Create Kafka engine table to read from Redpanda
DROP TABLE IF EXISTS raw_events_kafka;
CREATE TABLE raw_events_kafka (
  timestamp    DateTime64(3, 'UTC'),
  service      String,
  host         String,
  cpu_usage    Float32,
  mem_usage    Float32,
  level        String,
  message      String
) ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'redpanda:9092',
  kafka_topic_list = 'metrics',
  kafka_group_name = 'ch-metrics-consumer',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

-- Materialized view to stream data into 'events'
DROP VIEW IF EXISTS mv_consume_metrics;
CREATE MATERIALIZED VIEW mv_consume_metrics TO events AS
SELECT
  timestamp,
  service,
  host,
  cpu_usage,
  mem_usage,
  level,
  message
FROM raw_events_kafka;

-- Optional: simple rollup for UI
CREATE TABLE IF NOT EXISTS agg_minute (
  ts_min      DateTime('UTC'),
  service     LowCardinality(String),
  avg_cpu     Float32,
  p95_cpu     Float32,
  avg_mem     Float32,
  p95_mem     Float32,
  err_count   UInt32
) ENGINE = SummingMergeTree
ORDER BY (service, ts_min);

DROP VIEW IF EXISTS mv_agg_minute;
CREATE MATERIALIZED VIEW mv_agg_minute TO agg_minute AS
SELECT
  toStartOfMinute(timestamp) AS ts_min,
  service,
  avg(cpu_usage) AS avg_cpu,
  quantileExact(0.95)(cpu_usage) AS p95_cpu,
  avg(mem_usage) AS avg_mem,
  quantileExact(0.95)(mem_usage) AS p95_mem,
  sum(if(level = 'ERROR', 1, 0)) AS err_count
FROM events
GROUP BY ts_min, service;