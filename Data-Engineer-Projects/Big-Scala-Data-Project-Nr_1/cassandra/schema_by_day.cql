-- Log Analytics Pipeline - Time-Bucketed Cassandra Schema
-- These tables use Time Window Compaction Strategy (TWCS) and TTL for efficient time-series storage
-- Data is automatically partitioned by day and expired after 7 days

-- Keyspace for log analytics (if not already created)
CREATE KEYSPACE IF NOT EXISTS log_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE log_analytics;

-- Requests per minute, partitioned by day with TWCS and 7-day TTL
CREATE TABLE IF NOT EXISTS requests_per_minute_by_day (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    request_count bigint,
    metric_type text,
    batch_id bigint,
    PRIMARY KEY (bucket_date, window_start, window_end)
) WITH CLUSTERING ORDER BY (window_start DESC, window_end DESC)
   AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'days', 'compaction_window_size': 1}
   AND default_time_to_live = 604800
   AND gc_grace_seconds = 86400;

-- 5xx errors per minute, partitioned by day with TWCS and 7-day TTL
CREATE TABLE IF NOT EXISTS errors_per_minute_by_day (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    error_count bigint,
    metric_type text,
    batch_id bigint,
    PRIMARY KEY (bucket_date, window_start, window_end)
) WITH CLUSTERING ORDER BY (window_start DESC, window_end DESC)
   AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'days', 'compaction_window_size': 1}
   AND default_time_to_live = 604800
   AND gc_grace_seconds = 86400;

-- Status code distribution per minute, partitioned by day with TWCS and 7-day TTL
CREATE TABLE IF NOT EXISTS status_distribution_by_day (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    status_class text,
    count bigint,
    metric_type text,
    batch_id bigint,
    PRIMARY KEY (bucket_date, window_start, window_end, status_class)
) WITH CLUSTERING ORDER BY (window_start DESC, window_end DESC, status_class ASC)
   AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'days', 'compaction_window_size': 1}
   AND default_time_to_live = 604800
   AND gc_grace_seconds = 86400;

-- Top endpoints on sliding windows, partitioned by day with TWCS and 7-day TTL
CREATE TABLE IF NOT EXISTS top_endpoints_by_day (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    endpoint text,
    request_count bigint,
    metric_type text,
    batch_id bigint,
    PRIMARY KEY (bucket_date, window_start, window_end, endpoint)
) WITH CLUSTERING ORDER BY (window_start DESC, window_end DESC, request_count DESC)
   AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'days', 'compaction_window_size': 1}
   AND default_time_to_live = 604800
   AND gc_grace_seconds = 86400;

-- Kafka-specific requests per minute, partitioned by day with TWCS and 7-day TTL
CREATE TABLE IF NOT EXISTS kafka_requests_per_minute_by_day (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    request_count bigint,
    kafka_topic text,
    metric_type text,
    batch_id bigint,
    PRIMARY KEY (bucket_date, window_start, window_end)
) WITH CLUSTERING ORDER BY (window_start DESC, window_end DESC)
   AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'days', 'compaction_window_size': 1}
   AND default_time_to_live = 604800
   AND gc_grace_seconds = 86400;

-- Create indexes for time-bucketed tables
CREATE INDEX IF NOT EXISTS idx_requests_by_day_metric_type ON requests_per_minute_by_day (metric_type);
CREATE INDEX IF NOT EXISTS idx_errors_by_day_metric_type ON errors_per_minute_by_day (metric_type);
CREATE INDEX IF NOT EXISTS idx_status_dist_by_day_metric_type ON status_distribution_by_day (metric_type);
CREATE INDEX IF NOT EXISTS idx_top_endpoints_by_day_metric_type ON top_endpoints_by_day (metric_type);
CREATE INDEX IF NOT EXISTS idx_kafka_requests_by_day_metric_type ON kafka_requests_per_minute_by_day (metric_type);
CREATE INDEX IF NOT EXISTS idx_kafka_requests_by_day_topic ON kafka_requests_per_minute_by_day (kafka_topic);

-- Materialized views for efficient time-range queries (optional)
-- These can be useful for dashboard queries but add write overhead

-- View for latest metrics across all types
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_metrics_by_type AS
    SELECT bucket_date, window_start, metric_type, request_count, error_count, count
    FROM requests_per_minute_by_day
    WHERE bucket_date IS NOT NULL
      AND window_start IS NOT NULL
      AND metric_type IS NOT NULL
    PRIMARY KEY (metric_type, bucket_date, window_start);

-- View for error rate analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS error_rate_by_day AS
    SELECT bucket_date, window_start, request_count, error_count
    FROM requests_per_minute_by_day
    WHERE bucket_date IS NOT NULL
      AND window_start IS NOT NULL
      AND request_count IS NOT NULL
      AND error_count IS NOT NULL
    PRIMARY KEY (bucket_date, window_start);

-- Example queries for verification:
-- SELECT * FROM requests_per_minute_by_day WHERE bucket_date = '2023-12-01';
-- SELECT * FROM requests_per_minute_by_day WHERE bucket_date = '2023-12-01' AND window_start >= '2023-12-01 10:00:00' AND window_start < '2023-12-01 11:00:00';
-- SELECT * FROM status_distribution_by_day WHERE bucket_date = '2023-12-01' AND status_class = '5xx';
-- SELECT endpoint, request_count FROM top_endpoints_by_day WHERE bucket_date = '2023-12-01' AND window_start >= '2023-12-01 10:00:00' LIMIT 10;