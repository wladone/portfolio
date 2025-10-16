-- Log Analytics Pipeline - Base Cassandra Schema
-- These are the main tables for storing aggregated metrics

-- Keyspace for log analytics
CREATE KEYSPACE IF NOT EXISTS log_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE log_analytics;

-- Requests per minute aggregation
CREATE TABLE IF NOT EXISTS requests_per_minute (
    window_start timestamp,
    window_end timestamp,
    request_count bigint,
    metric_type text,
    PRIMARY KEY (window_start, window_end)
) WITH CLUSTERING ORDER BY (window_end DESC);

-- 5xx errors per minute aggregation
CREATE TABLE IF NOT EXISTS errors_per_minute (
    window_start timestamp,
    window_end timestamp,
    error_count bigint,
    metric_type text,
    PRIMARY KEY (window_start, window_end)
) WITH CLUSTERING ORDER BY (window_end DESC);

-- Status code distribution per minute
CREATE TABLE IF NOT EXISTS status_distribution (
    window_start timestamp,
    window_end timestamp,
    status_class text,
    count bigint,
    metric_type text,
    PRIMARY KEY (window_start, window_end, status_class)
) WITH CLUSTERING ORDER BY (window_end DESC, status_class ASC);

-- Top endpoints on sliding windows
CREATE TABLE IF NOT EXISTS top_endpoints (
    window_start timestamp,
    window_end timestamp,
    endpoint text,
    request_count bigint,
    metric_type text,
    PRIMARY KEY (window_start, window_end, endpoint)
) WITH CLUSTERING ORDER BY (window_end DESC, request_count DESC);

-- Kafka-specific requests per minute
CREATE TABLE IF NOT EXISTS kafka_requests_per_minute (
    window_start timestamp,
    window_end timestamp,
    request_count bigint,
    kafka_topic text,
    metric_type text,
    PRIMARY KEY (window_start, window_end)
) WITH CLUSTERING ORDER BY (window_end DESC);

-- Create indexes for efficient querying (if needed)
CREATE INDEX IF NOT EXISTS idx_requests_metric_type ON requests_per_minute (metric_type);
CREATE INDEX IF NOT EXISTS idx_errors_metric_type ON errors_per_minute (metric_type);
CREATE INDEX IF NOT EXISTS idx_status_dist_metric_type ON status_distribution (metric_type);
CREATE INDEX IF NOT EXISTS idx_top_endpoints_metric_type ON top_endpoints (metric_type);
CREATE INDEX IF NOT EXISTS idx_kafka_requests_metric_type ON kafka_requests_per_minute (metric_type);
CREATE INDEX IF NOT EXISTS idx_kafka_requests_topic ON kafka_requests_per_minute (kafka_topic);

-- Raw log entries table (for debugging, optional)
CREATE TABLE IF NOT EXISTS raw_log_entries (
    timestamp timestamp,
    client_ip text,
    request_line text,
    status_code int,
    response_size bigint,
    referer text,
    user_agent text,
    PRIMARY KEY (timestamp, client_ip, request_line)
) WITH CLUSTERING ORDER BY (client_ip ASC, request_line ASC);

-- Indexes for raw log entries
CREATE INDEX IF NOT EXISTS idx_raw_logs_status_code ON raw_log_entries (status_code);
CREATE INDEX IF NOT EXISTS idx_raw_logs_endpoint ON raw_log_entries (request_line);