"""Prometheus metrics for streaming components."""

from prometheus_client import Counter, Gauge, Histogram

# Kafka metrics
KAFKA_ASSIGNMENTS = Gauge(
    "ecom_kafka_partitions_assigned", "Partitions assigned", ["topic", "group"]
)

KAFKA_COMMITS = Counter(
    "ecom_kafka_commits_total", "Offset commits", ["topic", "group"]
)

KAFKA_POLL_RECORDS = Histogram(
    "ecom_kafka_poll_records",
    "Records per poll",
    buckets=[1, 2, 5, 10, 25, 50, 100, 250, 500, 1000],
)

KAFKA_CONSUMER_LAG = Gauge(
    "ecom_kafka_consumer_lag", "Consumer lag (approx)", ["topic", "partition", "group"]
)
