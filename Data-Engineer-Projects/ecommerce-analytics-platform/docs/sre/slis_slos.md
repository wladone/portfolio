# Service Level Objectives (SLOs)

This document defines our Service Level Indicators (SLIs), Objectives (SLOs), and Agreements (SLAs) for the E-commerce Analytics Platform.

## API Service Levels

### Availability

**SLI**: Ratio of successful requests (2xx/3xx) to total requests
```promql
sum(rate(ecom_requests_total{status_code=~"2..|3.."}[30d])) / sum(rate(ecom_requests_total[30d]))
```

**SLO**: 99.9% monthly availability
- Error budget: 43.2 minutes/month
- Measured across all API endpoints
- Excludes planned maintenance windows

### Latency

**SLIs**:
1. Sales Summary API p95 latency
```promql
histogram_quantile(0.95, sum by (le) (rate(ecom_request_latency_ms_bucket{path="/api/v1/sales/summary"}[30d])))
```

2. Recommendations API p95 latency
```promql
histogram_quantile(0.95, sum by (le) (rate(ecom_request_latency_ms_bucket{path=~"/api/v1/recs/.*"}[30d])))
```

**SLOs**:
- Sales Summary API: p95 < 250ms over 30 days
- Recommendations API: p95 < 200ms over 30 days
- Measured at API Gateway level
- Includes database and cache operations

## Data Freshness

### Batch Processing

**SLI**: Time since last successful ETL job completion
```promql
time() - max(etl_job_last_success_timestamp)
```

**SLO**:
- Maximum 30 minutes delay for batch data
- Measured from source CDC to DW availability
- 99% compliance over 30 days

### Streaming Processing

**SLI**: Kafka consumer lag
```promql
avg_over_time(ecom_kafka_consumer_lag[30d])
```

**SLO**:
- p95 lag < 500 messages
- Maximum 2 minutes delay p95
- Measured per consumer group

## Cache Performance

**SLI**: Cache hit rate
```promql
sum(rate(ecom_cache_hits_total[30d])) / sum(rate(ecom_cache_requests_total[30d]))
```

**SLO**:
- 90% hit rate over 30 days
- Measured across all cache keys
- Excludes cache warming periods

## Error Budgets

Monthly error budgets are tracked in Prometheus and visualized in our SLO dashboard:

1. API Availability Budget:
```promql
1 - (
  sum(rate(ecom_requests_total{status_code=~"2..|3.."}[30d]))
  /
  sum(rate(ecom_requests_total[30d]))
)
```

2. Latency Budget:
```promql
1 - (
  sum(rate(ecom_request_latency_ms_bucket{le="250"}[30d]))
  /
  sum(rate(ecom_request_latency_ms_count[30d]))
)
```

3. Data Freshness Budget:
```promql
count_over_time(
  (time() - max(etl_job_last_success_timestamp) > 1800)[30d:1m]
) / (30 * 24 * 60)
```

## SLA Commitments

Commercial SLAs are defined as:

1. API Service Level:
- 99.9% monthly availability
- Credit: 10% for every 0.1% below target

2. Data Freshness:
- 99% of data available within 30 minutes
- Credit: 5% for every 1% below target

3. Support Response:
- P1: 30 minutes
- P2: 2 hours
- P3: 8 hours

## Monitoring & Alerting

See [alerting_rules.yaml](alerting_rules.yaml) for the complete set of alerting rules that help us maintain these SLOs.

Key dashboards:
1. [SLO Overview](dashboards/overview.json)
2. [API Performance](dashboards/api.json)
3. [ETL Pipeline](dashboards/etl.json)
4. [Kafka Monitoring](dashboards/kafka.json)
