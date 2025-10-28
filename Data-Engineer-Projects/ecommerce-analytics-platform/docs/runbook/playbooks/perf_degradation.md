# Performance Degradation Playbook

## Detection

### Monitoring Signals

1. Latency Alerts:
   ```promql
   # P95 latency above 250ms
   histogram_quantile(0.95, sum by (le) (rate(ecom_request_latency_ms_bucket[5m]))) > 250
   ```

2. Cache Miss Rate:
   ```promql
   # Cache hit rate below 80%
   sum(rate(ecom_cache_hits_total[5m])) / sum(rate(ecom_cache_requests_total[5m])) < 0.8
   ```

3. Database Connection Pool:
   ```promql
   # High pool utilization
   max_over_time(ecom_db_pool_active_connections[5m]) / max_over_time(ecom_db_pool_max_connections[5m]) > 0.8
   ```

### Impact Assessment

1. Check affected endpoints:
   ```promql
   topk(10, sum by (path) (rate(ecom_request_latency_ms_sum[5m])) / sum by (path) (rate(ecom_request_latency_ms_count[5m])))
   ```

2. User impact:
   ```promql
   sum(rate(ecom_requests_total{status_code=~"5.."}[5m])) / sum(rate(ecom_requests_total[5m]))
   ```

## Immediate Actions

### 1. Quick Wins

```bash
# Clear Redis cache if high miss rate
redis-cli -h $REDIS_HOST FLUSHALL

# Restart API instances if memory pressure
docker compose restart api

# Check DB connection pool
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c '\
  SELECT * FROM pg_stat_activity WHERE state != '\''idle'\'';'
```

### 2. Scale Out (if needed)

```bash
# Scale API replicas
docker compose up -d --scale api=3

# Increase Redis maxmemory
redis-cli -h $REDIS_HOST CONFIG SET maxmemory "2gb"
```

## Diagnostic Steps

### 1. API Performance

Check slow endpoints:
```bash
# Top 10 slowest endpoints last 5m
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=topk(10, avg by (path) (rate(ecom_request_latency_ms_sum[5m]) / rate(ecom_request_latency_ms_count[5m])))' | jq
```

### 2. Database Analysis

Check query performance:
```sql
SELECT pid,
       now() - query_start as duration,
       query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC;
```

### 3. Resource Utilization

```bash
# Check container metrics
docker stats

# Check Redis memory
redis-cli -h $REDIS_HOST INFO memory

# Check Kafka lag
rpk topic consume orders -g monitor --offset latest
```

## Mitigation Steps

### Short-term fixes:

1. Selective cache invalidation:
```python
await cache.delete_pattern("sales:summary:*")
```

2. Database query optimization:
```sql
ANALYZE dw.fact_orders;
ANALYZE dw.dim_products;
```

3. Tune connection pool:
```python
DATABASE_POOL_SIZE = min(cpu_count() * 2 + 1, 20)
```

### Long-term solutions:

1. Implement query caching:
```python
@cache(ttl=300)  # 5 min
async def get_sales_summary():
    pass
```

2. Add database indexes:
```sql
CREATE INDEX CONCURRENTLY idx_fact_orders_date
ON dw.fact_orders (order_date);
```

3. Configure PgBouncer:
```ini
pool_mode = transaction
default_pool_size = 20
max_client_conn = 1000
```

## Prevention

1. Set up benchmark suite:
```bash
make benchmark-api
```

2. Configure autoscaling:
```yaml
deploy:
  replicas: 3
  update_config:
    parallelism: 1
    delay: 10s
  restart_policy:
    condition: on-failure
    max_attempts: 3
    window: 120s
```

3. Implement circuit breakers:
```python
@circuit_breaker(failure_threshold=5, reset_timeout=30)
async def external_service_call():
    pass
```

## Communication

### Status Updates

1. Initial notification:
```
[DEGRADED] High API latency detected
Impact: Sales summary endpoints p95 >250ms
Actions: Investigating DB load, scaling API
```

2. Resolution:
```
[RESOLVED] API performance restored
Root cause: Cache eviction pressure
Fix: Increased cache memory, optimized queries
```

### Stakeholder Management

1. Timeline template:
```
T+0: Alert triggered - p95 latency >250ms
T+5: Investigation started
T+10: Cache pressure identified
T+15: Memory increased
T+20: Performance restored
```

2. Incident review scheduling:
```
When: T+1 business day
What: Review alert thresholds
Who: SRE, API team leads
```
