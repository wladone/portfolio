from prometheus_client import Counter, Gauge, Histogram

# Counter for total requests with labels for endpoint, method, and status
REQUESTS = Counter(
    "requests_total", "Total number of HTTP requests", ["endpoint", "method", "status"]
)

# Histogram for request latency with labels for endpoint and method, and predefined buckets
REQUEST_LATENCY = Histogram(
    "request_latency_seconds",
    "Request latency in seconds",
    ["endpoint", "method"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)

# Counter for cache hits with endpoint label
CACHE_HIT = Counter("cache_hits_total", "Total number of cache hits", ["endpoint"])

# Counter for cache misses with endpoint label
CACHE_MISS = Counter("cache_misses_total", "Total number of cache misses", ["endpoint"])

# Counter for rate limited responses
RATE_LIMITED = Counter("rate_limited_total", "Total number of rate limited responses")

# Counter for cache invalidations
CACHE_INVALIDATIONS = Counter(
    "cache_invalidations_total",
    "Total number of cache invalidations",
    ["target", "strategy"],
)

# Gauge for cache entries with target label (optional, best-effort)
CACHE_ENTRIES_GAUGE = Gauge("cache_entries", "Number of cache entries", ["target"])

# Gauge for database pool connections with state label (optional)
DB_POOL_GAUGE = Gauge(
    "db_pool_connections", "Number of database pool connections by state", ["state"]
)


# Helper functions for incrementing/observing metrics


def increment_requests(endpoint: str, method: str, status: str):
    """Increment the requests counter."""
    REQUESTS.labels(endpoint=endpoint, method=method, status=status).inc()


def observe_latency(endpoint: str, method: str, latency: float):
    """Observe request latency in the histogram."""
    REQUEST_LATENCY.labels(endpoint=endpoint, method=method).observe(latency)


def increment_cache_hit(endpoint: str):
    """Increment the cache hits counter."""
    CACHE_HIT.labels(endpoint=endpoint).inc()


def increment_cache_miss(endpoint: str):
    """Increment the cache misses counter."""
    CACHE_MISS.labels(endpoint=endpoint).inc()


def increment_rate_limited():
    """Increment the rate limited counter."""
    RATE_LIMITED.inc()


def set_db_pool_gauge(state: str, value: float):
    """Set the database pool gauge for a specific state."""
    DB_POOL_GAUGE.labels(state=state).set(value)


def increment_cache_invalidations(target: str, strategy: str):
    """Increment the cache invalidations counter."""
    CACHE_INVALIDATIONS.labels(target=target, strategy=strategy).inc()


def set_cache_entries_gauge(target: str, value: float):
    """Set the cache entries gauge for a specific target."""
    CACHE_ENTRIES_GAUGE.labels(target=target).set(value)
