"""Health check module."""

import time
from dataclasses import dataclass

import httpx

from qa.util.metrics import MetricsParser


@dataclass
class HealthCheckResult:
    """Results from health checks."""

    endpoint: str
    status: str
    response_time: float
    details: str
    metrics: dict | None = None


class HealthChecker:
    """Check application health endpoints."""

    def __init__(self, base_url: str):
        """Initialize health checker."""
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=10.0)

    def check_endpoint(
        self, path: str, expected_status: int = 200, parse_json: bool = True
    ) -> HealthCheckResult:
        """Check a health endpoint."""
        start_time = time.time()
        try:
            response = self.client.get(f"{self.base_url}{path}")
            duration = time.time() - start_time

            if response.status_code != expected_status:
                return HealthCheckResult(
                    endpoint=path,
                    status="FAIL",
                    response_time=duration,
                    details=f"Expected status {expected_status}, got {response.status_code}",
                )

            details = ""
            if parse_json:
                try:
                    data = response.json()
                    details = str(data)
                except:
                    details = response.text

            return HealthCheckResult(
                endpoint=path, status="PASS", response_time=duration, details=details
            )

        except Exception as e:
            duration = time.time() - start_time
            return HealthCheckResult(
                endpoint=path, status="ERROR", response_time=duration, details=str(e)
            )

    def check_metrics(self) -> HealthCheckResult:
        """Check Prometheus metrics endpoint."""
        start_time = time.time()
        try:
            response = self.client.get(f"{self.base_url}/metrics")
            duration = time.time() - start_time

            if response.status_code != 200:
                return HealthCheckResult(
                    endpoint="/metrics",
                    status="FAIL",
                    response_time=duration,
                    details=f"Status code: {response.status_code}",
                )

            # Parse metrics
            parser = MetricsParser(response.text)
            metrics = {
                "requests": parser.get_metric("ecom_requests_total"),
                "latency": parser.get_metric("ecom_request_latency_ms_bucket"),
                "cache": {
                    name: parser.get_metric(f"ecom_cache_{name}_total")
                    for name in ["hits", "misses", "invalidations"]
                },
                "db_pool": parser.get_metric("ecom_db_pool_connections"),
                "kafka_lag": parser.get_metric("ecom_kafka_consumer_lag"),
                "etl_status": parser.get_metric("etl_job_status"),
            }

            return HealthCheckResult(
                endpoint="/metrics",
                status="PASS",
                response_time=duration,
                details="Metrics parsed successfully",
                metrics=metrics,
            )

        except Exception as e:
            duration = time.time() - start_time
            return HealthCheckResult(
                endpoint="/metrics",
                status="ERROR",
                response_time=duration,
                details=str(e),
            )

    def check_all(self) -> list[HealthCheckResult]:
        """Run all health checks."""
        return [
            self.check_endpoint("/health"),
            self.check_endpoint("/readyz"),
            self.check_metrics(),
        ]

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        self.client.close()
