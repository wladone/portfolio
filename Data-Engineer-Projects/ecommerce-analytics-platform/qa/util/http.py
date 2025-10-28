"""HTTP utility module for load testing."""

import asyncio
from dataclasses import dataclass
from datetime import datetime

import httpx
import numpy as np


@dataclass
class RequestResult:
    """Results from a single request."""

    path: str
    method: str
    status_code: int
    duration_ms: float
    timestamp: datetime
    error: str | None = None


class LoadGenerator:
    """Generate concurrent load on API endpoints."""

    def __init__(
        self, base_url: str, concurrency: int = 1, auth_token: str | None = None
    ):
        """Initialize load generator."""
        self.base_url = base_url.rstrip("/")
        self.concurrency = concurrency
        self.headers = {"Accept": "application/json"}
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            timeout=30.0,
            limits=httpx.Limits(
                max_keepalive_connections=concurrency, max_connections=concurrency
            ),
        )

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args):
        """Async context manager exit."""
        await self.client.aclose()

    async def request(self, method: str, path: str, **kwargs) -> RequestResult:
        """Make a single request and time it."""
        start = datetime.now()
        error = None
        try:
            response = await self.client.request(method, path, **kwargs)
            status_code = response.status_code
        except Exception as e:
            status_code = 500
            error = str(e)

        duration = (datetime.now() - start).total_seconds() * 1000

        return RequestResult(
            path=path,
            method=method,
            status_code=status_code,
            duration_ms=duration,
            timestamp=start,
            error=error,
        )

    async def generate_load(
        self, method: str, path: str, duration_seconds: int, **kwargs
    ) -> list[RequestResult]:
        """Generate load for specified duration."""
        results = []
        end_time = datetime.now().timestamp() + duration_seconds

        while datetime.now().timestamp() < end_time:
            tasks = [
                self.request(method, path, **kwargs) for _ in range(self.concurrency)
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

        return results


def calculate_percentiles(results: list[RequestResult]) -> dict[str, float]:
    """Calculate p50, p95, p99 latencies."""
    if not results:
        return {}

    durations = [r.duration_ms for r in results]
    return {
        "p50": float(np.percentile(durations, 50)),
        "p95": float(np.percentile(durations, 95)),
        "p99": float(np.percentile(durations, 99)),
        "mean": float(np.mean(durations)),
        "std": float(np.std(durations)),
        "count": len(results),
        "errors": sum(1 for r in results if r.status_code >= 400),
    }
