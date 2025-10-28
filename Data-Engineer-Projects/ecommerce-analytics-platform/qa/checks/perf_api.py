"""API performance checks module."""

from dataclasses import dataclass
from datetime import datetime

import numpy as np

from qa.budgets import BUDGETS
from qa.settings import QASettings
from qa.util.http import LoadGenerator
from qa.util.metrics import MetricsParser


@dataclass
class EndpointPerformance:
    """Performance results for an endpoint."""

    path: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mean_ms: float
    std_ms: float
    request_count: int
    error_count: int
    cache_hit_ratio: float | None = None


@dataclass
class PerformanceTestResult:
    """Results from performance test."""

    sales_summary: EndpointPerformance
    recommendations: EndpointPerformance
    cache_stats: dict[str, float]
    duration_seconds: float
    warmup_seconds: float
    concurrency: int
    verdict: str


class APIPerformanceTester:
    """Test API performance against budgets."""

    def __init__(self, settings: QASettings):
        """Initialize tester with settings."""
        self.settings = settings

    async def _get_metrics(self) -> str:
        """Get current metrics snapshot."""
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.settings.api_base_url}/metrics")
            return response.text

    async def test_endpoint(
        self,
        path: str,
        method: str = "GET",
        params: dict | None = None,
        warmup: bool = True,
    ) -> EndpointPerformance:
        """Test single endpoint performance."""
        # Get initial metrics
        before_metrics = MetricsParser(await self._get_metrics())

        async with LoadGenerator(
            self.settings.api_base_url, concurrency=self.settings.concurrency
        ) as generator:
            # Warmup
            if warmup:
                warmup_results = await generator.generate_load(
                    method, path, self.settings.warmup_seconds, params=params
                )

            # Main test
            results = await generator.generate_load(
                method, path, self.settings.duration_seconds, params=params
            )

        # Get final metrics
        after_metrics = MetricsParser(await self._get_metrics())

        # Calculate stats
        durations = [r.duration_ms for r in results]

        # Calculate cache hit ratio
        cache_hits_before = (
            before_metrics.get_value("ecom_cache_hits_total", {"path": path}) or 0
        )
        cache_hits_after = (
            after_metrics.get_value("ecom_cache_hits_total", {"path": path}) or 0
        )

        cache_total_before = (
            before_metrics.get_value("ecom_cache_requests_total", {"path": path}) or 0
        )
        cache_total_after = (
            after_metrics.get_value("ecom_cache_requests_total", {"path": path}) or 0
        )

        cache_hit_ratio = None
        if cache_total_after - cache_total_before > 0:
            cache_hit_ratio = (cache_hits_after - cache_hits_before) / (
                cache_total_after - cache_total_before
            )

        return EndpointPerformance(
            path=path,
            p50_ms=float(np.percentile(durations, 50)),
            p95_ms=float(np.percentile(durations, 95)),
            p99_ms=float(np.percentile(durations, 99)),
            mean_ms=float(np.mean(durations)),
            std_ms=float(np.std(durations)),
            request_count=len(results),
            error_count=sum(1 for r in results if r.status_code >= 400),
            cache_hit_ratio=cache_hit_ratio,
        )

    async def run_performance_test(self) -> PerformanceTestResult:
        """Run complete performance test suite."""
        start_time = datetime.now()

        # Test sales summary endpoint
        start_date, end_date = self.settings.date_range
        sales_summary = await self.test_endpoint(
            "/api/v1/sales/summary",
            params={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "granularity": "day",
            },
        )

        # Test recommendations endpoint
        recommendations = await self.test_endpoint(
            f"/api/v1/recs/user/{self.settings.user_id_probe}",
            params={"k": 10, "exclude_seen": "true"},
        )

        # Get cache stats
        metrics = MetricsParser(await self._get_metrics())
        cache_stats = {
            "hit_ratio": np.mean(
                [
                    p.cache_hit_ratio
                    for p in [sales_summary, recommendations]
                    if p.cache_hit_ratio is not None
                ]
            )
        }

        # Evaluate against budgets
        verdict = "PASS"
        if (
            sales_summary.p95_ms > BUDGETS["sales_summary_p95_ms"].hard
            or recommendations.p95_ms > BUDGETS["recs_user_p95_ms"].hard
            or cache_stats["hit_ratio"] < BUDGETS["cache_hit_ratio"].hard_min
        ):
            verdict = "FAIL"
        elif (
            sales_summary.p95_ms > BUDGETS["sales_summary_p95_ms"].soft
            or recommendations.p95_ms > BUDGETS["recs_user_p95_ms"].soft
            or cache_stats["hit_ratio"] < BUDGETS["cache_hit_ratio"].soft_min
        ):
            verdict = "WARN"

        return PerformanceTestResult(
            sales_summary=sales_summary,
            recommendations=recommendations,
            cache_stats=cache_stats,
            duration_seconds=self.settings.duration_seconds,
            warmup_seconds=self.settings.warmup_seconds,
            concurrency=self.settings.concurrency,
            verdict=verdict,
        )
