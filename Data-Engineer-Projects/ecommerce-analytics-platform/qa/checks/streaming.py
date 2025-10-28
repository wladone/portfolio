"""Streaming checks module."""

from dataclasses import dataclass

import numpy as np

from qa.budgets import BUDGETS
from qa.util.metrics import MetricsParser


@dataclass
class StreamingMetrics:
    """Streaming metrics results."""

    kafka_lag_p95: float | None
    kafka_lag_mean: float | None
    data_freshness_p95: float | None
    data_freshness_mean: float | None
    status: str
    details: str


class StreamingChecker:
    """Check streaming metrics."""

    def __init__(self, metrics_text: str):
        """Initialize checker with metrics."""
        self.parser = MetricsParser(metrics_text)

    def check_kafka_lag(self) -> dict[str, float]:
        """Calculate Kafka consumer lag statistics."""
        lag_metrics = self.parser.get_metric("ecom_kafka_consumer_lag")
        if not lag_metrics:
            return {}

        lag_values = [value for _, value in lag_metrics]
        if not lag_values:
            return {}

        return {
            "p95": float(np.percentile(lag_values, 95)),
            "mean": float(np.mean(lag_values)),
        }

    def check_data_freshness(self) -> dict[str, float]:
        """Calculate data freshness statistics."""
        # This assumes a metric like last_successful_processing_timestamp
        # or a calculated freshness metric
        freshness_metrics = self.parser.get_metric("ecom_data_freshness_seconds")
        if not freshness_metrics:
            return {}

        freshness_values = [value for _, value in freshness_metrics]
        if not freshness_values:
            return {}

        return {
            "p95": float(np.percentile(freshness_values, 95)),
            "mean": float(np.mean(freshness_values)),
        }

    def check_all(self) -> StreamingMetrics:
        """Run all streaming checks."""
        # Check Kafka lag
        lag_stats = self.check_kafka_lag()
        lag_p95 = lag_stats.get("p95")
        lag_mean = lag_stats.get("mean")

        # Check data freshness
        freshness_stats = self.check_data_freshness()
        freshness_p95 = freshness_stats.get("p95")
        if freshness_p95:
            # Convert seconds to minutes
            freshness_p95 = freshness_p95 / 60
        freshness_mean = freshness_stats.get("mean")
        if freshness_mean:
            freshness_mean = freshness_mean / 60

        # Evaluate results
        status = "PASS"
        details = []

        if lag_p95 is not None:
            if lag_p95 > BUDGETS["kafka_lag_p95"].hard_max:
                status = "FAIL"
                details.append(
                    f"Kafka lag p95 ({lag_p95:.0f}) exceeds hard limit "
                    f"({BUDGETS['kafka_lag_p95'].hard_max})"
                )
            elif lag_p95 > BUDGETS["kafka_lag_p95"].soft_max:
                status = "WARN"
                details.append(
                    f"Kafka lag p95 ({lag_p95:.0f}) exceeds soft limit "
                    f"({BUDGETS['kafka_lag_p95'].soft_max})"
                )

        if freshness_p95 is not None:
            if freshness_p95 > BUDGETS["freshness_minutes_p95"].hard_max:
                status = "FAIL"
                details.append(
                    f"Data freshness p95 ({freshness_p95:.1f}m) exceeds hard limit "
                    f"({BUDGETS['freshness_minutes_p95'].hard_max}m)"
                )
            elif freshness_p95 > BUDGETS["freshness_minutes_p95"].soft_max:
                status = "WARN"
                details.append(
                    f"Data freshness p95 ({freshness_p95:.1f}m) exceeds soft limit "
                    f"({BUDGETS['freshness_minutes_p95'].soft_max}m)"
                )

        return StreamingMetrics(
            kafka_lag_p95=lag_p95,
            kafka_lag_mean=lag_mean,
            data_freshness_p95=freshness_p95,
            data_freshness_mean=freshness_mean,
            status=status,
            details="\n".join(details) if details else "All metrics within budget",
        )
