"""Small metrics exporter stub for local testing and future Cloud Monitoring integration."""

from typing import Dict


def export_metrics(metrics: Dict[str, int]) -> None:
    """
    Simple exporter that logs metrics to stdout. In production this module can
    be replaced by an exporter that writes to Cloud Monitoring or Prometheus.
    """
    for k, v in metrics.items():
        print(f"METRIC {k}={v}")
