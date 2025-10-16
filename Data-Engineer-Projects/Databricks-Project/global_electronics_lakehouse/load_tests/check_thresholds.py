#!/usr/bin/env python3
"""
Check performance thresholds from Locust load test results
"""

import sys
import json
import re
from pathlib import Path


def parse_locust_report(report_path):
    """Parse the HTML report to extract key metrics"""
    with open(report_path, 'r') as f:
        content = f.read()

    # Extract key metrics using regex (simplified parsing)
    metrics = {}

    # Response time percentiles
    rt_patterns = {
        '50_percentile': r'50%</td><td>([\d.]+)',
        '95_percentile': r'95%</td><td>([\d.]+)',
        '99_percentile': r'99%</td><td>([\d.]+)',
    }

    for key, pattern in rt_patterns.items():
        match = re.search(pattern, content)
        if match:
            metrics[key] = float(match.group(1))

    # RPS (requests per second)
    rps_match = re.search(r'Total</td><td>([\d.]+)</td><td>RPS', content)
    if rps_match:
        metrics['rps'] = float(rps_match.group(1))

    # Failure rate
    failure_match = re.search(r'Failures</td><td>([\d.]+)%', content)
    if failure_match:
        metrics['failure_rate'] = float(failure_match.group(1))

    return metrics


def check_thresholds(metrics):
    """Check if metrics meet performance thresholds"""
    thresholds = {
        '50_percentile': 1000,  # ms
        '95_percentile': 2000,  # ms
        '99_percentile': 5000,  # ms
        'rps': 10,              # minimum requests per second
        'failure_rate': 5.0,    # maximum failure rate percentage
    }

    failures = []

    for metric, threshold in thresholds.items():
        if metric not in metrics:
            failures.append(f"Metric {metric} not found in report")
            continue

        value = metrics[metric]

        if metric in ['50_percentile', '95_percentile', '99_percentile']:
            if value > threshold:
                failures.append(f"{metric}: {value}ms > {threshold}ms")
        elif metric == 'rps':
            if value < threshold:
                failures.append(f"{metric}: {value} < {threshold}")
        elif metric == 'failure_rate':
            if value > threshold:
                failures.append(f"{metric}: {value}% > {threshold}%")

    return failures


def main():
    if len(sys.argv) != 2:
        print("Usage: python check_thresholds.py <report.html>")
        sys.exit(1)

    report_path = sys.argv[1]

    if not Path(report_path).exists():
        print(f"Report file {report_path} not found")
        sys.exit(1)

    try:
        metrics = parse_locust_report(report_path)
        print("Extracted metrics:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")

        failures = check_thresholds(metrics)

        if failures:
            print("\n❌ Performance thresholds not met:")
            for failure in failures:
                print(f"  - {failure}")
            sys.exit(1)
        else:
            print("\n✅ All performance thresholds met!")
            sys.exit(0)

    except Exception as e:
        print(f"Error parsing report: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
