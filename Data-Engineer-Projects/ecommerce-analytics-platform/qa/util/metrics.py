"""Prometheus metrics parsing utility."""

import re


class MetricsParser:
    """Parse Prometheus text format metrics."""

    def __init__(self, text: str):
        """Initialize parser with metrics text."""
        self.text = text
        self.metrics = self._parse()

    def _parse(self) -> dict[str, list[tuple[dict[str, str], float]]]:
        """Parse metrics text into structured format."""
        metrics = {}
        current_metric = None

        for line in self.text.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Split metric and value
            parts = line.split()
            if len(parts) != 2:
                continue

            metric_part, value = parts

            # Parse metric name and labels
            metric_match = re.match(r"([^{]+)({.+})?", metric_part)
            if not metric_match:
                continue

            name = metric_match.group(1)
            labels_str = metric_match.group(2) or ""

            # Parse labels
            labels = {}
            if labels_str:
                labels_str = labels_str.strip("{}")
                for label_pair in labels_str.split(","):
                    if "=" in label_pair:
                        k, v = label_pair.split("=", 1)
                        labels[k] = v.strip('"')

            # Parse value
            try:
                value = float(value)
            except ValueError:
                continue

            if name not in metrics:
                metrics[name] = []
            metrics[name].append((labels, value))

        return metrics

    def get_metric(self, name: str) -> list[tuple[dict[str, str], float]]:
        """Get all values for a metric."""
        return self.metrics.get(name, [])

    def get_value(
        self, name: str, labels: dict[str, str] | None = None
    ) -> float | None:
        """Get single value for metric matching labels."""
        values = self.get_metric(name)
        if not values:
            return None

        if not labels:
            return values[0][1]

        for metric_labels, value in values:
            if all(metric_labels.get(k) == v for k, v in labels.items()):
                return value

        return None

    def get_histogram_percentile(
        self, name: str, percentile: float, labels: dict[str, str] | None = None
    ) -> float | None:
        """Calculate percentile from histogram buckets."""
        if not name.endswith("_bucket"):
            name = f"{name}_bucket"

        buckets = []
        for metric_labels, value in self.get_metric(name):
            if labels and not all(metric_labels.get(k) == v for k, v in labels.items()):
                continue

            try:
                le = float(metric_labels.get("le", "inf"))
                buckets.append((le, value))
            except ValueError:
                continue

        if not buckets:
            return None

        # Sort by bucket upper bound
        buckets.sort()

        # Get total count
        total = buckets[-1][1]
        if total == 0:
            return 0

        # Find percentile bucket
        target = total * (percentile / 100.0)
        prev_upper = 0
        prev_count = 0

        for upper, count in buckets:
            if count >= target:
                # Interpolate within bucket
                bucket_size = upper - prev_upper
                bucket_count = count - prev_count
                if bucket_count == 0:
                    return upper

                position = (target - prev_count) / bucket_count
                return prev_upper + (bucket_size * position)

            prev_upper = upper
            prev_count = count

        return buckets[-1][0]  # Return highest bucket if not found

    def calculate_rate(
        self, name: str, labels: dict[str, str] | None = None
    ) -> float | None:
        """Calculate rate from counter values."""
        value = self.get_value(name, labels)
        if value is None:
            return None

        return value
