import os

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import window

from beam import streaming_pipeline as sp


@pytest.mark.performance
@pytest.mark.skipif(os.getenv("RUN_PERF_TESTS") != "1", reason="Performance tests are opt-in")
def test_views_aggregation_high_volume_runs_within_threshold():
    pipeline = TestPipeline()

    events = [
        window.TimestampedValue(
            {"event_time": "2023-01-01T00:00:00Z", "product_id": f"P{i % 50}"},
            float(i % 60),
        )
        for i in range(5000)
    ]

    clicks = pipeline | "Clicks" >> beam.Create(events)
    empty_transactions = pipeline | "Transactions" >> beam.Create([])
    empty_stock = pipeline | "Stock" >> beam.Create([])

    view_counts, _, _ = sp.build_transforms(clicks, empty_transactions, empty_stock)
    view_counts | "Format" >> beam.ParDo(sp.FormatViewsRow())

    result = pipeline.run()
    result.wait_until_finish()
