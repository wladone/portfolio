import json
import time
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam import streaming_pipeline as sp


def test_format_window_and_bounds():
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    formatted = sp.format_window(ts)
    assert formatted == "2025-01-01T12:00:00Z"
    bounds = sp.format_window_bounds(
        beam.transforms.window.IntervalWindow(ts, ts))
    assert bounds["window_start"] == "2025-01-01T12:00:00Z"


def test_parse_event_timestamp_valid():
    t = sp.parse_event_timestamp("2025-01-01T12:00:00Z")
    assert int(t) == 1735684800


def test_parse_event_timestamp_invalid():
    try:
        sp.parse_event_timestamp("invalid-timestamp")
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_require_fields():
    payload = {"event_time": "2025-01-01T12:00:00Z", "product_id": "P1"}
    sp.require_fields(payload, ("event_time", "product_id"))

    bad = {"event_time": "", "product_id": "P1"}
    try:
        sp.require_fields(bad, ("event_time",))
        assert False
    except ValueError:
        pass


class DummyValidator:
    def __call__(self, payload):
        sp.require_fields(payload, ("event_time", "product_id"))
        return payload


def test_parse_and_validate_event_do_fn_basic():
    fn = sp.ParseAndValidateEvent("click", DummyValidator())
    # create a valid message
    payload = {"event_time": "2025-01-01T12:00:00Z", "product_id": "P1"}
    data = json.dumps(payload).encode("utf-8")
    out = list(fn.process(data))
    assert len(out) == 1

    # invalid json
    bad = b"not-json"
    outs = list(fn.process(bad))
    # tagged dead_letter output
    assert any(hasattr(o, 'tag') or isinstance(o, dict) for o in outs)


def test_format_views_row_and_dofn():
    fn = sp.FormatViewsRow()
    out = list(fn.process(("P1", 3)))
    assert out[0]["product_id"] == "P1"
    assert out[0]["view_count"] == 3


def test_build_transforms_and_write_sinks_with_testpipeline():
    # Integration-style test: create small PCollections and assert transforms
    with TestPipeline() as p:
        clicks = p | 'C' >> beam.Create([
            {"event_time": "2025-01-01T12:00:00Z", "product_id": "P1"},
            {"event_time": "2025-01-01T12:00:01Z", "product_id": "P1"},
        ])
        transactions = p | 'T' >> beam.Create([
            {"event_time": "2025-01-01T12:00:00Z",
                "product_id": "P1", "store_id": "S1", "qty": 2}
        ])
        stock = p | 'S' >> beam.Create([
            {"event_time": "2025-01-01T12:00:00Z",
                "product_id": "P1", "warehouse_id": "W1", "delta": 5}
        ])

        view_counts, sales_counts, stock_counts = sp.build_transforms(
            clicks, transactions, stock)

        # materialize view counts
        def to_kv(x):
            return x

        assert_that(view_counts, equal_to([("P1", 2)]), label='views')
        assert_that(sales_counts, equal_to(
            [(("P1", "S1"), 2), (("P1", "ALL"), 2)]), label='sales')
        assert_that(stock_counts, equal_to(
            [(("P1", "W1"), 5), (("P1", "ALL"), 5)]), label='stock')
