import json
import time
from datetime import datetime, timezone

import pytest

from beam import streaming_pipeline as sp


def _ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp()


class TestParseAndValidateEvent:
    def test_process_valid_event(self):
        dofn = sp.ParseAndValidateEvent("click", sp.validate_click)
        payload = {"event_time": "2023-01-01T00:00:05Z", "product_id": "P123"}
        encoded = json.dumps(payload).encode("utf-8")

        results = list(dofn.process(encoded))

        assert len(results) == 1
        assert results[0].value["product_id"] == "P123"
        assert pytest.approx(results[0].timestamp) == _ts(
            "2023-01-01T00:00:05Z")

    def test_process_invalid_event_trips_circuit_breaker(self, monkeypatch):
        dofn = sp.ParseAndValidateEvent("click", sp.validate_click)
        bad_payload = b"not-json"

        for _ in range(dofn._CIRCUIT_THRESHOLD):
            list(dofn.process(bad_payload))

        assert dofn._circuit_open is True
        # Advance time beyond timeout to ensure circuit can close again
        monkeypatch.setattr(time, "time", lambda: dofn._next_attempt_time + 1)
        list(dofn.process(json.dumps(
            {"event_time": "2023-01-01T00:00:10Z", "product_id": "P1"}).encode()))
        assert dofn._circuit_open is False


@pytest.mark.parametrize(
    "paths",
    [
        [
            "projects/demo-project/topics/clicks",
            "projects/demo-project/topics/transactions",
            "projects/demo-project/topics/stock",
            "projects/demo-project/topics/dead-letter",
        ]
    ],
)
def test_validate_topic_paths_accepts_valid(paths):
    sp._validate_topic_paths(paths)


@pytest.mark.parametrize(
    "path",
    [
        "projects/demo-project/topics/invalid path",
        "projects/demo-project/topics/",
        "not/a/path",
    ],
)
def test_validate_topic_paths_rejects_invalid(path):
    with pytest.raises(ValueError):
        sp._validate_topic_paths([path])


def test_format_views_row_enriches_window():
    dofn = sp.FormatViewsRow()
    window = type("WindowStub", (), {
        "start": type("Start", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)})(),
        "end": type("End", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 1, tzinfo=timezone.utc)})(),
    })()

    rows = list(dofn.process(("P1", 3), window=window))

    assert rows == [
        {
            "product_id": "P1",
            "view_count": 3,
            "window_start": "2023-01-01T00:00:00Z",
            "window_end": "2023-01-01T00:01:00Z",
        }
    ]


def test_ensure_bigtable_configuration():
    # Should not raise
    sp.ensure_bigtable_configuration("test_table", "test_family")


def test_ensure_bigtable_configuration_missing_table():
    with pytest.raises(ValueError, match="Bigtable table must be configured"):
        sp.ensure_bigtable_configuration("", "test_family")


def test_ensure_bigtable_configuration_missing_family():
    with pytest.raises(ValueError, match="Bigtable column family must be configured"):
        sp.ensure_bigtable_configuration("test_table", "")


def test_parse_pipeline_args():
    args, _ = sp.parse_pipeline_args([
        "--input_topic_clicks", "clicks",
        "--input_topic_transactions", "transactions",
        "--input_topic_stock", "stock",
        "--dead_letter_topic", "dlq",
        "--output_bigquery_dataset", "dataset",
        "--bigtable_project", "project",
        "--bigtable_instance", "instance"
    ])
    assert args.input_topic_clicks == "clicks"
    assert args.bigtable_project == "project"


def test_setup_pipeline_options():
    # Just check it creates options without error
    options = sp.setup_pipeline_options([])
    assert options is not None


def test_format_sales_row():
    dofn = sp.FormatSalesRow()
    window = type("WindowStub", (), {
        "start": type("Start", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)})(),
        "end": type("End", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 5, tzinfo=timezone.utc)})(),
    })()

    rows = list(dofn.process((("P123", "S1"), 10), window=window))

    assert rows == [
        {
            "product_id": "P123",
            "store_id": "S1",
            "sales_count": 10,
            "window_start": "2023-01-01T00:00:00Z",
            "window_end": "2023-01-01T00:05:00Z",
        }
    ]


def test_format_stock_row():
    dofn = sp.FormatStockRow()
    window = type("WindowStub", (), {
        "start": type("Start", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)})(),
        "end": type("End", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 5, tzinfo=timezone.utc)})(),
    })()

    rows = list(dofn.process((("P123", "W1"), 5), window=window))

    assert rows == [
        {
            "product_id": "P123",
            "warehouse_id": "W1",
            "stock_count": 5,
            "window_start": "2023-01-01T00:00:00Z",
            "window_end": "2023-01-01T00:05:00Z",
        }
    ]


def test_to_bigtable_row():
    dofn = sp.ToBigtableRow()
    window = type("WindowStub", (), {
        "end": type("End", (), {"to_utc_datetime": lambda self: datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)})(),
    })()

    rows = list(dofn.process(("P123", 15), window=window))

    # Since DirectRow is mocked, check the row_key
    assert rows[0].row_key == b"P123"
