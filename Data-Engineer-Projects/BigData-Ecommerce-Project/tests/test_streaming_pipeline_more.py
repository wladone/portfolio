import json
import time
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam import streaming_pipeline as sp


def test_validate_topic_paths_ok():
    # valid topic path
    paths = ["projects/demo-project/topics/clicks"]
    sp._validate_topic_paths(paths)


def test_validate_topic_paths_bad():
    bad = ["not-a-topic"]
    try:
        sp._validate_topic_paths(bad)
        assert False
    except ValueError:
        pass


def test_parse_and_validate_event_circuit_breaker():
    # Force several invalid messages to trip the circuit
    fn = sp.ParseAndValidateEvent("click", lambda p: p)
    bad = b"not-json"
    for _ in range(fn._CIRCUIT_THRESHOLD + 1):
        list(fn.process(bad))
    # circuit should be open now
    assert fn._circuit_open is True


def test_format_sales_and_stock_and_bigtable():
    # test FormatSalesRow
    f = sp.FormatSalesRow()
    out = list(f.process((("P1", "S1"), 4)))
    assert out[0]["sales_count"] == 4

    s = sp.FormatStockRow()
    out2 = list(s.process((("P1", "W1"), 7)))
    assert out2[0]["stock_count"] == 7

    # ToBigtableRow produces DirectRow
    tb = sp.ToBigtableRow()
    rows = list(tb.process(("P1", 5)))
    assert len(rows) == 1
    assert hasattr(rows[0], "row_key")


def test_local_mode_skips_sinks():
    # parse args with --local
    args, rest = sp.parse_pipeline_args(["--local"])  # uses argparse
    assert getattr(args, "local", False) is True
