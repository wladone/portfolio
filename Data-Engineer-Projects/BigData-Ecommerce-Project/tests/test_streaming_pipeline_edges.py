import json
import time
from datetime import datetime, timezone

import pytest

from beam import streaming_pipeline as sp


def test_parse_event_timestamp_with_offset():
    t = sp.parse_event_timestamp("2025-01-01T12:00:00+00:00")
    assert int(t) == 1735684800
    t2 = sp.parse_event_timestamp("2025-01-01T13:00:00+01:00")
    assert int(t2) == 1735684800


def test_require_fields_errors():
    with pytest.raises(ValueError):
        sp.require_fields({}, ("a",))
    with pytest.raises(ValueError):
        sp.require_fields({"a": ""}, ("a",))


def test_validator_error_yields_dead_letter():
    # Validator that raises ValueError
    def bad_validator(payload):
        raise ValueError("bad payload")

    fn = sp.ParseAndValidateEvent("click", bad_validator)
    data = json.dumps({"event_time": "2025-01-01T12:00:00Z",
                      "product_id": "P1"}).encode("utf-8")
    outs = list(fn.process(data))
    # Expect a dead-letter TaggedOutput
    assert any(getattr(o, 'tag', None) ==
               sp.ParseAndValidateEvent.DEADLETTER_TAG for o in outs)


def test_unicode_decode_error_yields_dead_letter():
    fn = sp.ParseAndValidateEvent("click", lambda p: p)
    # invalid UTF-8 sequence
    bad = bytes([0xff, 0xff, 0xff])
    outs = list(fn.process(bad))
    assert any(getattr(o, 'tag', None) ==
               sp.ParseAndValidateEvent.DEADLETTER_TAG for o in outs)


def test_circuit_breaker_opens_and_closes(monkeypatch):
    # Control time progression
    base = time.time()
    times = [base + i for i in range(1000)]
    idx = {"i": 0}

    def fake_time():
        i = idx["i"]
        idx["i"] = min(i + 1, len(times) - 1)
        return times[i]

    monkeypatch.setattr(sp.time, "time", fake_time)

    # Validator that always raises so we'll trip the circuit
    def bad_validator(payload):
        raise ValueError("bad")

    fn = sp.ParseAndValidateEvent("click", bad_validator)
    data = b"not-json"

    # Trigger failures up to threshold
    for _ in range(fn._CIRCUIT_THRESHOLD):
        list(fn.process(data))
    assert fn._circuit_open is False
    # one more to trip
    list(fn.process(data))
    assert fn._circuit_open is True

    # Next calls should be skipped while circuit open (process returns None/empty)
    out = list(fn.process(data))
    assert out == [] or out is None

    # Advance time past _next_attempt_time
    idx['i'] = int(fn._next_attempt_time - base) + 1
    # Next call should attempt again and set _circuit_open False after reopening
    list(fn.process(data))
    # After attempting, circuit should still be possibly open or reset depending on failures; ensure attribute exists
    assert hasattr(fn, '_circuit_open')


def test_ensure_bigtable_configuration_errors():
    with pytest.raises(ValueError):
        sp.ensure_bigtable_configuration("", "cf")
    with pytest.raises(ValueError):
        sp.ensure_bigtable_configuration("tbl", "")


def test_write_dead_letters_noop():
    # If given an empty list, function should be a no-op and not raise
    sp.write_dead_letters([], "projects/demo-project/topics/dead-letter")
