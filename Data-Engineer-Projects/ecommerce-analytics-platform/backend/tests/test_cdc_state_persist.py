"""Tests ensuring CDC state table is updated correctly across runs."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select, text

from backend.app.models import CdcState
from etl.db import session_scope
from etl.incremental import run_customers_incremental

FIXTURES_DIR = Path("backend/tests/fixtures/incremental")


def _reset_meta_tables(db_engine) -> None:
    with db_engine.begin() as conn:
        conn.execute(text("TRUNCATE dw.fact_sales RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE dw.dim_customer RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE meta.dim_fingerprint RESTART IDENTITY"))
        conn.execute(text("TRUNCATE meta.cdc_state RESTART IDENTITY"))


def _write_customers(records: list[dict[str, object]], target: Path) -> None:
    target.write_text(json.dumps(records), encoding="utf-8")


def test_cdc_state_persist(db_engine, tmp_path) -> None:
    _reset_meta_tables(db_engine)
    records = json.loads(
        (FIXTURES_DIR / "customers_base.json").read_text(encoding="utf-8")
    )

    first_file = tmp_path / "customers_run1.json"
    _write_customers(records, first_file)
    result_first = run_customers_incremental(source_glob=str(first_file))
    assert result_first.rows_out == 1

    # Modify timestamp and run again to advance watermark
    records[0]["phone"] = "+40700111222"
    records[0]["updated_at"] = "2024-04-01T00:00:00Z"
    second_file = tmp_path / "customers_run2.json"
    _write_customers(records, second_file)
    result_second = run_customers_incremental(source_glob=str(second_file))
    assert result_second.rows_out == 1

    with session_scope() as session:
        state = session.execute(
            select(CdcState).where(CdcState.entity == "customers")
        ).scalar_one()
        assert state.watermark_ts is not None
        assert state.watermark_ts.isoformat().startswith("2024-04-01")
        assert state.details is not None
        assert state.details["strategy"] == "watermark"
        assert state.details["rows_out"] >= 1
