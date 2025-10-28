"""Tests for customer incremental loading using watermark strategy."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select, text

from backend.app.models import DimCustomer
from etl.db import session_scope
from etl.incremental import run_customers_incremental

FIXTURES_DIR = Path("backend/tests/fixtures/incremental")


def _reset_cdc_tables(db_engine) -> None:
    with db_engine.begin() as conn:
        conn.execute(text("TRUNCATE dw.fact_sales RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE dw.dim_customer RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE meta.dim_fingerprint RESTART IDENTITY"))
        conn.execute(text("TRUNCATE meta.cdc_state RESTART IDENTITY"))


def _load_customer_payload() -> list[dict[str, object]]:
    fixture = FIXTURES_DIR / "customers_base.json"
    return json.loads(fixture.read_text(encoding="utf-8"))


def test_incremental_customers_watermark(db_engine, tmp_path) -> None:
    _reset_cdc_tables(db_engine)

    base_records = _load_customer_payload()
    first_file = tmp_path / "customers_0001.json"
    first_file.write_text(json.dumps(base_records), encoding="utf-8")

    result_initial = run_customers_incremental(source_glob=str(first_file))
    assert result_initial.rows_out == 1
    assert result_initial.no_change == 0

    with session_scope() as session:
        customer = session.execute(
            select(DimCustomer).where(DimCustomer.customer_nk == "CUST-001")
        ).scalar_one()
        first_phone = customer.phone
        first_updated = customer.updated_at

    # Same customer with older timestamp should be skipped (no_change incremented)
    older_records = _load_customer_payload()
    older_records[0]["phone"] = "+40000000001"
    older_records[0]["updated_at"] = "2023-12-01T00:00:00Z"
    older_file = tmp_path / "customers_0002.json"
    older_file.write_text(json.dumps(older_records), encoding="utf-8")

    result_older = run_customers_incremental(source_glob=str(older_file))
    assert result_older.rows_out == 0
    assert result_older.no_change == 1
    assert result_older.skipped == 1

    with session_scope() as session:
        customer_after_skip = session.execute(
            select(DimCustomer).where(DimCustomer.customer_nk == "CUST-001")
        ).scalar_one()
        assert customer_after_skip.phone == first_phone

    # Newer record with changed phone should update dimension
    newer_records = _load_customer_payload()
    newer_records[0]["phone"] = "+40788001122"
    newer_records[0]["updated_at"] = "2024-03-01T00:00:00Z"
    newer_file = tmp_path / "customers_0003.json"
    newer_file.write_text(json.dumps(newer_records), encoding="utf-8")

    result_newer = run_customers_incremental(source_glob=str(newer_file))
    assert result_newer.rows_out == 1
    assert result_newer.no_change == 0

    with session_scope() as session:
        customer_latest = session.execute(
            select(DimCustomer).where(DimCustomer.customer_nk == "CUST-001")
        ).scalar_one()
        assert customer_latest.phone == "+40788001122"
        assert customer_latest.updated_at > first_updated
        assert customer_latest.updated_at.tzinfo is not None
