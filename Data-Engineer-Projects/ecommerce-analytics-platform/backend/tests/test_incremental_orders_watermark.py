"""Tests for order incremental loading with watermark and uniqueness checks."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select, text

from backend.app.models import FactSales
from etl.db import session_scope
from etl.dwh import upsert_dim_customer, upsert_dim_product
from etl.incremental import run_orders_incremental
from etl.transformers import compute_email_hash

FIXTURES_DIR = Path("backend/tests/fixtures/incremental")


def _reset_database(db_engine) -> None:
    with db_engine.begin() as conn:
        conn.execute(text("TRUNCATE dw.fact_sales RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE dw.dim_customer RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE dw.dim_product RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE meta.dim_fingerprint RESTART IDENTITY"))
        conn.execute(text("TRUNCATE meta.cdc_state RESTART IDENTITY"))


def _prepare_dimensions() -> None:
    with session_scope() as session:
        upsert_dim_customer(
            session,
            customer_nk="CUST-001",
            email_hash=compute_email_hash("alice@example.com"),
            first_name="Alice",
            last_name="Pop",
            phone="+40111222333",
            country_code="RO",
        )
        upsert_dim_product(
            session,
            sku="SKU-001",
            name="Sample Product",
            brand="Acme",
            category="Electronics",
            price_list=199.99,
        )


def _load_orders_fixture() -> list[dict[str, object]]:
    fixture = FIXTURES_DIR / "orders_base.json"
    return json.loads(fixture.read_text(encoding="utf-8"))


def _write_orders(records: list[dict[str, object]], target: Path) -> None:
    target.write_text(json.dumps(records), encoding="utf-8")


def _fact_sales_count() -> int:
    with session_scope() as session:
        return session.execute(select(func.count()).select_from(FactSales)).scalar_one()


def test_incremental_orders_watermark(db_engine, tmp_path) -> None:
    _reset_database(db_engine)
    _prepare_dimensions()

    # First run inserts baseline order
    first_records = _load_orders_fixture()
    first_path = tmp_path / "orders_0001.json"
    _write_orders(first_records, first_path)

    result_first = run_orders_incremental(
        source_glob=str(first_path),
        ensure_dim_date=True,
    )
    assert result_first.rows_out == 1
    assert result_first.no_change == 0
    assert _fact_sales_count() == 1

    # Second run with older timestamp should be skipped
    older_records = _load_orders_fixture()
    older_records[0]["order_id"] = "ORD-101"
    older_records[0]["order_line_nbr"] = 1
    older_records[0]["transaction_ts"] = "2023-12-15T09:00:00Z"
    older_path = tmp_path / "orders_older.json"
    _write_orders(older_records, older_path)

    result_older = run_orders_incremental(
        source_glob=str(older_path),
        ensure_dim_date=True,
    )
    assert result_older.rows_out == 0
    assert result_older.skipped == 1
    assert _fact_sales_count() == 1

    # Third run with newer timestamp adds another fact row
    newer_records = _load_orders_fixture()
    newer_records[0]["order_id"] = "ORD-102"
    newer_records[0]["transaction_ts"] = "2024-02-02T11:00:00Z"
    newer_path = tmp_path / "orders_newer.json"
    _write_orders(newer_records, newer_path)

    result_newer = run_orders_incremental(
        source_glob=str(newer_path),
        ensure_dim_date=True,
    )
    assert result_newer.rows_out == 1
    assert _fact_sales_count() == 2

    # Reprocess same newer file forcing processing via from_ts should hit duplicates
    result_duplicate = run_orders_incremental(
        source_glob=str(newer_path),
        ensure_dim_date=True,
        from_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert result_duplicate.rows_out == 0
    assert result_duplicate.duplicates == 1
    assert result_duplicate.no_change >= 1
    assert _fact_sales_count() == 2
