"""Tests for product incremental loading using hash strategy."""

from __future__ import annotations

import csv
from pathlib import Path

from sqlalchemy import select, text

from backend.app.models import DimFingerprint, DimProduct
from etl import settings as etl_settings
from etl.db import session_scope
from etl.incremental import run_products_incremental

FIXTURES_DIR = Path("backend/tests/fixtures/incremental")


def _reset_product_tables(db_engine) -> None:
    with db_engine.begin() as conn:
        conn.execute(text("TRUNCATE dw.fact_sales RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE dw.dim_product RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE meta.dim_fingerprint RESTART IDENTITY"))
        conn.execute(text("TRUNCATE meta.cdc_state RESTART IDENTITY"))


def _write_products_fixture(
    target: Path, price: str = "199.99", updated: str = "2024-01-01T00:00:00Z"
) -> None:
    source = FIXTURES_DIR / "products_base.csv"
    with source.open("r", encoding="utf-8", newline="") as fh:
        reader = list(csv.reader(fh))
    header = reader[0]
    row = reader[1]
    row[4] = price
    row[5] = updated
    with target.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerow(row)


def test_incremental_products_hash(monkeypatch, db_engine, tmp_path) -> None:
    _reset_product_tables(db_engine)

    monkeypatch.setattr(etl_settings.settings, "cdc_strategy_products", "hash")

    first_file = tmp_path / "products.csv"
    _write_products_fixture(first_file, price="199.99", updated="2024-01-01T00:00:00Z")

    result_first = run_products_incremental(source_glob=str(first_file))
    assert result_first.rows_out == 1
    assert result_first.no_change == 0

    with session_scope() as session:
        product = session.execute(
            select(DimProduct).where(DimProduct.sku == "SKU-001")
        ).scalar_one()
        assert product.price_list == 199.99
        fingerprint = session.execute(
            select(DimFingerprint.fingerprint).where(
                DimFingerprint.entity == "products",
                DimFingerprint.natural_key == "SKU-001",
            )
        ).scalar_one()
        assert len(fingerprint) == 64

    # Second run with identical payload should not trigger update when using hash strategy
    result_second = run_products_incremental(source_glob=str(first_file))
    assert result_second.rows_out == 0
    assert result_second.no_change == 1
    assert result_second.skipped == 0
