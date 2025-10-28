"""Ensure staging tables deduplicate payloads via unique hash."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from etl.db import session_scope
from etl.loaders import stage_records
from etl.validators import OrderRaw


@pytest.mark.usefixtures("db_engine")
def test_staging_deduplicates_payloads() -> None:
    source_file = "tests/fixtures/orders_one.json"
    order = OrderRaw.model_validate(
        {
            "order_id": "ORD-dup",
            "order_line_nbr": 1,
            "customer_nk": "cust-dup",
            "email": "duplicate@example.com",
            "sku": "SKU-dup",
            "quantity": 1,
            "unit_price": "10.00",
            "discount_amount": "0",
            "currency_code": "USD",
            "channel_code": "web",
            "transaction_ts": "2025-01-01T00:00:00Z",
        }
    )

    with session_scope() as session:
        session.execute(
            text("DELETE FROM stg.orders_raw WHERE source_file = :sf"),
            {"sf": source_file},
        )

    with session_scope() as session:
        inserted_first = stage_records(session, "orders", [order], source_file)
        inserted_second = stage_records(session, "orders", [order], source_file)

    assert inserted_first == 1
    assert inserted_second == 0

    with session_scope() as session:
        session.execute(
            text("DELETE FROM stg.orders_raw WHERE source_file = :sf"),
            {"sf": source_file},
        )
