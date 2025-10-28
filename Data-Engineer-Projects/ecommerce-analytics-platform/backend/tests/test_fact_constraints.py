"""Fact table integrity checks."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etl.db import session_scope
from etl.dwh import (
    ensure_date_key,
    get_channel_id,
    insert_fact_sales,
    upsert_dim_customer,
    upsert_dim_product,
)
from etl.transformers import compute_email_hash


@pytest.mark.usefixtures("db_engine")
def test_fact_sales_constraints_enforced() -> None:
    customer_nk = "cust-fact"
    product_sku = "SKU-fact"

    with session_scope() as session:
        session.execute(text("DELETE FROM dw.fact_sales WHERE order_id = 'ORD-fact'"))
        session.execute(
            text("DELETE FROM dw.dim_customer WHERE customer_nk = :nk"),
            {"nk": customer_nk},
        )
        session.execute(
            text("DELETE FROM dw.dim_product WHERE sku = :sku"),
            {"sku": product_sku},
        )

    with session_scope() as session:
        customer_id = upsert_dim_customer(
            session,
            customer_nk=customer_nk,
            email_hash=compute_email_hash("fact@example.com"),
            first_name="Fact",
            last_name="Test",
            phone=None,
            country_code="US",
        )
        product_id = upsert_dim_product(
            session,
            sku=product_sku,
            name="Fact Product",
            brand=None,
            category=None,
            price_list=Decimal("10"),
        )
        channel_id = get_channel_id(session, "web")
        assert channel_id is not None
        date_key = ensure_date_key(session, date(2025, 1, 10), ensure_if_missing=True)
        assert date_key is not None

    with session_scope() as session, pytest.raises(IntegrityError):
        insert_fact_sales(
            session,
            date_key=date_key,
            customer_id=customer_id,
            product_id=product_id,
            channel_id=channel_id,
            order_id="ORD-fact",
            order_line_nbr=1,
            transaction_ts=datetime.now(UTC),
            currency_code="US",  # invalid
            quantity=1,
            unit_price=Decimal("10"),
            discount_amount=Decimal("0"),
            cost_amount=None,
        )

    with session_scope() as session, pytest.raises(IntegrityError):
        insert_fact_sales(
            session,
            date_key=date_key,
            customer_id=customer_id,
            product_id=product_id,
            channel_id=channel_id,
            order_id="ORD-fact",
            order_line_nbr=1,
            transaction_ts=datetime.now(UTC),
            currency_code="USD",
            quantity=0,  # invalid quantity
            unit_price=Decimal("10"),
            discount_amount=Decimal("0"),
            cost_amount=None,
        )

    with session_scope() as session:
        session.execute(text("DELETE FROM dw.fact_sales WHERE order_id = 'ORD-fact'"))
        session.execute(
            text("DELETE FROM dw.dim_customer WHERE customer_nk = :nk"),
            {"nk": customer_nk},
        )
        session.execute(
            text("DELETE FROM dw.dim_product WHERE sku = :sku"),
            {"sku": product_sku},
        )
