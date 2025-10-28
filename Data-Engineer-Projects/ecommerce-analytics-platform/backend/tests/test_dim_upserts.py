"""Tests for SCD-1 dimension upserts."""

from __future__ import annotations

import time

import pytest
from sqlalchemy import select, text

from backend.app.models import DimCustomer
from etl.db import session_scope
from etl.dwh import upsert_dim_customer
from etl.transformers import compute_email_hash


@pytest.mark.usefixtures("db_engine")
def test_customer_upsert_updates_metadata() -> None:
    customer_nk = "cust-upsert"
    email_hash = compute_email_hash("upsert@example.com")

    with session_scope() as session:
        session.execute(
            text("DELETE FROM dw.dim_customer WHERE customer_nk = :nk"),
            {"nk": customer_nk},
        )

    with session_scope() as session:
        upsert_dim_customer(
            session,
            customer_nk=customer_nk,
            email_hash=email_hash,
            first_name="First",
            last_name="User",
            phone=None,
            country_code="US",
        )

    with session_scope() as session:
        before = session.execute(
            select(DimCustomer.updated_at).where(DimCustomer.customer_nk == customer_nk)
        ).scalar_one()

    time.sleep(0.01)

    with session_scope() as session:
        upsert_dim_customer(
            session,
            customer_nk=customer_nk,
            email_hash=email_hash,
            first_name="First",
            last_name="User",
            phone="+15550000000",
            country_code="US",
        )

    with session_scope() as session:
        after = session.execute(
            select(DimCustomer.updated_at, DimCustomer.phone).where(
                DimCustomer.customer_nk == customer_nk
            )
        ).one()

    assert after.updated_at >= before
    assert after.phone == "+15550000000"

    with session_scope() as session:
        session.execute(
            text("DELETE FROM dw.dim_customer WHERE customer_nk = :nk"),
            {"nk": customer_nk},
        )
