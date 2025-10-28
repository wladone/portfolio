"""Validate key constraints on warehouse tables."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError


@dataclass
class DimensionIds:
    """Container for dimension surrogate keys used in fact checks."""

    date_key: int
    customer_id: int
    product_id: int
    channel_id: int


def _seed_dimensions(conn: Connection) -> DimensionIds:
    """Insert temporary dimension rows (rolled back after test)."""
    customer_id = conn.execute(
        text(
            """
            INSERT INTO dw.dim_customer (customer_nk, email_hash, country_code)
            VALUES (:nk, :hash, :country)
            RETURNING customer_id
            """
        ),
        {
            "nk": f"customer-{uuid.uuid4()}",
            "hash": "a" * 64,
            "country": "US",
        },
    ).scalar_one()

    product_id = conn.execute(
        text(
            """
            INSERT INTO dw.dim_product (sku, name, category)
            VALUES (:sku, :name, :category)
            RETURNING product_id
            """
        ),
        {
            "sku": f"SKU-{uuid.uuid4().hex[:8]}",
            "name": "Test Product",
            "category": "Test Category",
        },
    ).scalar_one()

    channel_id = conn.execute(
        text(
            """
            INSERT INTO dw.dim_channel (channel_code, channel_name, is_digital)
            VALUES (:code, :name, :is_digital)
            RETURNING channel_id
            """
        ),
        {
            "code": f"channel-{uuid.uuid4().hex[:8]}",
            "name": "Test Channel",
            "is_digital": True,
        },
    ).scalar_one()

    base_date = date(2025, 1, 1)
    iso = base_date.isocalendar()
    date_key = int(base_date.strftime("%Y%m%d"))
    conn.execute(
        text(
            """
            INSERT INTO dw.dim_date
                (date_key, date, year, quarter, month, day, iso_week, dow, is_weekend)
            VALUES
                (:date_key, :date, :year, :quarter, :month, :day, :iso_week, :dow, :is_weekend)
            """
        ),
        {
            "date_key": date_key,
            "date": base_date,
            "year": base_date.year,
            "quarter": ((base_date.month - 1) // 3) + 1,
            "month": base_date.month,
            "day": base_date.day,
            "iso_week": iso.week,
            "dow": iso.weekday,
            "is_weekend": base_date.weekday() >= 5,
        },
    )

    return DimensionIds(
        date_key=date_key,
        customer_id=customer_id,
        product_id=product_id,
        channel_id=channel_id,
    )


def test_dim_product_unique_sku(db_engine: Engine) -> None:
    """`dim_product.sku` must be unique."""
    with db_engine.connect() as conn:
        trans = conn.begin()
        try:
            sku = f"SKU-{uuid.uuid4().hex[:8]}"
            conn.execute(
                text("INSERT INTO dw.dim_product (sku, name) VALUES (:sku, :name)"),
                {"sku": sku, "name": "Original"},
            )
            with pytest.raises(IntegrityError):
                conn.execute(
                    text("INSERT INTO dw.dim_product (sku, name) VALUES (:sku, :name)"),
                    {"sku": sku, "name": "Duplicate"},
                )
        finally:
            trans.rollback()


def test_dim_customer_unique_nk(db_engine: Engine) -> None:
    """`dim_customer.customer_nk` must be unique."""
    with db_engine.connect() as conn:
        trans = conn.begin()
        try:
            nk = f"customer-{uuid.uuid4()}"
            payload = {
                "nk": nk,
                "hash": "b" * 64,
                "country": "FR",
            }
            conn.execute(
                text(
                    """
                    INSERT INTO dw.dim_customer (customer_nk, email_hash, country_code)
                    VALUES (:nk, :hash, :country)
                    """
                ),
                payload,
            )
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        """
                        INSERT INTO dw.dim_customer (customer_nk, email_hash, country_code)
                        VALUES (:nk, :hash, :country)
                        """
                    ),
                    payload,
                )
        finally:
            trans.rollback()


def test_fact_sales_foreign_keys(db_engine: Engine) -> None:
    """fact_sales must reference all associated dimensions."""
    inspector = inspect(db_engine)
    fks = inspector.get_foreign_keys("fact_sales", schema="dw")

    expected = {
        ("date_key",): ("dw", "dim_date"),
        ("customer_id",): ("dw", "dim_customer"),
        ("product_id",): ("dw", "dim_product"),
        ("channel_id",): ("dw", "dim_channel"),
    }

    for constrained_cols, target in expected.items():
        assert any(
            fk["constrained_columns"] == list(constrained_cols)
            and fk["referred_table"] == target[1]
            and (fk.get("referred_schema") or "dw") == target[0]
            for fk in fks
        ), f"Missing FK for columns {constrained_cols}"


def test_fact_sales_currency_check(db_engine: Engine) -> None:
    """fact_sales should reject malformed currency codes."""
    with db_engine.connect() as conn:
        trans = conn.begin()
        try:
            dims = _seed_dimensions(conn)
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        """
                        INSERT INTO dw.fact_sales (
                            date_key, customer_id, product_id, channel_id,
                            order_id, order_line_nbr, transaction_ts,
                            currency_code, quantity, unit_price, net_amount
                        )
                        VALUES (
                            :date_key, :customer_id, :product_id, :channel_id,
                            :order_id, :order_line_nbr, :transaction_ts,
                            :currency_code, :quantity, :unit_price, :net_amount
                        )
                        """
                    ),
                    {
                        "date_key": dims.date_key,
                        "customer_id": dims.customer_id,
                        "product_id": dims.product_id,
                        "channel_id": dims.channel_id,
                        "order_id": f"ORD-{uuid.uuid4()}",
                        "order_line_nbr": 1,
                        "transaction_ts": datetime.now(UTC),
                        "currency_code": "usd",  # lower-case should fail check
                        "quantity": 1,
                        "unit_price": Decimal("10.00"),
                        "net_amount": Decimal("10.00"),
                    },
                )
        finally:
            trans.rollback()


def test_fact_sales_quantity_check(db_engine: Engine) -> None:
    """fact_sales should enforce strictly positive quantities."""
    with db_engine.connect() as conn:
        trans = conn.begin()
        try:
            dims = _seed_dimensions(conn)
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        """
                        INSERT INTO dw.fact_sales (
                            date_key, customer_id, product_id, channel_id,
                            order_id, order_line_nbr, transaction_ts,
                            currency_code, quantity, unit_price, net_amount
                        )
                        VALUES (
                            :date_key, :customer_id, :product_id, :channel_id,
                            :order_id, :order_line_nbr, :transaction_ts,
                            :currency_code, :quantity, :unit_price, :net_amount
                        )
                        """
                    ),
                    {
                        "date_key": dims.date_key,
                        "customer_id": dims.customer_id,
                        "product_id": dims.product_id,
                        "channel_id": dims.channel_id,
                        "order_id": f"ORD-{uuid.uuid4()}",
                        "order_line_nbr": 1,
                        "transaction_ts": datetime.now(UTC),
                        "currency_code": "USD",
                        "quantity": 0,  # should fail quantity > 0 check
                        "unit_price": Decimal("10.00"),
                        "net_amount": Decimal("0.00"),
                    },
                )
        finally:
            trans.rollback()
