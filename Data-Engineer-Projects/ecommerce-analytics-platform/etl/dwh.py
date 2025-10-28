"""Warehouse persistence helpers."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import Select, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.models import DimChannel, DimCustomer, DimDate, DimProduct, FactSales

from .transformers import calculate_net_amount

logger = structlog.get_logger(__name__)


def upsert_dim_customer(
    session: Session,
    *,
    customer_nk: str,
    email_hash: str | None,
    first_name: str | None,
    last_name: str | None,
    phone: str | None,
    country_code: str | None,
) -> int:
    email_hash_value = email_hash if email_hash else "0" * 64
    values: dict[str, Any] = {
        "customer_nk": customer_nk,
        "email_hash": email_hash_value,
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone,
        "country_code": country_code,
    }
    stmt = insert(DimCustomer).values(**values)
    updates: dict[str, Any] = {
        "email_hash": stmt.excluded.email_hash,
        "first_name": stmt.excluded.first_name,
        "last_name": stmt.excluded.last_name,
        "phone": stmt.excluded.phone,
        "country_code": stmt.excluded.country_code,
        "updated_at": func.now(),
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[DimCustomer.customer_nk],
        set_=updates,
        where=tuple_(
            DimCustomer.email_hash,
            DimCustomer.first_name,
            DimCustomer.last_name,
            DimCustomer.phone,
            DimCustomer.country_code,
        ).is_distinct_from(
            tuple_(
                stmt.excluded.email_hash,
                stmt.excluded.first_name,
                stmt.excluded.last_name,
                stmt.excluded.phone,
                stmt.excluded.country_code,
            )
        ),
    )
    # Execute and prefer returned id. Fallback to select by customer_nk
    result = session.execute(stmt.returning(DimCustomer.customer_id))
    customer_id = result.scalar_one_or_none()
    if customer_id is not None:
        return customer_id

    sel = select(DimCustomer.customer_id).where(DimCustomer.customer_nk == customer_nk)
    return session.execute(sel).scalar_one()


def upsert_dim_product(
    session: Session,
    *,
    sku: str,
    name: str,
    brand: str | None,
    category: str | None,
    price_list: Decimal | None,
) -> int:
    values: dict[str, Any] = {
        "sku": sku,
        "name": name,
        "brand": brand,
        "category": category,
        "price_list": price_list,
    }
    stmt = insert(DimProduct).values(**values)
    updates: dict[str, Any] = {
        "name": stmt.excluded.name,
        "brand": stmt.excluded.brand,
        "category": stmt.excluded.category,
        "price_list": stmt.excluded.price_list,
        "updated_at": func.now(),
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[DimProduct.sku],
        set_=updates,
        where=tuple_(
            DimProduct.name,
            DimProduct.brand,
            DimProduct.category,
            DimProduct.price_list,
        ).is_distinct_from(
            tuple_(
                stmt.excluded.name,
                stmt.excluded.brand,
                stmt.excluded.category,
                stmt.excluded.price_list,
            )
        ),
    )
    # Execute the upsert and prefer the returned id when present.
    result = session.execute(stmt.returning(DimProduct.product_id))
    product_id = result.scalar_one_or_none()
    if product_id is not None:
        return product_id

    # Fallback: when the DB did not return a row (some ON CONFLICT paths
    # may not return), select the product id by SKU.
    sel = select(DimProduct.product_id).where(DimProduct.sku == sku)
    product_id = session.execute(sel).scalar_one()
    return product_id


def get_channel_id(session: Session, channel_code: str) -> int | None:
    statement: Select[tuple[int]] = select(DimChannel.channel_id).where(
        DimChannel.channel_code == channel_code
    )
    channel_id = session.execute(statement).scalar_one_or_none()
    if channel_id is None:
        logger.warning("missing_channel_dimension", channel_code=channel_code)
    return channel_id


def ensure_date_key(
    session: Session, txn_date: date, *, ensure_if_missing: bool
) -> int | None:
    date_key = int(txn_date.strftime("%Y%m%d"))
    statement: Select[tuple[int]] = select(DimDate.date_key).where(
        DimDate.date_key == date_key
    )
    existing = session.execute(statement).scalar_one_or_none()
    if existing is not None:
        return existing
    if not ensure_if_missing:
        logger.warning("missing_date_dimension", date=str(txn_date))
        return None

    iso = txn_date.isocalendar()
    insert_stmt = (
        insert(DimDate)
        .values(
            date_key=date_key,
            date=txn_date,
            year=txn_date.year,
            quarter=((txn_date.month - 1) // 3) + 1,
            month=txn_date.month,
            day=txn_date.day,
            iso_week=iso.week,
            dow=iso.weekday,
            is_weekend=txn_date.weekday() >= 5,
        )
        .on_conflict_do_nothing(index_elements=[DimDate.date_key])
    )
    session.execute(insert_stmt)
    return date_key


def insert_fact_sales(
    session: Session,
    *,
    date_key: int,
    customer_id: int,
    product_id: int,
    channel_id: int,
    order_id: str,
    order_line_nbr: int,
    transaction_ts: datetime,
    currency_code: str,
    quantity: int,
    unit_price: Decimal,
    discount_amount: Decimal,
    cost_amount: Decimal | None,
) -> bool:
    net_amount = calculate_net_amount(quantity, unit_price, discount_amount)
    stmt = (
        insert(FactSales)
        .values(
            date_key=date_key,
            customer_id=customer_id,
            product_id=product_id,
            channel_id=channel_id,
            order_id=order_id,
            order_line_nbr=order_line_nbr,
            transaction_ts=transaction_ts,
            currency_code=currency_code.upper(),
            quantity=quantity,
            unit_price=unit_price,
            discount_amount=discount_amount,
            net_amount=net_amount,
            cost_amount=cost_amount,
        )
        .on_conflict_do_nothing(
            index_elements=[FactSales.order_id, FactSales.order_line_nbr]
        )
    )
    try:
        result = session.execute(stmt)
    except IntegrityError as exc:
        logger.error(
            "fact_insert_failed",
            order_id=order_id,
            order_line_nbr=order_line_nbr,
            error=str(exc.orig),
        )
        raise
    rowcount = int(getattr(result, "rowcount", 0) or 0)
    return rowcount > 0
