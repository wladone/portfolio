"""Transformation helpers for ETL pipelines."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any

from .validators import CHANNEL_LITERAL

ALNUM_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")
NON_ALNUM_PATTERN = re.compile(r"[^0-9a-zA-Z]+")

ORDER_ALIASES = {
    "orderlinenbr": "order_line_nbr",
    "order_line_number": "order_line_nbr",
    "orderlinenumber": "order_line_nbr",
    "orderlineid": "order_line_nbr",
    "customer_id": "customer_nk",
    "customer_email": "email",
    "productid": "sku",
    "product_id": "sku",
    "producttitle": "product_name",
    "product_title": "product_name",
    "channel": "channel_code",
    "transaction_timestamp": "transaction_ts",
}

PRODUCT_ALIASES = {
    "productid": "sku",
    "product_id": "sku",
    "productname": "name",
    "product_name": "name",
}

CUSTOMER_ALIASES = {
    "customer_id": "customer_nk",
    "email_address": "email",
}


def to_snake_case(value: str) -> str:
    value = ALNUM_PATTERN.sub("_", value).lower()
    value = NON_ALNUM_PATTERN.sub("_", value)
    return value.strip("_")


def normalize_keys(record: dict[str, Any]) -> dict[str, Any]:
    return {to_snake_case(key): value for key, value in record.items()}


def normalize_order_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for record in records:
        mapped = _apply_aliases(normalize_keys(record), ORDER_ALIASES)
        normalized.append(_clean_record(mapped))
    return normalized


def normalize_product_records(
    records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for record in records:
        mapped = _apply_aliases(normalize_keys(record), PRODUCT_ALIASES)
        normalized.append(_clean_record(mapped))
    return normalized


def normalize_customer_records(
    records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for record in records:
        mapped = _apply_aliases(normalize_keys(record), CUSTOMER_ALIASES)
        normalized.append(_clean_record(mapped))
    return normalized


def compute_email_hash(email: str | None) -> str | None:
    if not email:
        return None
    normalized = email.strip().lower()
    if not normalized:
        return None
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return digest


def sanitize_channel_code(channel_code: str | None) -> CHANNEL_LITERAL | None:
    if channel_code is None:
        return None
    value = channel_code.strip().lower()
    if value in {"web", "mobile", "store", "marketplace"}:
        return value  # type: ignore[return-value]
    return None


def calculate_net_amount(
    quantity: int, unit_price: Decimal, discount_amount: Decimal
) -> Decimal:
    gross = quantity * unit_price
    net = gross - discount_amount
    return net if net >= Decimal("0") else Decimal("0")


def ensure_ts(
    record: dict[str, Any],
    *,
    field: str,
    fallback: datetime,
) -> datetime:
    """Return timestamp from record or fallback value."""
    value = record.get(field)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # Support ISO-8601 strings, forgiving trailing Z
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            return fallback
    return fallback


def _apply_aliases(record: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    mapped = dict(record)
    for alias, canonical in aliases.items():
        if alias in mapped and canonical not in mapped:
            mapped[canonical] = mapped.pop(alias)
    return mapped


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                value = None
        cleaned[key] = value
    return cleaned
