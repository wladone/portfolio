"""Pydantic models and validation helpers for ETL inputs."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

CHANNEL_LITERAL = Literal["web", "mobile", "store", "marketplace"]

ModelType = TypeVar("ModelType", bound=BaseModel)


class OrderRaw(BaseModel):
    """Validated representation of an order line."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    order_id: str
    order_line_nbr: int = Field(gt=0)
    customer_nk: str
    email: str | None = None
    sku: str
    product_name: str | None = None
    brand: str | None = None
    category: str | None = None
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=0)
    discount_amount: Decimal = Field(default=Decimal("0"), ge=0)
    currency_code: str
    channel_code: CHANNEL_LITERAL
    transaction_ts: datetime

    @field_validator("currency_code")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        upper = value.strip().upper()
        if len(upper) != 3 or not upper.isalpha():
            raise ValueError("currency_code must be ISO-4217 alpha-3")
        return upper


class ProductRaw(BaseModel):
    """Validated representation of a product record."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    sku: str
    name: str
    brand: str | None = None
    category: str | None = None
    price_list: Decimal | None = Field(default=None, ge=0)
    updated_at: datetime | None = None


class CustomerRaw(BaseModel):
    """Validated representation of a customer record."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    customer_nk: str
    email: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    phone: str | None = None
    country_code: str
    updated_at: datetime | None = None

    @field_validator("country_code")
    @classmethod
    def normalize_country_code(cls, value: str) -> str:
        upper = value.strip().upper()
        if len(upper) != 2 or not upper.isalpha():
            raise ValueError("country_code must be a two-character ISO code")
        return upper


def validate_orders(
    records: Iterable[dict[str, Any]],
) -> tuple[list[OrderRaw], list[ValidationError]]:
    """Validate iterable of order dictionaries."""
    return _validate_collection(records, OrderRaw)


def validate_products(
    records: Iterable[dict[str, Any]],
) -> tuple[list[ProductRaw], list[ValidationError]]:
    """Validate iterable of product dictionaries."""
    return _validate_collection(records, ProductRaw)


def validate_customers(
    records: Iterable[dict[str, Any]],
) -> tuple[list[CustomerRaw], list[ValidationError]]:
    """Validate iterable of customer dictionaries."""
    return _validate_collection(records, CustomerRaw)


def _validate_collection(
    records: Iterable[dict[str, Any]], model_cls: type[ModelType]
) -> tuple[list[ModelType], list[ValidationError]]:
    valid: list[ModelType] = []
    errors: list[ValidationError] = []
    for record in records:
        try:
            valid.append(model_cls.model_validate(record))
        except ValidationError as exc:
            errors.append(exc)
    return valid, errors
