"""Schema definitions for sales-related payloads."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, conint


class SalesSummaryParams(BaseModel):
    """Request parameters for sales summary analytics."""

    from_: date | None = None
    to: date | None = None
    channel: str | None = None
    granularity: Literal["day", "month"] = "day"


class TopProductsParams(BaseModel):
    """Request parameters for top products analytics."""

    from_: date | None = None
    to: date | None = None
    channel: str | None = None
    metric: Literal["net", "items", "gross"] = "net"
    limit: conint(gt=0, le=200) = 50
    offset: conint(ge=0) = 0


class SalesSummaryRow(BaseModel):
    """Row data for sales summary response."""

    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    date: date | None = None
    year: int | None = None
    month: int | None = None
    channel_code: str
    orders: int
    items: int
    gross: Decimal
    discount: Decimal
    net: Decimal
    avg_order_value: Decimal


class SalesSummaryResponse(BaseModel):
    """Response payload for sales summary analytics."""

    rows: list[SalesSummaryRow]
    from_: date | None
    to: date | None
    channel: str | None
    granularity: Literal["day", "month"]


class TopProductRow(BaseModel):
    """Row data for top products response."""

    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    sku: str
    name: str
    category: str
    items: Decimal
    gross: Decimal
    net: Decimal


class TopProductsResponse(BaseModel):
    """Response payload for top products analytics."""

    rows: list[TopProductRow]
    metric: Literal["net", "items", "gross"]
    limit: int
    offset: int
    total: int
