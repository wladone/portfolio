"""Database access for sales analytics."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import case, func, select, text

from backend.app.models.dim_channel import DimChannel
from backend.app.models.dim_date import DimDate
from backend.app.models.dim_product import DimProduct
from backend.app.models.fact_sales import FactSales


class SalesRepository:
    """Repository for querying sales aggregates."""

    def __init__(self, session) -> None:
        self._session = session

    def fetch_sales_summary(
        self, dt_from: date, dt_to: date, channel: str | None, granularity: str
    ) -> list[dict[str, Any]]:
        """Fetch sales summary data."""
        if dt_from > dt_to:
            raise ValueError("dt_from cannot be after dt_to")

        if granularity == "day":
            query_str = """
                SELECT dd.date, dc.channel_code, v.orders, v.items, v.gross, v.discount, v.net, v.avg_order_value
                FROM dw.v_daily_sales_summary v
                JOIN dw.dim_date dd ON v.date_key = dd.date_key
                JOIN dw.dim_channel dc ON v.channel_id = dc.channel_id
                WHERE dd.date BETWEEN :dt_from AND :dt_to
            """
            params = {"dt_from": dt_from, "dt_to": dt_to}
            if channel:
                query_str += " AND dc.channel_code = :channel"
                params["channel"] = channel
            query = text(query_str)
            result = self._session.execute(query, params)
            return [
                {
                    "date": row.date,
                    "channel_code": row.channel_code,
                    "orders": row.orders,
                    "items": row.items,
                    "gross": row.gross,
                    "discount": row.discount,
                    "net": row.net,
                    "avg_order_value": row.avg_order_value,
                }
                for row in result
            ]

        elif granularity == "month":
            stmt = (
                select(
                    DimDate.year,
                    DimDate.month,
                    DimChannel.channel_code,
                    func.count(func.distinct(FactSales.order_id)).label("orders"),
                    func.sum(FactSales.quantity).label("items"),
                    func.sum(FactSales.unit_price * FactSales.quantity).label("gross"),
                    func.sum(FactSales.discount_amount).label("discount"),
                    func.sum(FactSales.net_amount).label("net"),
                    case(
                        (
                            func.count(func.distinct(FactSales.order_id)) > 0,
                            func.sum(FactSales.net_amount)
                            / func.count(func.distinct(FactSales.order_id)),
                        ),
                        else_=0,
                    ).label("avg_order_value"),
                )
                .select_from(FactSales)
                .join(DimDate, FactSales.date_key == DimDate.date_key)
                .join(DimChannel, FactSales.channel_id == DimChannel.channel_id)
                .where(DimDate.date.between(dt_from, dt_to))
                .group_by(DimDate.year, DimDate.month, DimChannel.channel_code)
            )
            if channel:
                stmt = stmt.where(DimChannel.channel_code == channel)
            result = self._session.execute(stmt)
            return [
                {
                    "year": row.year,
                    "month": row.month,
                    "channel_code": row.channel_code,
                    "orders": row.orders,
                    "items": row.items,
                    "gross": row.gross,
                    "discount": row.discount,
                    "net": row.net,
                    "avg_order_value": row.avg_order_value,
                }
                for row in result
            ]

        else:
            raise ValueError("Invalid granularity")

    def fetch_top_products(
        self,
        dt_from: date,
        dt_to: date,
        channel: str | None,
        metric: str,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch top products data."""
        if dt_from > dt_to:
            raise ValueError("dt_from cannot be after dt_to")

        # Get total count
        count_stmt = (
            select(func.count(func.distinct(FactSales.product_id)))
            .select_from(FactSales)
            .join(DimDate, FactSales.date_key == DimDate.date_key)
            .join(DimChannel, FactSales.channel_id == DimChannel.channel_id)
            .where(DimDate.date.between(dt_from, dt_to))
        )
        if channel:
            count_stmt = count_stmt.where(DimChannel.channel_code == channel)
        total_count = self._session.execute(count_stmt).scalar() or 0

        # Main query
        stmt = (
            select(
                DimProduct.sku,
                DimProduct.name,
                DimProduct.category,
                func.sum(FactSales.quantity).label("items"),
                func.sum(FactSales.unit_price * FactSales.quantity).label("gross"),
                func.sum(FactSales.net_amount).label("net"),
            )
            .select_from(FactSales)
            .join(DimProduct, FactSales.product_id == DimProduct.product_id)
            .join(DimDate, FactSales.date_key == DimDate.date_key)
            .join(DimChannel, FactSales.channel_id == DimChannel.channel_id)
            .where(DimDate.date.between(dt_from, dt_to))
            .group_by(
                DimProduct.product_id,
                DimProduct.sku,
                DimProduct.name,
                DimProduct.category,
            )
        )
        if channel:
            stmt = stmt.where(DimChannel.channel_code == channel)

        # Order by metric
        if metric == "net":
            stmt = stmt.order_by(func.sum(FactSales.net_amount).desc())
        elif metric == "items":
            stmt = stmt.order_by(func.sum(FactSales.quantity).desc())
        elif metric == "gross":
            stmt = stmt.order_by(
                func.sum(FactSales.unit_price * FactSales.quantity).desc()
            )
        else:
            raise ValueError("Invalid metric")

        stmt = stmt.limit(limit).offset(offset)
        result = self._session.execute(stmt)
        rows = [
            {
                "sku": row.sku,
                "name": row.name,
                "category": row.category or "",
                "items": row.items,
                "gross": row.gross,
                "net": row.net,
            }
            for row in result
        ]
        return rows, total_count

    async def bulk_insert_orders(self, session, orders: list[dict[str, Any]]) -> None:
        """Bulk insert orders into fact_sales table."""
        # This is a placeholder implementation - in a real scenario, you'd need to:
        # 1. Transform the order messages to fact_sales records
        # 2. Handle dimension lookups (customer, product, channel, date)
        # 3. Insert the records
        # For testing purposes, we'll just log the operation
        pass
        return rows, total_count
