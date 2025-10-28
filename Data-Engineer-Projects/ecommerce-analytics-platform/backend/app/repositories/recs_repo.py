"""Database access for recommendations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import distinct, select

from backend.app.models.dim_product import DimProduct
from backend.app.models.fact_sales import FactSales


class RecsRepository:
    """Repository for querying recommendation data."""

    def __init__(self, session) -> None:
        self._session = session

    def map_sku_to_product_id(self, sku: str) -> int | None:
        """Map SKU to product ID."""
        stmt = select(DimProduct.product_id).where(DimProduct.sku == sku)
        return self._session.execute(stmt).scalar()

    def map_product_ids_to_rows(self, product_ids: list[int]) -> list[dict[str, Any]]:
        """Map product IDs to rows with product_id, sku, name, category, preserving order."""
        if not product_ids:
            return []

        stmt = select(
            DimProduct.product_id,
            DimProduct.sku,
            DimProduct.name,
            DimProduct.category,
        ).where(DimProduct.product_id.in_(product_ids))

        result = self._session.execute(stmt)
        rows = [
            {
                "product_id": row.product_id,
                "sku": row.sku,
                "name": row.name,
                "category": row.category,
            }
            for row in result
        ]

        # Preserve order based on product_ids
        id_to_index = {pid: idx for idx, pid in enumerate(product_ids)}
        rows.sort(key=lambda r: id_to_index.get(r["product_id"], len(product_ids)))

        return rows

    def get_user_seen_product_ids(self, user_id: int, limit: int = 1000) -> set[int]:
        """Get distinct product IDs seen by user, ordered by most recent."""
        stmt = (
            select(distinct(FactSales.product_id))
            .where(FactSales.customer_id == user_id)
            .order_by(FactSales.updated_at.desc())
            .limit(limit)
        )
        result = self._session.execute(stmt)
        return {row.product_id for row in result}
