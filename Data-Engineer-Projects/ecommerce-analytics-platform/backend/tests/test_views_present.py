"""Validate analytical views are accessible."""

from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def test_daily_sales_view_exists(db_engine: Engine) -> None:
    """`dw.v_daily_sales_summary` should be registered as a view."""
    inspector = inspect(db_engine)
    view_names = inspector.get_view_names(schema="dw")
    assert "v_daily_sales_summary" in view_names

    with db_engine.connect() as conn:
        conn.execute(text("SELECT * FROM dw.v_daily_sales_summary LIMIT 1"))


def test_product_affinity_materialized_view_exists(db_engine: Engine) -> None:
    """`dw.v_product_affinity` materialized view should be queryable."""
    with db_engine.connect() as conn:
        conn.execute(text("SELECT * FROM dw.v_product_affinity LIMIT 1"))
        result = conn.execute(
            text(
                """
                SELECT 1
                FROM pg_matviews
                WHERE schemaname = 'dw' AND matviewname = 'v_product_affinity'
                """
            )
        )
        assert result.scalar() == 1
