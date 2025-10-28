"""create analytical views

Revision ID: 20241025_02
Revises: 20241025_01
Create Date: 2025-10-25 09:47:30.000000
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20241025_02_create_analytical_views"
down_revision: str = "20241025_01_create_core_star_schema"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW dw.v_daily_sales_summary AS
        SELECT
            fs.date_key,
            fs.channel_id,
            COUNT(DISTINCT fs.order_id) AS orders,
            SUM(fs.quantity) AS items,
            SUM(fs.unit_price * fs.quantity) AS gross,
            SUM(fs.discount_amount) AS discount,
            SUM(fs.net_amount) AS net,
            CASE
                WHEN COUNT(DISTINCT fs.order_id) > 0
                THEN SUM(fs.net_amount) / COUNT(DISTINCT fs.order_id)
                ELSE 0
            END AS avg_order_value
        FROM dw.fact_sales fs
        GROUP BY fs.date_key, fs.channel_id
        """
    )

    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dw.v_product_affinity AS
        SELECT
            LEAST(fs1.product_id, fs2.product_id) AS product_id_1,
            GREATEST(fs1.product_id, fs2.product_id) AS product_id_2,
            COUNT(DISTINCT fs1.order_id) AS cooccurrences
        FROM dw.fact_sales fs1
        JOIN dw.fact_sales fs2
            ON fs1.order_id = fs2.order_id
           AND fs1.product_id < fs2.product_id
        GROUP BY 1, 2
        HAVING COUNT(DISTINCT fs1.order_id) >= 5
        WITH NO DATA
        """
    )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_v_product_affinity_pair
            ON dw.v_product_affinity (product_id_1, product_id_2)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS dw.ix_v_product_affinity_pair")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS dw.v_product_affinity")
    op.execute("DROP VIEW IF EXISTS dw.v_daily_sales_summary")
