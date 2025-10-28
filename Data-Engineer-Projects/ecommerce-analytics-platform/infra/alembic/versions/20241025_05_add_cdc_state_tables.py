"""Add CDC state and fingerprint tables for incremental loads."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20241025_05_add_cdc_state_tables"
down_revision: str = "20241025_04_create_app_user_table"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Create CDC metadata tables."""
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")

    op.create_table(
        "cdc_state",
        sa.Column("entity", sa.Text(), primary_key=True),
        sa.Column("watermark_ts", sa.DateTime(timezone=True)),
        sa.Column("watermark_source", sa.Text()),
        sa.Column("details", sa.dialects.postgresql.JSONB()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="meta",
    )

    op.create_table(
        "dim_fingerprint",
        sa.Column("entity", sa.Text(), nullable=False),
        sa.Column("natural_key", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("entity", "natural_key", name="pk_dim_fingerprint"),
        schema="meta",
    )
    op.create_index(
        "ix_dim_fingerprint_entity",
        "dim_fingerprint",
        ["entity"],
        unique=False,
        schema="meta",
    )

    # Helpful indexes if not already present
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_dim_customer_customer_nk ON dw.dim_customer (customer_nk)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_dim_product_sku ON dw.dim_product (sku)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_fact_sales_transaction_ts ON dw.fact_sales (transaction_ts)"
    )


def downgrade() -> None:
    """Drop CDC metadata tables."""
    op.drop_index(
        "ix_dim_fingerprint_entity", table_name="dim_fingerprint", schema="meta"
    )
    op.drop_table("dim_fingerprint", schema="meta")
    op.drop_table("cdc_state", schema="meta")
