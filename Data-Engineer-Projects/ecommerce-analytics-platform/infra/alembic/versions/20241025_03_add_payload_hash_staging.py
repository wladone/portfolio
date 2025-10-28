"""add payload hash to staging tables

Revision ID: 20241025_03
Revises: 20241025_02
Create Date: 2025-10-25 10:20:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20241025_03_add_payload_hash_staging"
down_revision: str = "20241025_02_create_analytical_views"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    for table in ("orders_raw", "customers_raw", "products_raw"):
        op.add_column(
            table,
            sa.Column("payload_hash", sa.String(length=64), nullable=True),
            schema="stg",
        )
        op.execute(
            f"""
            UPDATE stg.{table}
            SET payload_hash = md5(payload::text) || md5(payload::text)
            WHERE payload_hash IS NULL
            """
        )
        op.alter_column(table, "payload_hash", nullable=False, schema="stg")
        op.create_unique_constraint(
            f"uq_{table}_source_file_payload_hash",
            table,
            ["source_file", "payload_hash"],
            schema="stg",
        )


def downgrade() -> None:
    for table in ("products_raw", "customers_raw", "orders_raw"):
        op.drop_constraint(
            f"uq_{table}_source_file_payload_hash",
            table_name=table,
            schema="stg",
        )
        op.drop_column(table, "payload_hash", schema="stg")
