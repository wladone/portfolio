"""Create meta.app_user table for authentication."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20241025_04_create_app_user_table"
down_revision: str = "20241025_03_add_payload_hash_staging"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Apply the migration."""
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")

    op.create_table(
        "app_user",
        sa.Column("user_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(length=150), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=20), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "role IN ('admin','analyst','app')",
            name="ck_app_user_role",
        ),
        schema="meta",
    )

    op.create_index(
        "ix_meta_app_user_username",
        "app_user",
        ["username"],
        unique=True,
        schema="meta",
    )
    op.create_index(
        "ix_meta_app_user_role",
        "app_user",
        ["role"],
        unique=False,
        schema="meta",
    )


def downgrade() -> None:
    """Revert the migration."""
    op.drop_index("ix_meta_app_user_role", table_name="app_user", schema="meta")
    op.drop_index("ix_meta_app_user_username", table_name="app_user", schema="meta")
    op.drop_table("app_user", schema="meta")
