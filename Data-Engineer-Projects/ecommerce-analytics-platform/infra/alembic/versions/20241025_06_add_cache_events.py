"""Create meta.cache_events table for cache invalidation audit."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20241025_06_add_cache_events"
down_revision: str = "20241025_05_add_cdc_state_tables"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Apply migration."""
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")

    op.create_table(
        "cache_events",
        sa.Column("event_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="meta",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_cache_events_type_created "
        "ON meta.cache_events (event_type, created_at DESC)"
    )


def downgrade() -> None:
    """Revert migration."""
    op.execute("DROP INDEX IF EXISTS meta.ix_cache_events_type_created")
    op.drop_table("cache_events", schema="meta")
