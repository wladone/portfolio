"""Create meta.stream_offsets and meta.stream_dedup tables"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20241025_07_create_stream_tables"
down_revision: str = "20241025_06_add_cache_events"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Apply the upgrade."""
    op.create_table(
        "stream_offsets",
        sa.Column("stream_name", sa.Text(), primary_key=True),
        sa.Column("partition", sa.Integer(), primary_key=True),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="meta",
    )
    op.create_index(
        "ix_meta_stream_offsets_updated_at",
        "stream_offsets",
        ["updated_at"],
        unique=False,
        schema="meta",
    )

    op.create_table(
        "stream_dedup",
        sa.Column("stream_name", sa.Text(), primary_key=True),
        sa.Column("partition", sa.Integer(), primary_key=True),
        sa.Column("message_key", sa.Text(), primary_key=True),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="meta",
    )
    op.create_index(
        "ix_meta_stream_dedup_processed_at",
        "stream_dedup",
        ["processed_at"],
        unique=False,
        schema="meta",
    )


def downgrade() -> None:
    """Revert the upgrade."""
    op.drop_index(
        "ix_meta_stream_dedup_processed_at", table_name="stream_dedup", schema="meta"
    )
    op.drop_table("stream_dedup", schema="meta")

    op.drop_index(
        "ix_meta_stream_offsets_updated_at", table_name="stream_offsets", schema="meta"
    )
    op.drop_table("stream_offsets", schema="meta")
