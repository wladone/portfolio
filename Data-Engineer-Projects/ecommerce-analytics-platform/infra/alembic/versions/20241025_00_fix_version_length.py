"""Fix alembic version column length."""

from alembic import op
from sqlalchemy import Column, String

revision = "20241025_00_fix_version_length"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Increase version_num column length."""
    # Drop existing version table if it exists
    op.drop_table("alembic_version", if_exists=True)

    # Create new version table with longer column
    op.create_table(
        "alembic_version",
        Column("version_num", String(128), nullable=False, primary_key=True),
    )


def downgrade() -> None:
    """Revert to original column length."""
    # Drop table with longer column
    op.drop_table("alembic_version")

    # Create original table
    op.create_table(
        "alembic_version",
        Column("version_num", String(128), nullable=False, primary_key=True),
    )
