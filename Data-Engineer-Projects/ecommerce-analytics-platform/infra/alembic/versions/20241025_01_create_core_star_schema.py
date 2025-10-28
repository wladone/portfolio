"""create core star schema

Revision ID: 20241025_01
Revises:
Create Date: 2025-10-25 09:47:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20241025_01_create_core_star_schema"
down_revision: str | None = "20241025_00_fix_version_length"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS stg")
    op.execute("CREATE SCHEMA IF NOT EXISTS dw")
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")

    op.create_table(
        "dim_customer",
        sa.Column("customer_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("customer_nk", sa.Text(), nullable=False, unique=True),
        sa.Column("email_hash", sa.String(length=64), nullable=False),
        sa.Column("first_name", sa.Text()),
        sa.Column("last_name", sa.Text()),
        sa.Column("phone", sa.Text()),
        sa.Column("country_code", sa.String(length=2)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "country_code ~ '^[A-Z]{2}$'", name="ck_dim_customer_country_code"
        ),
        schema="dw",
    )
    op.create_index(
        "ix_dim_customer_email_hash",
        "dim_customer",
        ["email_hash"],
        unique=False,
        schema="dw",
    )

    op.create_table(
        "dim_product",
        sa.Column("product_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("sku", sa.Text(), nullable=False, unique=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text()),
        sa.Column("category", sa.Text()),
        sa.Column("price_list", sa.Numeric(12, 2)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="dw",
    )
    op.create_index(
        "ix_dim_product_category",
        "dim_product",
        ["category"],
        unique=False,
        schema="dw",
    )

    op.create_table(
        "dim_channel",
        sa.Column(
            "channel_id", sa.SmallInteger(), primary_key=True, autoincrement=True
        ),
        sa.Column("channel_code", sa.Text(), nullable=False, unique=True),
        sa.Column("channel_name", sa.Text(), nullable=False),
        sa.Column("is_digital", sa.Boolean(), nullable=False),
        schema="dw",
    )

    op.create_table(
        "dim_date",
        sa.Column("date_key", sa.Integer(), primary_key=True),
        sa.Column("date", sa.Date(), nullable=False, unique=True),
        sa.Column("year", sa.SmallInteger(), nullable=False),
        sa.Column("quarter", sa.SmallInteger(), nullable=False),
        sa.Column("month", sa.SmallInteger(), nullable=False),
        sa.Column("day", sa.SmallInteger(), nullable=False),
        sa.Column("iso_week", sa.SmallInteger(), nullable=False),
        sa.Column("dow", sa.SmallInteger(), nullable=False),
        sa.Column("is_weekend", sa.Boolean(), nullable=False),
        sa.CheckConstraint("quarter BETWEEN 1 AND 4", name="ck_dim_date_quarter"),
        sa.CheckConstraint("month BETWEEN 1 AND 12", name="ck_dim_date_month"),
        sa.CheckConstraint("day BETWEEN 1 AND 31", name="ck_dim_date_day"),
        sa.CheckConstraint("dow BETWEEN 1 AND 7", name="ck_dim_date_dow"),
        schema="dw",
    )

    op.create_table(
        "fact_sales",
        sa.Column("sales_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("date_key", sa.Integer(), nullable=False),
        sa.Column("customer_id", sa.BigInteger(), nullable=False),
        sa.Column("product_id", sa.BigInteger(), nullable=False),
        sa.Column("channel_id", sa.SmallInteger(), nullable=False),
        sa.Column("order_id", sa.Text(), nullable=False),
        sa.Column("order_line_nbr", sa.SmallInteger(), nullable=False),
        sa.Column("transaction_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("currency_code", sa.String(length=3), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(12, 2), nullable=False),
        sa.Column(
            "discount_amount",
            sa.Numeric(12, 2),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("net_amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("cost_amount", sa.Numeric(14, 2)),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["date_key"], ["dw.dim_date.date_key"]),
        sa.ForeignKeyConstraint(["customer_id"], ["dw.dim_customer.customer_id"]),
        sa.ForeignKeyConstraint(["product_id"], ["dw.dim_product.product_id"]),
        sa.ForeignKeyConstraint(["channel_id"], ["dw.dim_channel.channel_id"]),
        sa.UniqueConstraint(
            "order_id", "order_line_nbr", name="uq_fact_sales_order_line"
        ),
        sa.CheckConstraint(
            "currency_code ~ '^[A-Z]{3}$'", name="ck_fact_sales_currency_code"
        ),
        sa.CheckConstraint("quantity > 0", name="ck_fact_sales_quantity_positive"),
        sa.CheckConstraint(
            "unit_price >= 0", name="ck_fact_sales_unit_price_non_negative"
        ),
        sa.CheckConstraint(
            "discount_amount >= 0", name="ck_fact_sales_discount_non_negative"
        ),
        sa.CheckConstraint("net_amount >= 0", name="ck_fact_sales_net_non_negative"),
        schema="dw",
    )
    op.create_index(
        "ix_fact_sales_date_channel",
        "fact_sales",
        ["date_key", "channel_id"],
        unique=False,
        schema="dw",
    )
    op.create_index(
        "ix_fact_sales_product_id",
        "fact_sales",
        ["product_id"],
        unique=False,
        schema="dw",
    )
    op.create_index(
        "ix_fact_sales_customer_id",
        "fact_sales",
        ["customer_id"],
        unique=False,
        schema="dw",
    )
    op.create_index(
        "ix_fact_sales_transaction_ts",
        "fact_sales",
        ["transaction_ts"],
        unique=False,
        schema="dw",
    )

    op.create_table(
        "etl_audit",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_name", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("rows_in", sa.BigInteger()),
        sa.Column("rows_out", sa.BigInteger()),
        sa.Column("rows_reject", sa.BigInteger()),
        sa.Column("status", sa.String(length=10), nullable=False),
        sa.Column("details", postgresql.JSONB()),
        sa.CheckConstraint(
            "status IN ('STARTED','OK','WARN','ERROR')",
            name="ck_etl_audit_status",
        ),
        schema="meta",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_etl_audit_job_started ON meta.etl_audit (job_name, started_at DESC)"
    )

    # Staging tables
    for table_name in ("orders_raw", "customers_raw", "products_raw"):
        op.create_table(
            table_name,
            sa.Column(
                "ingest_id", sa.BigInteger(), primary_key=True, autoincrement=True
            ),
            sa.Column("payload", postgresql.JSONB(), nullable=False),
            sa.Column("source_file", sa.Text()),
            sa.Column(
                "ingested_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            schema="stg",
        )
        op.create_index(
            f"ix_{table_name}_payload_gin",
            table_name,
            ["payload"],
            unique=False,
            postgresql_using="gin",
            schema="stg",
        )

    op.execute(
        """
        INSERT INTO dw.dim_channel (channel_code, channel_name, is_digital)
        VALUES
            ('web', 'Web', true),
            ('mobile', 'Mobile', true),
            ('store', 'Store', false)
        ON CONFLICT (channel_code) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM dw.dim_channel WHERE channel_code IN ('web','mobile','store')"
    )

    for table_name in ("products_raw", "customers_raw", "orders_raw"):
        op.drop_index(
            f"ix_{table_name}_payload_gin", table_name=table_name, schema="stg"
        )
        op.drop_table(table_name, schema="stg")

    op.execute("DROP INDEX IF EXISTS meta.ix_etl_audit_job_started")
    op.drop_table("etl_audit", schema="meta")

    op.drop_index("ix_fact_sales_transaction_ts", table_name="fact_sales", schema="dw")
    op.drop_index("ix_fact_sales_customer_id", table_name="fact_sales", schema="dw")
    op.drop_index("ix_fact_sales_product_id", table_name="fact_sales", schema="dw")
    op.drop_index("ix_fact_sales_date_channel", table_name="fact_sales", schema="dw")
    op.drop_table("fact_sales", schema="dw")

    op.drop_table("dim_date", schema="dw")
    op.drop_table("dim_channel", schema="dw")

    op.drop_index("ix_dim_product_category", table_name="dim_product", schema="dw")
    op.drop_table("dim_product", schema="dw")

    op.drop_index("ix_dim_customer_email_hash", table_name="dim_customer", schema="dw")
    op.drop_table("dim_customer", schema="dw")

    op.execute("DROP SCHEMA IF EXISTS stg CASCADE")
    op.execute("DROP SCHEMA IF EXISTS dw CASCADE")
    op.execute("DROP SCHEMA IF EXISTS meta CASCADE")
