"""ORM model for dw.fact_sales."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class FactSales(Base):
    """Transactional sales fact table."""

    __tablename__ = "fact_sales"
    __table_args__ = (
        UniqueConstraint("order_id", "order_line_nbr", name="uq_fact_sales_order_line"),
        CheckConstraint(
            "currency_code ~ '^[A-Z]{3}$'", name="ck_fact_sales_currency_code"
        ),
        CheckConstraint("quantity > 0", name="ck_fact_sales_quantity_positive"),
        CheckConstraint(
            "unit_price >= 0", name="ck_fact_sales_unit_price_non_negative"
        ),
        CheckConstraint(
            "discount_amount >= 0", name="ck_fact_sales_discount_non_negative"
        ),
        CheckConstraint("net_amount >= 0", name="ck_fact_sales_net_non_negative"),
        Index("ix_fact_sales_date_channel", "date_key", "channel_id"),
        Index("ix_fact_sales_product_id", "product_id"),
        Index("ix_fact_sales_customer_id", "customer_id"),
        Index("ix_fact_sales_transaction_ts", "transaction_ts"),
        {"schema": "dw"},
    )

    sales_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    date_key: Mapped[int] = mapped_column(
        ForeignKey("dw.dim_date.date_key", ondelete="RESTRICT"), nullable=False
    )
    customer_id: Mapped[int] = mapped_column(
        ForeignKey("dw.dim_customer.customer_id", ondelete="RESTRICT"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        ForeignKey("dw.dim_product.product_id", ondelete="RESTRICT"), nullable=False
    )
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("dw.dim_channel.channel_id", ondelete="RESTRICT"), nullable=False
    )
    order_id: Mapped[str] = mapped_column(Text, nullable=False)
    order_line_nbr: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    transaction_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    currency_code: Mapped[str] = mapped_column(String(3), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    discount_amount: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    net_amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    cost_amount: Mapped[Decimal | None] = mapped_column(Numeric(14, 2))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
