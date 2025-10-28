"""ORM model for dw.dim_date."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Boolean, CheckConstraint, Integer, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DimDate(Base):
    """Date dimension (calendar)."""

    __tablename__ = "dim_date"
    __table_args__ = (
        CheckConstraint("quarter BETWEEN 1 AND 4", name="ck_dim_date_quarter"),
        CheckConstraint("month BETWEEN 1 AND 12", name="ck_dim_date_month"),
        CheckConstraint("day BETWEEN 1 AND 31", name="ck_dim_date_day"),
        CheckConstraint("dow BETWEEN 1 AND 7", name="ck_dim_date_dow"),
        {"schema": "dw"},
    )

    date_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[date] = mapped_column(nullable=False, unique=True)
    year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    quarter: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    month: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    day: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    iso_week: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    dow: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    is_weekend: Mapped[bool] = mapped_column(Boolean, nullable=False)
