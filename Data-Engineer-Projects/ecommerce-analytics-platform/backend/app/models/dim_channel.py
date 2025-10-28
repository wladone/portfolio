"""ORM model for dw.dim_channel."""

from __future__ import annotations

from sqlalchemy import Boolean, SmallInteger, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DimChannel(Base):
    """Sales channel dimension."""

    __tablename__ = "dim_channel"
    __table_args__ = {"schema": "dw"}

    channel_id: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, autoincrement=True
    )
    channel_code: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    channel_name: Mapped[str] = mapped_column(Text, nullable=False)
    is_digital: Mapped[bool] = mapped_column(Boolean, nullable=False)
