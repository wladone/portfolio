"""CDC state tracking models."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class CdcState(Base):
    """Persist the latest processed watermark for incremental loads."""

    __tablename__ = "cdc_state"
    __table_args__ = {"schema": "meta"}

    entity: Mapped[str] = mapped_column(Text, primary_key=True)
    watermark_ts: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    watermark_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class DimFingerprint(Base):
    """Store entity fingerprints for SCD-1 change detection."""

    __tablename__ = "dim_fingerprint"
    __table_args__ = {"schema": "meta"}

    entity: Mapped[str] = mapped_column(Text, primary_key=True)
    natural_key: Mapped[str] = mapped_column(Text, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
