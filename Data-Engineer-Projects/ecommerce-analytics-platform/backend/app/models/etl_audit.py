"""ORM model for meta.etl_audit."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Index, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class EtlAudit(Base):
    """Track ETL job executions."""

    __tablename__ = "etl_audit"
    __table_args__ = (
        CheckConstraint(
            "status IN ('STARTED','OK','WARN','ERROR')", name="ck_etl_audit_status"
        ),
        Index("ix_etl_audit_job_started", "job_name", text("started_at DESC")),
        {"schema": "meta"},
    )

    run_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rows_in: Mapped[int | None] = mapped_column(BigInteger)
    rows_out: Mapped[int | None] = mapped_column(BigInteger)
    rows_reject: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(10), nullable=False)
    details: Mapped[dict[str, object] | None] = mapped_column(JSONB)
