"""Audit helpers for recording ETL jobs in meta.etl_audit."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

from backend.app.models import EtlAudit
from etl.db import session_scope
from etl.settings import get_settings

logger = structlog.get_logger(__name__)

# Prometheus metrics for ETL jobs
registry = CollectorRegistry()
etl_rows_in = Counter(
    "etl_rows_in",
    "Number of rows processed in ETL job",
    ["job_name", "run_id"],
    registry=registry,
)
etl_rows_out = Counter(
    "etl_rows_out",
    "Number of rows output from ETL job",
    ["job_name", "run_id"],
    registry=registry,
)
etl_rows_reject = Counter(
    "etl_rows_reject",
    "Number of rows rejected in ETL job",
    ["job_name", "run_id"],
    registry=registry,
)
etl_job_duration_seconds = Gauge(
    "etl_job_duration_seconds",
    "Duration of ETL job in seconds",
    ["job_name", "run_id"],
    registry=registry,
)
etl_job_status = Gauge(
    "etl_job_status",
    "Status of ETL job (1 for success, 0 for failure)",
    ["job_name", "run_id"],
    registry=registry,
)


@dataclass
class AuditTracker:
    run_id: UUID
    job_name: str
    correlation_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    rows_in: int = 0
    rows_out: int = 0
    rows_reject: int = 0
    status: str = "OK"
    details: dict[str, Any] = field(default_factory=dict)

    def increment(
        self, *, rows_in: int = 0, rows_out: int = 0, rows_reject: int = 0
    ) -> None:
        self.rows_in += rows_in
        self.rows_out += rows_out
        self.rows_reject += rows_reject

    def set_status(self, status: str) -> None:
        self.status = status

    def add_detail(self, key: str, value: Any) -> None:
        self.details[key] = value

    def extend_files(self, files: list[str]) -> None:
        existing = set(self.details.get("files", []))
        existing.update(files)
        self.details["files"] = sorted(existing)


@contextmanager
def audit_run(
    *, job_name: str, run_id: UUID, correlation_id: str
) -> Iterator[AuditTracker]:
    tracker = AuditTracker(
        run_id=run_id, job_name=job_name, correlation_id=correlation_id
    )
    _insert_start_record(tracker)
    try:
        yield tracker
    except Exception as exc:
        tracker.status = "ERROR"
        tracker.add_detail("error", str(exc))
        _finalize_record(tracker)
        logger.error(
            "etl_job_failed",
            job_name=tracker.job_name,
            run_id=str(tracker.run_id),
            error=str(exc),
        )
        raise
    else:
        _finalize_record(tracker)


def _insert_start_record(tracker: AuditTracker) -> None:
    with session_scope() as session:
        stmt = insert(EtlAudit).values(
            run_id=tracker.run_id,
            job_name=tracker.job_name,
            started_at=tracker.started_at,
            status="STARTED",
            details={
                "correlation_id": tracker.correlation_id,
            },
            rows_in=0,
            rows_out=0,
            rows_reject=0,
        )
        session.execute(stmt)


def _finalize_record(tracker: AuditTracker) -> None:
    ended_at = datetime.now(tz=UTC)
    with session_scope() as session:
        stmt = (
            update(EtlAudit)
            .where(EtlAudit.run_id == tracker.run_id)
            .values(
                ended_at=ended_at,
                status=tracker.status,
                rows_in=tracker.rows_in,
                rows_out=tracker.rows_out,
                rows_reject=tracker.rows_reject,
                details={
                    **tracker.details,
                    "correlation_id": tracker.correlation_id,
                },
            )
        )
        session.execute(stmt)

    # Push metrics to Pushgateway if configured
    settings = get_settings()
    if settings.pushgateway_url:
        try:
            duration = (ended_at - tracker.started_at).total_seconds()
            grouping_key = {
                "job_name": tracker.job_name,
                "run_id": str(tracker.run_id),
            }

            etl_rows_in.labels(**grouping_key).inc(tracker.rows_in)
            etl_rows_out.labels(**grouping_key).inc(tracker.rows_out)
            etl_rows_reject.labels(**grouping_key).inc(tracker.rows_reject)
            etl_job_duration_seconds.labels(**grouping_key).set(duration)
            etl_job_status.labels(**grouping_key).set(
                1 if tracker.status == "OK" else 0
            )

            push_to_gateway(
                settings.pushgateway_url,
                job="etl_job",
                registry=registry,
                grouping_key=grouping_key,
            )
            logger.info(
                "pushed_etl_metrics",
                job_name=tracker.job_name,
                run_id=str(tracker.run_id),
                pushgateway_url=settings.pushgateway_url,
            )
        except Exception as exc:
            logger.error(
                "failed_to_push_etl_metrics",
                job_name=tracker.job_name,
                run_id=str(tracker.run_id),
                error=str(exc),
            )
