"""Incremental ETL runner with CDC-style watermarking and fingerprinting."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import sqlalchemy as sa
import structlog
from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from structlog.contextvars import bind_contextvars, clear_contextvars

from backend.app.logging_config import configure_logging
from backend.app.models import CdcState, DimCustomer, DimFingerprint, DimProduct
from etl import extractors, validators
from etl.db import session_scope
from etl.dwh import (
    ensure_date_key,
    get_channel_id,
    insert_fact_sales,
    upsert_dim_customer,
    upsert_dim_product,
)
from etl.settings import get_settings
from etl.transformers import (
    compute_email_hash,
    ensure_ts,
    normalize_customer_records,
    normalize_order_records,
    normalize_product_records,
)
from etl.utils.audit import AuditTracker, audit_run

logger = structlog.get_logger(__name__)

UTC = UTC

STAGING_METADATA = sa.MetaData()
STG_CUSTOMERS = sa.Table(
    "customers_raw",
    STAGING_METADATA,
    sa.Column("payload", sa.dialects.postgresql.JSONB),
    sa.Column("source_file", sa.Text),
    sa.Column("ingested_at", sa.DateTime(timezone=True)),
    schema="stg",
)
STG_PRODUCTS = sa.Table(
    "products_raw",
    STAGING_METADATA,
    sa.Column("payload", sa.dialects.postgresql.JSONB),
    sa.Column("source_file", sa.Text),
    sa.Column("ingested_at", sa.DateTime(timezone=True)),
    schema="stg",
)
STG_ORDERS = sa.Table(
    "orders_raw",
    STAGING_METADATA,
    sa.Column("payload", sa.dialects.postgresql.JSONB),
    sa.Column("source_file", sa.Text),
    sa.Column("ingested_at", sa.DateTime(timezone=True)),
    schema="stg",
)


@dataclass
class IncrementalResult:
    """Summary for incremental execution."""

    rows_in: int = 0
    rows_out: int = 0
    rows_reject: int = 0
    no_change: int = 0
    duplicates: int = 0
    skipped: int = 0
    files_processed: list[str] = field(default_factory=list)
    min_watermark: datetime | None = None
    max_watermark: datetime | None = None

    def observe_file(self, path: Path) -> None:
        self.files_processed.append(str(path))

    def observe_watermark(self, value: datetime | None) -> None:
        if value is None:
            return
        value = _ensure_aware(value)
        if self.max_watermark is None or value > self.max_watermark:
            self.max_watermark = value
        if self.min_watermark is None or value < self.min_watermark:
            self.min_watermark = value

    def to_details(self) -> dict[str, Any]:
        return {
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_reject": self.rows_reject,
            "no_change": self.no_change,
            "duplicates": self.duplicates,
            "skipped": self.skipped,
            "files": self.files_processed,
            "watermark_min": _dt_to_iso(self.min_watermark),
            "watermark_max": _dt_to_iso(self.max_watermark),
        }


def canonicalize_dict(data: dict[str, Any], include_keys: list[str]) -> dict[str, Any]:
    canonical: dict[str, Any] = {}
    for key in include_keys:
        value = data.get(key)
        if isinstance(value, Decimal):
            canonical[key] = str(value)
        elif isinstance(value, datetime):
            canonical[key] = _ensure_aware(value).isoformat()
        else:
            canonical[key] = value
    return canonical


def sha256_fingerprint(payload: dict[str, Any]) -> str:
    text = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_cdc_state(session: Session, entity: str) -> CdcState | None:
    return session.get(CdcState, entity)


def update_cdc_state(
    session: Session,
    entity: str,
    *,
    watermark_ts: datetime | None,
    watermark_source: str,
    details: dict[str, Any],
) -> None:
    stmt = insert(CdcState).values(
        entity=entity,
        watermark_ts=watermark_ts,
        watermark_source=watermark_source,
        details=details,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[CdcState.entity],
        set_={
            "watermark_ts": stmt.excluded.watermark_ts,
            "watermark_source": stmt.excluded.watermark_source,
            "details": stmt.excluded.details,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


def get_fingerprint(session: Session, entity: str, natural_key: str) -> str | None:
    stmt: Select[tuple[str]] = (
        select(DimFingerprint.fingerprint)
        .where(
            DimFingerprint.entity == entity,
            DimFingerprint.natural_key == natural_key,
        )
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def upsert_fingerprint(
    session: Session,
    *,
    entity: str,
    natural_key: str,
    fingerprint: str,
) -> None:
    stmt = insert(DimFingerprint).values(
        entity=entity,
        natural_key=natural_key,
        fingerprint=fingerprint,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[DimFingerprint.entity, DimFingerprint.natural_key],
        set_={"fingerprint": stmt.excluded.fingerprint, "updated_at": func.now()},
    )
    session.execute(stmt)


def run_customers_incremental(
    *,
    source_glob: str | None,
    strategy: Literal["watermark", "hash"] | None = None,
    from_ts: datetime | None = None,
    chunk_size: int | None = None,
    dry_run: bool | None = None,
) -> IncrementalResult:
    settings = get_settings()
    effective_strategy = strategy or settings.cdc_strategy_customers
    effective_chunk = chunk_size or settings.cdc_batch_size
    dry_run = settings.dry_run if dry_run is None else dry_run

    result = IncrementalResult()
    fallback_field = settings.cdc_ts_field_customers
    entity = "customers"
    offset_days = settings.cdc_default_watermark_offset_days

    with session_scope() as session:
        state = get_cdc_state(session, entity)
        watermark_prev = _resolve_watermark(
            from_ts, state.watermark_ts if state else None
        )
        cutoff = _watermark_cutoff(watermark_prev, offset_days)
        for batch in _iterate_batches(
            session=session,
            source_glob=source_glob,
            chunk_size=effective_chunk,
            staging_table=STG_CUSTOMERS,
        ):
            result.observe_file(batch.path)
            normalized = normalize_customer_records(batch.records)
            valid, errors = validators.validate_customers(normalized)
            result.rows_in += len(batch.records)
            result.rows_reject += len(errors)
            for error in errors:
                logger.warning("customer_validation_error", error=str(error))
            if not valid:
                continue

            for record in valid:
                payload = record.model_dump(mode="python")
                record_ts = ensure_ts(
                    payload, field=fallback_field, fallback=batch.fallback_ts
                )
                result.observe_watermark(record_ts)
                if (
                    effective_strategy == "watermark"
                    and cutoff is not None
                    and not record_ts > cutoff
                ):
                    result.skipped += 1
                    result.no_change += 1
                    continue

                email_hash = compute_email_hash(payload.get("email"))
                dim_payload = {
                    "customer_nk": payload["customer_nk"],
                    "email_hash": email_hash,
                    "first_name": payload.get("first_name"),
                    "last_name": payload.get("last_name"),
                    "phone": payload.get("phone"),
                    "country_code": payload.get("country_code"),
                }
                fingerprint = sha256_fingerprint(
                    canonicalize_dict(
                        dim_payload,
                        [
                            "customer_nk",
                            "email_hash",
                            "first_name",
                            "last_name",
                            "phone",
                            "country_code",
                        ],
                    )
                )
                existing_fp = get_fingerprint(
                    session, entity=entity, natural_key=dim_payload["customer_nk"]
                )
                if existing_fp == fingerprint:
                    result.no_change += 1
                    continue

                if dry_run:
                    continue

                customer_id = upsert_dim_customer(
                    session,
                    customer_nk=dim_payload["customer_nk"],
                    email_hash=email_hash,
                    first_name=dim_payload["first_name"],
                    last_name=dim_payload["last_name"],
                    phone=dim_payload["phone"],
                    country_code=dim_payload["country_code"],
                )
                upsert_fingerprint(
                    session,
                    entity=entity,
                    natural_key=dim_payload["customer_nk"],
                    fingerprint=fingerprint,
                )
                logger.debug(
                    "customer_upserted",
                    customer_id=customer_id,
                    customer_nk=dim_payload["customer_nk"],
                )
                result.rows_out += 1

        if not dry_run and result.max_watermark is not None:
            update_cdc_state(
                session,
                entity,
                watermark_ts=result.max_watermark,
                watermark_source=(
                    fallback_field if effective_strategy == "watermark" else "hash"
                ),
                details={
                    **result.to_details(),
                    "strategy": effective_strategy,
                    "offset_days": offset_days,
                },
            )

    return result


def run_products_incremental(
    *,
    source_glob: str | None,
    strategy: Literal["watermark", "hash"] | None = None,
    from_ts: datetime | None = None,
    chunk_size: int | None = None,
    dry_run: bool | None = None,
) -> IncrementalResult:
    settings = get_settings()
    effective_strategy = strategy or settings.cdc_strategy_products
    effective_chunk = chunk_size or settings.cdc_batch_size
    dry_run = settings.dry_run if dry_run is None else dry_run

    result = IncrementalResult()
    fallback_field = settings.cdc_ts_field_products
    entity = "products"
    offset_days = settings.cdc_default_watermark_offset_days

    with session_scope() as session:
        state = get_cdc_state(session, entity)
        watermark_prev = _resolve_watermark(
            from_ts, state.watermark_ts if state else None
        )
        cutoff = _watermark_cutoff(watermark_prev, offset_days)
        for batch in _iterate_batches(
            session=session,
            source_glob=source_glob,
            chunk_size=effective_chunk,
            staging_table=STG_PRODUCTS,
        ):
            result.observe_file(batch.path)
            normalized = normalize_product_records(batch.records)
            valid, errors = validators.validate_products(normalized)
            result.rows_in += len(batch.records)
            result.rows_reject += len(errors)
            for error in errors:
                logger.warning("product_validation_error", error=str(error))
            if not valid:
                continue

            for record in valid:
                payload = record.model_dump(mode="python")
                record_ts = ensure_ts(
                    payload, field=fallback_field, fallback=batch.fallback_ts
                )
                result.observe_watermark(record_ts)
                if (
                    effective_strategy == "watermark"
                    and cutoff is not None
                    and not record_ts > cutoff
                ):
                    result.skipped += 1
                    result.no_change += 1
                    continue

                dim_payload = {
                    "sku": payload["sku"],
                    "name": payload["name"],
                    "brand": payload.get("brand"),
                    "category": payload.get("category"),
                    "price_list": payload.get("price_list"),
                }
                fingerprint = sha256_fingerprint(
                    canonicalize_dict(
                        dim_payload,
                        ["sku", "name", "brand", "category", "price_list"],
                    )
                )
                existing_fp = get_fingerprint(
                    session, entity=entity, natural_key=dim_payload["sku"]
                )
                if existing_fp == fingerprint:
                    result.no_change += 1
                    continue

                if dry_run:
                    continue

                product_id = upsert_dim_product(
                    session,
                    sku=dim_payload["sku"],
                    name=dim_payload["name"],
                    brand=dim_payload["brand"],
                    category=dim_payload["category"],
                    price_list=dim_payload["price_list"],
                )
                upsert_fingerprint(
                    session,
                    entity=entity,
                    natural_key=dim_payload["sku"],
                    fingerprint=fingerprint,
                )
                logger.debug(
                    "product_upserted", product_id=product_id, sku=dim_payload["sku"]
                )
                result.rows_out += 1

        if not dry_run and result.max_watermark is not None:
            update_cdc_state(
                session,
                entity,
                watermark_ts=result.max_watermark,
                watermark_source=(
                    fallback_field if effective_strategy == "watermark" else "hash"
                ),
                details={
                    **result.to_details(),
                    "strategy": effective_strategy,
                    "offset_days": offset_days,
                },
            )

    return result


def run_orders_incremental(
    *,
    source_glob: str | None,
    from_ts: datetime | None = None,
    chunk_size: int | None = None,
    dry_run: bool | None = None,
    ensure_dim_date: bool | None = None,
) -> IncrementalResult:
    settings = get_settings()
    effective_chunk = chunk_size or settings.cdc_batch_size
    dry_run = settings.dry_run if dry_run is None else dry_run
    ensure_dim_date = (
        ensure_dim_date if ensure_dim_date is not None else settings.ensure_dim_date
    )

    result = IncrementalResult()
    fallback_field = settings.cdc_ts_field_orders
    entity = "orders"
    offset_days = settings.cdc_default_watermark_offset_days

    with session_scope() as session:
        state = get_cdc_state(session, entity)
        watermark_prev = _resolve_watermark(
            from_ts, state.watermark_ts if state else None
        )
        cutoff = _watermark_cutoff(watermark_prev, offset_days)
        for batch in _iterate_batches(
            session=session,
            source_glob=source_glob,
            chunk_size=effective_chunk,
            staging_table=STG_ORDERS,
        ):
            result.observe_file(batch.path)
            normalized = normalize_order_records(batch.records)
            valid, errors = validators.validate_orders(normalized)
            result.rows_in += len(batch.records)
            result.rows_reject += len(errors)
            for error in errors:
                logger.warning("order_validation_error", error=str(error))
            if not valid:
                continue

            for record in valid:
                payload = record.model_dump(mode="python")
                record_ts = ensure_ts(
                    payload, field=fallback_field, fallback=batch.fallback_ts
                )
                result.observe_watermark(record_ts)
                if cutoff is not None and not record_ts > cutoff:
                    result.skipped += 1
                    result.no_change += 1
                    continue

                if dry_run:
                    continue

                customer_id = _lookup_customer_id(session, payload["customer_nk"])
                product_id = _lookup_product_id(session, payload["sku"])
                channel_id = get_channel_id(session, payload["channel_code"])
                if not all((customer_id, product_id, channel_id)):
                    result.rows_reject += 1
                    logger.warning(
                        "order_missing_dimension",
                        order_id=payload["order_id"],
                        order_line_nbr=payload["order_line_nbr"],
                    )
                    continue

                date_key = ensure_date_key(
                    session,
                    payload["transaction_ts"].date(),
                    ensure_if_missing=ensure_dim_date,
                )
                if date_key is None:
                    result.rows_reject += 1
                    continue

                inserted = insert_fact_sales(
                    session,
                    date_key=date_key,
                    customer_id=customer_id,
                    product_id=product_id,
                    channel_id=channel_id,
                    order_id=payload["order_id"],
                    order_line_nbr=payload["order_line_nbr"],
                    transaction_ts=payload["transaction_ts"],
                    currency_code=payload["currency_code"],
                    quantity=payload["quantity"],
                    unit_price=payload["unit_price"],
                    discount_amount=payload["discount_amount"],
                    cost_amount=None,
                )
                if inserted:
                    result.rows_out += 1
                else:
                    result.duplicates += 1
                    result.no_change += 1

        if not dry_run and result.max_watermark is not None:
            update_cdc_state(
                session,
                entity,
                watermark_ts=result.max_watermark,
                watermark_source=fallback_field,
                details={
                    **result.to_details(),
                    "strategy": "watermark",
                    "offset_days": offset_days,
                },
            )

    return result


@dataclass
class SourceBatch:
    path: Path
    fallback_ts: datetime
    records: list[dict[str, Any]]


def _iterate_batches(
    *,
    session: Session,
    source_glob: str | None,
    chunk_size: int,
    staging_table: sa.Table,
) -> Iterable[SourceBatch]:
    if source_glob:
        for path, records in extractors.read_mixed_stream(
            source_glob, chunk_size=chunk_size
        ):
            yield SourceBatch(path=path, fallback_ts=_path_mtime(path), records=records)
        return

    stmt = select(
        staging_table.c.source_file,
        staging_table.c.payload,
        staging_table.c.ingested_at,
    ).order_by(staging_table.c.ingested_at)
    rows = session.execute(stmt).all()
    buffer: list[dict[str, Any]] = []
    latest_ts = datetime.now(tz=UTC)
    for row in rows:
        payload = dict(row.payload or {})
        buffer.append(payload)
        latest_ts = _ensure_aware(row.ingested_at or latest_ts)
        if len(buffer) >= chunk_size:
            yield SourceBatch(
                path=Path(f"staging/{staging_table.name}"),
                fallback_ts=latest_ts,
                records=buffer,
            )
            buffer = []
    if buffer:
        yield SourceBatch(
            path=Path(f"staging/{staging_table.name}"),
            fallback_ts=latest_ts,
            records=buffer,
        )


def _resolve_watermark(
    override: datetime | None, stored: datetime | None
) -> datetime | None:
    if override:
        return _ensure_aware(override)
    if stored:
        return _ensure_aware(stored)
    return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _watermark_cutoff(base: datetime | None, offset_days: int) -> datetime | None:
    if base is None:
        return None
    if offset_days <= 0:
        return base
    return base - timedelta(days=offset_days)


def _path_mtime(path: Path) -> datetime:
    try:
        stat = path.stat()
        return datetime.fromtimestamp(stat.st_mtime, tz=UTC)
    except FileNotFoundError:
        return datetime.now(tz=UTC)


def _lookup_customer_id(session: Session, customer_nk: str) -> int | None:
    stmt: Select[tuple[int]] = (
        select(DimCustomer.customer_id)
        .where(DimCustomer.customer_nk == customer_nk)
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _lookup_product_id(session: Session, sku: str) -> int | None:
    stmt: Select[tuple[int]] = (
        select(DimProduct.product_id).where(DimProduct.sku == sku).limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _dt_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _ensure_aware(value).isoformat()


def _apply_incremental_result(
    tracker: AuditTracker,
    result: IncrementalResult,
    *,
    dry_run: bool,
) -> None:
    tracker.increment(
        rows_in=result.rows_in,
        rows_out=result.rows_out,
        rows_reject=result.rows_reject,
    )
    tracker.extend_files(result.files_processed)
    tracker.add_detail(
        "result",
        {
            **result.to_details(),
            "dry_run": dry_run,
        },
    )
    tracker.set_status("OK")
    logger.info(
        "incremental_stats",
        job_name=tracker.job_name,
        run_id=str(tracker.run_id),
        rows_in=result.rows_in,
        rows_out=result.rows_out,
        rows_reject=result.rows_reject,
        no_change=result.no_change,
        duplicates=result.duplicates,
        skipped=result.skipped,
        dry_run=dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        prog="etl.incremental", description="Incremental ETL loader"
    )
    parser.add_argument(
        "--correlation-id", help="Correlation identifier for traceability."
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--source",
            help="Glob pattern for input files; omit to reprocess staging tables.",
        )
        subparser.add_argument(
            "--chunk-size",
            type=int,
            default=settings.cdc_batch_size,
            help=f"Chunk size (default {settings.cdc_batch_size}).",
        )
        subparser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate without writing data.",
        )
        subparser.add_argument(
            "--strategy",
            choices=["watermark", "hash"],
            help="Override CDC strategy for this run.",
        )
        subparser.add_argument(
            "--from-ts",
            help="Override watermark starting timestamp (ISO-8601).",
        )
        subparser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose logging (DEBUG level).",
        )

    customers_parser = subparsers.add_parser(
        "customers", help="Incremental load for customers dimension."
    )
    add_common(customers_parser)

    products_parser = subparsers.add_parser(
        "products", help="Incremental load for products dimension."
    )
    add_common(products_parser)

    orders_parser = subparsers.add_parser(
        "orders", help="Incremental load for fact sales."
    )
    add_common(orders_parser)
    orders_parser.add_argument(
        "--ensure-dim-date",
        action="store_true",
        help="Insert missing dim_date rows when needed.",
    )

    all_parser = subparsers.add_parser(
        "all", help="Run customers, products, and orders sequentially."
    )
    add_common(all_parser)
    all_parser.add_argument(
        "--ensure-dim-date",
        action="store_true",
        help="Insert missing dim_date rows while processing orders.",
    )

    return parser


def _parse_from_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    return datetime.fromisoformat(cleaned)


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = build_parser()
    args = parser.parse_args(argv)

    correlation_id = args.correlation_id or uuid4().hex
    log_level = "DEBUG" if args.verbose else settings.app_log_level
    configure_logging(log_level)
    bind_contextvars(correlation_id=correlation_id)

    run_id = uuid4()
    job_name = f"{args.command}_incremental"
    from_ts = _parse_from_ts(getattr(args, "from_ts", None))
    strategy = getattr(args, "strategy", None)
    chunk_size = getattr(args, "chunk_size", settings.cdc_batch_size)
    dry_run = getattr(args, "dry_run", False)

    logger.info(
        "incremental_job_start",
        job_name=job_name,
        run_id=str(run_id),
        source=args.source,
        strategy=strategy,
        from_ts=_dt_to_iso(from_ts),
        dry_run=dry_run,
    )

    try:
        with audit_run(
            job_name=job_name, run_id=run_id, correlation_id=correlation_id
        ) as tracker:
            if args.command == "customers":
                result = run_customers_incremental(
                    source_glob=args.source,
                    strategy=strategy,
                    from_ts=from_ts,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                )
                _apply_incremental_result(tracker, result, dry_run=dry_run)
            elif args.command == "products":
                result = run_products_incremental(
                    source_glob=args.source,
                    strategy=strategy,
                    from_ts=from_ts,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                )
                _apply_incremental_result(tracker, result, dry_run=dry_run)
            elif args.command == "orders":
                result = run_orders_incremental(
                    source_glob=args.source,
                    from_ts=from_ts,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                    ensure_dim_date=args.ensure_dim_date,
                )
                _apply_incremental_result(tracker, result, dry_run=dry_run)
            elif args.command == "all":
                sub_results: dict[str, IncrementalResult] = {}
                sub_results["customers"] = run_customers_incremental(
                    source_glob=_resolve_source(args.source, "customers_*.json"),
                    strategy=strategy,
                    from_ts=from_ts,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                )
                sub_results["products"] = run_products_incremental(
                    source_glob=_resolve_source(args.source, "products*.csv"),
                    strategy=strategy,
                    from_ts=from_ts,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                )
                sub_results["orders"] = run_orders_incremental(
                    source_glob=_resolve_source(args.source, "orders_*.json"),
                    from_ts=from_ts,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                    ensure_dim_date=args.ensure_dim_date,
                )
                aggregate = IncrementalResult()
                for name, sub_result in sub_results.items():
                    aggregate.rows_in += sub_result.rows_in
                    aggregate.rows_out += sub_result.rows_out
                    aggregate.rows_reject += sub_result.rows_reject
                    aggregate.no_change += sub_result.no_change
                    aggregate.duplicates += sub_result.duplicates
                    aggregate.skipped += sub_result.skipped
                    aggregate.files_processed.extend(sub_result.files_processed)
                    aggregate.observe_watermark(sub_result.min_watermark)
                    aggregate.observe_watermark(sub_result.max_watermark)
                _apply_incremental_result(tracker, aggregate, dry_run=dry_run)
                tracker.add_detail(
                    "subjobs",
                    {name: res.to_details() for name, res in sub_results.items()},
                )
            else:
                raise ValueError(f"Unsupported command '{args.command}'")

            tracker.add_detail("strategy_override", strategy)
            tracker.add_detail("from_ts_override", _dt_to_iso(from_ts))
            tracker.add_detail("chunk_size", chunk_size)
            tracker.add_detail("dry_run", dry_run)
    except FileNotFoundError as exc:
        logger.error("incremental_source_missing", error=str(exc))
        clear_contextvars()
        return 1
    finally:
        clear_contextvars()

    logger.info("incremental_job_completed", job_name=job_name, run_id=str(run_id))
    return 0


def _resolve_source(base: str | None, pattern: str) -> str | None:
    if base is None:
        return None
    base_path = Path(base)
    if base_path.is_dir():
        return str(base_path / pattern)
    return base


if __name__ == "__main__":
    raise SystemExit(main())
