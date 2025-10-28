"""Orchestrators for staging and warehouse loading."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import structlog
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import extractors, validators
from .db import session_scope
from .dwh import (
    ensure_date_key,
    get_channel_id,
    insert_fact_sales,
    upsert_dim_customer,
    upsert_dim_product,
)
from .transformers import (
    compute_email_hash,
    normalize_customer_records,
    normalize_order_records,
    normalize_product_records,
    sanitize_channel_code,
)

logger = structlog.get_logger(__name__)


@dataclass
class JobResult:
    rows_in: int = 0
    rows_out: int = 0
    rows_reject: int = 0
    files_processed: list[str] = field(default_factory=list)
    duplicates: int = 0
    processed_records: list[dict[str, Any]] = field(default_factory=list)

    def merge(self, other: JobResult) -> None:
        self.rows_in += other.rows_in
        self.rows_out += other.rows_out
        self.rows_reject += other.rows_reject
        self.duplicates += other.duplicates
        self.files_processed.extend(other.files_processed)
        self.processed_records.extend(other.processed_records)


def load_products_to_stg(
    source: str,
    *,
    chunk_size: int,
    limit: int | None,
    dry_run: bool,
) -> JobResult:
    result = JobResult()
    for path, raw_records in extractors.read_csv_stream(source, chunk_size=chunk_size):
        if limit is not None and result.rows_in >= limit:
            break
        normalized = normalize_product_records(raw_records)
        valid, errors = validators.validate_products(normalized)
        result.rows_in += len(raw_records)
        result.rows_reject += len(errors)
        result.files_processed.append(str(path))

        if dry_run or not valid:
            continue

        if limit is not None:
            previous_total = result.rows_in - len(raw_records)
            remaining = max(0, limit - previous_total)
            to_process = valid[:remaining]
        else:
            to_process = valid

        with session_scope() as session:
            staged = stage_records(session, "products", to_process, str(path))
            result.duplicates += len(to_process) - staged
            for product in to_process:
                payload = product.model_dump(mode="python")
                upsert_dim_product(
                    session,
                    sku=payload["sku"],
                    name=payload["name"],
                    brand=payload.get("brand"),
                    category=payload.get("category"),
                    price_list=payload.get("price_list"),
                )
                result.rows_out += 1
    return result


def load_customers_to_stg(
    source: str,
    *,
    chunk_size: int,
    limit: int | None,
    dry_run: bool,
) -> JobResult:
    result = JobResult()
    for path, raw_records in extractors.read_json_stream(source, chunk_size=chunk_size):
        if limit is not None and result.rows_in >= limit:
            break
        normalized = normalize_customer_records(raw_records)
        valid, errors = validators.validate_customers(normalized)
        result.rows_in += len(raw_records)
        result.rows_reject += len(errors)
        result.files_processed.append(str(path))

        if dry_run or not valid:
            continue

        if limit is not None:
            previous_total = result.rows_in - len(raw_records)
            remaining = max(0, limit - previous_total)
            to_process = valid[:remaining]
        else:
            to_process = valid

        with session_scope() as session:
            staged = stage_records(session, "customers", to_process, str(path))
            result.duplicates += len(to_process) - staged
            for customer in to_process:
                payload = customer.model_dump(mode="python")
                email_hash = compute_email_hash(payload.get("email"))
                upsert_dim_customer(
                    session,
                    customer_nk=payload["customer_nk"],
                    email_hash=email_hash,
                    first_name=payload.get("first_name"),
                    last_name=payload.get("last_name"),
                    phone=payload.get("phone"),
                    country_code=payload.get("country_code"),
                )
                result.rows_out += 1
    return result


def load_orders_to_stg(
    source: str,
    *,
    chunk_size: int,
    limit: int | None,
    dry_run: bool,
    ensure_dim_date: bool,
) -> JobResult:
    result = JobResult()
    for path, raw_records in extractors.read_json_stream(source, chunk_size=chunk_size):
        if limit is not None and result.rows_in >= limit:
            break
        normalized = normalize_order_records(raw_records)
        valid, errors = validators.validate_orders(normalized)
        result.rows_in += len(raw_records)
        result.rows_reject += len(errors)
        result.files_processed.append(str(path))

        if dry_run or not valid:
            continue

        if limit is not None:
            previous_total = result.rows_in - len(raw_records)
            remaining = max(0, limit - previous_total)
            to_process = valid[:remaining]
        else:
            to_process = valid

        if not to_process:
            continue

        with session_scope() as session:
            staged = stage_records(session, "orders", to_process, str(path))
            result.duplicates += len(to_process) - staged
            for order in to_process:
                payload = order.model_dump(mode="python")
                email_hash = compute_email_hash(payload.get("email"))

                customer_id = upsert_dim_customer(
                    session,
                    customer_nk=payload["customer_nk"],
                    email_hash=email_hash,
                    first_name=None,
                    last_name=None,
                    phone=None,
                    country_code=None,
                )
                product_id = upsert_dim_product(
                    session,
                    sku=payload["sku"],
                    name=payload.get("product_name") or payload["sku"],
                    brand=payload.get("brand"),
                    category=payload.get("category"),
                    price_list=None,
                )

                channel_code = sanitize_channel_code(payload.get("channel_code"))
                if channel_code is None:
                    result.rows_reject += 1
                    logger.warning(
                        "order_invalid_channel", order_id=payload["order_id"]
                    )
                    continue
                channel_id = get_channel_id(session, channel_code)
                if channel_id is None:
                    result.rows_reject += 1
                    continue

                txn_dt = payload["transaction_ts"].date()
                date_key = ensure_date_key(
                    session, txn_dt, ensure_if_missing=ensure_dim_date
                )
                if date_key is None:
                    result.rows_reject += 1
                    continue

                try:
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
                        discount_amount=payload.get("discount_amount", Decimal("0")),
                        cost_amount=None,
                    )
                except IntegrityError:
                    result.rows_reject += 1
                    continue

                if inserted:
                    result.rows_out += 1
                    # Collect processed record for cache invalidation
                    result.processed_records.append(payload)
                else:
                    result.duplicates += 1

    return result


def stage_records(
    session: Session,
    entity: str,
    records: Sequence[BaseModel],
    source_file: str,
) -> int:
    """Stage validated records into the raw schema, returning inserted rows."""
    if not records:
        return 0

    insert_stmt = text(
        f"""
        INSERT INTO stg.{entity}_raw (payload, payload_hash, source_file)
        VALUES (cast(:payload AS jsonb), :payload_hash, :source_file)
        ON CONFLICT (source_file, payload_hash) DO NOTHING
        """
    )

    inserted = 0
    for record in records:
        payload_dict: dict[str, Any] = record.model_dump(mode="json")
        payload_json = _canonical_json(payload_dict)
        payload_hash = _hash_payload(payload_json)
        result = session.execute(
            insert_stmt,
            {
                "payload": payload_json,
                "payload_hash": payload_hash,
                "source_file": source_file,
            },
        )
        rowcount = int(getattr(result, "rowcount", 0) or 0)
        inserted += rowcount
    return inserted


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _hash_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
