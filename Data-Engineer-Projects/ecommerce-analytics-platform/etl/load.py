"""Command-line interface for batch ETL jobs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import uuid4

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

from backend.app.logging_config import configure_logging

from .loaders import (
    JobResult,
    load_customers_to_stg,
    load_orders_to_stg,
    load_products_to_stg,
)
from .settings import get_settings
from .utils.audit import AuditTracker, audit_run
from .utils.invalidation import publish_invalidation

logger = structlog.get_logger(__name__)


def build_parser(
    default_chunk_size: int, default_dry_run: bool
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl.load", description="Batch ETL loader")
    parser.add_argument(
        "--correlation-id",
        dest="correlation_id",
        help="Correlation identifier propagated to logs and audit.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--source",
            required=True,
            help="Glob pattern (or directory for `all`) pointing to input files.",
        )
        subparser.add_argument(
            "--chunk-size",
            type=int,
            default=default_chunk_size,
            help=f"Batch size for processing chunks (default: {default_chunk_size}).",
        )
        subparser.add_argument(
            "--limit",
            type=int,
            help="Optional limit for number of records processed.",
        )
        subparser.add_argument(
            "--dry-run",
            action="store_true",
            default=default_dry_run,
            help="Validate and log without persisting changes.",
        )
        subparser.add_argument(
            "--ensure-dim-date",
            action="store_true",
            help="Insert missing dim_date records when required.",
        )

    add_common_arguments(subparsers.add_parser("orders", help="Load orders from JSON"))
    add_common_arguments(
        subparsers.add_parser("products", help="Load products from CSV")
    )
    add_common_arguments(
        subparsers.add_parser("customers", help="Load customers from JSON")
    )
    add_common_arguments(
        subparsers.add_parser(
            "all", help="Execute customers, products, and orders sequentially"
        )
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = build_parser(settings.default_chunk_size, settings.dry_run)
    args = parser.parse_args(argv)

    correlation_id = args.correlation_id or uuid4().hex
    configure_logging(settings.app_log_level)
    bind_contextvars(correlation_id=correlation_id)

    run_id = uuid4()
    job_name = _job_name(args.command)
    logger.info("etl_job_start", job_name=job_name, run_id=str(run_id))

    try:
        with audit_run(
            job_name=job_name, run_id=run_id, correlation_id=correlation_id
        ) as tracker:
            if args.command == "orders":
                result = load_orders_to_stg(
                    source=args.source,
                    chunk_size=args.chunk_size,
                    limit=args.limit,
                    dry_run=args.dry_run,
                    ensure_dim_date=args.ensure_dim_date or settings.ensure_dim_date,
                )
                _apply_result(tracker, result, args.dry_run)
            elif args.command == "products":
                result = load_products_to_stg(
                    source=args.source,
                    chunk_size=args.chunk_size,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
                _apply_result(tracker, result, args.dry_run)
            elif args.command == "customers":
                result = load_customers_to_stg(
                    source=args.source,
                    chunk_size=args.chunk_size,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
                _apply_result(tracker, result, args.dry_run)
            elif args.command == "all":
                results = _execute_all(
                    base=args.source,
                    chunk_size=args.chunk_size,
                    limit=args.limit,
                    dry_run=args.dry_run,
                    ensure_dim_date=args.ensure_dim_date or settings.ensure_dim_date,
                )
                aggregated = _aggregate_results(results)
                _apply_result(tracker, aggregated, args.dry_run)
                tracker.add_detail(
                    "subjobs",
                    {name: result.__dict__ for name, result in results.items()},
                )
            else:
                raise ValueError(f"Unsupported command {args.command}")

            tracker.add_detail("dry_run", args.dry_run)
            tracker.add_detail("chunk_size", args.chunk_size)
            if args.limit is not None:
                tracker.add_detail("limit", args.limit)
    except FileNotFoundError as exc:
        logger.error("etl_source_missing", error=str(exc))
        clear_contextvars()
        return 1
    except Exception:
        clear_contextvars()
        raise
    else:
        logger.info("etl_job_completed", job_name=job_name, run_id=str(run_id))
        clear_contextvars()
        return 0


def _job_name(command: str) -> str:
    return f"{command}_batch"


def _apply_result(tracker: AuditTracker, result: JobResult, dry_run: bool) -> None:
    tracker.increment(
        rows_in=result.rows_in,
        rows_out=result.rows_out,
        rows_reject=result.rows_reject,
    )
    tracker.extend_files(result.files_processed)
    tracker.add_detail("duplicates", result.duplicates)
    tracker.set_status("OK" if not dry_run else "OK")
    logger.info(
        "etl_job_stats",
        job_name=tracker.job_name,
        run_id=str(tracker.run_id),
        rows_in=result.rows_in,
        rows_out=result.rows_out,
        rows_reject=result.rows_reject,
        duplicates=result.duplicates,
        dry_run=dry_run,
    )

    # Trigger cache invalidation if successful and not dry run
    if not dry_run and result.rows_out > 0:
        _invalidate_cache_for_job(tracker.job_name, result)


def _execute_all(
    *,
    base: str,
    chunk_size: int,
    limit: int | None,
    dry_run: bool,
    ensure_dim_date: bool,
) -> dict[str, JobResult]:
    base_path = Path(base)
    base_dir = base_path if base_path.is_dir() else base_path.parent
    sources = {
        "customers": str(base_dir / "customers_*.json"),
        "products": str(base_dir / "products*.csv"),
        "orders": str(base_dir / "orders_*.json"),
    }

    results: dict[str, JobResult] = {}
    results["customers"] = load_customers_to_stg(
        source=sources["customers"],
        chunk_size=chunk_size,
        limit=limit,
        dry_run=dry_run,
    )
    results["products"] = load_products_to_stg(
        source=sources["products"],
        chunk_size=chunk_size,
        limit=limit,
        dry_run=dry_run,
    )
    results["orders"] = load_orders_to_stg(
        source=sources["orders"],
        chunk_size=chunk_size,
        limit=limit,
        dry_run=dry_run,
        ensure_dim_date=ensure_dim_date,
    )
    return results


def _aggregate_results(results: dict[str, JobResult]) -> JobResult:
    aggregated = JobResult()
    for result in results.values():
        aggregated.merge(result)
    return aggregated


def _invalidate_cache_for_job(job_name: str, result: JobResult) -> None:
    """Trigger cache invalidation based on job type and processed data."""
    import asyncio

    from .utils.invalidation import collect_orders_invalidation_payload

    settings = get_settings()

    async def _async_invalidate():
        try:
            if "orders" in job_name:
                # For orders, collect date range and channels from processed data
                payload = collect_orders_invalidation_payload(result.processed_records)
                await publish_invalidation(
                    target="sales",
                    strategy=settings.cache_invalidation_strategy,
                    payload=payload,
                )
            elif "customers" in job_name or "products" in job_name:
                # For customers/products, trigger recs namespace invalidation
                await publish_invalidation(
                    target="recs",
                    strategy="namespace",
                )
        except Exception as e:
            logger.error("Cache invalidation failed", job_name=job_name, error=str(e))

    # Run async invalidation in event loop
    try:
        asyncio.run(_async_invalidate())
    except RuntimeError:
        # If already in an event loop, create task
        asyncio.create_task(_async_invalidate())


if __name__ == "__main__":
    sys.exit(main())
