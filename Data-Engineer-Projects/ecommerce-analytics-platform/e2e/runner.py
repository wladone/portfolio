"""End-to-end pipeline runner."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from backend.app.core.db import session_scope
from backend.app.models import DimCustomer, DimProduct, FactSales
from etl import load as etl_load
from infra.seed import generate_seed as seed_generate
from infra.seed import seed_dim_date as seed_dates
from ml.als_train import train_als_model

from . import checks, report
from .models import E2EResult, StepResult
from .settings import E2ESettings

DATA_DIR = Path("infra/seed/data")
CONFIG_PATH = Path("infra/seed/seed_config.yaml")


def _step(name: str, func) -> StepResult:
    start = time.perf_counter()
    try:
        detail, data = func()
        status = "pass"
    except Exception as exc:  # pragma: no cover - instrumentation of failure path
        detail = str(exc)
        data = None
        status = "fail"
    duration = time.perf_counter() - start
    return StepResult(
        name=name, status=status, duration=duration, detail=detail, data=data
    )


def _load_json_count(path: Path) -> int:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return len(payload)


def run(settings: E2ESettings | None = None) -> E2EResult:
    settings = settings or E2ESettings()
    started_at = datetime.now(tz=UTC)
    result = E2EResult(started_at=started_at, finished_at=started_at)
    result.settings_snapshot = settings.model_dump()

    steps: list[StepResult] = []

    def record(step: StepResult) -> None:
        steps.append(step)
        if not step.ok:
            result.errors.append(f"{step.name}: {step.detail}")

    # Seed dim_date
    record(
        _step(
            "seed-dim-date",
            lambda: (
                f"seeded {seed_dates.seed_dim_date(settings.seed_start_date, settings.seed_end_date)} rows",
                {
                    "start": settings.seed_start_date.isoformat(),
                    "end": settings.seed_end_date.isoformat(),
                },
            ),
        )
    )
    if not steps[-1].ok:
        result.steps = steps
        result.finished_at = datetime.now(tz=UTC)
        return result

    # Generate synthetic data
    def generate() -> tuple[str, dict[str, Any]]:
        cfg = seed_generate.load_config(CONFIG_PATH)
        seed_generate.generate_dataset(cfg, DATA_DIR)
        products = 0
        with (DATA_DIR / "products.csv").open(encoding="utf-8") as fh:
            next(fh, None)  # skip header
            for _ in fh:
                if _.strip():
                    products += 1
        customer_files = sorted(DATA_DIR.glob("customers_*.json"))
        order_files = sorted(DATA_DIR.glob("orders_*.json"))
        customers = sum(_load_json_count(path) for path in customer_files)
        orders = sum(_load_json_count(path) for path in order_files)
        return (
            f"generated {customers} customers, {orders} orders, {products} products",
            {"customers": customers, "orders": orders, "products": products},
        )

    record(_step("seed-generate", generate))
    if not steps[-1].ok:
        result.steps = steps
        result.finished_at = datetime.now(tz=UTC)
        return result

    # ETL loads
    def run_etl(args: list[str]) -> tuple[str, dict[str, Any]]:
        exit_code = etl_load.main(args)
        if exit_code not in (0, None):
            raise RuntimeError(
                f"etl.load returned non-zero exit code ({exit_code}) for args {args}"
            )
        return ("etl load completed", {"args": args})

    record(
        _step(
            "etl-products",
            lambda: run_etl(["products", "--source", str(DATA_DIR / "products.csv")]),
        )
    )
    if not steps[-1].ok:
        result.steps = steps
        result.finished_at = datetime.now(tz=UTC)
        return result

    record(
        _step(
            "etl-customers",
            lambda: run_etl(
                ["customers", "--source", str(DATA_DIR / "customers_*.json")]
            ),
        )
    )
    if not steps[-1].ok:
        result.steps = steps
        result.finished_at = datetime.now(tz=UTC)
        return result

    order_args = ["orders", "--source", str(DATA_DIR / "orders_*.json")]
    if settings.ensure_dim_date:
        order_args.append("--ensure-dim-date")
    record(_step("etl-orders", lambda: run_etl(order_args)))
    if not steps[-1].ok:
        result.steps = steps
        result.finished_at = datetime.now(tz=UTC)
        return result

    # Train ALS model
    def train() -> tuple[str, dict[str, Any]]:
        artifact_dir = train_als_model(lookback_days=settings.lookback_days)
        return ("ALS model trained", {"artifact_dir": str(artifact_dir)})

    record(_step("train-als", train))
    result.steps = steps

    # Database counts
    with session_scope() as session:
        dim_customer_count = session.execute(
            select(func.count()).select_from(DimCustomer)
        ).scalar_one()
        dim_product_count = session.execute(
            select(func.count()).select_from(DimProduct)
        ).scalar_one()
        fact_sales_count = session.execute(
            select(func.count()).select_from(FactSales)
        ).scalar_one()
        result.dim_counts = {
            "dw.dim_customer": dim_customer_count,
            "dw.dim_product": dim_product_count,
        }
        result.fact_sales_count = fact_sales_count

    # API checks
    api_checks: list[StepResult] = []
    now = datetime.utcnow().date()
    window_start = (now - timedelta(days=30)).isoformat()
    window_end = now.isoformat()

    outcome = checks.wait_for_api_health(
        str(settings.api_base_url), settings.wait_api_seconds
    )
    api_checks.append(
        StepResult(
            name="wait-health",
            status="pass" if outcome.ok else "fail",
            duration=outcome.latency,
            detail=outcome.detail,
            data=outcome.data if isinstance(outcome.data, dict) else None,
        )
    )

    if outcome.ok:
        ready = checks.check_readyz(str(settings.api_base_url))
        api_checks.append(
            StepResult(
                name="readyz",
                status="pass" if ready.ok else "fail",
                duration=ready.latency,
                detail=ready.detail,
                data=ready.data if isinstance(ready.data, dict) else None,
            )
        )

        summary_day = checks.check_sales_summary(
            str(settings.api_base_url), window_start, window_end, granularity="day"
        )
        api_checks.append(
            StepResult(
                name="sales-summary-day",
                status="pass" if summary_day.ok else "fail",
                duration=summary_day.latency,
                detail=summary_day.detail,
            )
        )

        summary_month = checks.check_sales_summary(
            str(settings.api_base_url), window_start, window_end, granularity="month"
        )
        api_checks.append(
            StepResult(
                name="sales-summary-month",
                status="pass" if summary_month.ok else "fail",
                duration=summary_month.latency,
                detail=summary_month.detail,
            )
        )

        top_products = checks.check_top_products(
            str(settings.api_base_url), window_start, window_end, metric="net"
        )
        api_checks.append(
            StepResult(
                name="top-products",
                status="pass" if top_products.ok else "fail",
                duration=top_products.latency,
                detail=top_products.detail,
            )
        )

        # Sample customer and product ids
        with session_scope() as session:
            user_id = session.execute(
                select(DimCustomer.customer_id)
                .order_by(DimCustomer.customer_id)
                .limit(1)
            ).scalar_one_or_none()
            sku = session.execute(
                select(DimProduct.sku).order_by(DimProduct.sku).limit(1)
            ).scalar_one_or_none()

        if user_id is not None:
            user_recs = checks.check_user_recs(
                str(settings.api_base_url), int(user_id), settings.recs_topk
            )
            api_checks.append(
                StepResult(
                    name="user-recs",
                    status="pass" if user_recs.ok else "fail",
                    duration=user_recs.latency,
                    detail=user_recs.detail,
                )
            )
        else:
            api_checks.append(
                StepResult(
                    name="user-recs",
                    status="fail",
                    duration=0.0,
                    detail="no customers found",
                )
            )

        if sku is not None:
            similar = checks.check_similar(
                str(settings.api_base_url), str(sku), settings.recs_topk
            )
            api_checks.append(
                StepResult(
                    name="similar-products",
                    status="pass" if similar.ok else "fail",
                    duration=similar.latency,
                    detail=similar.detail,
                )
            )
        else:
            api_checks.append(
                StepResult(
                    name="similar-products",
                    status="fail",
                    duration=0.0,
                    detail="no products found",
                )
            )

        metrics = checks.check_metrics_expose(str(settings.api_base_url))
        api_checks.append(
            StepResult(
                name="metrics",
                status="pass" if metrics.ok else "fail",
                duration=metrics.latency,
                detail=metrics.detail,
            )
        )

    result.steps = steps
    result.api_checks = api_checks
    for step in api_checks:
        if not step.ok:
            result.errors.append(f"{step.name}: {step.detail}")
    result.finished_at = datetime.now(tz=UTC)
    return result


def main(argv: list[str] | None = None) -> E2EResult:
    parser = argparse.ArgumentParser(description="Run the end-to-end pipeline")
    parser.add_argument("--junit", type=Path, default=Path("e2e_report.xml"))
    parser.add_argument("--markdown", type=Path, default=Path("e2e_report.md"))
    args = parser.parse_args(argv)

    settings = E2ESettings()
    result = run(settings)
    report.write_junit_xml(result, args.junit)
    report.write_markdown(result, args.markdown)
    if not result.ok:
        raise SystemExit(1)
    return result


if __name__ == "__main__":  # pragma: no cover - CLI execution
    main()
