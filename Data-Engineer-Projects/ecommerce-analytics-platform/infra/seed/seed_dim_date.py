"""Populate dw.dim_date with deterministic calendar entries."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

import structlog
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from backend.app.models import DimDate
from etl.settings import get_settings

logger = structlog.get_logger(__name__)


def _build_row(current: date) -> dict[str, object]:
    _, iso_week, iso_weekday = current.isocalendar()
    return {
        "date_key": int(current.strftime("%Y%m%d")),
        "date": current,
        "year": current.year,
        "quarter": ((current.month - 1) // 3) + 1,
        "month": current.month,
        "day": current.day,
        "iso_week": iso_week,
        "dow": iso_weekday,
        "is_weekend": iso_weekday >= 6,
    }


def seed_dim_date(start: date, end: date, chunk_size: int = 500) -> int:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
    inserted = 0
    current = start
    with engine.begin() as connection:
        while current <= end:
            chunk: list[dict[str, object]] = []
            for _ in range(chunk_size):
                if current > end:
                    break
                chunk.append(_build_row(current))
                current += timedelta(days=1)
            if not chunk:
                break
            stmt = (
                insert(DimDate)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=[DimDate.date])
            )
            result = connection.execute(stmt)
            inserted += int(getattr(result, "rowcount", 0) or 0)
    logger.info(
        "seed_dim_date_complete",
        start=start.isoformat(),
        end=end.isoformat(),
        inserted=inserted,
    )
    return inserted


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate dw.dim_date between two dates."
    )
    parser.add_argument(
        "--start", required=True, type=str, help="Inclusive start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", required=True, type=str, help="Inclusive end date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--chunk-size", type=int, default=500, help="Batch size for inserts"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    start = datetime.fromisoformat(args.start).date()
    end = datetime.fromisoformat(args.end).date()
    if end < start:
        raise ValueError("end date must not precede start date")
    seed_dim_date(start, end, chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
