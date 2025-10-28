"""Tests for dim_date seeding script."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import select, text

from backend.app.models import DimDate
from etl.db import session_scope
from infra.seed.seed_dim_date import seed_dim_date


@pytest.mark.usefixtures("db_engine")
def test_seed_dim_date_populates_range() -> None:
    start = date(2024, 1, 1)
    end = date(2024, 1, 10)

    with session_scope() as session:
        session.execute(
            text("DELETE FROM dw.dim_date WHERE date BETWEEN :start AND :end"),
            {"start": start, "end": end},
        )

    inserted = seed_dim_date(start, end, chunk_size=16)
    assert inserted >= 10

    with session_scope() as session:
        results = (
            session.execute(
                select(DimDate)
                .where(DimDate.date.between(start, end))
                .order_by(DimDate.date)
            )
            .scalars()
            .all()
        )

    assert len(results) == 10
    sample = {row.date: row for row in results}
    # date_key format YYYYMMDD
    assert sample[date(2024, 1, 1)].date_key == 20240101
    assert sample[date(2024, 1, 6)].is_weekend is True  # Saturday
    assert sample[date(2024, 1, 7)].dow == 7  # Sunday

    with session_scope() as session:
        session.execute(
            text("DELETE FROM dw.dim_date WHERE date BETWEEN :start AND :end"),
            {"start": start, "end": end},
        )
