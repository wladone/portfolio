"""Shared pytest fixtures for database-aware tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from backend.app.config import get_settings


@pytest.fixture(scope="session")
def db_engine() -> Iterator[Engine]:
    """Provide a SQLAlchemy engine for integration tests, skipping if DB unavailable."""
    settings = get_settings()
    connect_args = (
        {"connect_timeout": 2} if settings.database_url.startswith("postgresql") else {}
    )
    engine = create_engine(
        settings.database_url, future=True, connect_args=connect_args
    )

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        pytest.skip(f"Database unavailable for integration tests: {exc!s}")

    yield engine
    engine.dispose()
