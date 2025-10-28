"""Ensure core database schemas are present."""

from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine


def test_required_schemas_exist(db_engine: Engine) -> None:
    """The warehouse, staging, and metadata schemas must exist."""
    inspector = inspect(db_engine)
    schemas = set(inspector.get_schema_names())
    assert {"dw", "stg", "meta"} <= schemas
