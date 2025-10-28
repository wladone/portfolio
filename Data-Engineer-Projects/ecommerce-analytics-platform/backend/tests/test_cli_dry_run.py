"""CLI smoke tests for dry-run execution."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text

from etl.db import session_scope

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.mark.usefixtures("db_engine")
def test_orders_cli_dry_run_does_not_mutate_db() -> None:
    fixture_path = FIXTURE_DIR / "orders_one.json"

    with session_scope() as session:
        before = session.execute(
            text("SELECT COUNT(*) FROM dw.fact_sales")
        ).scalar_one()

    subprocess.run(
        [
            sys.executable,
            "-m",
            "etl.load",
            "orders",
            "--source",
            str(fixture_path),
            "--dry-run",
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[2],
    )

    with session_scope() as session:
        after = session.execute(text("SELECT COUNT(*) FROM dw.fact_sales")).scalar_one()

    assert before == after
