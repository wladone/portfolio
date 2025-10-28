"""Utility runners wired into Poetry scripts."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


def _run_command(command: list[str]) -> None:
    """Run a subprocess command with error propagation."""
    subprocess.run(command, check=True)


def run_tests() -> int:
    """Execute the pytest suite with coverage."""
    return pytest.main(["--cov"])


def run_ci() -> int:
    """Replicate the CI pipeline (lint, type-check, tests, security scan)."""
    commands = [
        [sys.executable, "-m", "ruff", "."],
        [sys.executable, "-m", "black", "--check", "."],
        [sys.executable, "-m", "mypy", "."],
    ]

    for command in commands:
        _run_command(command)

    test_exit_code = run_tests()
    if test_exit_code != 0:
        return test_exit_code

    with tempfile.NamedTemporaryFile(
        "w+", delete=False, suffix=".txt"
    ) as requirements_file:
        requirements_path = Path(requirements_file.name)

    try:
        _run_command(
            [
                "poetry",
                "export",
                "-f",
                "requirements.txt",
                "--without-hashes",
                "-o",
                str(requirements_path),
            ]
        )
        _run_command([sys.executable, "-m", "pip_audit", "-r", str(requirements_path)])
    finally:
        if requirements_path.exists():
            requirements_path.unlink()

    return 0
