"""Code quality checks module."""

import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CodeQualityResult:
    """Results from a code quality check."""

    tool: str
    status: str
    duration: float
    error_count: int
    details: str


class CodeQualityChecker:
    """Run code quality checks."""

    def __init__(self, project_root: Path):
        """Initialize checker."""
        self.project_root = project_root

    def run_check(
        self, cmd: list[str], tool: str, parse_output: bool = True
    ) -> CodeQualityResult:
        """Run a code quality check command."""
        start_time = time.time()
        try:
            result = subprocess.run(
                cmd, cwd=self.project_root, capture_output=True, text=True, check=False
            )
            duration = time.time() - start_time

            # Parse output
            error_count = 0
            if parse_output:
                if result.stdout:
                    try:
                        # Try parsing JSON output
                        data = json.loads(result.stdout)
                        if isinstance(data, list):
                            error_count = len(data)
                        elif isinstance(data, dict):
                            error_count = len(data.get("errors", []))
                    except json.JSONDecodeError:
                        # Count lines if not JSON
                        error_count = len(result.stdout.strip().split("\n"))

            status = "PASS" if result.returncode == 0 else "FAIL"
            details = result.stdout if result.returncode != 0 else ""

            return CodeQualityResult(
                tool=tool,
                status=status,
                duration=duration,
                error_count=error_count,
                details=details,
            )

        except Exception as e:
            duration = time.time() - start_time
            return CodeQualityResult(
                tool=tool,
                status="ERROR",
                duration=duration,
                error_count=-1,
                details=str(e),
            )

    def run_all(self) -> list[CodeQualityResult]:
        """Run all code quality checks."""
        results = []

        # Ruff
        results.append(self.run_check(["ruff", ".", "--output-format", "json"], "ruff"))

        # Black
        results.append(
            self.run_check(
                ["black", ".", "--check", "--quiet"], "black", parse_output=False
            )
        )

        # MyPy
        results.append(self.run_check(["mypy", ".", "--json"], "mypy"))

        # Pytest
        results.append(
            self.run_check(
                [
                    "pytest",
                    "-q",
                    "--maxfail=1",
                    "--disable-warnings",
                    "--junitxml=.dist/pytest-unit.xml",
                ],
                "pytest",
                parse_output=False,
            )
        )

        # Security checks
        from qa.settings import QASettings

        settings = QASettings()

        if settings.enable_security_checks:
            # Bandit
            results.append(
                self.run_check(
                    ["bandit", "-q", "-r", ".", "-x", "tests", "--json"], "bandit"
                )
            )

            # detect-secrets
            results.append(
                self.run_check(
                    ["detect-secrets", "scan", "--baseline", ".secrets.baseline", "."],
                    "detect-secrets",
                )
            )

        return results
