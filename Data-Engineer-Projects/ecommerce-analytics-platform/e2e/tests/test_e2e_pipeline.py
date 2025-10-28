"""Pytest entrypoint for the end-to-end pipeline."""

from __future__ import annotations

import pytest

from e2e import report, runner
from e2e.checks import wait_for_api_health
from e2e.settings import E2ESettings


@pytest.mark.e2e
def test_e2e_pipeline() -> None:
    settings = E2ESettings()

    try:
        health = wait_for_api_health(
            str(settings.api_base_url), timeout_s=min(settings.wait_api_seconds, 15)
        )
    except Exception as exc:  # pragma: no cover - skip path
        pytest.skip(f"API not reachable: {exc}")

    if not health.ok:
        pytest.skip(f"API health check failed before E2E run: {health.detail}")

    result = runner.run(settings)

    report.write_junit_xml(result, "e2e_report.xml")
    report.write_markdown(result, "e2e_report.md")

    assert result.ok, f"E2E pipeline failed: {result.errors}"
    assert result.fact_sales_count > 0
    assert len(result.api_checks) >= 4
