"""QA audit runner."""

import asyncio
import time
from datetime import datetime
from pathlib import Path

import click

from qa.budgets import PerformanceBudgets
from qa.checks.code_quality import run_code_quality_checks
from qa.checks.db_perf import run_db_performance_checks
from qa.checks.health import run_health_checks
from qa.checks.perf_api import run_api_performance_tests
from qa.checks.streaming import get_streaming_metrics
from qa.report import QAReport
from qa.settings import Settings


async def run_qa_audit(settings: Settings, budgets: PerformanceBudgets) -> QAReport:
    """Run all QA checks and generate report."""
    start_time = time.time()

    # Run checks in parallel
    code_quality_task = asyncio.create_task(run_code_quality_checks())
    health_task = asyncio.create_task(run_health_checks(settings.health))
    perf_task = asyncio.create_task(
        run_api_performance_tests(settings.load_test, budgets.latency)
    )
    db_task = asyncio.create_task(run_db_performance_checks(settings.database))
    stream_task = asyncio.create_task(get_streaming_metrics(settings.metrics))

    # Gather results
    code_quality = await code_quality_task
    health = await health_task
    performance = await perf_task
    db_performance = await db_task
    streaming = await stream_task

    # Calculate overall status
    statuses = [
        all(r.status == "PASS" for r in code_quality),
        all(r.status == "PASS" for r in health),
        performance.status == "PASS",
        all(r.status == "PASS" for r in db_performance),
        streaming.status == "PASS",
    ]

    if all(statuses):
        overall = "PASS"
    elif any(s == "FAIL" for s in statuses):
        overall = "FAIL"
    else:
        overall = "WARN"

    return QAReport(
        timestamp=datetime.now(),
        duration=time.time() - start_time,
        code_quality=code_quality,
        health=health,
        performance=performance,
        db_performance=db_performance,
        streaming=streaming,
        overall_status=overall,
    )


@click.command()
@click.option(
    "--output",
    "-o",
    type=click.Choice(["markdown", "junit"]),
    default="markdown",
    help="Output format",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default="reports",
    help="Output directory for reports",
)
@click.option(
    "--fail-on-warn", is_flag=True, help="Return non-zero exit code on warnings"
)
async def main(output: str, output_dir: str, fail_on_warn: bool):
    """Run QA health audit.

    Creates a report checking:
    - Code quality (lint, type checks, tests)
    - Runtime health checks
    - API performance tests
    - Database performance metrics
    - Streaming latency and throughput
    """
    settings = Settings()
    budgets = PerformanceBudgets()

    # Run audit
    report = await run_qa_audit(settings, budgets)

    # Create output directory
    out_dir = Path(output_dir)
    out_dir.mkdir(exist_ok=True)

    # Write report
    if output == "markdown":
        out_file = out_dir / f"qa_report_{report.timestamp:%Y%m%d_%H%M%S}.md"
        report.write_markdown(out_file)
    else:
        out_file = out_dir / f"qa_report_{report.timestamp:%Y%m%d_%H%M%S}.xml"
        report.write_junit_xml(out_file)

    click.echo(f"Report written to {out_file}")

    # Exit with status
    exit_code = report.get_exit_code()
    if fail_on_warn and report.overall_status == "WARN":
        exit_code = 1
    raise SystemExit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())
