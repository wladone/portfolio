"""Report writers for end-to-end pipeline results."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from .models import E2EResult


def write_junit_xml(result: E2EResult, path: str | Path = "e2e_report.xml") -> Path:
    """Serialize results to JUnit XML format."""
    path = Path(path)
    tests = list(result.iter_results())
    testsuite = ET.Element(
        "testsuite",
        attrib={
            "name": "e2e",
            "tests": str(len(tests)),
            "failures": str(sum(0 if step.ok else 1 for step in tests)),
            "time": f"{result.duration:.3f}",
        },
    )
    for step in tests:
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            attrib={"name": step.name, "time": f"{step.duration:.3f}"},
        )
        if not step.ok:
            failure = ET.SubElement(testcase, "failure")
            failure.text = step.detail
    tree = ET.ElementTree(testsuite)
    path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(path, encoding="utf-8", xml_declaration=True)
    return path


def write_markdown(result: E2EResult, path: str | Path = "e2e_report.md") -> Path:
    """Write a human-friendly Markdown summary."""
    path = Path(path)
    lines: list[str] = []
    status = "✅" if result.ok else "❌"
    lines.append(f"# End-to-End Report {status}")
    lines.append("")
    lines.append(f"Generated at: {datetime.utcnow().isoformat()}Z")
    lines.append(f"Duration: {result.duration:.2f}s")
    lines.append("")
    lines.append("## Stage Results")
    lines.append("| Stage | Status | Duration (s) | Detail |")
    lines.append("| --- | --- | --- | --- |")
    for step in result.steps:
        lines.append(
            f"| {step.name} | {'PASS' if step.ok else 'FAIL'} | {step.duration:.2f} | {step.detail or '-'} |"
        )
    if result.api_checks:
        lines.append("")
        lines.append("## API Checks")
        lines.append("| Check | Status | Duration (s) | Detail |")
        lines.append("| --- | --- | --- | --- |")
        for step in result.api_checks:
            lines.append(
                f"| {step.name} | {'PASS' if step.ok else 'FAIL'} | {step.duration:.3f} | {step.detail or '-'} |"
            )
    lines.append("")
    lines.append("## Warehouse Counts")
    lines.append("| Table | Rows |")
    lines.append("| --- | --- |")
    for table, count in sorted(result.dim_counts.items()):
        lines.append(f"| {table} | {count} |")
    lines.append(f"| dw.fact_sales | {result.fact_sales_count} |")
    if result.errors:
        lines.append("")
        lines.append("## Errors")
        for err in result.errors:
            lines.append(f"- {err}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
