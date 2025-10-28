"""Report generation module."""

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime

from qa.checks.code_quality import CodeQualityResult
from qa.checks.db_perf import DBPerformanceResult
from qa.checks.health import HealthCheckResult
from qa.checks.perf_api import PerformanceTestResult
from qa.checks.streaming import StreamingMetrics


@dataclass
class QAReport:
    """Complete QA report results."""

    timestamp: datetime
    duration: float
    code_quality: list[CodeQualityResult]
    health: list[HealthCheckResult]
    performance: PerformanceTestResult
    db_performance: list[DBPerformanceResult]
    streaming: StreamingMetrics
    overall_status: str

    def write_markdown(self, path: str):
        """Write report as Markdown."""
        with open(path, "w") as f:
            f.write("# QA Health Report\n\n")
            f.write(f"Generated: {self.timestamp.isoformat()}\n")
            f.write(f"Duration: {self.duration:.1f}s\n")
            f.write(f"Status: **{self.overall_status}**\n\n")

            # Code Quality
            f.write("## Code Quality Checks\n\n")
            f.write("| Tool | Status | Duration | Issues |\n")
            f.write("|------|--------|-----------|--------|\n")
            for r in self.code_quality:
                f.write(
                    f"| {r.tool} | {r.status} | {r.duration:.1f}s | "
                    f"{r.error_count} |\n"
                )
            f.write("\n")

            # Health Checks
            f.write("## Health Checks\n\n")
            f.write("| Endpoint | Status | Response Time | Details |\n")
            f.write("|----------|--------|---------------|----------|\n")
            for r in self.health:
                f.write(
                    f"| {r.endpoint} | {r.status} | {r.response_time*1000:.0f}ms | "
                    f"{r.details[:50]}... |\n"
                )
            f.write("\n")

            # API Performance
            f.write("## API Performance\n\n")
            f.write("### Sales Summary Endpoint\n\n")
            f.write(f"- Path: {self.performance.sales_summary.path}\n")
            f.write(f"- P95 Latency: {self.performance.sales_summary.p95_ms:.1f}ms\n")
            f.write(
                f"- Cache Hit Ratio: {self.performance.sales_summary.cache_hit_ratio:.2%}\n"
            )
            f.write("\n")

            f.write("### Recommendations Endpoint\n\n")
            f.write(f"- Path: {self.performance.recommendations.path}\n")
            f.write(f"- P95 Latency: {self.performance.recommendations.p95_ms:.1f}ms\n")
            f.write(
                f"- Cache Hit Ratio: {self.performance.recommendations.cache_hit_ratio:.2%}\n"
            )
            f.write("\n")

            # Database Performance
            f.write("## Database Performance\n\n")
            for r in self.db_performance:
                f.write(f"### {r.query_name}\n\n")
                f.write(f"Status: {r.status}\n\n")
                f.write("```\n")
                f.write(f"Planning Time: {r.explain_result.planning_time:.1f}ms\n")
                f.write(f"Execution Time: {r.explain_result.execution_time:.1f}ms\n")
                f.write(f"Plan Type: {r.explain_result.plan_type}\n")
                if r.explain_result.index_used:
                    f.write(f"Index Used: {r.explain_result.index_used}\n")
                f.write("```\n\n")

            # Streaming Metrics
            f.write("## Streaming Metrics\n\n")
            if self.streaming.kafka_lag_p95 is not None:
                f.write(
                    f"- Kafka Lag P95: {self.streaming.kafka_lag_p95:.0f} messages\n"
                )
            if self.streaming.data_freshness_p95 is not None:
                f.write(
                    f"- Data Freshness P95: {self.streaming.data_freshness_p95:.1f}m\n"
                )
            f.write(f"\nStatus: {self.streaming.status}\n")
            f.write(f"Details: {self.streaming.details}\n")

    def write_junit_xml(self, path: str):
        """Write report as JUnit XML."""
        root = ET.Element("testsuites")

        # Code Quality Suite
        code_suite = ET.SubElement(root, "testsuite")
        code_suite.set("name", "code_quality")
        for r in self.code_quality:
            case = ET.SubElement(code_suite, "testcase")
            case.set("name", r.tool)
            case.set("time", str(r.duration))
            if r.status != "PASS":
                failure = ET.SubElement(case, "failure")
                failure.set("message", f"{r.error_count} issues found")
                failure.text = r.details

        # Health Suite
        health_suite = ET.SubElement(root, "testsuite")
        health_suite.set("name", "health")
        for r in self.health:
            case = ET.SubElement(health_suite, "testcase")
            case.set("name", r.endpoint)
            case.set("time", str(r.response_time))
            if r.status != "PASS":
                failure = ET.SubElement(case, "failure")
                failure.set("message", r.details)

        # Performance Suite
        perf_suite = ET.SubElement(root, "testsuite")
        perf_suite.set("name", "performance")

        # Sales endpoint
        sales_case = ET.SubElement(perf_suite, "testcase")
        sales_case.set("name", "sales_summary_p95")
        if self.performance.sales_summary.p95_ms > 250:
            failure = ET.SubElement(sales_case, "failure")
            failure.set(
                "message",
                f"P95 latency {self.performance.sales_summary.p95_ms:.1f}ms > 250ms",
            )

        # Write XML
        tree = ET.ElementTree(root)
        tree.write(path, encoding="utf-8", xml_declaration=True)

    def get_exit_code(self) -> int:
        """Get process exit code based on results."""
        if self.overall_status == "FAIL":
            return 1
        elif self.overall_status == "WARN":
            return 2
        return 0
