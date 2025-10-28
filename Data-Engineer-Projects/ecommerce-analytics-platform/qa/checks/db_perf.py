"""Database performance checks module."""

from dataclasses import dataclass

from sqlalchemy import create_engine

from qa.budgets import DB_PERF
from qa.util.db import DatabaseHelper, ExplainResult


@dataclass
class DBPerformanceResult:
    """Results from database performance checks."""

    query_name: str
    explain_result: ExplainResult
    status: str
    details: str


class DBPerformanceChecker:
    """Check database query performance."""

    def __init__(self, database_url: str):
        """Initialize checker."""
        self.engine = create_engine(database_url)
        self.db = DatabaseHelper(self.engine)

    def check_query(
        self, name: str, query: str, required_indexes: list[str] | None = None
    ) -> DBPerformanceResult:
        """Check performance of a single query."""
        explain = self.db.explain_analyze(query)

        # Check performance thresholds
        issues = []
        status = "PASS"

        if explain.planning_time > DB_PERF["planning_time_max"]:
            issues.append(
                f"Planning time ({explain.planning_time:.1f}ms) exceeds "
                f"threshold ({DB_PERF['planning_time_max']}ms)"
            )
            status = "WARN"

        if explain.execution_time > DB_PERF["execution_time_max"]:
            issues.append(
                f"Execution time ({explain.execution_time:.1f}ms) exceeds "
                f"threshold ({DB_PERF['execution_time_max']}ms)"
            )
            status = "FAIL"

        # Check for expected indexes
        if required_indexes and not explain.index_used:
            issues.append(
                f"Query not using any index. Expected one of: {required_indexes}"
            )
            status = "WARN"
        elif (
            required_indexes
            and explain.index_used
            and explain.index_used not in required_indexes
        ):
            issues.append(
                f"Query using unexpected index: {explain.index_used}. "
                f"Expected one of: {required_indexes}"
            )
            status = "WARN"

        # Check estimation accuracy
        if explain.rows_actual > explain.rows_planned * 2:
            issues.append(
                f"Large row estimation error: planned={explain.rows_planned}, "
                f"actual={explain.rows_actual}"
            )
            status = "WARN"

        return DBPerformanceResult(
            query_name=name,
            explain_result=explain,
            status=status,
            details="\n".join(issues) if issues else "Query performance acceptable",
        )

    def check_canonical_queries(self) -> list[DBPerformanceResult]:
        """Check performance of canonical queries."""
        results = []

        # Sales summary query
        results.append(
            self.check_query(
                "daily_sales_summary",
                """
            SELECT *
            FROM dw.v_daily_sales_summary
            WHERE date_key BETWEEN CURRENT_DATE - INTERVAL '30 days'
            AND CURRENT_DATE
            ORDER BY date_key DESC
            LIMIT 1000
            """,
                required_indexes=["idx_fact_sales_date"],
            )
        )

        # Top products query
        results.append(
            self.check_query(
                "top_products",
                """
            SELECT p.product_id,
                   p.name,
                   COUNT(*) as order_count,
                   SUM(fs.quantity * fs.unit_price) as total_revenue
            FROM dw.fact_sales fs
            JOIN dw.dim_products p ON fs.product_id = p.product_id
            WHERE fs.order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY p.product_id, p.name
            ORDER BY total_revenue DESC
            LIMIT 100
            """,
                required_indexes=["idx_fact_sales_product", "idx_fact_sales_date"],
            )
        )

        return results

    def check_pool_usage(self) -> dict[str, float]:
        """Check database connection pool usage."""
        stats = self.db.get_pool_stats()

        total = stats["total"]
        if total == 0:
            return {"usage_ratio": 0.0}

        return {
            "usage_ratio": stats["active"] / total,
            "total_connections": total,
            "active_connections": stats["active"],
            "idle_connections": stats["idle"],
        }
