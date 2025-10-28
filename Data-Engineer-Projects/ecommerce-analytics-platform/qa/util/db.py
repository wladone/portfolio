"""Database utility functions."""

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine import Engine


@dataclass
class ExplainResult:
    """Results from EXPLAIN ANALYZE."""

    query_text: str
    planning_time: float
    execution_time: float
    total_time: float
    rows_planned: int
    rows_actual: int
    plan_type: str
    buffers_hit: int | None = None
    buffers_read: int | None = None
    index_used: str | None = None


class DatabaseHelper:
    """Helper for database operations and analysis."""

    def __init__(self, engine: Engine):
        """Initialize with SQLAlchemy engine."""
        self.engine = engine

    def explain_analyze(self, query: str) -> ExplainResult:
        """Run EXPLAIN (ANALYZE, BUFFERS, VERBOSE) on query."""
        explain_sql = f"""
        EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON)
        {query}
        """

        with self.engine.connect() as conn:
            result = conn.execute(sa.text(explain_sql)).scalar()

        plan = result[0]["Plan"]
        timing = result[0]["Planning Time"], result[0]["Execution Time"]

        # Extract plan details
        index_used = None
        if (
            "Index Scan" in plan["Node Type"]
            or "Bitmap Index Scan" in plan["Node Type"]
        ):
            index_used = plan.get("Index Name")

        return ExplainResult(
            query_text=query,
            planning_time=timing[0],
            execution_time=timing[1],
            total_time=sum(timing),
            rows_planned=plan["Plan Rows"],
            rows_actual=plan["Actual Rows"],
            plan_type=plan["Node Type"],
            buffers_hit=plan.get("Shared Hit Blocks", 0),
            buffers_read=plan.get("Shared Read Blocks", 0),
            index_used=index_used,
        )

    def get_pool_stats(self) -> dict[str, int]:
        """Get database connection pool statistics."""
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text(
                    """
                SELECT count(*) as total,
                       count(*) FILTER (WHERE state = 'active') as active,
                       count(*) FILTER (WHERE state = 'idle') as idle
                FROM pg_stat_activity
                WHERE backend_type = 'client backend'
            """
                )
            ).first()

        return {"total": result.total, "active": result.active, "idle": result.idle}

    def get_table_stats(self, schema: str, table: str) -> dict[str, int]:
        """Get table statistics."""
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text(
                    """
                SELECT n_live_tup, n_dead_tup, last_vacuum, last_analyze
                FROM pg_stat_user_tables
                WHERE schemaname = :schema AND relname = :table
            """
                ),
                {"schema": schema, "table": table},
            ).first()

        return {
            "live_rows": result.n_live_tup,
            "dead_rows": result.n_dead_tup,
            "last_vacuum": result.last_vacuum,
            "last_analyze": result.last_analyze,
        }
