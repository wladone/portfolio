"""Performance budgets and thresholds."""

from dataclasses import dataclass


@dataclass
class Budget:
    """Performance budget with hard and soft thresholds."""

    hard: float | None
    soft: float | None
    hard_min: float | None = None
    soft_min: float | None = None
    hard_max: float | None = None
    soft_max: float | None = None

    def evaluate(self, value: float) -> str:
        """Evaluate a value against budgets."""
        if self.hard is not None:
            if abs(value) > self.hard:
                return "FAIL"
        if self.hard_min is not None:
            if value < self.hard_min:
                return "FAIL"
        if self.hard_max is not None:
            if value > self.hard_max:
                return "FAIL"

        if self.soft is not None:
            if abs(value) > self.soft:
                return "WARN"
        if self.soft_min is not None:
            if value < self.soft_min:
                return "WARN"
        if self.soft_max is not None:
            if value > self.soft_max:
                return "WARN"

        return "PASS"


# Performance budgets
BUDGETS: dict[str, Budget] = {
    "sales_summary_p95_ms": Budget(hard=250, soft=200),
    "recs_user_p95_ms": Budget(hard=200, soft=150),
    "cache_hit_ratio": Budget(hard_min=0.60, soft_min=0.70),
    "db_pool_in_use_pct": Budget(hard_max=0.80, soft_max=0.70),
    "kafka_lag_p95": Budget(hard_max=500, soft_max=200),
    "freshness_minutes_p95": Budget(hard_max=2, soft_max=1),
}

# Code quality thresholds
CODE_QUALITY = {
    "ruff_max_errors": 0,
    "black_max_errors": 0,
    "mypy_max_errors": 0,
    "pytest_max_failures": 0,
    "bandit_max_issues": 0,
}

# Database performance thresholds (ms)
DB_PERF = {
    "planning_time_max": 50,
    "execution_time_max": 200,
}
