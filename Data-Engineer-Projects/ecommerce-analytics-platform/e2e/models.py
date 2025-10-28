"""Data models used by the E2E runner and reporters."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class StepResult:
    name: str
    status: str
    duration: float
    detail: str = ""
    data: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.status.lower() == "pass"


@dataclass
class E2EResult:
    started_at: datetime
    finished_at: datetime
    steps: list[StepResult] = field(default_factory=list)
    api_checks: list[StepResult] = field(default_factory=list)
    dim_counts: dict[str, int] = field(default_factory=dict)
    fact_sales_count: int = 0
    errors: list[str] = field(default_factory=list)
    settings_snapshot: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors and all(step.ok for step in self.iter_results())

    def iter_results(self) -> Iterable[StepResult]:
        yield from self.steps
        yield from self.api_checks

    @property
    def duration(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()
