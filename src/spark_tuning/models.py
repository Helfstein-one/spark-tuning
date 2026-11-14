from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class RunState(str, Enum):
    INTAKE = "intake"
    BASELINE = "baseline"
    DIAGNOSE = "diagnose"
    EXPERIMENT = "experiment"
    VERIFY = "verify"
    ITERATE = "iterate"
    ACCEPTED = "accepted"
    ROLLBACK = "rollback"
    BLOCKED = "blocked"
    PR_READY = "pr_ready"


class Decision(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    ROLLBACK = "rollback"
    INCONCLUSIVE = "inconclusive"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class Metrics:
    duration_seconds: float
    cost_usd: float
    shuffle_write_bytes: int = 0
    spill_disk_bytes: int = 0
    gc_seconds: float = 0.0
    task_p95_seconds: float = 0.0
    failures: int = 0
    correctness: bool = True


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    skill: str
    objective: str
    baseline: Metrics
    max_iterations: int = 7
    max_total_cost_usd: float = 200.0
    max_runtime_minutes: float = 120.0
    max_changes_per_iteration: int = 1
    min_duration_improvement: float = 0.10
    min_cost_improvement: float = 0.10
    max_p95_regression: float = 0.20
    max_spill_regression: float = 0.20
    hypotheses: tuple[dict[str, Any], ...] = ()


@dataclass
class Iteration:
    number: int
    hypothesis: str
    metrics: Metrics | None = None
    decision: Decision | None = None
    reason: str = ""


@dataclass
class LoopResult:
    run_id: str
    state: RunState
    decision: Decision
    iterations: list[Iteration] = field(default_factory=list)
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
