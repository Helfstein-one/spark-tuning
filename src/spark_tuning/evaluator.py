from __future__ import annotations

from dataclasses import dataclass

from .models import Decision, Metrics, RunSpec


@dataclass(frozen=True)
class Evaluation:
    decision: Decision
    reason: str


def _delta(candidate: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0 if candidate == 0 else float("inf")
    return (candidate - baseline) / baseline


def evaluate(spec: RunSpec, candidate: Metrics) -> Evaluation:
    if not candidate.correctness:
        return Evaluation(Decision.ROLLBACK, "correctness gate failed")
    if candidate.failures != 0:
        return Evaluation(Decision.ROLLBACK, "candidate has unexpected failures")

    duration_delta = _delta(candidate.duration_seconds, spec.baseline.duration_seconds)
    cost_delta = _delta(candidate.cost_usd, spec.baseline.cost_usd)
    p95_delta = _delta(candidate.task_p95_seconds, spec.baseline.task_p95_seconds)
    spill_delta = _delta(candidate.spill_disk_bytes, spec.baseline.spill_disk_bytes)

    if p95_delta > spec.max_p95_regression:
        return Evaluation(Decision.ROLLBACK, "task p95 regression exceeded guardrail")
    if spill_delta > spec.max_spill_regression:
        return Evaluation(Decision.ROLLBACK, "spill regression exceeded guardrail")

    improved = (
        duration_delta <= -spec.min_duration_improvement
        or cost_delta <= -spec.min_cost_improvement
    )
    if improved:
        return Evaluation(Decision.ACCEPT, "performance or cost target met")
    return Evaluation(Decision.INCONCLUSIVE, "candidate did not meet improvement target")
