from __future__ import annotations

import math
from dataclasses import dataclass, field

from .models import Decision, Metrics, MetricStatus, RunSpec

REQUIRED_GUARDRAILS: tuple[str, ...] = (
    "correctness",
    "failures",
    "duration_seconds",
    "cost_usd",
    "task_p95_seconds",
    "spill_disk_bytes",
)


@dataclass(frozen=True)
class Evaluation:
    decision: Decision
    reason: str
    reasons: tuple[str, ...] = ()
    unassessed_guardrails: tuple[str, ...] = ()
    deltas: dict[str, float | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.reasons and self.reason:
            object.__setattr__(self, "reasons", (self.reason,))


def _delta(candidate: float | int | None, baseline: float | int | None) -> float | None:
    if candidate is None or baseline is None:
        return None
    if isinstance(candidate, bool) or isinstance(baseline, bool):
        return None
    cand_f = float(candidate)
    base_f = float(baseline)
    if not (math.isfinite(cand_f) and math.isfinite(base_f)):
        return None
    if base_f == 0.0:
        return 0.0 if cand_f == 0.0 else None
    return (cand_f - base_f) / base_f


def _usable_value(metrics: Metrics, name: str) -> float | int | None:
    mv = metrics.get_metric(name)
    if not mv.is_usable:
        return None
    return mv.value


def evaluate(spec: RunSpec, candidate: Metrics) -> Evaluation:
    cand_corr = candidate.get_metric("correctness")
    if cand_corr.is_usable and float(cand_corr.value) == 0.0:  # type: ignore[arg-type]
        return Evaluation(
            Decision.ROLLBACK,
            "correctness gate failed",
            reasons=("correctness gate failed",),
        )
    cand_failures = candidate.get_metric("failures")
    if cand_failures.is_usable and float(cand_failures.value) != 0.0:  # type: ignore[arg-type]
        return Evaluation(
            Decision.ROLLBACK,
            "candidate has unexpected failures",
            reasons=("candidate has unexpected failures",),
        )

    unassessed: list[str] = []
    reasons: list[str] = []

    # Validate baseline & candidate correctness / failures guardrails
    base_corr = spec.baseline.get_metric("correctness")
    if not base_corr.is_usable:
        unassessed.append("correctness")
        reasons.append(f"baseline correctness is {base_corr.status.value}")
    elif float(base_corr.value) == 0.0:  # type: ignore[arg-type]
        unassessed.append("correctness")
        reasons.append("baseline correctness gate failed")
    if not cand_corr.is_usable:
        unassessed.append("correctness")
        reasons.append(f"candidate correctness is {cand_corr.status.value}")

    base_fail = spec.baseline.get_metric("failures")
    if not base_fail.is_usable:
        unassessed.append("failures")
        reasons.append(f"baseline failures is {base_fail.status.value}")
    elif float(base_fail.value) != 0.0:  # type: ignore[arg-type]
        unassessed.append("failures")
        reasons.append("baseline has non-zero failures")
    if not cand_failures.is_usable:
        unassessed.append("failures")
        reasons.append(f"candidate failures is {cand_failures.status.value}")

    # Validate required numeric guardrails
    for field_name in ("duration_seconds", "cost_usd", "task_p95_seconds", "spill_disk_bytes"):
        bm = spec.baseline.get_metric(field_name)
        cm = candidate.get_metric(field_name)
        if not bm.is_usable:
            unassessed.append(field_name)
            reasons.append(f"baseline {field_name} is {bm.status.value}")
        if not cm.is_usable:
            unassessed.append(field_name)
            reasons.append(f"candidate {field_name} is {cm.status.value}")

    base_dur = _usable_value(spec.baseline, "duration_seconds")
    cand_dur = _usable_value(candidate, "duration_seconds")
    base_cost = _usable_value(spec.baseline, "cost_usd")
    cand_cost = _usable_value(candidate, "cost_usd")
    base_p95 = _usable_value(spec.baseline, "task_p95_seconds")
    cand_p95 = _usable_value(candidate, "task_p95_seconds")
    base_spill = _usable_value(spec.baseline, "spill_disk_bytes")
    cand_spill = _usable_value(candidate, "spill_disk_bytes")

    duration_delta = _delta(cand_dur, base_dur)
    cost_delta = _delta(cand_cost, base_cost)
    p95_delta = _delta(cand_p95, base_p95)
    spill_delta = _delta(cand_spill, base_spill)
    deltas: dict[str, float | None] = {
        "duration_seconds": duration_delta,
        "cost_usd": cost_delta,
        "task_p95_seconds": p95_delta,
        "spill_disk_bytes": spill_delta,
    }

    # Check comparability (workload_id, unit mismatches, INVALID metrics)
    comp_errors = spec.baseline.validate_comparable(candidate)
    for err in comp_errors:
        reasons.append(err)
        for field_name in REQUIRED_GUARDRAILS:
            if f"'{field_name}'" in err:
                unassessed.append(field_name)

    # Check ParseDiagnostics completeness
    for label, diag in (("baseline", spec.baseline.parse_diagnostics), ("candidate", candidate.parse_diagnostics)):
        if diag is not None and not diag.is_complete:
            detail = "; ".join(diag.warnings) if diag.warnings else "missing start/end or malformed lines"
            reasons.append(f"{label} event-log parse is incomplete: {detail}")
            unassessed.append("parse_diagnostics")

    unassessed_tuple = tuple(dict.fromkeys(unassessed))

    # Explicit guardrail regressions trigger ROLLBACK even if another metric was missing
    p95_zero_regression = base_p95 is not None and cand_p95 is not None and float(base_p95) == 0.0 and float(cand_p95) > 0.0
    if (p95_delta is not None and p95_delta > spec.max_p95_regression) or p95_zero_regression:
        all_reasons = tuple(dict.fromkeys(("task p95 regression exceeded guardrail", *reasons)))
        return Evaluation(
            Decision.ROLLBACK,
            "task p95 regression exceeded guardrail",
            reasons=all_reasons,
            unassessed_guardrails=unassessed_tuple,
            deltas=deltas,
        )
    spill_zero_regression = base_spill is not None and cand_spill is not None and float(base_spill) == 0.0 and float(cand_spill) > 0.0
    if (spill_delta is not None and spill_delta > spec.max_spill_regression) or spill_zero_regression:
        all_reasons = tuple(dict.fromkeys(("spill regression exceeded guardrail", *reasons)))
        return Evaluation(
            Decision.ROLLBACK,
            "spill regression exceeded guardrail",
            reasons=all_reasons,
            unassessed_guardrails=unassessed_tuple,
            deltas=deltas,
        )

    dur_zero_regression = base_dur is not None and cand_dur is not None and float(base_dur) == 0.0 and float(cand_dur) > 0.0
    cost_zero_regression = base_cost is not None and cand_cost is not None and float(base_cost) == 0.0 and float(cand_cost) > 0.0
    if dur_zero_regression:
        reasons.append("duration_seconds regressed from zero baseline")
    if cost_zero_regression:
        reasons.append("cost_usd regressed from zero baseline")

    # Never ACCEPT with unassessed guardrails, comparability errors, or incomplete diagnostics
    reasons_tuple = tuple(dict.fromkeys(reasons))
    if unassessed_tuple or comp_errors or reasons_tuple:
        summary = "; ".join(reasons_tuple)
        if (dur_zero_regression or cost_zero_regression) and not unassessed_tuple and not comp_errors:
            return Evaluation(
                Decision.ROLLBACK,
                summary,
                reasons=reasons_tuple,
                unassessed_guardrails=unassessed_tuple,
                deltas=deltas,
            )
        return Evaluation(
            Decision.INCONCLUSIVE,
            summary,
            reasons=reasons_tuple,
            unassessed_guardrails=unassessed_tuple,
            deltas=deltas,
        )

    improved = (
        (duration_delta is not None and duration_delta <= -spec.min_duration_improvement)
        or (cost_delta is not None and cost_delta <= -spec.min_cost_improvement)
    )
    if improved:
        return Evaluation(
            Decision.ACCEPT,
            "performance or cost target met",
            reasons=("performance or cost target met",),
            unassessed_guardrails=(),
            deltas=deltas,
        )
    return Evaluation(
        Decision.INCONCLUSIVE,
        "candidate did not meet improvement target",
        reasons=("candidate did not meet improvement target",),
        unassessed_guardrails=(),
        deltas=deltas,
    )
