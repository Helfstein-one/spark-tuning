"""Multi-objective Pareto decisioning engine evaluating performance vs cost trade-offs."""

from __future__ import annotations

import math
from dataclasses import dataclass
from spark_tuning.models import Metrics, MetricValue


@dataclass
class ParetoCandidate:
    candidate_id: str
    metrics: Metrics
    is_pareto_optimal: bool = False
    trade_off_notes: str = ""
    unassessed_dimensions: tuple[str, ...] = ()


def _metric_num(metrics: Metrics, field_name: str) -> float | int | None:
    if hasattr(metrics, "get_metric"):
        mv = metrics.get_metric(field_name)
        return mv.value if mv.is_usable else None
    val = getattr(metrics, field_name, None)
    if isinstance(val, MetricValue):
        return val.value if val.is_usable else None
    if val is None or isinstance(val, bool):
        return None
    return val if math.isfinite(float(val)) else None


def _correctness_bool(metrics: Metrics) -> bool | None:
    if hasattr(metrics, "get_metric"):
        mv = metrics.get_metric("correctness")
        if not mv.is_usable:
            return None
        return float(mv.value) != 0.0  # type: ignore[arg-type]
    return getattr(metrics, "correctness", None)


def _has_incomplete_diagnostics(metrics: Metrics) -> bool:
    diag = getattr(metrics, "parse_diagnostics", None)
    return diag is not None and not diag.is_complete


class ParetoDecisionEngine:
    """Evaluates multi-objective trade-offs (e.g. Duration vs Cost vs Spill vs GC)."""

    _REQUIRED_OBJECTIVES: tuple[str, ...] = ("duration_seconds", "cost_usd", "failures")
    _OPTIONAL_OBJECTIVES: tuple[str, ...] = ("spill_disk_bytes", "gc_seconds")

    @classmethod
    def dominates(cls, a: Metrics, b: Metrics) -> bool:
        """Returns True if candidate 'a' dominates 'b' (better or equal in all, strictly better in at least one)."""
        if _has_incomplete_diagnostics(a) or _has_incomplete_diagnostics(b):
            return False
        if hasattr(a, "validate_comparable") and a.validate_comparable(b):
            return False

        a_corr = _correctness_bool(a)
        b_corr = _correctness_bool(b)
        if a_corr is not True or b_corr is None:
            return False

        a_dur = _metric_num(a, "duration_seconds")
        b_dur = _metric_num(b, "duration_seconds")
        a_cost = _metric_num(a, "cost_usd")
        b_cost = _metric_num(b, "cost_usd")
        a_fail = _metric_num(a, "failures")
        b_fail = _metric_num(b, "failures")

        if None in (a_dur, b_dur, a_cost, b_cost, a_fail, b_fail):
            return False

        if a_dur > b_dur or a_cost > b_cost or a_fail > b_fail:  # type: ignore[operator]
            return False
        if int(a_corr) < int(b_corr):
            return False

        strictly_better = (
            a_dur < b_dur  # type: ignore[operator]
            or a_cost < b_cost  # type: ignore[operator]
            or a_fail < b_fail  # type: ignore[operator]
            or (a_corr and not b_corr)
        )

        # Optional objectives: symmetric None is neutral; asymmetric None blocks dominance.
        for field_name in cls._OPTIONAL_OBJECTIVES:
            va = _metric_num(a, field_name)
            vb = _metric_num(b, field_name)
            if va is None and vb is None:
                continue
            if (va is None) != (vb is None):
                return False
            if va > vb:  # type: ignore[operator]
                return False
            if va < vb:  # type: ignore[operator]
                strictly_better = True

        return strictly_better

    @classmethod
    def compute_frontier(
        cls,
        baseline: Metrics,
        candidates: dict[str, Metrics],
    ) -> list[ParetoCandidate]:
        results: list[ParetoCandidate] = []
        all_metrics = {"baseline": baseline, **candidates}

        measured_optional: set[str] = set()
        for metric_obj in all_metrics.values():
            for field_name in cls._OPTIONAL_OBJECTIVES:
                if _metric_num(metric_obj, field_name) is not None:
                    measured_optional.add(field_name)

        base_dur = _metric_num(baseline, "duration_seconds")
        base_cost = _metric_num(baseline, "cost_usd")
        base_incomplete = _has_incomplete_diagnostics(baseline)

        for cand_id, cand_metric in candidates.items():
            unassessed: list[str] = []
            for req_field in cls._REQUIRED_OBJECTIVES:
                if _metric_num(cand_metric, req_field) is None:
                    unassessed.append(req_field)
            cand_corr = _correctness_bool(cand_metric)
            if cand_corr is None:
                unassessed.append("correctness")
            for opt_field in cls._OPTIONAL_OBJECTIVES:
                if opt_field in measured_optional and _metric_num(cand_metric, opt_field) is None:
                    unassessed.append(opt_field)

            comp_errors = baseline.validate_comparable(cand_metric) if hasattr(baseline, "validate_comparable") else []
            for err in comp_errors:
                matched = False
                for known_field in (*cls._REQUIRED_OBJECTIVES, *cls._OPTIONAL_OBJECTIVES, "correctness"):
                    if f"'{known_field}'" in err:
                        unassessed.append(known_field)
                        matched = True
                if "workload_id" in err:
                    unassessed.append("workload_id")
                    matched = True
                if not matched:
                    unassessed.append("comparability")

            cand_incomplete = _has_incomplete_diagnostics(cand_metric)
            if cand_incomplete or base_incomplete:
                unassessed.append("parse_diagnostics")

            unassessed_tuple = tuple(dict.fromkeys(unassessed))
            if cand_corr is not True:
                is_optimal = False
                notes = (
                    "Ineligible: correctness gate failed"
                    if cand_corr is False
                    else "Ineligible: correctness status unknown"
                )
            elif cand_incomplete:
                is_optimal = False
                diag = getattr(cand_metric, "parse_diagnostics", None)
                detail = "; ".join(diag.warnings) if diag and diag.warnings else "incomplete event-log parse"
                notes = f"Ineligible: incomplete parse diagnostics ({detail})"
            elif base_incomplete:
                is_optimal = False
                notes = "Incomparable: baseline event-log parse is incomplete"
            elif comp_errors:
                is_optimal = False
                notes = f"Incomparable: {'; '.join(comp_errors)}"
            elif unassessed_tuple:
                is_optimal = False
                notes = f"Incomparable: missing telemetry ({', '.join(unassessed_tuple)})"
            else:
                dominated = False
                for other_id, other_metric in all_metrics.items():
                    if other_id == cand_id:
                        continue
                    if cls.dominates(other_metric, cand_metric):
                        dominated = True
                        break

                is_optimal = not dominated
                notes = ""
                if is_optimal and base_dur is not None and base_cost is not None:
                    cand_dur = _metric_num(cand_metric, "duration_seconds")
                    cand_cost = _metric_num(cand_metric, "cost_usd")
                    if cand_dur is not None and cand_cost is not None:
                        if cand_dur < base_dur and cand_cost > base_cost:
                            notes = "Trade-off: Faster execution but higher cost."
                        elif cand_cost < base_cost and cand_dur > base_dur:
                            notes = "Trade-off: Cost savings with slightly higher runtime."
                        elif cand_dur < base_dur and cand_cost <= base_cost:
                            notes = "Dominant win: Improved duration without cost increase."

            results.append(
                ParetoCandidate(
                    candidate_id=cand_id,
                    metrics=cand_metric,
                    is_pareto_optimal=is_optimal,
                    trade_off_notes=notes,
                    unassessed_dimensions=unassessed_tuple,
                )
            )

        return results
