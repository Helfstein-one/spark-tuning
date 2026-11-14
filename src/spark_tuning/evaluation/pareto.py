"""Multi-objective Pareto decisioning engine evaluating performance vs cost trade-offs."""

from __future__ import annotations

from dataclasses import dataclass
from spark_tuning.models import Metrics


@dataclass
class ParetoCandidate:
    candidate_id: str
    metrics: Metrics
    is_pareto_optimal: bool = False
    trade_off_notes: str = ""


class ParetoDecisionEngine:
    """Evaluates multi-objective trade-offs (e.g. Duration vs Cost vs Spill vs GC)."""

    @classmethod
    def dominates(cls, a: Metrics, b: Metrics) -> bool:
        """Returns True if candidate 'a' dominates 'b' (better or equal in all, strictly better in at least one)."""
        # We want to minimize: duration_seconds, cost_usd, spill_disk_bytes, gc_seconds, failures
        a_spill = a.spill_disk_bytes or 0
        b_spill = b.spill_disk_bytes or 0
        a_gc = a.gc_seconds or 0.0
        b_gc = b.gc_seconds or 0.0

        not_worse = (
            a.duration_seconds <= b.duration_seconds
            and a.cost_usd <= b.cost_usd
            and a_spill <= b_spill
            and a_gc <= b_gc
            and a.failures <= b.failures
            and (a.correctness >= b.correctness)
        )

        strictly_better = (
            a.duration_seconds < b.duration_seconds
            or a.cost_usd < b.cost_usd
            or a_spill < b_spill
            or a_gc < b_gc
            or a.failures < b.failures
        )

        return not_worse and strictly_better

    @classmethod
    def compute_frontier(
        cls,
        baseline: Metrics,
        candidates: dict[str, Metrics],
    ) -> list[ParetoCandidate]:
        results: list[ParetoCandidate] = []
        all_metrics = {"baseline": baseline, **candidates}

        for cand_id, cand_metric in candidates.items():
            # Check if any candidate or baseline strictly dominates this candidate
            dominated = False
            for other_id, other_metric in all_metrics.items():
                if other_id == cand_id:
                    continue
                if cls.dominates(other_metric, cand_metric):
                    dominated = True
                    break

            is_optimal = not dominated
            notes = ""
            if is_optimal:
                if cand_metric.duration_seconds < baseline.duration_seconds and cand_metric.cost_usd > baseline.cost_usd:
                    notes = "Trade-off: Faster execution but higher cost."
                elif cand_metric.cost_usd < baseline.cost_usd and cand_metric.duration_seconds > baseline.duration_seconds:
                    notes = "Trade-off: Cost savings with slightly higher runtime."
                elif cand_metric.duration_seconds < baseline.duration_seconds and cand_metric.cost_usd <= baseline.cost_usd:
                    notes = "Dominant win: Improved duration without cost increase."

            results.append(
                ParetoCandidate(
                    candidate_id=cand_id,
                    metrics=cand_metric,
                    is_pareto_optimal=is_optimal,
                    trade_off_notes=notes,
                )
            )

        return results
