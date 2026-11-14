"""Statistical benchmark comparison for multiple execution repetitions."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from enum import Enum
from spark_tuning.models import Metrics


class BenchmarkOutcome(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"


@dataclass
class StatisticalSummary:
    metric_name: str
    sample_count: int
    mean: float
    median: float
    p95: float
    stdev: float
    min_val: float
    max_val: float


@dataclass
class BenchmarkComparison:
    outcome: BenchmarkOutcome
    improvement_ratio: float
    statistically_significant: bool
    baseline_summary: StatisticalSummary
    candidate_summary: StatisticalSummary
    reason: str


class StatisticalBenchmarkComparator:
    """Compares repetitions of baseline vs candidate runs to eliminate noisy decisions."""

    @classmethod
    def summarize(cls, metric_name: str, values: list[float]) -> StatisticalSummary:
        if not values:
            raise ValueError("Cannot summarize empty values list")
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        p95_idx = int(0.95 * (n - 1))
        stdev = statistics.stdev(sorted_vals) if n > 1 else 0.0

        return StatisticalSummary(
            metric_name=metric_name,
            sample_count=n,
            mean=round(statistics.mean(sorted_vals), 4),
            median=round(statistics.median(sorted_vals), 4),
            p95=round(sorted_vals[p95_idx], 4),
            stdev=round(stdev, 4),
            min_val=round(sorted_vals[0], 4),
            max_val=round(sorted_vals[-1], 4),
        )

    @classmethod
    def compare(
        cls,
        baseline_runs: list[Metrics],
        candidate_runs: list[Metrics],
        min_improvement: float = 0.10,
    ) -> BenchmarkComparison:
        base_durations = [m.duration_seconds for m in baseline_runs]
        cand_durations = [m.duration_seconds for m in candidate_runs]

        base_sum = cls.summarize("duration_seconds", base_durations)
        cand_sum = cls.summarize("duration_seconds", cand_durations)

        if base_sum.median == 0:
            improvement = 0.0
        else:
            improvement = (base_sum.median - cand_sum.median) / base_sum.median

        # Check coefficient of variation (high variance marks inconclusive)
        high_variance = (cand_sum.stdev / cand_sum.mean > 0.25) if cand_sum.mean > 0 else False

        if high_variance:
            return BenchmarkComparison(
                outcome=BenchmarkOutcome.INCONCLUSIVE,
                improvement_ratio=round(improvement, 4),
                statistically_significant=False,
                baseline_summary=base_sum,
                candidate_summary=cand_sum,
                reason="High variance across candidate repetitions (>25% CV). Inconclusive.",
            )

        significant = improvement >= min_improvement
        outcome = BenchmarkOutcome.PASS if significant else BenchmarkOutcome.FAIL
        reason = (
            f"Achieved {round(improvement * 100, 2)}% duration improvement with low variance."
            if significant
            else f"Improvement ({round(improvement * 100, 2)}%) did not meet target ({round(min_improvement * 100, 2)}%)."
        )

        return BenchmarkComparison(
            outcome=outcome,
            improvement_ratio=round(improvement, 4),
            statistically_significant=significant,
            baseline_summary=base_sum,
            candidate_summary=cand_sum,
            reason=reason,
        )
