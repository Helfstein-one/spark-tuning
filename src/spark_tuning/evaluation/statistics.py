"""Statistical benchmark comparison for multiple execution repetitions."""

from __future__ import annotations

import math
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
    mean: float | None
    median: float | None
    p95: float | None
    stdev: float | None
    min_val: float | None
    max_val: float | None


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
    def summarize(cls, metric_name: str, values: list[float | int | None]) -> StatisticalSummary:
        if not values:
            raise ValueError("Cannot summarize empty values list")
        valid_vals = [
            float(v)
            for v in values
            if v is not None and not isinstance(v, bool) and math.isfinite(float(v))
        ]
        if not valid_vals:
            return StatisticalSummary(
                metric_name=metric_name,
                sample_count=0,
                mean=None,
                median=None,
                p95=None,
                stdev=None,
                min_val=None,
                max_val=None,
            )
        sorted_vals = sorted(valid_vals)
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
        if not baseline_runs or not candidate_runs:
            raise ValueError("Baseline and candidate runs must contain at least one metric execution.")

        def _extract_duration(m: Metrics) -> float | None:
            mv = m.get_metric("duration_seconds") if hasattr(m, "get_metric") else None
            if mv is not None:
                return float(mv.value) if mv.is_usable else None
            val = m.duration_seconds
            if val is None or isinstance(val, bool):
                return None
            fval = float(val)
            return fval if math.isfinite(fval) else None

        base_durations = [_extract_duration(m) for m in baseline_runs]
        cand_durations = [_extract_duration(m) for m in candidate_runs]

        base_sum = cls.summarize("duration_seconds", base_durations)
        cand_sum = cls.summarize("duration_seconds", cand_durations)

        if any(d is None for d in base_durations) or any(d is None for d in cand_durations):
            return BenchmarkComparison(
                outcome=BenchmarkOutcome.INCONCLUSIVE,
                improvement_ratio=0.0,
                statistically_significant=False,
                baseline_summary=base_sum,
                candidate_summary=cand_sum,
                reason="Missing or non-finite duration_seconds in benchmark runs. Inconclusive.",
            )

        assert base_sum.median is not None and cand_sum.median is not None
        assert cand_sum.stdev is not None and cand_sum.mean is not None

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
