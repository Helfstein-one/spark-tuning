"""Experimentation abstraction layer: harness, scorecard, and trial tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from spark_tuning.models import Metrics
from spark_tuning.evaluation.statistics import StatisticalBenchmarkComparator, BenchmarkComparison
from spark_tuning.evaluation.pareto import ParetoDecisionEngine, ParetoCandidate
from spark_tuning.safety.quality_gate import DataQualityGate, DataQualityReport


@dataclass
class TrialSpec:
    trial_id: str
    skill_name: str
    workload_name: str
    repetitions: int = 3
    warmup_runs: int = 1
    cost_per_second_usd: float = 0.05
    min_improvement_ratio: float = 0.15


@dataclass
class SkillScorecard:
    skill_name: str
    workload_name: str
    duration_improvement_pct: float
    cost_improvement_pct: float
    spill_eliminated: bool
    quality_preserved: bool
    is_pareto_optimal: bool
    statistically_significant: bool
    summary: str


@dataclass
class ExperimentComparisonReport:
    trial_id: str
    skill_name: str
    baseline_metrics: List[Metrics]
    candidate_metrics: List[Metrics]
    statistical_comparison: BenchmarkComparison
    quality_report: DataQualityReport
    scorecard: SkillScorecard
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ExperimentationHarness:
    """Executes paired A/B trials to prove the measurable power and efficiency of a tuning skill."""

    @classmethod
    def evaluate_trial(
        cls,
        spec: TrialSpec,
        baseline_runs: List[Metrics],
        candidate_runs: List[Metrics],
        baseline_quality_meta: Dict[str, Any],
        candidate_quality_meta: Dict[str, Any],
    ) -> ExperimentComparisonReport:
        if not baseline_runs or not candidate_runs:
            raise ValueError("Baseline and candidate runs must contain at least one metric execution.")

        # 1. Statistical analysis
        stat_comp = StatisticalBenchmarkComparator.compare(
            baseline_runs=baseline_runs,
            candidate_runs=candidate_runs,
            min_improvement=spec.min_improvement_ratio,
        )

        # 2. Quality gate
        quality_rep = DataQualityGate.evaluate(baseline_quality_meta, candidate_quality_meta)

        # 3. Pareto frontier
        cand_failures = (
            sum(m.failures for m in candidate_runs)
            if all(m.failures is not None for m in candidate_runs)
            else None
        )
        base_failures = (
            sum(m.failures for m in baseline_runs)
            if all(m.failures is not None for m in baseline_runs)
            else None
        )
        cand_median_metric = Metrics(
            duration_seconds=stat_comp.candidate_summary.median,
            cost_usd=round(stat_comp.candidate_summary.median * spec.cost_per_second_usd, 4),
            spill_disk_bytes=candidate_runs[0].spill_disk_bytes,
            gc_seconds=candidate_runs[0].gc_seconds,
            failures=cand_failures,
            correctness=quality_rep.passed,
        )
        base_median_metric = Metrics(
            duration_seconds=stat_comp.baseline_summary.median,
            cost_usd=round(stat_comp.baseline_summary.median * spec.cost_per_second_usd, 4),
            spill_disk_bytes=baseline_runs[0].spill_disk_bytes,
            gc_seconds=baseline_runs[0].gc_seconds,
            failures=base_failures,
            correctness=True,
        )

        frontier = ParetoDecisionEngine.compute_frontier(
            baseline=base_median_metric,
            candidates={"candidate": cand_median_metric},
        )
        is_pareto = frontier[0].is_pareto_optimal if frontier else False

        # 4. Scorecard calculations
        dur_pct = round(stat_comp.improvement_ratio * 100, 2)
        cost_base = base_median_metric.cost_usd
        cost_cand = cand_median_metric.cost_usd
        cost_pct = (
            round(((cost_base - cost_cand) / cost_base) * 100, 2)
            if cost_base is not None and cost_cand is not None and cost_base > 0
            else 0.0
        )

        base_spill = baseline_runs[0].spill_disk_bytes
        cand_spill = candidate_runs[0].spill_disk_bytes
        spill_eliminated = (
            base_spill is not None
            and cand_spill is not None
            and base_spill > 0
            and cand_spill == 0
        )

        quality_text = (
            "with 100% data quality preservation."
            if quality_rep.passed
            else f"but data quality gate failed ({quality_rep.reason})."
        )

        scorecard = SkillScorecard(
            skill_name=spec.skill_name,
            workload_name=spec.workload_name,
            duration_improvement_pct=dur_pct,
            cost_improvement_pct=cost_pct,
            spill_eliminated=spill_eliminated,
            quality_preserved=quality_rep.passed,
            is_pareto_optimal=is_pareto,
            statistically_significant=stat_comp.statistically_significant,
            summary=(
                f"Skill '{spec.skill_name}' delivered {dur_pct}% speedup and {cost_pct}% cost reduction "
                f"{quality_text}"
            ),
        )

        return ExperimentComparisonReport(
            trial_id=spec.trial_id,
            skill_name=spec.skill_name,
            baseline_metrics=baseline_runs,
            candidate_metrics=candidate_runs,
            statistical_comparison=stat_comp,
            quality_report=quality_rep,
            scorecard=scorecard,
        )
