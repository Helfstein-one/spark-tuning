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
        def _usable_num(m: Metrics, field_name: str) -> float | int | None:
            mv = m.get_metric(field_name)
            return mv.value if mv.is_usable else None

        def _agg_correctness(runs: List[Metrics], gate_passed: bool | None = None) -> bool | None:
            saw_unknown = False
            if gate_passed is False:
                return False
            if gate_passed is None:
                pass
            for run in runs:
                mv = run.get_metric("correctness")
                if not mv.is_usable:
                    saw_unknown = True
                elif float(mv.value) == 0.0:  # type: ignore[arg-type]
                    return False
            return None if saw_unknown else True

        cand_fail_vals = [_usable_num(m, "failures") for m in candidate_runs]
        base_fail_vals = [_usable_num(m, "failures") for m in baseline_runs]
        cand_failures = (
            sum(int(v) for v in cand_fail_vals if v is not None)
            if all(v is not None for v in cand_fail_vals)
            else None
        )
        base_failures = (
            sum(int(v) for v in base_fail_vals if v is not None)
            if all(v is not None for v in base_fail_vals)
            else None
        )

        cand_durs_usable = all(_usable_num(m, "duration_seconds") is not None for m in candidate_runs)
        base_durs_usable = all(_usable_num(m, "duration_seconds") is not None for m in baseline_runs)
        cand_dur = stat_comp.candidate_summary.median if cand_durs_usable else None
        base_dur = stat_comp.baseline_summary.median if base_durs_usable else None
        cand_cost = round(cand_dur * spec.cost_per_second_usd, 4) if cand_dur is not None else None
        base_cost = round(base_dur * spec.cost_per_second_usd, 4) if base_dur is not None else None

        cand_spill_vals = [_usable_num(m, "spill_disk_bytes") for m in candidate_runs]
        base_spill_vals = [_usable_num(m, "spill_disk_bytes") for m in baseline_runs]
        cand_spill = (
            max(int(v) for v in cand_spill_vals if v is not None)
            if all(v is not None for v in cand_spill_vals)
            else None
        )
        base_spill = (
            max(int(v) for v in base_spill_vals if v is not None)
            if all(v is not None for v in base_spill_vals)
            else None
        )

        cand_gc_vals = [_usable_num(m, "gc_seconds") for m in candidate_runs]
        base_gc_vals = [_usable_num(m, "gc_seconds") for m in baseline_runs]
        cand_gc = (
            max(float(v) for v in cand_gc_vals if v is not None)
            if all(v is not None for v in cand_gc_vals)
            else None
        )
        base_gc = (
            max(float(v) for v in base_gc_vals if v is not None)
            if all(v is not None for v in base_gc_vals)
            else None
        )

        cand_correctness = _agg_correctness(candidate_runs, gate_passed=quality_rep.passed)
        base_correctness = _agg_correctness(baseline_runs)
        cand_incomplete_diag = next(
            (m.parse_diagnostics for m in candidate_runs if m.parse_diagnostics is not None and not m.parse_diagnostics.is_complete),
            None,
        )
        base_incomplete_diag = next(
            (m.parse_diagnostics for m in baseline_runs if m.parse_diagnostics is not None and not m.parse_diagnostics.is_complete),
            None,
        )

        cand_median_metric = Metrics(
            duration_seconds=cand_dur,
            cost_usd=cand_cost,
            spill_disk_bytes=cand_spill,
            gc_seconds=cand_gc,
            failures=cand_failures,
            correctness=cand_correctness,
            parse_diagnostics=cand_incomplete_diag,
        )
        base_median_metric = Metrics(
            duration_seconds=base_dur,
            cost_usd=base_cost,
            spill_disk_bytes=base_spill,
            gc_seconds=base_gc,
            failures=base_failures,
            correctness=base_correctness,
            parse_diagnostics=base_incomplete_diag,
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

        spill_eliminated = (
            all(v is not None for v in base_spill_vals)
            and any(v > 0 for v in base_spill_vals if v is not None)
            and all(v is not None and v == 0 for v in cand_spill_vals)
        )

        cand_runs_correct = _agg_correctness(candidate_runs)
        quality_preserved = bool(quality_rep.passed and cand_runs_correct is True)
        if not quality_rep.passed:
            quality_text = f"but data quality gate failed ({quality_rep.reason})."
        elif cand_runs_correct is False:
            quality_text = "but candidate correctness gate failed."
        elif cand_runs_correct is None:
            quality_text = "but candidate correctness status is unknown."
        else:
            quality_text = "with 100% data quality preservation."

        scorecard = SkillScorecard(
            skill_name=spec.skill_name,
            workload_name=spec.workload_name,
            duration_improvement_pct=dur_pct,
            cost_improvement_pct=cost_pct,
            spill_eliminated=spill_eliminated,
            quality_preserved=quality_preserved,
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
