"""Tier 1 E2E Tests — F1 (#35): Metric Semantics & Missing-Value Evaluation."""

from __future__ import annotations

import unittest

from spark_tuning.evaluation.pareto import ParetoDecisionEngine
from spark_tuning.evaluator import evaluate
from spark_tuning.models import Decision, Iteration, LoopResult, Metrics, RunSpec, RunState
from spark_tuning.reporting.report_generator import OptimizationReportGenerator
from spark_tuning.safety.quality_gate import DataQualityGate, QualityCheckStatus
from e2e_tests.helpers import get_unassessed_guardrails


class TestTier1F1Metrics(unittest.TestCase):
    """T1-F1-01..05: Happy-path metric semantics, guardrails, Pareto, report, quality gate."""

    def test_t1_f1_01_metric_value_creation_and_metadata_retention(self) -> None:
        """T1-F1-01: MetricValue retains value, unit, source, timestamp, quality, and MetricStatus.OK."""
        from spark_tuning.models import MetricStatus, MetricValue

        mv = MetricValue(
            value=120.5,
            unit="seconds",
            source="event_log",
            timestamp="2026-11-18T00:00:00Z",
            quality="high",
            status=MetricStatus.OK,
        )
        self.assertEqual(mv.value, 120.5)
        self.assertEqual(mv.unit, "seconds")
        self.assertEqual(mv.source, "event_log")
        self.assertEqual(mv.timestamp, "2026-11-18T00:00:00Z")
        self.assertEqual(mv.quality, "high")
        self.assertEqual(mv.status, MetricStatus.OK)

        # Backward-compatible Metrics construction leaves omitted optional fields as None
        m = Metrics(duration_seconds=120.5, cost_usd=1.2)
        self.assertEqual(m.duration_seconds, 120.5)
        self.assertEqual(m.cost_usd, 1.2)
        self.assertIsNone(m.spill_disk_bytes)
        self.assertIsNone(m.task_p95_seconds)

    def test_t1_f1_02_evaluator_accept_when_all_guardrails_present_and_target_met(self) -> None:
        """T1-F1-02: evaluate() returns ACCEPT with empty unassessed_guardrails when all guardrails pass."""
        baseline = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            task_p95_seconds=10.0,
            spill_disk_bytes=1000,
            failures=0,
            correctness=True,
        )
        spec = RunSpec(
            run_id="t1-f1-02",
            skill="partition-sizing",
            objective="improve duration and cost",
            baseline=baseline,
        )
        candidate = Metrics(
            duration_seconds=80.0,
            cost_usd=8.0,
            task_p95_seconds=11.0,
            spill_disk_bytes=1100,
            failures=0,
            correctness=True,
        )

        evaluation = evaluate(spec, candidate)
        self.assertEqual(evaluation.decision, Decision.ACCEPT)
        self.assertEqual(get_unassessed_guardrails(evaluation), ())

    def test_t1_f1_03_evaluator_inconclusive_when_target_missed_and_guardrails_pass(self) -> None:
        """T1-F1-03: evaluate() returns INCONCLUSIVE when improvement target missed but guardrails pass."""
        baseline = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            task_p95_seconds=10.0,
            spill_disk_bytes=1000,
            failures=0,
            correctness=True,
        )
        spec = RunSpec(
            run_id="t1-f1-03",
            skill="partition-sizing",
            objective="10% improvement",
            baseline=baseline,
            min_duration_improvement=0.10,
            min_cost_improvement=0.10,
        )
        candidate = Metrics(
            duration_seconds=95.0,
            cost_usd=9.5,
            task_p95_seconds=10.0,
            spill_disk_bytes=1000,
            failures=0,
            correctness=True,
        )

        evaluation = evaluate(spec, candidate)
        self.assertEqual(evaluation.decision, Decision.INCONCLUSIVE)
        self.assertEqual(get_unassessed_guardrails(evaluation), ())

    def test_t1_f1_04_pareto_frontier_preserves_multi_objective_tradeoffs(self) -> None:
        """T1-F1-04: ParetoDecisionEngine marks non-dominated trade-off optimal and worse candidate dominated."""
        baseline = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            spill_disk_bytes=500,
            gc_seconds=20.0,
        )
        cand_a = Metrics(
            duration_seconds=80.0,
            cost_usd=12.0,
            spill_disk_bytes=200,
            gc_seconds=15.0,
        )
        cand_b = Metrics(
            duration_seconds=110.0,
            cost_usd=15.0,
            spill_disk_bytes=600,
            gc_seconds=25.0,
        )

        frontier = {
            c.candidate_id: c
            for c in ParetoDecisionEngine.compute_frontier(
                baseline, {"cand_a": cand_a, "cand_b": cand_b}
            )
        }
        self.assertTrue(frontier["cand_a"].is_pareto_optimal)
        self.assertIn("Trade-off", frontier["cand_a"].trade_off_notes)
        self.assertFalse(frontier["cand_b"].is_pareto_optimal)

    def test_t1_f1_05_report_generator_and_quality_gate_happy_path(self) -> None:
        """T1-F1-05: OptimizationReportGenerator & DataQualityGate succeed on complete matching evidence."""
        baseline = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            spill_disk_bytes=512,
            gc_seconds=4.5,
            task_p95_seconds=12.0,
        )
        cand = Metrics(
            duration_seconds=75.0,
            cost_usd=7.5,
            spill_disk_bytes=128,
            gc_seconds=2.0,
            task_p95_seconds=9.0,
        )
        result = LoopResult(
            run_id="t1-f1-05",
            state=RunState.PR_READY,
            decision=Decision.ACCEPT,
            iterations=[
                Iteration(
                    number=1,
                    hypothesis="aqe-coalesce",
                    metrics=cand,
                    decision=Decision.ACCEPT,
                    reason="performance target met",
                )
            ],
            reason="performance target met",
        )
        markdown = OptimizationReportGenerator.generate_markdown(result, baseline)
        self.assertIn("512", markdown)
        self.assertIn("128", markdown)
        self.assertIn("4.5", markdown)
        self.assertIn("2.0", markdown)

        report = DataQualityGate.evaluate(
            {"row_count": 5000, "checksum": "sha256:abc", "schema": ["id", "amount"]},
            {"row_count": 5000, "checksum": "sha256:abc", "schema": ["id", "amount"]},
        )
        self.assertTrue(report.passed)
        self.assertEqual(len(report.checks), 3)
        self.assertTrue(all(c.status == QualityCheckStatus.PASSED for c in report.checks))


if __name__ == "__main__":
    unittest.main()
