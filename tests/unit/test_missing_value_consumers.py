import unittest

from spark_tuning.diagnostics.engine import BottleneckCategory, BottleneckDiagnosisEngine
from spark_tuning.evaluation.pareto import ParetoDecisionEngine
from spark_tuning.experimentation.harness import ExperimentationHarness, TrialSpec
from spark_tuning.models import Decision, Iteration, LoopResult, Metrics, MetricValue, RunState
from spark_tuning.reporting.dashboard import InteractiveDashboardGenerator
from spark_tuning.reporting.report_generator import OptimizationReportGenerator
from spark_tuning.safety.quality_gate import DataQualityGate, QualityCheckStatus


class MissingValueConsumersTests(unittest.TestCase):
    def test_pareto_does_not_coerce_none_to_zero_or_promote_incorrect(self):
        base = Metrics(100.0, 10.0, spill_disk_bytes=500, gc_seconds=1.0)
        missing_spill = Metrics(90.0, 9.0, spill_disk_bytes=None, gc_seconds=1.0)
        incorrect_fast = Metrics(50.0, 5.0, spill_disk_bytes=0, gc_seconds=0.5, correctness=False)
        true_better = Metrics(80.0, 8.0, spill_disk_bytes=100, gc_seconds=0.5)

        self.assertFalse(ParetoDecisionEngine.dominates(missing_spill, base))
        self.assertFalse(ParetoDecisionEngine.dominates(incorrect_fast, base))
        self.assertTrue(ParetoDecisionEngine.dominates(true_better, base))

        frontier = ParetoDecisionEngine.compute_frontier(
            base,
            {
                "missing_spill": missing_spill,
                "incorrect_fast": incorrect_fast,
                "true_better": true_better,
            },
        )
        by_id = {c.candidate_id: c for c in frontier}
        self.assertFalse(by_id["missing_spill"].is_pareto_optimal)
        self.assertIn("spill_disk_bytes", by_id["missing_spill"].unassessed_dimensions)
        self.assertFalse(by_id["incorrect_fast"].is_pareto_optimal)
        self.assertTrue(by_id["true_better"].is_pareto_optimal)

    def test_report_generator_renders_unknown_for_missing_and_preserves_zero(self):
        base = Metrics(100.0, 10.0, spill_disk_bytes=None, gc_seconds=None)
        cand_zero = Metrics(80.0, 8.0, spill_disk_bytes=0, gc_seconds=0.0)
        cand_unknown_mv = Metrics(
            90.0,
            9.0,
            spill_disk_bytes=0,
            gc_seconds=0.0,
            metric_values={"spill_disk_bytes": MetricValue.unknown(unit="bytes")},
        )
        loop_res = LoopResult(
            run_id="rep-test",
            state=RunState.BLOCKED,
            decision=Decision.INCONCLUSIVE,
            iterations=[
                Iteration(1, "h1", cand_zero, Decision.ACCEPT, "ok"),
                Iteration(2, "h2", cand_unknown_mv, Decision.INCONCLUSIVE, "missing"),
            ],
            reason="partial telemetry",
        )
        md = OptimizationReportGenerator.generate_markdown(loop_res, base)
        lines = md.splitlines()
        base_line = next(l for l in lines if "0 (Baseline)" in l)
        h1_line = next(l for l in lines if "| 1 | h1 |" in l)
        h2_line = next(l for l in lines if "| 2 | h2 |" in l)

        self.assertIn("| unknown | unknown | BASELINE |", base_line)
        self.assertIn("| 0 | 0.0 | accept |", h1_line)
        self.assertIn("| unknown | 0.0 | inconclusive |", h2_line)

    def test_quality_gate_blocks_missing_checksum_and_asymmetric_schema(self):
        res_no_checksum = DataQualityGate.evaluate(
            {"row_count": 100},
            {"row_count": 100},
        )
        self.assertFalse(res_no_checksum.passed)
        self.assertTrue(
            any(c.check_name == "checksum" and c.status == QualityCheckStatus.UNKNOWN for c in res_no_checksum.checks)
        )

        res_asym_schema = DataQualityGate.evaluate(
            {"row_count": 100, "checksum": "abc", "schema": ["id", "val"]},
            {"row_count": 100, "checksum": "abc"},
        )
        self.assertFalse(res_asym_schema.passed)
        self.assertTrue(
            any(c.check_name == "schema" and c.status == QualityCheckStatus.UNKNOWN for c in res_asym_schema.checks)
        )

        res_req_schema = DataQualityGate.evaluate(
            {"row_count": 100, "checksum": "abc"},
            {"row_count": 100, "checksum": "abc"},
            require_schema=True,
        )
        self.assertFalse(res_req_schema.passed)

    def test_harness_does_not_claim_spill_eliminated_when_candidate_spill_is_none(self):
        spec = TrialSpec("t1", "partition-sizing", "wl")
        base_runs = [Metrics(100.0, 10.0, spill_disk_bytes=1024)]
        cand_runs = [Metrics(70.0, 7.0, spill_disk_bytes=None)]
        report = ExperimentationHarness.evaluate_trial(
            spec,
            base_runs,
            cand_runs,
            {"row_count": 100, "checksum": "x"},
            {"row_count": 100, "checksum": "x"},
        )
        self.assertFalse(report.scorecard.spill_eliminated)

    def test_dashboard_renders_unknown_and_null_instead_of_zero(self):
        base = Metrics(100.0, 10.0, spill_disk_bytes=None, gc_seconds=None)
        res = LoopResult(
            run_id="dash-test",
            state=RunState.BLOCKED,
            decision=Decision.INCONCLUSIVE,
            iterations=[Iteration(1, "h1", None, Decision.ROLLBACK, "failed")],
            reason="failed",
        )
        html = InteractiveDashboardGenerator.generate_html(res, base)
        self.assertIn("<td>unknown</td>", html)
        self.assertNotIn("Nones", html)
        self.assertIn("data: [null, null]", html)

    def test_diagnosis_engine_handles_none_duration_safely(self):
        m = Metrics(duration_seconds=None, cost_usd=None, gc_seconds=1.0, task_p95_seconds=2.0, tasks_count=4)
        diag = BottleneckDiagnosisEngine.diagnose(m)
        self.assertEqual(diag.primary_cause, BottleneckCategory.UNKNOWN)


if __name__ == "__main__":
    unittest.main()
