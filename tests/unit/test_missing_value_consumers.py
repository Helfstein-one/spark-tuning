import unittest

from spark_tuning.diagnostics.engine import BottleneckCategory, BottleneckDiagnosisEngine
from spark_tuning.evaluation.pareto import ParetoDecisionEngine
from spark_tuning.evaluation.statistics import BenchmarkOutcome, StatisticalBenchmarkComparator
from spark_tuning.experimentation.harness import ExperimentationHarness, TrialSpec
from spark_tuning.models import Decision, Iteration, LoopResult, Metrics, MetricValue, ParseDiagnostics, RunState
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

    def test_harness_and_statistics_handle_none_durations_correctness_and_mixed_spill(self):
        spec = TrialSpec("t-missing", "partition-sizing", "wl")
        base_runs = [Metrics(100.0, 10.0, spill_disk_bytes=1024)]

        comp_none = StatisticalBenchmarkComparator.compare(
            base_runs,
            [Metrics(duration_seconds=None, cost_usd=None, spill_disk_bytes=0)],
        )
        self.assertEqual(comp_none.outcome, BenchmarkOutcome.INCONCLUSIVE)
        self.assertFalse(comp_none.statistically_significant)

        rep_none = ExperimentationHarness.evaluate_trial(
            spec,
            base_runs,
            [Metrics(duration_seconds=None, cost_usd=None, spill_disk_bytes=0)],
            {"row_count": 100, "checksum": "x"},
            {"row_count": 100, "checksum": "x"},
        )
        self.assertEqual(rep_none.statistical_comparison.outcome, BenchmarkOutcome.INCONCLUSIVE)
        self.assertFalse(rep_none.scorecard.is_pareto_optimal)

        rep_bad_corr = ExperimentationHarness.evaluate_trial(
            spec,
            base_runs,
            [Metrics(50.0, 2.5, spill_disk_bytes=0, correctness=False)],
            {"row_count": 100, "checksum": "x"},
            {"row_count": 100, "checksum": "x"},
        )
        self.assertFalse(rep_bad_corr.scorecard.is_pareto_optimal)
        self.assertFalse(rep_bad_corr.scorecard.quality_preserved)

        rep_unk_corr = ExperimentationHarness.evaluate_trial(
            spec,
            base_runs,
            [Metrics(50.0, 2.5, spill_disk_bytes=0, correctness=None)],
            {"row_count": 100, "checksum": "x"},
            {"row_count": 100, "checksum": "x"},
        )
        self.assertFalse(rep_unk_corr.scorecard.is_pareto_optimal)
        self.assertFalse(rep_unk_corr.scorecard.quality_preserved)

        rep_mixed_spill = ExperimentationHarness.evaluate_trial(
            spec,
            base_runs,
            [
                Metrics(50.0, 2.5, spill_disk_bytes=0),
                Metrics(50.0, 2.5, spill_disk_bytes=500000),
            ],
            {"row_count": 100, "checksum": "x"},
            {"row_count": 100, "checksum": "x"},
        )
        self.assertFalse(rep_mixed_spill.scorecard.spill_eliminated)

    def test_pareto_rejects_unit_mismatches_and_incomplete_diagnostics(self):
        base = Metrics(100.0, 10.0, spill_disk_bytes=100)
        unit_bad = Metrics(50.0, 5.0, spill_disk_bytes=0, units={"duration_seconds": "ms"})
        diag_bad = Metrics(
            50.0,
            5.0,
            spill_disk_bytes=0,
            parse_diagnostics=ParseDiagnostics(is_complete=False, truncated=True),
        )
        self.assertFalse(ParetoDecisionEngine.dominates(unit_bad, base))
        self.assertFalse(ParetoDecisionEngine.dominates(diag_bad, base))

        frontier = ParetoDecisionEngine.compute_frontier(
            base,
            {"unit_bad": unit_bad, "diag_bad": diag_bad},
        )
        by_id = {c.candidate_id: c for c in frontier}
        self.assertFalse(by_id["unit_bad"].is_pareto_optimal)
        self.assertIn("duration_seconds", by_id["unit_bad"].unassessed_dimensions)
        self.assertFalse(by_id["diag_bad"].is_pareto_optimal)
        self.assertIn("parse_diagnostics", by_id["diag_bad"].unassessed_dimensions)

    def test_quality_gate_rejects_bool_and_negative_row_counts(self):
        rep_bool = DataQualityGate.evaluate(
            {"row_count": True, "checksum": "x"},
            {"row_count": 1, "checksum": "x"},
        )
        self.assertFalse(rep_bool.passed)
        self.assertTrue(
            any(c.check_name == "row_count" and c.status == QualityCheckStatus.FAILED for c in rep_bool.checks)
        )

        rep_neg = DataQualityGate.evaluate(
            {"row_count": -1, "checksum": "x"},
            {"row_count": -1, "checksum": "x"},
        )
        self.assertFalse(rep_neg.passed)


if __name__ == "__main__":
    unittest.main()
