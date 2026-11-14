import unittest
from spark_tuning.models import Metrics
from spark_tuning.diagnostics.engine import BottleneckDiagnosisEngine, BottleneckCategory
from spark_tuning.safety.quality_gate import DataQualityGate, QualityCheckStatus
from spark_tuning.safety.change_adapter import SafeChangeAdapter, ChangeCandidate
from spark_tuning.evaluation.statistics import StatisticalBenchmarkComparator, BenchmarkOutcome
from spark_tuning.core.prioritization import HypothesisPrioritizer, HypothesisProposal
from spark_tuning.evaluation.pareto import ParetoDecisionEngine


class Batch1PillarsTests(unittest.TestCase):
    def test_bottleneck_diagnosis_engine(self):
        # Spill scenario
        spill_metrics = Metrics(duration_seconds=100.0, cost_usd=1.0, spill_disk_bytes=500000)
        diag = BottleneckDiagnosisEngine.diagnose(spill_metrics)
        self.assertEqual(diag.primary_cause, BottleneckCategory.SPILL)
        self.assertIn("partition-sizing", diag.recommended_experiments)

        # GC scenario
        gc_metrics = Metrics(duration_seconds=100.0, cost_usd=1.0, gc_seconds=25.0)
        diag_gc = BottleneckDiagnosisEngine.diagnose(gc_metrics)
        self.assertEqual(diag_gc.primary_cause, BottleneckCategory.MEMORY_GC)

    def test_data_quality_gate(self):
        base = {"row_count": 1000, "checksum": "abc"}
        cand_ok = {"row_count": 1000, "checksum": "abc"}
        cand_bad = {"row_count": 999, "checksum": "def"}

        report_ok = DataQualityGate.evaluate(base, cand_ok)
        self.assertTrue(report_ok.passed)

        report_bad = DataQualityGate.evaluate(base, cand_bad)
        self.assertFalse(report_bad.passed)

    def test_safe_change_adapter(self, tmp_path=None):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp_dir:
            allowed_file = Path(tmp_dir) / "conf" / "spark.conf"
            blocked_file = Path(tmp_dir) / "critical.py"
            adapter = SafeChangeAdapter(tmp_dir, allowlist=[str(allowed_file)])

            # Allowed change
            adapter.apply_candidate(ChangeCandidate(target_file=str(allowed_file), content="spark.executor.memory=4g", description="tune"))
            self.assertEqual(allowed_file.read_text(), "spark.executor.memory=4g")

            # Rollback
            adapter.rollback_all()
            self.assertFalse(allowed_file.exists())

            # Blocked change
            with self.assertRaises(PermissionError):
                adapter.apply_candidate(ChangeCandidate(target_file=str(blocked_file), content="hack", description="bad"))

    def test_statistical_benchmark_comparator(self):
        base_runs = [Metrics(duration_seconds=100.0, cost_usd=1.0) for _ in range(3)]
        cand_good = [Metrics(duration_seconds=80.0, cost_usd=0.8) for _ in range(3)]

        res = StatisticalBenchmarkComparator.compare(base_runs, cand_good, min_improvement=0.15)
        self.assertEqual(res.outcome, BenchmarkOutcome.PASS)
        self.assertTrue(res.statistically_significant)

    def test_hypothesis_prioritizer(self):
        diag = BottleneckDiagnosisEngine.diagnose(Metrics(duration_seconds=100.0, cost_usd=1.0, spill_disk_bytes=1000))
        p1 = HypothesisProposal(id="h1", target_category=BottleneckCategory.CPU, description="cpu tune", expected_impact=0.8, confidence=0.8, risk=0.2, estimated_cost_usd=10.0)
        p2 = HypothesisProposal(id="h2", target_category=BottleneckCategory.SPILL, description="fix spill", expected_impact=0.8, confidence=0.8, risk=0.2, estimated_cost_usd=10.0)

        ranked = HypothesisPrioritizer.rank_hypotheses([p1, p2], diag)
        self.assertEqual(ranked[0].hypothesis.id, "h2")

    def test_pareto_decision_engine(self):
        base = Metrics(duration_seconds=100.0, cost_usd=10.0)
        fast_but_expensive = Metrics(duration_seconds=60.0, cost_usd=15.0)
        strictly_worse = Metrics(duration_seconds=110.0, cost_usd=12.0)

        frontier = ParetoDecisionEngine.compute_frontier(base, {"cand_fast": fast_but_expensive, "cand_bad": strictly_worse})
        cand_fast_res = next(c for c in frontier if c.candidate_id == "cand_fast")
        cand_bad_res = next(c for c in frontier if c.candidate_id == "cand_bad")

        self.assertTrue(cand_fast_res.is_pareto_optimal)
        self.assertFalse(cand_bad_res.is_pareto_optimal)


if __name__ == "__main__":
    unittest.main()
