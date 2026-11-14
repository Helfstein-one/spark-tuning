import unittest
from spark_tuning.models import Metrics, LoopResult, RunState, Decision
from spark_tuning.reporting.finops import FinOpsEngine, BudgetGuardrail
from spark_tuning.reporting.lineage import LineageObserver, RunEvent
from spark_tuning.core.compatibility import CompatibilityMatrix, RuntimeProfile
from spark_tuning.evaluation.chaos import ChaosBenchmarkSuite
from spark_tuning.safety.pr_gate import PRAutomationGate


class Batch2PillarsTests(unittest.TestCase):
    def test_finops_budget_guardrails(self):
        budget = BudgetGuardrail(max_budget_usd=100.0)
        report_ok = FinOpsEngine.evaluate_cost(current_cost_usd=50.0, budget=budget, row_count=1_000_000)
        self.assertFalse(report_ok.budget_exceeded)
        self.assertFalse(report_ok.budget_alert)
        self.assertEqual(report_ok.cost_per_million_rows, 50.0)

        report_exceeded = FinOpsEngine.evaluate_cost(current_cost_usd=120.0, budget=budget)
        self.assertTrue(report_exceeded.budget_exceeded)

    def test_lineage_observer(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            obs = LineageObserver(export_dir=tmp_dir)
            event = RunEvent(run_id="run-123", event_type="COMPLETE", inputs=["table_a"], outputs=["table_b"])
            saved_file = obs.record_run_event(event)
            self.assertTrue(saved_file.exists())

    def test_compatibility_matrix(self):
        valid = RuntimeProfile(spark_version="3.5.0", python_version="3.11", java_version="17", cloud_platform="EMR")
        ok, _ = CompatibilityMatrix.validate_profile(valid)
        self.assertTrue(ok)

        invalid = RuntimeProfile(spark_version="2.4.0", python_version="2.7", java_version="8", cloud_platform="UnknownCloud")
        ok_bad, msg = CompatibilityMatrix.validate_profile(invalid)
        self.assertFalse(ok_bad)

    def test_chaos_suite(self):
        m_recovered = Metrics(duration_seconds=30.0, cost_usd=1.0, failures=0, correctness=True)
        res = ChaosBenchmarkSuite.evaluate_spot_interruption(m_recovered, max_allowed_recovery_seconds=60.0)
        self.assertTrue(res.recovered_successfully)
        self.assertFalse(res.slo_violated)

    def test_pr_automation_gate(self):
        accepted_loop = LoopResult(run_id="r1", state=RunState.PR_READY, decision=Decision.ACCEPT, reason="target met")
        spec = PRAutomationGate.build_pr_spec(accepted_loop, "partition-sizing")
        self.assertIn("r1", spec.title)

        blocked_loop = LoopResult(run_id="r2", state=RunState.ROLLBACK, decision=Decision.ROLLBACK, reason="failed")
        with self.assertRaises(PermissionError):
            PRAutomationGate.build_pr_spec(blocked_loop, "join-strategy")


if __name__ == "__main__":
    unittest.main()
