"""Unit tests for the three new performance and efficiency modules."""

import unittest
from spark_tuning.core.bayesian_opt import BayesianOptimizer, HyperparameterSpace
from spark_tuning.runners.live_monitor import SparkLiveMonitor
from spark_tuning.reporting.dashboard import InteractiveDashboardGenerator
from spark_tuning.models import LoopResult, Metrics, Iteration, RunState, Decision


class AdvancedSkillsExtensionTests(unittest.TestCase):
    def test_bayesian_optimizer_active_search(self):
        # Optimize shuffle partitions between 100 and 800 with step 50
        space = HyperparameterSpace(param_name="spark.sql.shuffle.partitions", min_value=100, max_value=800, step=50)
        optimizer = BayesianOptimizer(space=space)

        # First recommendation is midpoint
        cand1 = optimizer.suggest_next()
        self.assertEqual(cand1, 450)

        # Feed trial observation
        optimizer.observe(value=450, score=120.0)
        cand2 = optimizer.suggest_next()
        self.assertNotEqual(cand2, 450)
        self.assertTrue(100 <= cand2 <= 800)

    def test_live_monitor_early_abort(self):
        monitor = SparkLiveMonitor(max_allowed_spill_bytes=5 * 1024 * 1024 * 1024)

        # Normal stage metrics
        ok_metrics = {"diskBytesSpilled": 1024, "memoryBytesSpilled": 1024, "jvmGcTime": 500}
        dec_ok = monitor.evaluate_live_stage_metrics(ok_metrics, elapsed_seconds=20.0)
        self.assertFalse(dec_ok.should_abort)

        # Catastrophic spill early abort
        bad_metrics = {"diskBytesSpilled": 8 * 1024 * 1024 * 1024, "memoryBytesSpilled": 0, "jvmGcTime": 500}
        dec_abort = monitor.evaluate_live_stage_metrics(bad_metrics, elapsed_seconds=20.0)
        self.assertTrue(dec_abort.should_abort)
        self.assertIn("Catastrophic disk spill", dec_abort.reason)

    def test_interactive_dashboard_generator(self):
        import tempfile
        from pathlib import Path

        base = Metrics(duration_seconds=300.0, cost_usd=15.0, spill_disk_bytes=1024 * 1024 * 500, gc_seconds=30.0)
        result = LoopResult(
            run_id="run-opt-999",
            state=RunState.PR_READY,
            decision=Decision.ACCEPT,
            iterations=[
                Iteration(
                    number=1,
                    hypothesis="partition-tuning",
                    metrics=Metrics(duration_seconds=80.0, cost_usd=4.0, spill_disk_bytes=0, gc_seconds=5.0),
                    decision=Decision.ACCEPT,
                    reason="Target met",
                )
            ],
            reason="All guardrails met",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = Path(tmp_dir) / "dashboard.html"
            html = InteractiveDashboardGenerator.generate_html(result, base, output_path=out_file)
            self.assertTrue(out_file.exists())
            self.assertIn("chart.js", html.lower())
            self.assertIn("run-opt-999", html)
            self.assertIn("ACCEPT", html)


if __name__ == "__main__":
    unittest.main()
