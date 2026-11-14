import unittest
from spark_tuning.models import Metrics, Decision, LoopResult, RunState
from spark_tuning.drift.detector import DriftDetector
from spark_tuning.safety.canary import CanaryValidator
from spark_tuning.safety.sanitizer import ArtifactSanitizer
from spark_tuning.core.registry import ExperimentRegistry, HistoricExperimentRecord
from spark_tuning.reporting.report_generator import OptimizationReportGenerator


class Batch3PillarsTests(unittest.TestCase):
    def test_drift_detector(self):
        base_meta = {"schema": "a INT, b STRING", "row_count": 1_000_000}
        same_meta = {"schema": "a INT, b STRING", "row_count": 1_050_000}
        drifted_meta = {"schema": "a INT, b STRING", "row_count": 2_000_000}

        r1 = DriftDetector.detect_drift(base_meta, same_meta)
        self.assertFalse(r1.drift_detected)

        r2 = DriftDetector.detect_drift(base_meta, drifted_meta)
        self.assertTrue(r2.drift_detected)
        self.assertEqual(r2.volume_drift_ratio, 1.0)

    def test_canary_validator(self):
        base = Metrics(duration_seconds=100.0, cost_usd=1.0)
        cand_good = Metrics(duration_seconds=95.0, cost_usd=0.9, failures=0, correctness=True)
        cand_fail = Metrics(duration_seconds=90.0, cost_usd=0.9, failures=1, correctness=False)

        res_ok = CanaryValidator.evaluate_canary(cand_good, base)
        self.assertTrue(res_ok.promotion_approved)

        res_bad = CanaryValidator.evaluate_canary(cand_fail, base)
        self.assertFalse(res_bad.promotion_approved)
        self.assertTrue(res_bad.rollback_required)

    def test_artifact_sanitizer(self):
        text = "spark.authenticate.secret=MySecretKey123 and gho_abc123456789012345678901234567890"
        sanitized = ArtifactSanitizer.sanitize_text(text)
        self.assertNotIn("MySecretKey123", sanitized)
        self.assertNotIn("gho_abc", sanitized)

    def test_experiment_registry(self):
        reg = ExperimentRegistry()
        reg.record_experiment(
            HistoricExperimentRecord(
                workload_fingerprint="etl_daily_v1",
                hypothesis_id="hyp_skew_salt",
                decision=Decision.REJECT,
                metrics=Metrics(duration_seconds=200.0, cost_usd=2.0),
            )
        )
        self.assertTrue(reg.is_previously_rejected("etl_daily_v1", "hyp_skew_salt"))
        self.assertFalse(reg.is_previously_rejected("etl_daily_v1", "hyp_new_idea"))

    def test_report_generator(self):
        base = Metrics(duration_seconds=100.0, cost_usd=1.0)
        loop_res = LoopResult(run_id="run-test", state=RunState.PR_READY, decision=Decision.ACCEPT, reason="All guardrails met")
        md = OptimizationReportGenerator.generate_markdown(loop_res, base)
        self.assertIn("Spark Optimization Report (run-test)", md)
        self.assertIn("pr_ready", md)


if __name__ == "__main__":
    unittest.main()
