import unittest
from spark_tuning.models import Metrics
from spark_tuning.experimentation.harness import (
    ExperimentationHarness,
    TrialSpec,
)


class ExperimentationHarnessTests(unittest.TestCase):
    def test_evaluate_trial_proves_efficiency_gain(self):
        spec = TrialSpec(
            trial_id="trial_001",
            skill_name="partition-sizing",
            workload_name="large_aggregation",
            repetitions=3,
            min_improvement_ratio=0.15,
        )

        # Baseline: slow, expensive, with disk spill (low performance)
        baseline_runs = [
            Metrics(duration_seconds=280.0, cost_usd=14.0, spill_disk_bytes=14 * 1024 * 1024 * 1024),
            Metrics(duration_seconds=285.0, cost_usd=14.2, spill_disk_bytes=14 * 1024 * 1024 * 1024),
            Metrics(duration_seconds=275.0, cost_usd=13.8, spill_disk_bytes=14 * 1024 * 1024 * 1024),
        ]

        # Candidate: fast, cheap, zero spill (high performance)
        candidate_runs = [
            Metrics(duration_seconds=82.0, cost_usd=4.1, spill_disk_bytes=0),
            Metrics(duration_seconds=80.0, cost_usd=4.0, spill_disk_bytes=0),
            Metrics(duration_seconds=84.0, cost_usd=4.2, spill_disk_bytes=0),
        ]

        base_meta = {"row_count": 500_000_000, "checksum": "sha256:abc"}
        cand_meta = {"row_count": 500_000_000, "checksum": "sha256:abc"}

        report = ExperimentationHarness.evaluate_trial(
            spec=spec,
            baseline_runs=baseline_runs,
            candidate_runs=candidate_runs,
            baseline_quality_meta=base_meta,
            candidate_quality_meta=cand_meta,
        )

        # Assertions proving high performance leap
        self.assertTrue(report.quality_report.passed)
        self.assertTrue(report.scorecard.statistically_significant)
        self.assertTrue(report.scorecard.spill_eliminated)
        self.assertTrue(report.scorecard.is_pareto_optimal)
        self.assertGreater(report.scorecard.duration_improvement_pct, 65.0)
        self.assertGreater(report.scorecard.cost_improvement_pct, 65.0)


if __name__ == "__main__":
    unittest.main()
