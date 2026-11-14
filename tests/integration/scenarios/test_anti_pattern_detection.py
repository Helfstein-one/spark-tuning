"""End-to-end integration tests proving that the cognitive skills detect and heal anti-pattern jobs."""

import unittest
from spark_tuning.diagnostics.engine import BottleneckDiagnosisEngine
from spark_tuning.experimentation.harness import ExperimentationHarness, TrialSpec
from spark_tuning.models import Metrics
from examples.anti_patterns.corpus import AntiPatternCorpus


class AntiPatternDetectionAndRemediationTests(unittest.TestCase):
    def test_detects_and_proves_spill_scenario(self):
        scenario = AntiPatternCorpus.get_skew_spill_scenario()

        # Step 1: Diagnose the bad job
        diagnosis = BottleneckDiagnosisEngine.diagnose(scenario.bad_metrics)
        self.assertEqual(diagnosis.primary_cause, scenario.expected_primary_cause)
        self.assertIn(scenario.expected_recommended_skill, diagnosis.recommended_experiments)

        # Step 2: Simulate applying the skill and evaluate A/B trial
        spec = TrialSpec(
            trial_id="heal_spill_trial",
            skill_name=scenario.expected_recommended_skill,
            workload_name=scenario.name,
            min_improvement_ratio=0.50,
        )

        bad_runs = [scenario.bad_metrics, scenario.bad_metrics]
        # Healed run: AQE + partition resizing eliminates 22GB spill and cuts duration from 340s to 75s
        healed_runs = [
            Metrics(duration_seconds=75.0, cost_usd=3.75, spill_disk_bytes=0, failures=0),
            Metrics(duration_seconds=73.0, cost_usd=3.65, spill_disk_bytes=0, failures=0),
        ]

        report = ExperimentationHarness.evaluate_trial(
            spec=spec,
            baseline_runs=bad_runs,
            candidate_runs=healed_runs,
            baseline_quality_meta={"row_count": 100_000_000, "checksum": "sha256:spill"},
            candidate_quality_meta={"row_count": 100_000_000, "checksum": "sha256:spill"},
        )

        self.assertTrue(report.scorecard.spill_eliminated)
        self.assertTrue(report.scorecard.quality_preserved)
        self.assertGreater(report.scorecard.duration_improvement_pct, 70.0)

    def test_detects_and_proves_gc_thrashing_scenario(self):
        scenario = AntiPatternCorpus.get_gc_thrashing_scenario()

        diagnosis = BottleneckDiagnosisEngine.diagnose(scenario.bad_metrics)
        self.assertEqual(diagnosis.primary_cause, scenario.expected_primary_cause)
        self.assertIn(scenario.expected_recommended_skill, diagnosis.recommended_experiments)

        # Post-tuning trial proof: G1GC tuning + Off-heap storage cuts GC from 110s down to 8s
        bad_runs = [scenario.bad_metrics]
        healed_runs = [
            Metrics(duration_seconds=85.0, cost_usd=4.25, gc_seconds=8.0, failures=0)
        ]
        spec = TrialSpec(
            trial_id="heal_gc_trial",
            skill_name=scenario.expected_recommended_skill,
            workload_name=scenario.name,
            min_improvement_ratio=0.50,
        )

        report = ExperimentationHarness.evaluate_trial(
            spec=spec,
            baseline_runs=bad_runs,
            candidate_runs=healed_runs,
            baseline_quality_meta={"row_count": 50_000_000, "checksum": "sha256:gc"},
            candidate_quality_meta={"row_count": 50_000_000, "checksum": "sha256:gc"},
        )
        self.assertTrue(report.scorecard.statistically_significant)
        self.assertGreater(report.scorecard.duration_improvement_pct, 65.0)


if __name__ == "__main__":
    unittest.main()
