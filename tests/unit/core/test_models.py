import unittest
from spark_tuning.core import Decision, Metrics, RunSpec, RunState
from spark_tuning.ports import ExperimentRunner


class CoreModelsTests(unittest.TestCase):
    def test_metrics_defaults_preserve_none(self):
        m = Metrics(duration_seconds=10.0, cost_usd=1.0)
        self.assertIsNone(m.spill_disk_bytes)
        self.assertIsNone(m.shuffle_write_bytes)
        self.assertIsNone(m.gc_seconds)
        self.assertEqual(m.failures, 0)
        self.assertTrue(m.correctness)

    def test_run_spec_initialization(self):
        spec = RunSpec(
            run_id="run-1",
            skill="join-strategy",
            objective="latency",
            baseline=Metrics(10.0, 1.0),
        )
        self.assertEqual(spec.max_iterations, 7)
        self.assertEqual(spec.min_duration_improvement, 0.10)


if __name__ == "__main__":
    unittest.main()
