import sys
import unittest
from pathlib import Path

_SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from spark_tuning.adapters import (
    FixtureExperimentRunner,
    SparkEventLogParser,
    SparkSubmitExperimentRunner,
)
from spark_tuning.models import Metrics, RunSpec

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_event_log.jsonl"

class EventLogRunnerTests(unittest.TestCase):
    def test_spark_event_log_parser(self):
        metrics = SparkEventLogParser.parse_file(FIXTURE_PATH, cost_per_second=0.01)
        self.assertEqual(metrics.duration_seconds, 3.0)
        self.assertEqual(metrics.cost_usd, 0.03)
        self.assertEqual(metrics.stages_count, 1)
        self.assertEqual(metrics.tasks_count, 2)
        self.assertEqual(metrics.task_retries, 0)
        self.assertEqual(metrics.failures, 0)
        self.assertTrue(metrics.correctness)
        self.assertEqual(metrics.bytes_read, 6144)
        self.assertEqual(metrics.bytes_written, 3072)
        self.assertEqual(metrics.shuffle_read_bytes, 1200)
        self.assertEqual(metrics.shuffle_write_bytes, 1400)
        self.assertEqual(metrics.spill_disk_bytes, 512)
        self.assertEqual(metrics.spill_memory_bytes, 1024)
        self.assertEqual(metrics.gc_seconds, 0.11)

    def test_fixture_runner_with_event_log(self):
        runner = FixtureExperimentRunner()
        spec = RunSpec(
            run_id="test_run",
            skill="partition-sizing",
            objective="reduce spill",
            baseline=Metrics(duration_seconds=10.0, cost_usd=1.0),
        )
        hypothesis = {
            "id": "hyp_event_log",
            "event_log": str(FIXTURE_PATH),
        }
        metrics = runner.run(spec, 1, hypothesis)
        self.assertEqual(metrics.duration_seconds, 3.0)
        self.assertEqual(metrics.shuffle_write_bytes, 1400)

    def test_spark_submit_runner_missing_executable(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            runner = SparkSubmitExperimentRunner(
                spark_home="/invalid/spark/home",
                artifacts_dir=tmp_dir,
            )
            spec = RunSpec(
                run_id="test_missing_bin",
                skill="partition-sizing",
                objective="test",
                baseline=Metrics(duration_seconds=10.0, cost_usd=1.0),
            )
            hypothesis = {
                "id": "hyp_missing",
                "app_resource": "job.py",
            }
            with self.assertRaises(FileNotFoundError):
                runner.run(spec, 1, hypothesis)

if __name__ == "__main__":
    unittest.main()
