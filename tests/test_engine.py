import json
import sys
import tempfile
import unittest
from pathlib import Path

_SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from spark_tuning.adapters import FixtureExperimentRunner, JsonArtifactStore, load_spec
from spark_tuning.engine import LoopEngine
from spark_tuning.models import Decision, Metrics, RunSpec


class LoopEngineTests(unittest.TestCase):
    def test_accepts_first_candidate_that_meets_target(self):
        spec = RunSpec(
            run_id="accept",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(100, 10, task_p95_seconds=10, spill_disk_bytes=0),
            hypotheses=(
                {"id": "bad", "metrics": {**Metrics(110, 11, task_p95_seconds=10, spill_disk_bytes=0).__dict__}},
                {"id": "good", "metrics": {**Metrics(80, 8, task_p95_seconds=10, spill_disk_bytes=0).__dict__}},
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            result = LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(directory)).run(spec)
            self.assertEqual(result.decision, Decision.ACCEPT)
            self.assertEqual(len(result.iterations), 2)

    def test_rolls_back_on_correctness_failure(self):
        spec = RunSpec(
            run_id="incorrect",
            skill="parquet-layout",
            objective="cost",
            baseline=Metrics(100, 10),
            hypotheses=(
                {"id": "unsafe", "metrics": {**Metrics(70, 7, correctness=False).__dict__}},
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            result = LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(directory)).run(spec)
            self.assertEqual(result.decision, Decision.BLOCKED)
            self.assertEqual(result.iterations[0].decision, Decision.ROLLBACK)

    def test_persists_artifact(self):
        spec = load_spec("examples/runs/demo.json")
        with tempfile.TemporaryDirectory() as directory:
            LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(directory)).run(spec)
            payload = json.loads(Path(directory, "demo-join-strategy.json").read_text())
            self.assertEqual(payload["run_id"], spec.run_id)
            self.assertIn("iterations", payload)

    def test_blocks_promotion_when_guardrail_metrics_are_missing(self):
        spec = RunSpec(
            run_id="missing-guardrails",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(100, 10, task_p95_seconds=10, spill_disk_bytes=0),
            hypotheses=(
                {"id": "missing_spill", "metrics": {**Metrics(70, 7, task_p95_seconds=8).__dict__}},
                {"id": "missing_p95", "metrics": {**Metrics(70, 7, spill_disk_bytes=0).__dict__}},
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            result = LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(directory)).run(spec)
            self.assertEqual(result.decision, Decision.BLOCKED)
            self.assertEqual(result.iterations[0].decision, Decision.INCONCLUSIVE)
            self.assertEqual(result.iterations[1].decision, Decision.INCONCLUSIVE)

    def test_blocks_promotion_on_truncated_event_log(self):
        truncated_log = Path(__file__).parent / "fixtures" / "truncated_event_log.jsonl"
        spec = RunSpec(
            run_id="truncated-log",
            skill="partition-sizing",
            objective="duration",
            baseline=Metrics(100, 10, task_p95_seconds=10, spill_disk_bytes=0),
            hypotheses=({"id": "trunc", "event_log": str(truncated_log)},),
        )
        with tempfile.TemporaryDirectory() as directory:
            result = LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(directory)).run(spec)
            self.assertNotEqual(result.decision, Decision.ACCEPT)
            self.assertEqual(result.decision, Decision.BLOCKED)
            self.assertEqual(result.iterations[0].decision, Decision.INCONCLUSIVE)


if __name__ == "__main__":
    unittest.main()
