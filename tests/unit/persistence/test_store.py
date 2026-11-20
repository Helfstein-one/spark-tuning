from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spark_tuning.models import Decision, Iteration, LoopPhase, Metrics, RunSpec, RunState
from spark_tuning.persistence.store import LoopStateStore


class TestLoopStateStore(unittest.TestCase):
    def test_sqlite_store_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            store = LoopStateStore(db_path)

            spec = RunSpec(
                run_id="run-persist-1",
                skill="partition-sizing",
                objective="test sqlite persistence",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            store.init_run(spec)

            # Lease operations
            self.assertTrue(store.acquire_lease("run-persist-1", owner="w1", ttl_seconds=10.0))
            self.assertFalse(store.acquire_lease("run-persist-1", owner="w2", ttl_seconds=10.0))
            store.release_lease("run-persist-1", owner="w1")
            self.assertTrue(store.acquire_lease("run-persist-1", owner="w2", ttl_seconds=10.0))

            it1 = Iteration(
                number=1,
                hypothesis="h-1",
                metrics=Metrics(duration_seconds=80.0, cost_usd=4.0),
                decision=Decision.ACCEPT,
                reason="faster",
            )
            store.save_iteration("run-persist-1", it1)
            store.update_run_status(
                "run-persist-1",
                state=RunState.PR_READY,
                decision=Decision.ACCEPT,
                reason="success",
                phases=[
                    LoopPhase.OBSERVE,
                    LoopPhase.CLASSIFY,
                    LoopPhase.HYPOTHESIZE,
                    LoopPhase.EXPERIMENT,
                    LoopPhase.MEASURE,
                    LoopPhase.VERIFY,
                    LoopPhase.DECIDE,
                    LoopPhase.LEARN,
                ],
            )

            loaded = store.load_run("run-persist-1")
            self.assertEqual(loaded.run_id, "run-persist-1")
            self.assertEqual(loaded.decision, Decision.ACCEPT)
            self.assertEqual(loaded.state, RunState.PR_READY)
            self.assertEqual(len(loaded.iterations), 1)
            self.assertEqual(loaded.iterations[0].hypothesis, "h-1")
            self.assertIn("h-1", loaded.tried_hypotheses)
            self.assertEqual(len(loaded.phases), 8)


if __name__ == "__main__":
    unittest.main()
