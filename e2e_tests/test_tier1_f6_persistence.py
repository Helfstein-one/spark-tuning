"""Tier 1 E2E Tests — F6 (#34): Idempotent Persistence & Crash Recovery."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spark_tuning.adapters import JsonArtifactStore
from spark_tuning.engine import LoopEngine
from spark_tuning.models import Decision, Metrics, RunSpec, RunState
from e2e_tests.helpers import FaultInjectingRunner, SimulatedProcessCrash


class TestTier1F6Persistence(unittest.TestCase):
    """T1-F6-01..05: LoopStateStore checkpoints, mid-loop resume, idempotency, leases, cost ledger."""

    def _two_step_spec(self, run_id: str, max_total_cost_usd: float = 50.0) -> RunSpec:
        return RunSpec(
            run_id=run_id,
            skill="partition-sizing",
            objective="checkpoint and resume",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=20.0,
                spill_disk_bytes=10_000_000,
                task_p95_seconds=80.0,
            ),
            max_iterations=4,
            max_total_cost_usd=max_total_cost_usd,
            hypotheses=(
                {
                    "id": "h1-inconclusive",
                    "target_category": "spill",
                    "expected_impact": 0.95,
                    "confidence": 0.95,
                    "reversibility": 1.0,
                    "risk": 0.05,
                    "estimated_cost_usd": 2.0,
                    "metrics": {
                        "duration_seconds": 195.0,
                        "cost_usd": 15.0,
                        "spill_disk_bytes": 10_000_000,
                        "task_p95_seconds": 80.0,
                        "correctness": False,
                    },
                },
                {
                    "id": "h2-accept",
                    "target_category": "spill",
                    "expected_impact": 0.50,
                    "confidence": 0.60,
                    "reversibility": 0.80,
                    "risk": 0.30,
                    "estimated_cost_usd": 15.0,
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 4.0,
                        "spill_disk_bytes": 2_000_000,
                        "task_p95_seconds": 50.0,
                        "correctness": True,
                    },
                },
            ),
        )

    def test_t1_f6_01_persists_phase_and_iteration_checkpoints(self) -> None:
        """T1-F6-01: LoopStateStore durably records run metadata, phases, tried hypotheses, and iterations."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = self._two_step_spec("t1-f6-01")
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "loop_state.db")
            engine = LoopEngine(
                FaultInjectingRunner(),
                JsonArtifactStore(Path(tmp) / "artifacts"),
                state_store=store,
            )
            result = engine.run(spec)
            self.assertEqual(result.decision, Decision.ACCEPT)

            saved = store.load_run("t1-f6-01")
            self.assertIsNotNone(saved)
            self.assertEqual(len(saved.iterations), 2)
            self.assertIn("h1-inconclusive", saved.tried_hypotheses)
            self.assertIn("h2-accept", saved.tried_hypotheses)

    def test_t1_f6_02_resume_after_mid_loop_crash_skips_completed_iterations(self) -> None:
        """T1-F6-02: Resuming after crash on iteration 2 does NOT re-execute iteration 1."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = self._two_step_spec("t1-f6-02")
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "loop_state.db")
            crashing_runner = FaultInjectingRunner(
                fail_on_calls={2},
                exception_factory=lambda call_idx, hyp_id: SimulatedProcessCrash(
                    f"Simulated crash at call={call_idx}, hypothesis={hyp_id}"
                ),
            )
            engine1 = LoopEngine(
                crashing_runner,
                JsonArtifactStore(Path(tmp) / "artifacts"),
                state_store=store,
            )
            with self.assertRaises(SimulatedProcessCrash):
                engine1.run(spec)
            self.assertEqual(len(crashing_runner.calls), 2)

            healthy_runner = FaultInjectingRunner()
            engine2 = LoopEngine(
                healthy_runner,
                JsonArtifactStore(Path(tmp) / "artifacts"),
                state_store=store,
            )
            resumed = engine2.resume("t1-f6-02", spec)
            self.assertEqual(resumed.decision, Decision.ACCEPT)
            self.assertEqual(resumed.state, RunState.PR_READY)
            # Only iteration 2 should have been executed after resume
            self.assertEqual(len(healthy_runner.calls), 1)
            self.assertEqual(healthy_runner.calls[0][2], "h2-accept")

    def test_t1_f6_03_resume_on_already_finished_run_is_idempotent_noop(self) -> None:
        """T1-F6-03: Resuming an already-accepted run performs 0 runner executions and returns stored result."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = self._two_step_spec("t1-f6-03")
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "loop_state.db")
            runner1 = FaultInjectingRunner()
            artifacts = JsonArtifactStore(Path(tmp) / "artifacts")
            res1 = LoopEngine(runner1, artifacts, state_store=store).run(spec)
            self.assertEqual(res1.decision, Decision.ACCEPT)

            runner2 = FaultInjectingRunner()
            res2 = LoopEngine(runner2, artifacts, state_store=store).resume("t1-f6-03", spec)
            self.assertEqual(len(runner2.calls), 0)
            self.assertEqual(res2.decision, Decision.ACCEPT)
            self.assertEqual(res2.state, RunState.PR_READY)

    def test_t1_f6_04_run_id_lease_acquisition_and_clean_release(self) -> None:
        """T1-F6-04: Lease is acquired during execution and released when run reaches terminal state."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = self._two_step_spec("t1-f6-04")
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "loop_state.db")
            artifacts = JsonArtifactStore(Path(tmp) / "artifacts")
            res = LoopEngine(FaultInjectingRunner(), artifacts, state_store=store).run(spec)
            self.assertEqual(res.decision, Decision.ACCEPT)

            # After completion, a new lease can be cleanly acquired because the prior lease was released
            acquired = store.acquire_lease("t1-f6-04", owner="worker-2", ttl_seconds=10.0)
            self.assertTrue(acquired)
            store.release_lease("t1-f6-04", owner="worker-2")

    def test_t1_f6_05_tried_hypotheses_and_cost_ledger_preserved_across_resume(self) -> None:
        """T1-F6-05: Accumulated cost from pre-crash iteration ($15) + iter 2 ($4) stays $19 <= $20."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = self._two_step_spec("t1-f6-05", max_total_cost_usd=20.0)
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "loop_state.db")
            artifacts = JsonArtifactStore(Path(tmp) / "artifacts")
            crashing_runner = FaultInjectingRunner(
                fail_on_calls={2},
                exception_factory=lambda call_idx, hyp_id: SimulatedProcessCrash(
                    f"Simulated crash at call={call_idx}, hypothesis={hyp_id}"
                ),
            )
            with self.assertRaises(SimulatedProcessCrash):
                LoopEngine(crashing_runner, artifacts, state_store=store).run(spec)

            # Resume with $15 already spent; iter 2 spends $4 -> $19 <= $20 -> ACCEPT
            healthy_runner = FaultInjectingRunner()
            res = LoopEngine(healthy_runner, artifacts, state_store=store).resume("t1-f6-05", spec)
            self.assertEqual(res.decision, Decision.ACCEPT)
            self.assertEqual([it.hypothesis for it in res.iterations], ["h1-inconclusive", "h2-accept"])
            self.assertEqual(len(healthy_runner.calls), 1)


if __name__ == "__main__":
    unittest.main()
