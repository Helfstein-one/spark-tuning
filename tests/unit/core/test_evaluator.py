import unittest

from spark_tuning.evaluator import evaluate
from spark_tuning.models import (
    Decision,
    Metrics,
    MetricStatus,
    MetricValue,
    ParseDiagnostics,
    RunSpec,
)


class EvaluatorGuardrailTests(unittest.TestCase):
    def _complete_spec(self) -> RunSpec:
        return RunSpec(
            run_id="eval-test",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=10.0,
                spill_disk_bytes=1024,
                workload_id="wl-a",
            ),
        )

    def test_accepts_complete_improving_candidate(self):
        spec = self._complete_spec()
        cand = Metrics(
            duration_seconds=80.0,
            cost_usd=8.0,
            task_p95_seconds=9.0,
            spill_disk_bytes=512,
            workload_id="wl-a",
        )
        res = evaluate(spec, cand)
        self.assertEqual(res.decision, Decision.ACCEPT)
        self.assertEqual(res.unassessed_guardrails, ())
        self.assertAlmostEqual(res.deltas["duration_seconds"], -0.20)
        self.assertAlmostEqual(res.deltas["spill_disk_bytes"], -0.50)

    def test_missing_p95_or_spill_returns_inconclusive_never_accept(self):
        spec = self._complete_spec()

        missing_p95 = Metrics(80.0, 8.0, task_p95_seconds=None, spill_disk_bytes=512, workload_id="wl-a")
        res_p95 = evaluate(spec, missing_p95)
        self.assertEqual(res_p95.decision, Decision.INCONCLUSIVE)
        self.assertIn("task_p95_seconds", res_p95.unassessed_guardrails)
        self.assertIsNone(res_p95.deltas["task_p95_seconds"])

        missing_spill = Metrics(80.0, 8.0, task_p95_seconds=9.0, spill_disk_bytes=None, workload_id="wl-a")
        res_spill = evaluate(spec, missing_spill)
        self.assertEqual(res_spill.decision, Decision.INCONCLUSIVE)
        self.assertIn("spill_disk_bytes", res_spill.unassessed_guardrails)
        self.assertIsNone(res_spill.deltas["spill_disk_bytes"])

    def test_missing_duration_cost_correctness_or_failures_returns_inconclusive(self):
        spec = self._complete_spec()
        for kwargs in (
            {"duration_seconds": None, "cost_usd": 8.0, "task_p95_seconds": 9.0, "spill_disk_bytes": 512},
            {"duration_seconds": 80.0, "cost_usd": None, "task_p95_seconds": 9.0, "spill_disk_bytes": 512},
            {"duration_seconds": 80.0, "cost_usd": 8.0, "task_p95_seconds": 9.0, "spill_disk_bytes": 512, "correctness": None},
            {"duration_seconds": 80.0, "cost_usd": 8.0, "task_p95_seconds": 9.0, "spill_disk_bytes": 512, "failures": None},
        ):
            cand = Metrics(workload_id="wl-a", **kwargs)
            res = evaluate(spec, cand)
            self.assertEqual(res.decision, Decision.INCONCLUSIVE)
            self.assertTrue(len(res.unassessed_guardrails) >= 1)

    def test_rejects_unit_and_workload_mismatches_and_invalid_status(self):
        spec = self._complete_spec()

        wrong_unit = Metrics(
            80.0,
            8.0,
            task_p95_seconds=9.0,
            spill_disk_bytes=512,
            workload_id="wl-a",
            units={"duration_seconds": "milliseconds"},
        )
        res_unit = evaluate(spec, wrong_unit)
        self.assertEqual(res_unit.decision, Decision.INCONCLUSIVE)
        self.assertIn("duration_seconds", res_unit.unassessed_guardrails)

        wrong_wl = Metrics(80.0, 8.0, task_p95_seconds=9.0, spill_disk_bytes=512, workload_id="wl-other")
        res_wl = evaluate(spec, wrong_wl)
        self.assertEqual(res_wl.decision, Decision.INCONCLUSIVE)

        invalid_mv = Metrics(
            80.0,
            8.0,
            task_p95_seconds=9.0,
            spill_disk_bytes=512,
            workload_id="wl-a",
            metric_values={"task_p95_seconds": MetricValue.invalid(unit="seconds")},
        )
        res_inv = evaluate(spec, invalid_mv)
        self.assertEqual(res_inv.decision, Decision.INCONCLUSIVE)
        self.assertIn("task_p95_seconds", res_inv.unassessed_guardrails)

    def test_blocks_incomplete_parse_diagnostics(self):
        spec = self._complete_spec()
        cand = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=5.0,
            spill_disk_bytes=0,
            workload_id="wl-a",
            parse_diagnostics=ParseDiagnostics(
                total_lines=4,
                valid_events=3,
                invalid_json_lines=1,
                truncated=True,
                missing_app_end=True,
                completeness_score=0.35,
                is_complete=False,
                warnings=("Truncated log",),
            ),
        )
        res = evaluate(spec, cand)
        self.assertEqual(res.decision, Decision.INCONCLUSIVE)
        self.assertIn("parse_diagnostics", res.unassessed_guardrails)

    def test_explicit_regression_still_triggers_rollback(self):
        spec = self._complete_spec()
        regressed = Metrics(80.0, 8.0, task_p95_seconds=15.0, spill_disk_bytes=None, workload_id="wl-a")
        res = evaluate(spec, regressed)
        self.assertEqual(res.decision, Decision.ROLLBACK)
        self.assertIn("spill_disk_bytes", res.unassessed_guardrails)


if __name__ == "__main__":
    unittest.main()
