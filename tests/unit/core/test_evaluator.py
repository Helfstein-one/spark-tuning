import json
import unittest
from dataclasses import asdict

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

    def test_metric_values_validation_and_correctness_failures_guardrails(self):
        with self.assertRaises(ValueError):
            Metrics(
                duration_seconds=None,
                cost_usd=5.0,
                task_p95_seconds=5.0,
                spill_disk_bytes=0,
                metric_values={"duration_seconds": MetricValue(-5.0, unit="seconds")},
            )
        with self.assertRaises(ValueError):
            Metrics(
                duration_seconds=50.0,
                cost_usd=5.0,
                task_p95_seconds=5.0,
                spill_disk_bytes=0,
                metric_values={"correctness": MetricValue(2, unit="bool")},
            )

        spec = self._complete_spec()

        cand_fail_mv = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=5.0,
            spill_disk_bytes=0,
            workload_id="wl-a",
            failures=0,
            metric_values={"failures": MetricValue(5, unit="count")},
        )
        self.assertEqual(evaluate(spec, cand_fail_mv).decision, Decision.ROLLBACK)

        cand_corr_zero_mv = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=5.0,
            spill_disk_bytes=0,
            workload_id="wl-a",
            correctness=True,
            metric_values={"correctness": MetricValue(0, unit="bool")},
        )
        self.assertEqual(evaluate(spec, cand_corr_zero_mv).decision, Decision.ROLLBACK)

        cand_corr_unknown_mv = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=5.0,
            spill_disk_bytes=0,
            workload_id="wl-a",
            correctness=True,
            metric_values={"correctness": MetricValue.unknown(unit="bool")},
        )
        self.assertEqual(evaluate(spec, cand_corr_unknown_mv).decision, Decision.INCONCLUSIVE)

        base_fail_spec = RunSpec(
            run_id="base-fail-mv",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=10.0,
                spill_disk_bytes=1024,
                workload_id="wl-a",
                failures=0,
                metric_values={"failures": MetricValue(3, unit="count")},
            ),
        )
        good_cand = Metrics(50.0, 5.0, task_p95_seconds=5.0, spill_disk_bytes=0, workload_id="wl-a")
        self.assertEqual(evaluate(base_fail_spec, good_cand).decision, Decision.INCONCLUSIVE)

        base_corr_spec = RunSpec(
            run_id="base-corr-mv",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=10.0,
                spill_disk_bytes=1024,
                workload_id="wl-a",
                correctness=True,
                metric_values={"correctness": MetricValue(0, unit="bool")},
            ),
        )
        self.assertEqual(evaluate(base_corr_spec, good_cand).decision, Decision.INCONCLUSIVE)

    def test_zero_baseline_regression_never_accepts_and_is_strict_json(self):
        spec_zero_dur = RunSpec(
            run_id="zero-dur",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(
                duration_seconds=0.0,
                cost_usd=50.0,
                task_p95_seconds=0.0,
                spill_disk_bytes=0,
            ),
        )
        regressed_dur = Metrics(
            duration_seconds=100.0,
            cost_usd=25.0,
            task_p95_seconds=0.0,
            spill_disk_bytes=0,
        )
        ev = evaluate(spec_zero_dur, regressed_dur)
        self.assertNotEqual(ev.decision, Decision.ACCEPT)
        self.assertEqual(ev.decision, Decision.ROLLBACK)
        self.assertIsNone(ev.deltas["duration_seconds"])
        serialized = json.dumps(asdict(ev), allow_nan=False)
        self.assertNotIn("Infinity", serialized)

        still_zero_dur = Metrics(
            duration_seconds=0.0,
            cost_usd=25.0,
            task_p95_seconds=0.0,
            spill_disk_bytes=0,
        )
        ev_pass = evaluate(spec_zero_dur, still_zero_dur)
        self.assertEqual(ev_pass.decision, Decision.ACCEPT)
        self.assertEqual(ev_pass.deltas["duration_seconds"], 0.0)

        p95_zero_reg = Metrics(
            duration_seconds=0.0,
            cost_usd=25.0,
            task_p95_seconds=1.0,
            spill_disk_bytes=0,
        )
        ev_p95 = evaluate(spec_zero_dur, p95_zero_reg)
        self.assertEqual(ev_p95.decision, Decision.ROLLBACK)
        self.assertIsNone(ev_p95.deltas["task_p95_seconds"])

    def test_empty_unit_on_canonical_metric_rejected(self):
        spec = self._complete_spec()
        empty_unit_cand = Metrics(
            duration_seconds=None,
            cost_usd=5.0,
            task_p95_seconds=5.0,
            spill_disk_bytes=0,
            workload_id="wl-a",
            metric_values={"duration_seconds": MetricValue(50.0, unit="")},
        )
        res = evaluate(spec, empty_unit_cand)
        self.assertEqual(res.decision, Decision.INCONCLUSIVE)
        self.assertIn("duration_seconds", res.unassessed_guardrails)

        both_empty_spec = RunSpec(
            run_id="both-empty",
            skill="join-strategy",
            objective="duration",
            baseline=Metrics(
                duration_seconds=None,
                cost_usd=10.0,
                task_p95_seconds=10.0,
                spill_disk_bytes=1024,
                metric_values={"duration_seconds": MetricValue(100.0, unit="")},
            ),
        )
        both_empty_cand = Metrics(
            duration_seconds=None,
            cost_usd=5.0,
            task_p95_seconds=5.0,
            spill_disk_bytes=0,
            metric_values={"duration_seconds": MetricValue(50.0, unit="")},
        )
        res_both = evaluate(both_empty_spec, both_empty_cand)
        self.assertEqual(res_both.decision, Decision.INCONCLUSIVE)
        self.assertIn("duration_seconds", res_both.unassessed_guardrails)


if __name__ == "__main__":
    unittest.main()
