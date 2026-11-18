import math
import unittest
from dataclasses import asdict

from spark_tuning.core import (
    Decision,
    Metrics,
    MetricStatus,
    MetricValue,
    ParseDiagnostics,
    RunSpec,
    RunState,
)
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

    def test_metric_value_statuses_and_validation(self):
        mv_none = MetricValue(None, unit="seconds")
        self.assertEqual(mv_none.status, MetricStatus.UNKNOWN)
        self.assertFalse(mv_none.is_known)
        self.assertFalse(mv_none.is_usable)

        mv_nan = MetricValue(float("nan"), unit="seconds")
        self.assertEqual(mv_nan.status, MetricStatus.INVALID)
        self.assertFalse(mv_nan.is_usable)

        mv_inf = MetricValue(float("inf"), unit="seconds")
        self.assertEqual(mv_inf.status, MetricStatus.INVALID)

        mv_zero = MetricValue(0.0, unit="seconds", source="event_log", quality="complete")
        self.assertEqual(mv_zero.status, MetricStatus.OK)
        self.assertTrue(mv_zero.is_known)
        self.assertTrue(mv_zero.is_usable)

        with self.assertRaises(ValueError):
            MetricValue(True)  # bool rejected

        with self.assertRaises(ValueError):
            MetricValue(None, status=MetricStatus.OK)

    def test_metric_value_comparability(self):
        s1 = MetricValue(10.0, unit="seconds")
        s2 = MetricValue(20.0, unit="seconds")
        ms = MetricValue(10000, unit="milliseconds")
        unk = MetricValue.unknown(unit="seconds")

        self.assertTrue(s1.is_comparable_to(s2))
        self.assertFalse(s1.is_comparable_to(ms))
        self.assertFalse(s1.is_comparable_to(unk))

    def test_metrics_validation_rejects_negative_nan_inf_bool(self):
        with self.assertRaises(ValueError):
            Metrics(duration_seconds=-1.0, cost_usd=1.0)
        with self.assertRaises(ValueError):
            Metrics(duration_seconds=math.nan, cost_usd=1.0)
        with self.assertRaises(ValueError):
            Metrics(duration_seconds=math.inf, cost_usd=1.0)
        with self.assertRaises(ValueError):
            Metrics(duration_seconds=True, cost_usd=1.0)
        with self.assertRaises(ValueError):
            Metrics(duration_seconds=10.0, cost_usd=1.0, correctness="yes")  # type: ignore[arg-type]

    def test_metrics_roundtrip_and_comparability(self):
        diag = ParseDiagnostics(
            spark_version="3.5.1",
            spark_major=3,
            total_lines=6,
            valid_events=6,
            warnings=["minor warning"],  # type: ignore[arg-type]
        )
        m = Metrics(
            duration_seconds=10.0,
            cost_usd=1.0,
            task_p95_seconds=1.2,
            spill_disk_bytes=0,
            workload_id="wl-1",
            metric_values={"custom": MetricValue(5.0, unit="ops")},
            parse_diagnostics=diag,
        )
        reloaded = Metrics(**asdict(m))
        self.assertIsInstance(reloaded.parse_diagnostics, ParseDiagnostics)
        self.assertEqual(reloaded.parse_diagnostics.warnings, ("minor warning",))
        self.assertIsInstance(reloaded.metric_values["custom"], MetricValue)
        self.assertEqual(reloaded.get_metric("spill_disk_bytes").status, MetricStatus.OK)
        self.assertEqual(reloaded.get_metric("gc_seconds").status, MetricStatus.UNKNOWN)
        self.assertTrue(reloaded.correctness_ok)

        other_wl = Metrics(10.0, 1.0, workload_id="wl-2")
        self.assertTrue(any("workload_id" in err for err in m.validate_comparable(other_wl)))

        other_unit = Metrics(10.0, 1.0, units={"duration_seconds": "milliseconds"})
        self.assertTrue(any("duration_seconds" in err for err in m.validate_comparable(other_unit)))


if __name__ == "__main__":
    unittest.main()
