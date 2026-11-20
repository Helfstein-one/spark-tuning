"""Tier 1 E2E Tests — F2 (#48): Event-Log Parser Hardening & Version Detection."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spark_tuning.adapters import FixtureExperimentRunner, JsonArtifactStore, SparkEventLogParser
from spark_tuning.engine import LoopEngine
from spark_tuning.models import Decision, Metrics, RunSpec, RunState
from e2e_tests.helpers import EVENT_LOGS_DIR, get_parse_diagnostics


class TestTier1F2EventLogs(unittest.TestCase):
    """T1-F2-01..05: Event-log parsing across Spark 2/3/4, custom events, cost calculation."""

    def test_t1_f2_01_parse_complete_spark3_event_log(self) -> None:
        """T1-F2-01: Spark 3.5.1 complete event log extracts exact metrics and complete diagnostics."""
        metrics = SparkEventLogParser.parse_file(
            EVENT_LOGS_DIR / "spark3_complete.jsonl",
            cost_per_second=0.05,
        )
        self.assertEqual(metrics.duration_seconds, 221.0)
        self.assertEqual(metrics.spill_disk_bytes, 9448928051)
        self.assertEqual(metrics.gc_seconds, 12.0)
        self.assertEqual(metrics.task_p95_seconds, 75.0)
        self.assertEqual(metrics.shuffle_write_bytes, 4000000000)
        self.assertEqual(metrics.stages_count, 1)
        self.assertEqual(metrics.tasks_count, 4)

        diag = get_parse_diagnostics(metrics)
        self.assertIsNotNone(diag, "ParseDiagnostics must be attached to parsed Metrics")
        self.assertEqual(diag.spark_major, 3)
        self.assertEqual(diag.spark_version, "3.5.1")
        self.assertTrue(diag.is_complete)
        self.assertEqual(diag.completeness_score, 1.0)
        self.assertEqual(diag.invalid_json_lines, 0)

    def test_t1_f2_02_detect_and_parse_spark2_event_log_variant(self) -> None:
        """T1-F2-02: Spark 2.4.8 event log detects spark_major=2 and extracts accurate metrics."""
        metrics = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark2_complete.jsonl")
        self.assertEqual(metrics.duration_seconds, 100.0)
        self.assertEqual(metrics.spill_disk_bytes, 3072)
        self.assertEqual(metrics.gc_seconds, 4.0)
        self.assertEqual(metrics.shuffle_write_bytes, 100000)
        self.assertEqual(metrics.tasks_count, 2)

        diag = get_parse_diagnostics(metrics)
        self.assertIsNotNone(diag)
        self.assertEqual(diag.spark_major, 2)
        self.assertEqual(diag.spark_version, "2.4.8")
        self.assertTrue(diag.is_complete)

    def test_t1_f2_03_detect_and_parse_spark4_event_log_variant(self) -> None:
        """T1-F2-03: Spark 4.0.0 event log detects spark_major=4 and extracts accurate metrics."""
        metrics = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark4_complete.jsonl")
        self.assertEqual(metrics.duration_seconds, 75.0)
        self.assertEqual(metrics.spill_disk_bytes, 2000000000)
        self.assertEqual(metrics.gc_seconds, 6.0)
        self.assertEqual(metrics.shuffle_write_bytes, 1500000000)
        self.assertEqual(metrics.tasks_count, 2)

        diag = get_parse_diagnostics(metrics)
        self.assertIsNotNone(diag)
        self.assertEqual(diag.spark_major, 4)
        self.assertEqual(diag.spark_version, "4.0.0")
        self.assertTrue(diag.is_complete)

    def test_t1_f2_04_track_unknown_benign_listener_events_without_failing(self) -> None:
        """T1-F2-04: Custom/unknown listener events increment unknown_events without failing complete run."""
        base_lines = (EVENT_LOGS_DIR / "spark3_complete.jsonl").read_text(encoding="utf-8").splitlines()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "spark3_with_custom_events.jsonl"
            lines = [
                base_lines[0],
                '{"Event":"SparkListenerSQLExecutionStart","executionId":1,"time":1700000000500}',
                '{"Event":"CustomVendorMetricEvent","metric":"cache_hit_ratio","value":0.92}',
                *base_lines[1:],
            ]
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

            metrics = SparkEventLogParser.parse_file(log_path)
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertGreaterEqual(diag.unknown_events, 1)
            self.assertEqual(diag.invalid_json_lines, 0)
            self.assertTrue(diag.is_complete)
            self.assertEqual(metrics.duration_seconds, 221.0)

    def test_t1_f2_05_cost_calculation_and_fixture_runner_loop_integration(self) -> None:
        """T1-F2-05: Cost computes from duration * cost_per_second and complete log runs through LoopEngine."""
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "spark3_12_5s.jsonl"
            log_path.write_text(
                "\n".join(
                    [
                        '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
                        '{"Event":"SparkListenerApplicationStart","App Name":"CostJob","App ID":"app-1","Timestamp":1700000000000}',
                        '{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{"Task ID":0,"Attempt":0,"Launch Time":1700000001000,"Finish Time":1700000006000,"Failed":false},"Task Metrics":{"JVM GC Time":500,"Disk Bytes Spilled":100}}',
                        '{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":0}}',
                        '{"Event":"SparkListenerApplicationEnd","Timestamp":1700000012500}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            metrics = SparkEventLogParser.parse_file(log_path, cost_per_second=0.04)
            self.assertEqual(metrics.duration_seconds, 12.5)
            self.assertEqual(metrics.cost_usd, 0.5)
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertGreater(diag.valid_events, 0)

            spec = RunSpec(
                run_id="t1-f2-05",
                skill="partition-sizing",
                objective="accept complete event log",
                baseline=Metrics(
                    duration_seconds=30.0,
                    cost_usd=1.2,
                    spill_disk_bytes=1000,
                    task_p95_seconds=10.0,
                ),
                hypotheses=({"id": "complete-log-hyp", "event_log": str(log_path)},),
            )
            result = LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(tmp)).run(spec)
            self.assertEqual(result.decision, Decision.ACCEPT)
            self.assertEqual(result.state, RunState.PR_READY)


if __name__ == "__main__":
    unittest.main()
