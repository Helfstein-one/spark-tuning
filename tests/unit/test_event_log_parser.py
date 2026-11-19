import tempfile
import unittest
from pathlib import Path

from spark_tuning.adapters import SparkEventLogParser
from spark_tuning.evaluator import evaluate
from spark_tuning.models import Decision, Metrics, MetricStatus, RunSpec
from spark_tuning.runners.live_monitor import SparkLiveMonitor

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


class EventLogParserHardeningTests(unittest.TestCase):
    def _baseline_spec(self) -> RunSpec:
        return RunSpec(
            run_id="parser-eval",
            skill="partition-sizing",
            objective="duration",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=10.0,
                spill_disk_bytes=1024,
            ),
        )

    def test_sample_event_log_diagnostics_complete(self):
        metrics, diag = SparkEventLogParser.parse_with_diagnostics(
            FIXTURES_DIR / "sample_event_log.jsonl",
            cost_per_second=0.01,
        )
        self.assertTrue(diag.is_complete)
        self.assertEqual(diag.completeness_score, 1.0)
        self.assertEqual(diag.total_lines, 6)
        self.assertEqual(diag.valid_events, 6)
        self.assertEqual(diag.invalid_json_lines, 0)
        self.assertEqual(diag.unknown_events, 0)
        self.assertFalse(diag.truncated)
        self.assertIs(metrics.parse_diagnostics, diag)

    def test_spark_2x_3x_4x_version_and_schema_detection(self):
        m2, d2 = SparkEventLogParser.parse_with_diagnostics(FIXTURES_DIR / "spark_2x_event_log.jsonl")
        self.assertEqual(d2.spark_version, "2.4.8")
        self.assertEqual(d2.spark_major, 2)
        self.assertTrue(d2.is_complete)
        self.assertEqual(m2.duration_seconds, 4.0)
        self.assertEqual(m2.shuffle_read_bytes, 700)
        self.assertEqual(m2.shuffle_write_bytes, 900)

        m3, d3 = SparkEventLogParser.parse_with_diagnostics(FIXTURES_DIR / "spark_3x_event_log.jsonl")
        self.assertEqual(d3.spark_version, "3.5.1")
        self.assertEqual(d3.spark_major, 3)
        self.assertTrue(d3.is_complete)
        self.assertEqual(m3.duration_seconds, 5.0)
        self.assertEqual(m3.spill_disk_bytes, 256)

        m4, d4 = SparkEventLogParser.parse_with_diagnostics(FIXTURES_DIR / "spark_4x_event_log.jsonl")
        self.assertEqual(d4.spark_version, "4.0.0")
        self.assertEqual(d4.spark_major, 4)
        self.assertTrue(d4.is_complete)
        self.assertEqual(m4.duration_seconds, 2.5)
        self.assertEqual(m4.shuffle_write_bytes, 800)

    def test_heuristic_version_detection_without_log_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_4x = Path(tmp) / "no_log_start_4x.jsonl"
            log_4x.write_text(
                '{"Event":"SparkListenerApplicationStart","Time":1000}\n'
                '{"Event":"SparkListenerApplicationEnd","Time":3000}\n',
                encoding="utf-8",
            )
            _, d4 = SparkEventLogParser.parse_with_diagnostics(log_4x)
            self.assertIsNone(d4.spark_version)
            self.assertEqual(d4.spark_major, 4)
            self.assertTrue(d4.is_complete)

            log_2x = Path(tmp) / "no_log_start_2x.jsonl"
            log_2x.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":1000}\n'
                '{"Event":"SparkListenerTaskEnd","Task Info":{"Launch Time":1100,"Finish Time":1500},"Task Metrics":{"Shuffle Read Metrics":{"Remote Bytes Read":100,"Local Bytes Read":50}}}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":2000}\n',
                encoding="utf-8",
            )
            m2, d2 = SparkEventLogParser.parse_with_diagnostics(log_2x)
            self.assertEqual(d2.spark_major, 2)
            self.assertEqual(m2.shuffle_read_bytes, 150)

    def test_falsy_zero_timestamps_and_partial_null_task_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "falsy_zero.jsonl"
            log.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":0}\n'
                '{"Event":"SparkListenerStageCompleted","Stage Info":null}\n'
                '{"Event":"SparkListenerTaskEnd","Task Info":{"Task ID":0,"Attempt":0,"Launch Time":0,"Finish Time":500,"Failed":false},"Task Metrics":{"JVM GC Time":50,"Input Metrics":null,"Shuffle Read Metrics":null}}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":2000,"ExitCode":0}\n',
                encoding="utf-8",
            )
            metrics, diag = SparkEventLogParser.parse_with_diagnostics(log, cost_per_second=0.5)
            self.assertTrue(diag.is_complete)
            self.assertEqual(metrics.duration_seconds, 2.0)
            self.assertEqual(metrics.cost_usd, 1.0)
            self.assertEqual(metrics.task_p95_seconds, 0.5)
            self.assertEqual(metrics.gc_seconds, 0.05)
            self.assertIsNone(metrics.spill_disk_bytes)
            self.assertIsNone(metrics.shuffle_write_bytes)
            self.assertIsNone(metrics.bytes_read)

    def test_malformed_truncated_and_empty_logs_never_accept(self):
        spec = self._baseline_spec()

        m_mal, d_mal = SparkEventLogParser.parse_with_diagnostics(FIXTURES_DIR / "malformed_event_log.jsonl")
        self.assertEqual(d_mal.invalid_json_lines, 2)
        self.assertEqual(d_mal.unknown_events, 1)
        self.assertFalse(d_mal.is_complete)
        self.assertLess(d_mal.completeness_score, 1.0)
        self.assertNotEqual(evaluate(spec, m_mal).decision, Decision.ACCEPT)
        self.assertEqual(evaluate(spec, m_mal).decision, Decision.INCONCLUSIVE)

        m_trunc, d_trunc = SparkEventLogParser.parse_with_diagnostics(FIXTURES_DIR / "truncated_event_log.jsonl")
        self.assertTrue(d_trunc.truncated)
        self.assertTrue(d_trunc.missing_app_end)
        self.assertFalse(d_trunc.is_complete)
        self.assertIsNone(m_trunc.duration_seconds)
        self.assertIsNone(m_trunc.cost_usd)
        self.assertEqual(evaluate(spec, m_trunc).decision, Decision.INCONCLUSIVE)

        with tempfile.TemporaryDirectory() as tmp:
            empty_log = Path(tmp) / "empty.jsonl"
            empty_log.write_text("\n\n", encoding="utf-8")
            m_empty, d_empty = SparkEventLogParser.parse_with_diagnostics(empty_log)
            self.assertEqual(d_empty.total_lines, 0)
            self.assertEqual(d_empty.completeness_score, 0.0)
            self.assertFalse(d_empty.is_complete)
            self.assertIsNone(m_empty.duration_seconds)
            self.assertEqual(evaluate(spec, m_empty).decision, Decision.INCONCLUSIVE)

    def test_nonzero_exit_code_marks_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "failed_exit.jsonl"
            log.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":1000}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":2000,"ExitCode":1}\n',
                encoding="utf-8",
            )
            metrics, _ = SparkEventLogParser.parse_with_diagnostics(log)
            self.assertEqual(metrics.failures, 1)
            self.assertFalse(metrics.correctness)
            self.assertEqual(evaluate(self._baseline_spec(), metrics).decision, Decision.ROLLBACK)

    def test_live_monitor_handles_none_values(self):
        monitor = SparkLiveMonitor()
        dec = monitor.evaluate_live_stage_metrics(
            {"diskBytesSpilled": None, "memoryBytesSpilled": None, "jvmGcTime": None},
            elapsed_seconds=20.0,
        )
        self.assertFalse(dec.should_abort)
        self.assertEqual(dec.observed_spill_bytes, 0)
        self.assertEqual(dec.observed_gc_ms, 0)

    def test_binary_and_non_utf8_logs_return_incomplete_without_crashing(self):
        spec = self._baseline_spec()
        with tempfile.TemporaryDirectory() as tmp:
            pure_bin = Path(tmp) / "pure.bin"
            pure_bin.write_bytes(b"\xff\xfe\x00\x80\x81\x90\xde\xad\xbe\xef")
            m_bin, d_bin = SparkEventLogParser.parse_with_diagnostics(pure_bin)
            self.assertEqual(d_bin.total_lines, 1)
            self.assertEqual(d_bin.invalid_json_lines, 1)
            self.assertFalse(d_bin.is_complete)
            self.assertEqual(d_bin.completeness_score, 0.0)
            self.assertEqual(evaluate(spec, m_bin).decision, Decision.INCONCLUSIVE)

            mixed = Path(tmp) / "mixed.jsonl"
            mixed.write_bytes(
                b'{"Event":"SparkListenerApplicationStart","Timestamp":1000}\n'
                b'{"Event":"SparkListenerTaskEnd","Task Info":{"Launch Time":1100,"Finish Time":1500},"Task Metrics":{"Disk Bytes Spilled":0}}\n'
                b'\xff\xfe bad utf8 line\n'
                b'{"Event":"SparkListenerApplicationEnd","Timestamp":5000}\n'
            )
            m_mix, d_mix = SparkEventLogParser.parse_with_diagnostics(mixed)
            self.assertEqual(d_mix.total_lines, 4)
            self.assertEqual(d_mix.valid_events, 3)
            self.assertEqual(d_mix.invalid_json_lines, 1)
            self.assertFalse(d_mix.is_complete)
            self.assertLess(d_mix.completeness_score, 1.0)
            self.assertEqual(evaluate(spec, m_mix).decision, Decision.INCONCLUSIVE)

    def test_negative_metrics_and_timestamps_quarantined_and_never_accept(self):
        spec = self._baseline_spec()
        with tempfile.TemporaryDirectory() as tmp:
            neg_single = Path(tmp) / "neg_single.jsonl"
            neg_single.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":1000}\n'
                '{"Event":"SparkListenerTaskEnd","Task Info":{"Launch Time":1100,"Finish Time":1500},"Task Metrics":{"Disk Bytes Spilled":-100,"JVM GC Time":-50}}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":5000}\n',
                encoding="utf-8",
            )
            m_single, d_single = SparkEventLogParser.parse_with_diagnostics(neg_single)
            self.assertFalse(d_single.is_complete)
            self.assertLess(d_single.completeness_score, 1.0)
            self.assertIsNone(m_single.spill_disk_bytes)
            self.assertIsNone(m_single.gc_seconds)
            self.assertEqual(m_single.get_metric("spill_disk_bytes").status, MetricStatus.INVALID)
            self.assertEqual(m_single.get_metric("gc_seconds").status, MetricStatus.INVALID)
            self.assertEqual(evaluate(spec, m_single).decision, Decision.INCONCLUSIVE)

            neg_cancel = Path(tmp) / "neg_cancel.jsonl"
            neg_cancel.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":1000}\n'
                '{"Event":"SparkListenerTaskEnd","Task Info":{"Launch Time":1100,"Finish Time":1500},"Task Metrics":{"Disk Bytes Spilled":2000}}\n'
                '{"Event":"SparkListenerTaskEnd","Task Info":{"Launch Time":1200,"Finish Time":1600},"Task Metrics":{"Disk Bytes Spilled":-1500}}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":5000}\n',
                encoding="utf-8",
            )
            m_cancel, d_cancel = SparkEventLogParser.parse_with_diagnostics(neg_cancel)
            self.assertEqual(m_cancel.spill_disk_bytes, 2000)
            self.assertFalse(d_cancel.is_complete)
            self.assertEqual(m_cancel.get_metric("spill_disk_bytes").status, MetricStatus.INVALID)
            self.assertEqual(evaluate(spec, m_cancel).decision, Decision.INCONCLUSIVE)

            neg_ts = Path(tmp) / "neg_ts.jsonl"
            neg_ts.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":-5000}\n'
                '{"Event":"SparkListenerTaskEnd","Task Info":{"Launch Time":-4000,"Finish Time":-2000},"Task Metrics":{"Disk Bytes Spilled":0}}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":-1000}\n',
                encoding="utf-8",
            )
            m_ts, d_ts = SparkEventLogParser.parse_with_diagnostics(neg_ts)
            self.assertIsNone(m_ts.duration_seconds)
            self.assertIsNone(m_ts.task_p95_seconds)
            self.assertFalse(d_ts.is_complete)
            self.assertEqual(evaluate(spec, m_ts).decision, Decision.INCONCLUSIVE)

    def test_job_end_job_failed_triggers_rollback(self):
        spec = self._baseline_spec()
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "job_failed.jsonl"
            log.write_text(
                '{"Event":"SparkListenerApplicationStart","Timestamp":1000}\n'
                '{"Event":"SparkListenerJobEnd","Job ID":0,"Job Result":{"Result":"JobFailed","Exception":{"Message":"Stage failed"}}}\n'
                '{"Event":"SparkListenerApplicationEnd","Timestamp":5000}\n',
                encoding="utf-8",
            )
            metrics, _ = SparkEventLogParser.parse_with_diagnostics(log)
            self.assertGreaterEqual(metrics.failures, 1)
            self.assertFalse(metrics.correctness)
            self.assertEqual(evaluate(spec, metrics).decision, Decision.ROLLBACK)

    def test_live_monitor_handles_nan_inf_and_negative_values(self):
        monitor = SparkLiveMonitor()
        dec = monitor.evaluate_live_stage_metrics(
            {
                "diskBytesSpilled": float("nan"),
                "memoryBytesSpilled": float("inf"),
                "jvmGcTime": -5000,
            },
            elapsed_seconds=20.0,
        )
        self.assertFalse(dec.should_abort)
        self.assertEqual(dec.observed_spill_bytes, 0)
        self.assertEqual(dec.observed_gc_ms, 0)


if __name__ == "__main__":
    unittest.main()
