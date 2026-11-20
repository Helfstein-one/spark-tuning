"""Tier 2 E2E Tests — Boundary, Edge & Negative Rejection Cases (F1–F7, 37 tests)."""

from __future__ import annotations

import json
import math
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from spark_tuning.adapters import (
    JsonArtifactStore,
    SparkEventLogParser,
    SparkSubmitExperimentRunner,
    load_spec,
)
from spark_tuning.core.prioritization import HypothesisPrioritizer, HypothesisProposal
from spark_tuning.core.registry import ExperimentRegistry, HistoricExperimentRecord
from spark_tuning.diagnostics.engine import BottleneckCategory, BottleneckDiagnosisEngine
from spark_tuning.engine import LoopEngine
from spark_tuning.evaluation.pareto import ParetoDecisionEngine
from spark_tuning.evaluator import evaluate
from spark_tuning.models import Decision, Iteration, LoopResult, Metrics, RunSpec, RunState
from spark_tuning.reporting.report_generator import OptimizationReportGenerator
from spark_tuning.safety.sanitizer import ArtifactSanitizer
from e2e_tests.helpers import (
    EVENT_LOGS_DIR,
    REPO_ROOT,
    SPECS_DIR,
    FaultInjectingRunner,
    assert_no_raw_secrets,
    get_parse_diagnostics,
    get_unassessed_guardrails,
    make_temp_spark_submit,
    run_cli,
)


class TestTier2Boundaries(unittest.TestCase):
    """37 boundary, edge, and negative rejection E2E tests across F1–F7."""

    # --- F1 (#35) Metric Semantics & Missing-Value Evaluation ---

    def test_t2_f1_01_missing_task_p95_blocks_accept(self) -> None:
        """T2-F1-01: Missing candidate task_p95_seconds blocks ACCEPT and lists unassessed guardrail."""
        spec = RunSpec(
            run_id="t2-f1-01",
            skill="partition-sizing",
            objective="guardrail check",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=100.0,
                spill_disk_bytes=1000,
            ),
        )
        candidate = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=None,
            spill_disk_bytes=500,
        )
        ev = evaluate(spec, candidate)
        self.assertNotEqual(ev.decision, Decision.ACCEPT)
        self.assertIn(ev.decision, (Decision.INCONCLUSIVE, Decision.BLOCKED, Decision.ROLLBACK))
        unassessed = get_unassessed_guardrails(ev)
        self.assertTrue(
            any("p95" in g.lower() for g in unassessed),
            f"Expected p95 in unassessed_guardrails, got {unassessed}",
        )

    def test_t2_f1_02_missing_spill_disk_bytes_blocks_accept(self) -> None:
        """T2-F1-02: Missing candidate spill_disk_bytes blocks ACCEPT and lists unassessed guardrail."""
        spec = RunSpec(
            run_id="t2-f1-02",
            skill="partition-sizing",
            objective="guardrail check",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=50.0,
                spill_disk_bytes=1000,
            ),
        )
        candidate = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=40.0,
            spill_disk_bytes=None,
        )
        ev = evaluate(spec, candidate)
        self.assertNotEqual(ev.decision, Decision.ACCEPT)
        unassessed = get_unassessed_guardrails(ev)
        self.assertTrue(
            any("spill" in g.lower() for g in unassessed),
            f"Expected spill in unassessed_guardrails, got {unassessed}",
        )

    def test_t2_f1_03_missing_baseline_guardrail_blocks_accept(self) -> None:
        """T2-F1-03: Missing baseline.task_p95_seconds blocks ACCEPT with unassessed_guardrails."""
        spec = RunSpec(
            run_id="t2-f1-03",
            skill="partition-sizing",
            objective="baseline guardrail check",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=None,
                spill_disk_bytes=1000,
            ),
        )
        candidate = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=10.0,
            spill_disk_bytes=500,
        )
        ev = evaluate(spec, candidate)
        self.assertNotEqual(ev.decision, Decision.ACCEPT)
        unassessed = get_unassessed_guardrails(ev)
        self.assertTrue(any("p95" in g.lower() for g in unassessed))

    def test_t2_f1_04_pareto_and_report_do_not_coerce_none_to_zero(self) -> None:
        """T2-F1-04: ParetoDecisionEngine & ReportGenerator must not coerce None spill/gc to 0."""
        baseline = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            spill_disk_bytes=500,
            gc_seconds=20.0,
            task_p95_seconds=10.0,
        )
        cand_missing = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            spill_disk_bytes=None,
            gc_seconds=None,
            task_p95_seconds=None,
        )
        self.assertFalse(
            ParetoDecisionEngine.dominates(cand_missing, baseline),
            "Missing spill/gc must NOT be treated as 0 to dominate a known baseline",
        )

        result = LoopResult(
            run_id="t2-f1-04",
            state=RunState.BLOCKED,
            decision=Decision.BLOCKED,
            iterations=[
                Iteration(
                    number=1,
                    hypothesis="missing-metrics-hyp",
                    metrics=cand_missing,
                    decision=Decision.INCONCLUSIVE,
                    reason="missing guardrails",
                )
            ],
            reason="missing guardrails",
        )
        md = OptimizationReportGenerator.generate_markdown(result, baseline)
        iter_row = [line for line in md.splitlines() if "missing-metrics-hyp" in line][0]
        cells = [c.strip().lower() for c in iter_row.split("|")]
        # Spill Disk (B) is column 5, GC (s) is column 6 (1-indexed after leading empty split)
        self.assertNotIn(cells[5], ("0", "0.0"), f"Spill None coerced to 0 in report: {iter_row}")
        self.assertNotIn(cells[6], ("0", "0.0"), f"GC None coerced to 0.0 in report: {iter_row}")

    def test_t2_f1_05_reject_negative_nan_inf_or_incomparable_metric_value(self) -> None:
        """T2-F1-05: Negative/NaN/Inf MetricValues mark INVALID or raise; unit mismatch rejected."""
        from spark_tuning.models import MetricStatus, MetricValue

        with self.assertRaises(ValueError):
            Metrics(duration_seconds=-5.0, cost_usd=1.0)

        nan_val = MetricValue(
            value=math.nan,
            unit="seconds",
            source="test",
            timestamp="2026-11-18T00:00:00Z",
            quality="high",
        )
        self.assertEqual(nan_val.status, MetricStatus.INVALID)

        with self.assertRaises(ValueError):
            MetricValue(
                value=math.inf,
                unit="seconds",
                source="test",
                timestamp="2026-11-18T00:00:00Z",
                quality="high",
                status=MetricStatus.OK,
            )

        sec_val = MetricValue(
            value=10.0,
            unit="seconds",
            source="test",
            timestamp="2026-11-18T00:00:00Z",
            quality="high",
            status=MetricStatus.OK,
        )
        ms_val = MetricValue(
            value=5000.0,
            unit="milliseconds",
            source="test",
            timestamp="2026-11-18T00:00:00Z",
            quality="high",
            status=MetricStatus.OK,
        )
        self.assertFalse(sec_val.is_comparable_to(ms_val))

        m_sec = Metrics(duration_seconds=10.0, cost_usd=1.0, units={"duration_seconds": "seconds"})
        m_ms = Metrics(duration_seconds=5000.0, cost_usd=1.0, units={"duration_seconds": "milliseconds"})
        self.assertTrue(len(m_sec.validate_comparable(m_ms)) > 0)

    def test_t2_f1_06_exact_boundary_thresholds_and_zero_baseline_spill(self) -> None:
        """T2-F1-06: Exact -10% duration / +20% p95 passes; baseline.spill=0 -> 100 triggers ROLLBACK."""
        baseline = Metrics(
            duration_seconds=100.0,
            cost_usd=10.0,
            task_p95_seconds=10.0,
            spill_disk_bytes=1000,
        )
        spec = RunSpec(
            run_id="t2-f1-06a",
            skill="partition-sizing",
            objective="exact boundaries",
            baseline=baseline,
            min_duration_improvement=0.10,
            max_p95_regression=0.20,
        )
        exact_boundary = Metrics(
            duration_seconds=90.0,
            cost_usd=10.0,
            task_p95_seconds=12.0,
            spill_disk_bytes=1200,
        )
        ev_ok = evaluate(spec, exact_boundary)
        self.assertEqual(ev_ok.decision, Decision.ACCEPT)

        zero_spill_spec = RunSpec(
            run_id="t2-f1-06b",
            skill="partition-sizing",
            objective="zero baseline spill regression",
            baseline=Metrics(
                duration_seconds=100.0,
                cost_usd=10.0,
                task_p95_seconds=10.0,
                spill_disk_bytes=0,
            ),
        )
        regressed_spill = Metrics(
            duration_seconds=50.0,
            cost_usd=5.0,
            task_p95_seconds=8.0,
            spill_disk_bytes=100,
        )
        ev_rb = evaluate(zero_spill_spec, regressed_spill)
        self.assertEqual(ev_rb.decision, Decision.ROLLBACK)

    # --- F2 (#48) Event-Log Parser Hardening & Version Detection ---

    def test_t2_f2_01_empty_event_log_file_is_incomplete(self) -> None:
        """T2-F2-01: 0-byte event log yields total_lines=0, valid_events=0, is_complete=False."""
        with tempfile.TemporaryDirectory() as tmp:
            empty_log = Path(tmp) / "empty.jsonl"
            empty_log.write_text("", encoding="utf-8")
            metrics = SparkEventLogParser.parse_file(empty_log)
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertEqual(diag.total_lines, 0)
            self.assertEqual(diag.valid_events, 0)
            self.assertFalse(diag.is_complete)
            self.assertEqual(diag.completeness_score, 0.0)

    def test_t2_f2_02_truncated_json_at_eof_without_application_end(self) -> None:
        """T2-F2-02: Truncated EOF event log reports truncated=True, missing_app_end=True, is_complete=False."""
        metrics = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark3_truncated.jsonl")
        diag = get_parse_diagnostics(metrics)
        self.assertIsNotNone(diag)
        self.assertGreaterEqual(diag.invalid_json_lines, 1)
        self.assertTrue(diag.truncated)
        self.assertTrue(diag.missing_app_end)
        self.assertFalse(diag.is_complete)
        self.assertLess(diag.completeness_score, 1.0)

    def test_t2_f2_03_missing_app_start_and_end_blocks_silent_zero_duration_win(self) -> None:
        """T2-F2-03: Log with only TaskEnd reports missing_app_start/end and is_complete=False."""
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "tasks_only.jsonl"
            log.write_text(
                '{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{"Task ID":0,"Attempt":0,"Launch Time":1000,"Finish Time":2000,"Failed":false},"Task Metrics":{"JVM GC Time":10,"Disk Bytes Spilled":0}}\n',
                encoding="utf-8",
            )
            metrics = SparkEventLogParser.parse_file(log)
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertTrue(diag.missing_app_start)
            self.assertTrue(diag.missing_app_end)
            self.assertFalse(diag.is_complete)

    def test_t2_f2_04_corrupted_middle_and_non_dict_json_lines(self) -> None:
        """T2-F2-04: Non-dict JSON lines ([1,2,3], "str", 123) & corrupt JSON counted safely without crash."""
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "mixed_bad_lines.jsonl"
            log.write_text(
                "\n".join(
                    [
                        '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
                        '{"Event":"SparkListenerApplicationStart","Timestamp":1700000000000}',
                        "[1, 2, 3]",
                        '"raw json string"',
                        "12345",
                        "{broken json",
                        '{"Event":"SparkListenerApplicationEnd","Timestamp":1700000010000}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            metrics = SparkEventLogParser.parse_file(log)
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertGreaterEqual(diag.invalid_json_lines, 1)
            self.assertEqual(diag.valid_events, 3)
            self.assertFalse(diag.is_complete)

    def test_t2_f2_05_zero_timestamp_tasks_not_dropped(self) -> None:
        """T2-F2-05: Tasks with Launch Time=0 are not dropped by truthiness checks."""
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "zero_launch.jsonl"
            log.write_text(
                "\n".join(
                    [
                        '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
                        '{"Event":"SparkListenerApplicationStart","Timestamp":1000}',
                        '{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{"Task ID":0,"Attempt":0,"Launch Time":0,"Finish Time":0,"Failed":false},"Task Metrics":{"JVM GC Time":0}}',
                        '{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{"Task ID":1,"Attempt":0,"Launch Time":0,"Finish Time":500,"Failed":false},"Task Metrics":{"JVM GC Time":0}}',
                        '{"Event":"SparkListenerApplicationEnd","Timestamp":2000}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            metrics = SparkEventLogParser.parse_file(log)
            self.assertEqual(metrics.tasks_count, 2)
            self.assertIsNotNone(metrics.task_p95_seconds)

    def test_t2_f2_06_inverted_timestamps_and_missing_file(self) -> None:
        """T2-F2-06: Missing log raises FileNotFoundError; inverted End<Start flags incomplete/warning."""
        with self.assertRaises(FileNotFoundError):
            SparkEventLogParser.parse_file("/nonexistent/path/spark_event.jsonl")

        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "inverted.jsonl"
            log.write_text(
                "\n".join(
                    [
                        '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
                        '{"Event":"SparkListenerApplicationStart","Timestamp":2000000}',
                        '{"Event":"SparkListenerApplicationEnd","Timestamp":1000000}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            metrics = SparkEventLogParser.parse_file(log)
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertTrue(len(diag.warnings) > 0 or not diag.is_complete)

    # --- F3 (#54) Structured Secret & PII Sanitizer ---

    def test_t2_f3_01_deeply_nested_secrets_inside_lists_tuples_dicts(self) -> None:
        """T2-F3-01: Deeply nested list/tuple/dict combinations redact secrets while preserving shape."""
        raw_jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiIxMjMifQ."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        nested = {
            "a": [
                (
                    "b",
                    {
                        "authorization": f"Bearer {raw_jwt}",
                        "emails": ["user.deep@example.com"],
                    },
                )
            ]
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(nested)
        assert_no_raw_secrets(self, sanitized, [raw_jwt, "user.deep@example.com"])
        self.assertIsInstance(sanitized["a"], list)
        self.assertIsInstance(sanitized["a"][0], tuple)
        self.assertGreaterEqual(report.total_redactions, 2)

    def test_t2_f3_02_case_insensitive_header_and_key_variations(self) -> None:
        """T2-F3-02: Mixed-case secret keys & bearer prefixes are consistently redacted."""
        payload = {
            "X-Api-Key": "val1",
            "SECRET_VALUE": "val2",
            "PassWord": "val3",
            "Access_Token": "val4",
            "privateKey": "val5",
            "header": "bearer lower_case_secret_token_xyz",
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(payload)
        assert_no_raw_secrets(
            self,
            sanitized,
            ["val1", "val2", "val3", "val4", "val5", "lower_case_secret_token_xyz"],
        )
        self.assertGreaterEqual(report.total_redactions, 6)

    def test_t2_f3_03_circular_self_referential_dict_does_not_infinite_loop(self) -> None:
        """T2-F3-03: Self-referential structures terminate without RecursionError and redact secrets."""
        secret = "gho_circularSecretToken1234567890abcdef"
        d: dict = {"token": secret}
        d["self"] = d

        sanitized, report = ArtifactSanitizer.sanitize_with_report(d)
        assert_no_raw_secrets(self, sanitized, [secret])
        self.assertGreaterEqual(report.total_redactions, 1)

    def test_t2_f3_04_allowlist_bypass_embedded_secret_still_redacted(self) -> None:
        """T2-F3-04: Structured secret inside an allowlisted key's value is still redacted."""
        payload = {"spark.app.name": "job-with-AKIAIOSFODNN7EXAMPLE"}
        sanitized, report = ArtifactSanitizer.sanitize_with_report(
            payload, allowlist={"spark.app.name"}
        )
        assert_no_raw_secrets(self, sanitized, ["AKIAIOSFODNN7EXAMPLE"])
        self.assertGreaterEqual(report.total_redactions, 1)

    def test_t2_f3_05_non_string_scalar_and_empty_input_edge_cases(self) -> None:
        """T2-F3-05: Empty and scalar inputs return cleanly with zero redactions."""
        for clean in ("", {}, [], (), None, 42, True):
            sanitized, report = ArtifactSanitizer.sanitize_with_report(clean)
            self.assertEqual(report.total_redactions, 0)
            self.assertEqual(sanitized, clean)

    # --- F4 (#37) Secure Runner Sandbox Policy ---

    def test_t2_f4_01_rejects_path_traversal_and_symlink_escape(self) -> None:
        """T2-F4-01: Path traversal (../) and symlink escapes raise SandboxViolationError."""
        from spark_tuning.safety.sandbox import SandboxPolicy, SandboxViolationError

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            allowed = tmp_path / "allowed"
            outside = tmp_path / "outside"
            allowed.mkdir()
            outside.mkdir()
            secret_target = outside / "secret.py"
            secret_target.write_text("print('outside')\n", encoding="utf-8")
            symlink_escape = allowed / "link.py"
            symlink_escape.symlink_to(secret_target)

            policy = SandboxPolicy(allowed_roots=[allowed])
            spec = RunSpec(
                run_id="t2-f4-01",
                skill="partition-sizing",
                objective="traversal block",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            for bad_path in ("../../etc/passwd", str(symlink_escape)):
                with self.assertRaises(SandboxViolationError) as ctx:
                    policy.validate_run(
                        spec=spec,
                        iteration=1,
                        hypothesis={"id": "h-trav", "app_resource": bad_path},
                    )
                self.assertTrue(bool(ctx.exception.code))
                self.assertTrue(bool(ctx.exception.field))
                self.assertTrue(bool(ctx.exception.detail))

    def test_t2_f4_02_rejects_forbidden_prod_master_and_prod_uris(self) -> None:
        """T2-F4-02: Production masters (yarn/k8s prod) and prod URIs raise SandboxViolationError."""
        from spark_tuning.safety.sandbox import SandboxPolicy, SandboxViolationError

        with tempfile.TemporaryDirectory() as tmp:
            allowed = Path(tmp) / "jobs"
            allowed.mkdir()
            job = allowed / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[allowed], block_prod_masters=True)
            spec = RunSpec(
                run_id="t2-f4-02",
                skill="partition-sizing",
                objective="prod guard",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )

            with self.assertRaises(SandboxViolationError) as ctx1:
                policy.validate_run(
                    spec=spec,
                    iteration=1,
                    hypothesis={"id": "h-yarn", "app_resource": str(job), "master": "yarn"},
                )
            self.assertIn("master", ctx1.exception.field.lower())

            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    spec=spec,
                    iteration=1,
                    hypothesis={
                        "id": "h-prod-uri",
                        "app_resource": str(job),
                        "master": "local[2]",
                        "app_args": ["s3a://prod-warehouse/table"],
                    },
                )

    def test_t2_f4_03_rejects_disallowed_spark_conf_and_shell_injection(self) -> None:
        """T2-F4-03: Shell injection characters in extraJavaOptions or blocked conf raise SandboxViolationError."""
        from spark_tuning.safety.sandbox import SandboxPolicy, SandboxViolationError

        with tempfile.TemporaryDirectory() as tmp:
            allowed = Path(tmp) / "jobs"
            allowed.mkdir()
            job = allowed / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[allowed])
            spec = RunSpec(
                run_id="t2-f4-03",
                skill="partition-sizing",
                objective="injection guard",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    spec=spec,
                    iteration=1,
                    hypothesis={
                        "id": "h-inject",
                        "app_resource": str(job),
                        "spark_conf": {"spark.driver.extraJavaOptions": "-Dfoo=$(whoami)"},
                    },
                )

    def test_t2_f4_04_rejects_resource_limit_overflow(self) -> None:
        """T2-F4-04: Memory, cores, or timeout exceeding policy maximums raises SandboxViolationError."""
        from spark_tuning.safety.sandbox import SandboxPolicy, SandboxViolationError

        with tempfile.TemporaryDirectory() as tmp:
            allowed = Path(tmp) / "jobs"
            allowed.mkdir()
            job = allowed / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(
                allowed_roots=[allowed],
                max_driver_memory_mb=2048,
                max_executor_memory_mb=4096,
                max_cores=4,
                timeout_seconds=60.0,
            )
            spec = RunSpec(
                run_id="t2-f4-04",
                skill="partition-sizing",
                objective="limit guard",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    spec=spec,
                    iteration=1,
                    hypothesis={
                        "id": "h-mem",
                        "app_resource": str(job),
                        "spark_conf": {"spark.driver.memory": "64g"},
                    },
                )
            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    spec=spec,
                    iteration=1,
                    hypothesis={
                        "id": "h-timeout",
                        "app_resource": str(job),
                        "timeout_seconds": 99999,
                    },
                )

    def test_t2_f4_05_subprocess_timeout_terminates_runaway_process(self) -> None:
        """T2-F4-05: Mock spark-submit exceeding timeout_seconds terminates cleanly with error."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "slow.py"
            job.write_text("print('slow')\n", encoding="utf-8")
            spark_home = make_temp_spark_submit(
                tmp_path,
                event_log_lines=[],
                sleep_seconds=5.0,
            )
            policy = SandboxPolicy(allowed_roots=[jobs_dir], timeout_seconds=0.3)
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=tmp_path / "artifacts",
                sandbox_policy=policy,
            )
            spec = RunSpec(
                run_id="t2-f4-05",
                skill="partition-sizing",
                objective="timeout guard",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            with self.assertRaises((subprocess.TimeoutExpired, RuntimeError, TimeoutError)):
                runner.run(spec, 1, {"id": "h-slow", "app_resource": str(job)})

    # --- F5 (#36) O-C-H-E-M-V-D-L Cognitive Loop Integration ---

    def test_t2_f5_01_empty_hypotheses_blocks_cleanly(self) -> None:
        """T2-F5-01: Empty hypotheses tuple yields RunState.BLOCKED, Decision.BLOCKED, 0 runner calls."""
        spec = RunSpec(
            run_id="t2-f5-01",
            skill="partition-sizing",
            objective="empty hypotheses",
            baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            hypotheses=(),
        )
        runner = FaultInjectingRunner()
        with tempfile.TemporaryDirectory() as tmp:
            res = LoopEngine(runner, JsonArtifactStore(tmp)).run(spec)
            self.assertEqual(res.state, RunState.BLOCKED)
            self.assertEqual(res.decision, Decision.BLOCKED)
            self.assertEqual(len(res.iterations), 0)
            self.assertEqual(len(runner.calls), 0)

    def test_t2_f5_02_all_hypotheses_previously_rejected_blocks_cleanly(self) -> None:
        """T2-F5-02: When all hypotheses were previously rejected in registry, loop blocks without re-running."""
        baseline = Metrics(
            duration_seconds=200.0,
            cost_usd=10.0,
            spill_disk_bytes=5_000_000,
            task_p95_seconds=60.0,
        )
        spec = RunSpec(
            run_id="t2-f5-02-seed",
            skill="partition-sizing",
            objective="all rejected",
            baseline=baseline,
            hypotheses=(
                {
                    "id": "h1-bad",
                    "target_category": "spill",
                    "metrics": {
                        "duration_seconds": 180.0,
                        "cost_usd": 9.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 50.0,
                        "correctness": False,
                    },
                },
                {
                    "id": "h2-bad",
                    "target_category": "spill",
                    "metrics": {
                        "duration_seconds": 180.0,
                        "cost_usd": 9.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 50.0,
                        "correctness": False,
                    },
                },
            ),
        )
        registry = ExperimentRegistry()
        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            LoopEngine(
                FaultInjectingRunner(),
                JsonArtifactStore(tmp1),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
            ).run(spec)

            runner2 = FaultInjectingRunner()
            spec2 = RunSpec(
                run_id="t2-f5-02-repeat",
                skill=spec.skill,
                objective=spec.objective,
                baseline=spec.baseline,
                hypotheses=spec.hypotheses,
            )
            res2 = LoopEngine(
                runner2,
                JsonArtifactStore(tmp2),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
            ).run(spec2)
            self.assertEqual(res2.decision, Decision.BLOCKED)
            self.assertEqual(len(runner2.calls), 0)

    def test_t2_f5_03_zero_or_negative_risk_cost_guard_in_prioritizer(self) -> None:
        """T2-F5-03: Zero or negative risk/cost in HypothesisProposal does not raise ZeroDivisionError."""
        diagnosis = BottleneckDiagnosisEngine.diagnose(
            Metrics(duration_seconds=100.0, cost_usd=10.0)
        )
        p_zero = HypothesisProposal(
            id="p-zero",
            target_category=BottleneckCategory.SPILL,
            description="zero risk & cost",
            expected_impact=0.8,
            confidence=0.9,
            reversibility=1.0,
            risk=0.0,
            estimated_cost_usd=0.0,
        )
        p_neg = HypothesisProposal(
            id="p-neg",
            target_category=BottleneckCategory.IO,
            description="negative guard",
            expected_impact=0.5,
            confidence=0.5,
            reversibility=0.5,
            risk=-0.1,
            estimated_cost_usd=-1.0,
        )
        ranked = HypothesisPrioritizer.rank_hypotheses([p_zero, p_neg], diagnosis)
        self.assertEqual(len(ranked), 2)
        self.assertTrue(all(math.isfinite(r.priority_score) for r in ranked))

    def test_t2_f5_04_tie_breaking_determinism_on_identical_scores(self) -> None:
        """T2-F5-04: Equal priority inputs break ties deterministically regardless of input order."""
        diagnosis = BottleneckDiagnosisEngine.diagnose(
            Metrics(duration_seconds=100.0, cost_usd=10.0, spill_disk_bytes=1000)
        )
        p1 = HypothesisProposal(
            id="hyp-alpha",
            target_category=BottleneckCategory.SPILL,
            description="first",
            expected_impact=0.8,
            confidence=0.8,
            reversibility=0.8,
            risk=0.2,
            estimated_cost_usd=5.0,
        )
        p2 = HypothesisProposal(
            id="hyp-beta",
            target_category=BottleneckCategory.SPILL,
            description="second",
            expected_impact=0.8,
            confidence=0.8,
            reversibility=0.8,
            risk=0.2,
            estimated_cost_usd=5.0,
        )
        r1 = [r.hypothesis.id for r in HypothesisPrioritizer.rank_hypotheses([p1, p2], diagnosis)]
        r2 = [r.hypothesis.id for r in HypothesisPrioritizer.rank_hypotheses([p2, p1], diagnosis)]
        self.assertEqual(r1, r2)

    def test_t2_f5_05_cost_budget_exhaustion_stops_loop(self) -> None:
        """T2-F5-05: Exceeding max_total_cost_usd stops loop BLOCKED and prevents iteration 3."""
        spec = RunSpec(
            run_id="t2-f5-05",
            skill="partition-sizing",
            objective="budget stop",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=100.0,
                spill_disk_bytes=5000,
                task_p95_seconds=50.0,
            ),
            max_iterations=5,
            max_total_cost_usd=200.0,
            hypotheses=(
                {
                    "id": "h1",
                    "metrics": {
                        "duration_seconds": 195.0,
                        "cost_usd": 150.0,
                        "spill_disk_bytes": 5000,
                        "task_p95_seconds": 50.0,
                        "correctness": False,
                    },
                },
                {
                    "id": "h2",
                    "metrics": {
                        "duration_seconds": 190.0,
                        "cost_usd": 60.0,
                        "spill_disk_bytes": 5000,
                        "task_p95_seconds": 50.0,
                    },
                },
                {
                    "id": "h3",
                    "metrics": {
                        "duration_seconds": 100.0,
                        "cost_usd": 10.0,
                        "spill_disk_bytes": 1000,
                        "task_p95_seconds": 20.0,
                    },
                },
            ),
        )
        runner = FaultInjectingRunner()
        with tempfile.TemporaryDirectory() as tmp:
            res = LoopEngine(runner, JsonArtifactStore(tmp)).run(spec)
            self.assertEqual(res.state, RunState.BLOCKED)
            self.assertEqual(res.decision, Decision.BLOCKED)
            self.assertEqual(len(runner.calls), 2)

    # --- F6 (#34) Idempotent Persistence & Crash Recovery ---

    def test_t2_f6_01_resume_unknown_run_id_raises_explicit_error(self) -> None:
        """T2-F6-01: LoopEngine.resume on unknown run_id raises KeyError/ValueError/FileNotFoundError."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = RunSpec(
            run_id="nonexistent-run",
            skill="partition-sizing",
            objective="unknown resume",
            baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "state.db")
            engine = LoopEngine(FaultInjectingRunner(), JsonArtifactStore(tmp), state_store=store)
            with self.assertRaises((KeyError, ValueError, FileNotFoundError)):
                engine.resume("nonexistent-run", spec)

    def test_t2_f6_02_concurrent_active_lease_blocks_second_worker(self) -> None:
        """T2-F6-02: Active lease held by worker-a blocks worker-b from acquiring/resuming."""
        from spark_tuning.persistence.store import LoopStateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "state.db")
            self.assertTrue(store.acquire_lease("run-lease", owner="worker-a", ttl_seconds=60.0))
            with self.assertRaises(Exception):
                acquired = store.acquire_lease("run-lease", owner="worker-b", ttl_seconds=60.0)
                if not acquired:
                    raise RuntimeError("lease conflict")

    def test_t2_f6_03_expired_lease_allows_crash_recovery_takeover(self) -> None:
        """T2-F6-03: Expired lease is reclaimed cleanly allowing worker-b takeover."""
        from spark_tuning.persistence.store import LoopStateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "state.db")
            self.assertTrue(store.acquire_lease("run-expired", owner="worker-a", ttl_seconds=0.01))
            time.sleep(0.05)
            self.assertTrue(store.acquire_lease("run-expired", owner="worker-b", ttl_seconds=30.0))

    def test_t2_f6_04_duplicate_iteration_checkpoint_is_idempotent(self) -> None:
        """T2-F6-04: Saving (run_id, iteration) twice retains exactly 1 record."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = RunSpec(
            run_id="t2-f6-04",
            skill="partition-sizing",
            objective="idempotent checkpoint",
            baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "state.db")
            store.init_run(spec)
            it1 = Iteration(
                number=1,
                hypothesis="h1",
                metrics=Metrics(duration_seconds=90.0, cost_usd=4.5),
                decision=Decision.INCONCLUSIVE,
                reason="first try",
            )
            store.save_iteration("t2-f6-04", it1)
            store.save_iteration("t2-f6-04", it1)
            loaded = store.load_run("t2-f6-04")
            self.assertEqual(len(loaded.iterations), 1)

    def test_t2_f6_05_corrupt_state_file_fails_loudly(self) -> None:
        """T2-F6-05: Corrupt/truncated state file raises an explicit error rather than silently resetting."""
        from spark_tuning.persistence.store import LoopStateStore

        with tempfile.TemporaryDirectory() as tmp:
            corrupt_path = Path(tmp) / "corrupt_state.json"
            corrupt_path.write_bytes(b"{not valid json\x00\xff")
            with self.assertRaises(Exception):
                store = LoopStateStore(corrupt_path)
                store.load_run("any-run")

    # --- F7 CLI, Spec Contract & Gate ---

    def test_t2_f7_01_cli_invalid_subcommand_and_missing_spec(self) -> None:
        """T2-F7-01: Invalid CLI subcommand or missing spec path exits non-zero."""
        proc_cmd = run_cli(["invalid-subcommand", "examples/runs/demo.json"])
        self.assertNotEqual(proc_cmd.returncode, 0)

        proc_missing = run_cli(["run", "/nonexistent/does_not_exist_spec.json"])
        self.assertNotEqual(proc_missing.returncode, 0)

    def test_t2_f7_02_load_spec_rejects_malformed_and_incomplete_specs(self) -> None:
        """T2-F7-02: load_spec rejects malformed_spec.badjson and specs missing required fields."""
        with self.assertRaises(json.JSONDecodeError):
            load_spec(SPECS_DIR / "malformed_spec.badjson")

        with tempfile.TemporaryDirectory() as tmp:
            incomplete = Path(tmp) / "incomplete.json"
            incomplete.write_text(json.dumps({"run_id": "only-id"}), encoding="utf-8")
            with self.assertRaises((KeyError, TypeError, ValueError)):
                load_spec(incomplete)

    def test_t2_f7_03_cli_creates_nested_artifacts_dir_and_matches_stdout_schema(self) -> None:
        """T2-F7-03: CLI run creates nested --artifacts directory on demand and writes JSON matching stdout."""
        with tempfile.TemporaryDirectory() as tmp:
            nested_dir = Path(tmp) / "nested" / "deep" / "artifacts"
            self.assertFalse(nested_dir.exists())

            proc = run_cli(["run", "examples/runs/demo.json", "--artifacts", str(nested_dir)])
            self.assertEqual(
                proc.returncode,
                0,
                f"CLI failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
            )
            self.assertTrue(nested_dir.is_dir())

            stdout_payload = json.loads(proc.stdout)
            artifact_path = nested_dir / "demo-join-strategy.json"
            self.assertTrue(artifact_path.exists())
            artifact_payload = json.loads(artifact_path.read_text(encoding="utf-8"))

            for required_key in ("run_id", "state", "decision", "iterations", "reason"):
                self.assertIn(required_key, stdout_payload)
                self.assertEqual(artifact_payload.get(required_key), stdout_payload.get(required_key))

    def test_t2_f7_04_load_spec_rejects_non_dict_root_and_invalid_baseline(self) -> None:
        """T2-F7-04: load_spec rejects non-dict JSON root [] and missing/invalid baseline metrics."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            list_root = tmp_path / "list_root.json"
            list_root.write_text("[]", encoding="utf-8")
            with self.assertRaises((AttributeError, TypeError, ValueError)):
                load_spec(list_root)

            bad_baseline_type = tmp_path / "bad_baseline_type.json"
            bad_baseline_type.write_text(
                json.dumps(
                    {
                        "run_id": "bad-base",
                        "skill": "partition-sizing",
                        "objective": "reject invalid baseline",
                        "baseline": ["not", "a", "dict"],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises((TypeError, ValueError)):
                load_spec(bad_baseline_type)

            missing_baseline = tmp_path / "missing_baseline.json"
            missing_baseline.write_text(
                json.dumps(
                    {
                        "run_id": "missing-base",
                        "skill": "partition-sizing",
                        "objective": "reject missing baseline block",
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises((KeyError, TypeError, ValueError)):
                load_spec(missing_baseline)

            invalid_baseline_metric = tmp_path / "invalid_duration.json"
            invalid_baseline_metric.write_text(
                json.dumps(
                    {
                        "run_id": "neg-dur",
                        "skill": "partition-sizing",
                        "objective": "reject negative baseline duration",
                        "baseline": {"duration_seconds": -5.0, "cost_usd": 10.0},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises((KeyError, TypeError, ValueError)):
                load_spec(invalid_baseline_metric)

    def test_t2_f7_05_git_diff_check_reports_zero_whitespace_errors(self) -> None:
        """T2-F7-05: Repository maintains clean formatting (git diff --check exits 0)."""
        proc = subprocess.run(
            ["git", "diff", "--check"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"git diff --check detected whitespace errors:\n{proc.stdout}\n{proc.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
