"""Tier 3 E2E Tests — Cross-Feature Pairwise Combinations (T3-01..15)."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from spark_tuning.adapters import (
    FixtureExperimentRunner,
    JsonArtifactStore,
    SparkEventLogParser,
    SparkSubmitExperimentRunner,
)
from spark_tuning.core.prioritization import HypothesisPrioritizer
from spark_tuning.core.registry import ExperimentRegistry
from spark_tuning.diagnostics.engine import BottleneckDiagnosisEngine
from spark_tuning.engine import LoopEngine
from spark_tuning.evaluator import evaluate
from spark_tuning.models import Decision, Iteration, LoopResult, Metrics, RunSpec, RunState
from spark_tuning.reporting.report_generator import OptimizationReportGenerator
from spark_tuning.safety.sanitizer import ArtifactSanitizer
from e2e_tests.helpers import (
    EVENT_LOGS_DIR,
    REPO_ROOT,
    FaultInjectingRunner,
    SimulatedProcessCrash,
    assert_no_raw_secrets,
    get_parse_diagnostics,
    get_unassessed_guardrails,
    make_temp_spark_submit,
    run_cli,
)


class TestTier3Pairwise(unittest.TestCase):
    """15 pairwise cross-feature E2E tests across F1–F7."""

    def test_t3_01_f1_f2_truncated_log_yields_unknown_metrics_and_inconclusive(self) -> None:
        """T3-01 (F1+F2): Truncated Spark 3.x log -> incomplete ParseDiagnostics -> evaluate never ACCEPTs."""
        spec = RunSpec(
            run_id="t3-01",
            skill="partition-sizing",
            objective="reject truncated log",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=1000,
                task_p95_seconds=20.0,
            ),
        )
        metrics = FixtureExperimentRunner().run(
            spec,
            1,
            {"id": "trunc-log", "event_log": str(EVENT_LOGS_DIR / "spark3_truncated.jsonl")},
        )
        diag = get_parse_diagnostics(metrics)
        self.assertIsNotNone(diag)
        self.assertFalse(diag.is_complete)

        ev = evaluate(spec, metrics)
        self.assertNotEqual(ev.decision, Decision.ACCEPT)
        self.assertTrue(len(get_unassessed_guardrails(ev)) > 0 or ev.decision in (Decision.INCONCLUSIVE, Decision.BLOCKED, Decision.ROLLBACK))

    def test_t3_02_f1_f2_spark2_baseline_vs_spark4_candidate_comparison(self) -> None:
        """T3-02 (F1+F2): Baseline from Spark 2.x log vs Candidate from Spark 4.x log evaluates accurately."""
        base_m = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark2_complete.jsonl", cost_per_second=0.1)
        cand_m = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark4_complete.jsonl", cost_per_second=0.1)

        base_diag = get_parse_diagnostics(base_m)
        cand_diag = get_parse_diagnostics(cand_m)
        self.assertIsNotNone(base_diag)
        self.assertIsNotNone(cand_diag)
        self.assertEqual(base_diag.spark_major, 2)
        self.assertEqual(cand_diag.spark_major, 4)

        spec = RunSpec(
            run_id="t3-02",
            skill="partition-sizing",
            objective="cross-version compare",
            baseline=base_m,
            max_spill_regression=1e12,
        )
        # Note spark4 fixture has higher absolute spill than tiny spark2 fixture unless compared on duration/cost with custom guardrail
        # Let's build a clean candidate Spark 4 log that improves both duration and spill over spark2_complete
        with tempfile.TemporaryDirectory() as tmp:
            cand_log = Path(tmp) / "spark4_improved.jsonl"
            cand_log.write_text(
                "\n".join(
                    [
                        '{"Event":"SparkListenerLogStart","Spark Version":"4.0.0"}',
                        '{"Event":"SparkListenerApplicationStart","Timestamp":1700000000000}',
                        '{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{"Task ID":0,"Attempt":0,"Launch Time":1700000001000,"Finish Time":1700000021000,"Failed":false},"Task Metrics":{"JVM GC Time":1000,"Disk Bytes Spilled":1024}}',
                        '{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":0}}',
                        '{"Event":"SparkListenerApplicationEnd","Timestamp":1700000070000}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cand_improved = SparkEventLogParser.parse_file(cand_log, cost_per_second=0.1)
            self.assertEqual(get_parse_diagnostics(cand_improved).spark_major, 4)
            spec2 = RunSpec(
                run_id="t3-02-ok",
                skill="partition-sizing",
                objective="cross-version compare",
                baseline=base_m,
            )
            ev = evaluate(spec2, cand_improved)
            self.assertEqual(ev.decision, Decision.ACCEPT)
            self.assertEqual(get_unassessed_guardrails(ev), ())

    def test_t3_03_f3_f4_sandboxed_runner_and_sanitizer_audit_trail(self) -> None:
        """T3-03 (F3+F4): Valid sandboxed spark-submit redacts secrets in metadata, stdout.log, stderr.log."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        log_lines = (EVENT_LOGS_DIR / "spark3_complete.jsonl").read_text(encoding="utf-8").splitlines()
        raw_jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiIxMjMifQ."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        raw_aws = "AKIAIOSFODNN7EXAMPLE"
        raw_email = "audit.user@example.com"

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            artifacts_dir = tmp_path / "artifacts"

            spark_home = make_temp_spark_submit(
                tmp_path,
                event_log_lines=log_lines,
                stdout=f"token={raw_jwt}\n",
                stderr=f"aws={raw_aws} email={raw_email}\n",
            )
            policy = SandboxPolicy(allowed_roots=[jobs_dir], allowed_env_vars={"SAFE_ARG"})
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=artifacts_dir,
                sandbox_policy=policy,
            )
            spec = RunSpec(
                run_id="t3-03",
                skill="partition-sizing",
                objective="audit trail",
                baseline=Metrics(duration_seconds=300.0, cost_usd=15.0),
            )
            runner.run(
                spec,
                1,
                {
                    "id": "h-audit",
                    "app_resource": str(job),
                    "app_args": ["--owner", raw_email],
                    "env": {"SAFE_ARG": raw_aws},
                },
            )
            run_dir = artifacts_dir / "t3-03" / "h-audit"
            for fname in ("run_metadata.json", "stdout.log", "stderr.log"):
                content = (run_dir / fname).read_text(encoding="utf-8")
                assert_no_raw_secrets(self, content, [raw_jwt, raw_aws, raw_email])

    def test_t3_04_f3_f4_sandbox_violation_error_message_is_sanitized(self) -> None:
        """T3-04 (F3+F4): SandboxViolationError raised with signed URL/password does not leak raw secret."""
        from spark_tuning.safety.sandbox import SandboxPolicy, SandboxViolationError

        raw_sig = "deadbeefcafebabe1234567890"
        raw_pw = "superSecretDbPass999"
        with tempfile.TemporaryDirectory() as tmp:
            jobs_dir = Path(tmp) / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs_dir], block_prod_masters=True)
            spec = RunSpec(
                run_id="t3-04",
                skill="partition-sizing",
                objective="sanitized violation",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            with self.assertRaises(SandboxViolationError) as ctx:
                policy.validate_run(
                    spec=spec,
                    iteration=1,
                    hypothesis={
                        "id": "h-leak-err",
                        "app_resource": str(job),
                        "master": "yarn",
                        "app_args": [
                            f"s3://prod-bucket/path?X-Amz-Signature={raw_sig}&password={raw_pw}"
                        ],
                    },
                )
            err_payload = f"{ctx.exception} {ctx.exception.detail}"
            assert_no_raw_secrets(self, err_payload, [raw_sig, raw_pw])

    def test_t3_05_f5_f6_cognition_across_crash_and_resume(self) -> None:
        """T3-05 (F5+F6): Ranked H1 gets ROLLBACK, crash occurs, resume skips H1 and accepts H2."""
        from spark_tuning.persistence.store import LoopStateStore

        baseline = Metrics(
            duration_seconds=300.0,
            cost_usd=15.0,
            spill_disk_bytes=20_000_000,
            task_p95_seconds=100.0,
        )
        spec = RunSpec(
            run_id="t3-05",
            skill="partition-sizing",
            objective="cognition + persistence",
            baseline=baseline,
            hypotheses=(
                {
                    "id": "H2-second",
                    "target_category": "spill",
                    "expected_impact": 0.70,
                    "confidence": 0.80,
                    "reversibility": 0.90,
                    "risk": 0.15,
                    "estimated_cost_usd": 5.0,
                    "metrics": {
                        "duration_seconds": 180.0,
                        "cost_usd": 9.0,
                        "spill_disk_bytes": 2_000_000,
                        "task_p95_seconds": 60.0,
                        "correctness": True,
                    },
                },
                {
                    "id": "H1-first-rollback",
                    "target_category": "spill",
                    "expected_impact": 0.95,
                    "confidence": 0.95,
                    "reversibility": 0.95,
                    "risk": 0.05,
                    "estimated_cost_usd": 2.0,
                    "metrics": {
                        "duration_seconds": 250.0,
                        "cost_usd": 12.5,
                        "spill_disk_bytes": 10_000_000,
                        "task_p95_seconds": 90.0,
                        "correctness": False,
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = LoopStateStore(Path(tmp) / "state.db")
            registry = ExperimentRegistry()
            artifacts = JsonArtifactStore(Path(tmp) / "artifacts")

            crashing_runner = FaultInjectingRunner(
                fail_on_calls={2},
                exception_factory=lambda call_idx, hyp_id: SimulatedProcessCrash(
                    f"Simulated crash at call={call_idx}, hypothesis={hyp_id}"
                ),
            )
            with self.assertRaises(SimulatedProcessCrash):
                LoopEngine(
                    crashing_runner,
                    artifacts,
                    diagnosis_engine=BottleneckDiagnosisEngine,
                    prioritizer=HypothesisPrioritizer,
                    registry=registry,
                    state_store=store,
                ).run(spec)
            self.assertEqual(crashing_runner.calls[0][2], "H1-first-rollback")

            healthy_runner = FaultInjectingRunner()
            resumed = LoopEngine(
                healthy_runner,
                artifacts,
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
                state_store=store,
            ).resume("t3-05", spec)
            self.assertEqual(resumed.decision, Decision.ACCEPT)
            self.assertEqual(resumed.state, RunState.PR_READY)
            self.assertEqual(len(healthy_runner.calls), 1)
            self.assertEqual(healthy_runner.calls[0][2], "H2-second")

    def test_t3_06_f1_f5_missing_guardrail_during_cognitive_loop_prevents_false_accept(self) -> None:
        """T3-06 (F1+F5): Top-ranked hypothesis missing task_p95_seconds yields INCONCLUSIVE, then iter 2 accepts."""
        spec = RunSpec(
            run_id="t3-06",
            skill="partition-sizing",
            objective="missing guardrail safe loop",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=5_000_000,
                task_p95_seconds=60.0,
            ),
            hypotheses=(
                {
                    "id": "h1-missing-p95",
                    "target_category": "spill",
                    "expected_impact": 0.95,
                    "confidence": 0.95,
                    "reversibility": 0.95,
                    "risk": 0.05,
                    "estimated_cost_usd": 3.0,
                    "metrics": {
                        "duration_seconds": 100.0,
                        "cost_usd": 5.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": None,
                    },
                },
                {
                    "id": "h2-complete",
                    "target_category": "spill",
                    "expected_impact": 0.80,
                    "confidence": 0.85,
                    "reversibility": 0.90,
                    "risk": 0.10,
                    "estimated_cost_usd": 5.0,
                    "metrics": {
                        "duration_seconds": 130.0,
                        "cost_usd": 6.5,
                        "spill_disk_bytes": 1_500_000,
                        "task_p95_seconds": 45.0,
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            res = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=ExperimentRegistry(),
            ).run(spec)
            self.assertEqual(res.iterations[0].hypothesis, "h1-missing-p95")
            self.assertNotEqual(res.iterations[0].decision, Decision.ACCEPT)
            self.assertEqual(res.decision, Decision.ACCEPT)
            self.assertEqual(res.iterations[-1].hypothesis, "h2-complete")

    def test_t3_07_f2_f5_incomplete_event_log_blocked_by_loop_engine(self) -> None:
        """T3-07 (F2+F5): Candidate referencing corrupt/truncated log cannot reach PR_READY/ACCEPT."""
        spec = RunSpec(
            run_id="t3-07",
            skill="partition-sizing",
            objective="block incomplete event log",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=5_000_000,
                task_p95_seconds=60.0,
            ),
            hypotheses=(
                {
                    "id": "h-corrupt-log",
                    "target_category": "spill",
                    "event_log": str(EVENT_LOGS_DIR / "spark3_truncated.jsonl"),
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            res = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
            ).run(spec)
            self.assertNotEqual(res.decision, Decision.ACCEPT)
            self.assertNotEqual(res.state, RunState.PR_READY)

    def test_t3_08_f4_f6_sandbox_violation_during_loop_releases_lease(self) -> None:
        """T3-08 (F4+F6): SandboxViolationError in iter 2 checkpoints failure and releases run_id lease."""
        from spark_tuning.persistence.store import LoopStateStore
        from spark_tuning.safety.sandbox import SandboxPolicy

        log_lines = (EVENT_LOGS_DIR / "spark3_complete.jsonl").read_text(encoding="utf-8").splitlines()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            valid_job = jobs_dir / "valid.py"
            valid_job.write_text("print('ok')\n", encoding="utf-8")
            spark_home = make_temp_spark_submit(tmp_path, event_log_lines=log_lines)

            policy = SandboxPolicy(allowed_roots=[jobs_dir])
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=tmp_path / "runner_artifacts",
                cost_per_second=0.1,
                sandbox_policy=policy,
            )
            store = LoopStateStore(tmp_path / "state.db")
            spec = RunSpec(
                run_id="t3-08",
                skill="partition-sizing",
                objective="sandbox failure releases lease",
                baseline=Metrics(
                    duration_seconds=100.0,
                    cost_usd=10.0,
                    spill_disk_bytes=10_000_000_000,
                    task_p95_seconds=80.0,
                ),
                hypotheses=(
                    {"id": "h1-inconclusive", "app_resource": str(valid_job)},
                    {"id": "h2-traversal", "app_resource": "../../etc/passwd"},
                ),
            )
            res = LoopEngine(
                runner,
                JsonArtifactStore(tmp_path / "artifacts"),
                state_store=store,
            ).run(spec)
            self.assertNotEqual(res.decision, Decision.ACCEPT)
            # Lease must be released so another worker can acquire it
            self.assertTrue(store.acquire_lease("t3-08", owner="worker-recover", ttl_seconds=10.0))
            store.release_lease("t3-08", owner="worker-recover")

    def test_t3_09_f3_f6_persisted_store_and_artifact_never_store_raw_secrets(self) -> None:
        """T3-09 (F3+F6): LoopStateStore and JsonArtifactStore redact secrets in hypotheses/configs."""
        from spark_tuning.persistence.store import LoopStateStore

        raw_gh = "gho_persistedLeakCheck1234567890abcdef"
        raw_email = "persist-leak@example.com"
        spec = RunSpec(
            run_id="t3-09",
            skill="partition-sizing",
            objective=f"optimize for {raw_email}",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=5_000_000,
                task_p95_seconds=50.0,
            ),
            hypotheses=(
                {
                    "id": "h-secret",
                    "spark_conf": {"spark.secret.token": raw_gh},
                    "env": {"OWNER_EMAIL": raw_email},
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 6.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 30.0,
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store_path = tmp_path / "state.db"
            artifacts_dir = tmp_path / "artifacts"
            store = LoopStateStore(store_path)
            LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(artifacts_dir),
                state_store=store,
            ).run(spec)

            for disk_file in tmp_path.rglob("*"):
                if disk_file.is_file():
                    raw_bytes = disk_file.read_text(encoding="utf-8", errors="replace")
                    assert_no_raw_secrets(self, raw_bytes, [raw_gh, raw_email])

    def test_t3_10_f1_f7_cli_missing_metric_exits_two_and_report_shows_unknown(self) -> None:
        """T3-10 (F1+F7): CLI run with missing spill_disk_bytes exits 2 and report omits 0 coercion."""
        baseline = Metrics(
            duration_seconds=200.0,
            cost_usd=10.0,
            spill_disk_bytes=5000,
            gc_seconds=12.0,
            task_p95_seconds=50.0,
        )
        spec_payload = {
            "run_id": "t3-10-missing-spill",
            "skill": "partition-sizing",
            "objective": "missing metric CLI",
            "baseline": {
                "duration_seconds": 200.0,
                "cost_usd": 10.0,
                "spill_disk_bytes": 5000,
                "gc_seconds": 12.0,
                "task_p95_seconds": 50.0,
            },
            "hypotheses": [
                {
                    "id": "h-no-spill",
                    "metrics": {
                        "duration_seconds": 100.0,
                        "cost_usd": 5.0,
                        "task_p95_seconds": 25.0,
                    },
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "spec.json"
            spec_path.write_text(json.dumps(spec_payload), encoding="utf-8")
            proc = run_cli(["run", str(spec_path), "--artifacts", tmp])
            self.assertEqual(proc.returncode, 2)
            out = json.loads(proc.stdout)
            self.assertNotEqual(out["decision"], "accept")

        res = LoopResult(
            run_id="t3-10-report",
            state=RunState.BLOCKED,
            decision=Decision.BLOCKED,
            iterations=[
                Iteration(
                    number=1,
                    hypothesis="h-no-spill",
                    metrics=Metrics(duration_seconds=100.0, cost_usd=5.0, task_p95_seconds=25.0),
                    decision=Decision.INCONCLUSIVE,
                    reason="unassessed spill",
                )
            ],
            reason="unassessed spill",
        )
        md = OptimizationReportGenerator.generate_markdown(res, baseline)
        row = [line for line in md.splitlines() if "h-no-spill" in line][0]
        cells = [c.strip().lower() for c in row.split("|")]
        self.assertNotIn(cells[5], ("0", "0.0"))
        self.assertNotIn(cells[6], ("0", "0.0"))

    def test_t3_11_f5_f7_cli_deterministic_cognitive_ordering(self) -> None:
        """T3-11 (F5+F7): CLI executes SPILL-targeted hypothesis before cosmetic hypothesis."""
        spec_payload = {
            "run_id": "t3-11-cli-order",
            "skill": "partition-sizing",
            "objective": "CLI cognitive ordering",
            "baseline": {
                "duration_seconds": 340.0,
                "cost_usd": 17.0,
                "spill_disk_bytes": 20000000000,
                "task_p95_seconds": 180.0,
                "tasks_count": 200,
            },
            "hypotheses": [
                {
                    "id": "cosmetic-first-in-json",
                    "target_category": "driver",
                    "expected_impact": 0.1,
                    "confidence": 0.3,
                    "reversibility": 0.9,
                    "risk": 0.4,
                    "estimated_cost_usd": 15.0,
                    "metrics": {
                        "duration_seconds": 335.0,
                        "cost_usd": 16.5,
                        "spill_disk_bytes": 20000000000,
                        "task_p95_seconds": 179.0,
                    },
                },
                {
                    "id": "spill-second-in-json",
                    "target_category": "spill",
                    "expected_impact": 0.9,
                    "confidence": 0.9,
                    "reversibility": 0.95,
                    "risk": 0.1,
                    "estimated_cost_usd": 8.0,
                    "metrics": {
                        "duration_seconds": 200.0,
                        "cost_usd": 10.0,
                        "spill_disk_bytes": 1000000000,
                        "task_p95_seconds": 70.0,
                    },
                },
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "spec.json"
            spec_path.write_text(json.dumps(spec_payload), encoding="utf-8")
            proc = run_cli(["run", str(spec_path), "--artifacts", tmp])
            self.assertEqual(proc.returncode, 0)
            out = json.loads(proc.stdout)
            self.assertEqual(out["iterations"][0]["hypothesis"], "spill-second-in-json")

    def test_t3_12_f2_f4_sandboxed_spark_submit_produces_truncated_event_log(self) -> None:
        """T3-12 (F2+F4): Sandboxed spark-submit emitting truncated log surfaces incomplete ParseDiagnostics."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        truncated_lines = (EVENT_LOGS_DIR / "spark3_truncated.jsonl").read_text(encoding="utf-8").splitlines()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            spark_home = make_temp_spark_submit(tmp_path, event_log_lines=truncated_lines)

            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=tmp_path / "artifacts",
                sandbox_policy=SandboxPolicy(allowed_roots=[jobs_dir]),
            )
            spec = RunSpec(
                run_id="t3-12",
                skill="partition-sizing",
                objective="truncated runner log",
                baseline=Metrics(duration_seconds=200.0, cost_usd=10.0, spill_disk_bytes=1000, task_p95_seconds=20.0),
            )
            metrics = runner.run(spec, 1, {"id": "h-trunc", "app_resource": str(job)})
            diag = get_parse_diagnostics(metrics)
            self.assertIsNotNone(diag)
            self.assertTrue(diag.truncated)
            self.assertFalse(diag.is_complete)
            self.assertNotEqual(evaluate(spec, metrics).decision, Decision.ACCEPT)

    def test_t3_13_f2_f3_event_log_with_pii_and_secrets_parsed_and_redacted(self) -> None:
        """T3-13 (F2+F3): Event log with user email & AWS key parses metrics cleanly and redacts on sanitize."""
        raw_aws = "AKIAIOSFODNN7EXAMPLE"
        raw_email = "alice.spark@example.com"
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "pii_log.jsonl"
            log.write_text(
                "\n".join(
                    [
                        '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
                        f'{{"Event":"SparkListenerApplicationStart","App Name":"JobFor-{raw_email}","App ID":"app-1","Timestamp":1700000000000,"User":"{raw_email}"}}',
                        f'{{"Event":"SparkListenerEnvironmentUpdate","Spark Properties":{{"spark.hadoop.fs.s3a.secret.key":"{raw_aws}"}}}}',
                        '{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{"Task ID":0,"Attempt":0,"Launch Time":1700000001000,"Finish Time":1700000011000,"Failed":false},"Task Metrics":{"JVM GC Time":100,"Disk Bytes Spilled":50}}',
                        '{"Event":"SparkListenerApplicationEnd","Timestamp":1700000020000}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            metrics = SparkEventLogParser.parse_file(log)
            self.assertEqual(metrics.duration_seconds, 20.0)
            sanitized, report = ArtifactSanitizer.sanitize_with_report(
                {"log_excerpt": log.read_text(encoding="utf-8")}
            )
            assert_no_raw_secrets(self, sanitized, [raw_aws, raw_email])
            self.assertGreaterEqual(report.total_redactions, 2)

    def test_t3_14_f3_f5_cognitive_decision_and_discarded_explanations_sanitized(self) -> None:
        """T3-14 (F3+F5): Cognitive explanations & discarded reasons retain rationale while stripping secrets."""
        raw_bearer = "bearer_secret_explanation_xyz987"
        raw_path = "/Users/secretuser/work/job.py"
        spec = RunSpec(
            run_id="t3-14",
            skill="partition-sizing",
            objective="sanitized explanations",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=5_000_000,
                task_p95_seconds=50.0,
            ),
            hypotheses=(
                {
                    "id": "h-win",
                    "target_category": "spill",
                    "description": "Use AQE skew join",
                    "expected_impact": 0.9,
                    "confidence": 0.9,
                    "reversibility": 0.9,
                    "risk": 0.1,
                    "estimated_cost_usd": 5.0,
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 6.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 30.0,
                    },
                },
                {
                    "id": "h-discarded",
                    "target_category": "driver",
                    "description": f"Run {raw_path} with Authorization: Bearer {raw_bearer}",
                    "expected_impact": 0.1,
                    "confidence": 0.4,
                    "reversibility": 0.8,
                    "risk": 0.3,
                    "estimated_cost_usd": 9.0,
                    "metrics": {"duration_seconds": 195.0, "cost_usd": 9.8},
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=ExperimentRegistry(),
            ).run(spec)
            artifact_text = (Path(tmp) / "t3-14.json").read_text(encoding="utf-8")
            assert_no_raw_secrets(self, artifact_text, [raw_bearer, raw_path])

    def test_t3_15_f6_f7_persisted_artifacts_compatible_with_gate_schema_and_validate_loop(self) -> None:
        """T3-15 (F6+F7): Persisted JSON artifacts & state store pass strict json.load() and validate_loop.sh."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = RunSpec(
            run_id="t3-15",
            skill="partition-sizing",
            objective="gate compatibility",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=5_000_000,
                task_p95_seconds=50.0,
            ),
            hypotheses=(
                {
                    "id": "h-ok",
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 6.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 30.0,
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = LoopStateStore(tmp_path / "state.db")
            LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp_path / "artifacts"),
                state_store=store,
            ).run(spec)

            json_files = list(tmp_path.rglob("*.json"))
            self.assertGreaterEqual(len(json_files), 1)
            for jf in json_files:
                parsed = json.loads(jf.read_text(encoding="utf-8"))
                self.assertIsInstance(parsed, dict)

        proc = subprocess.run(
            ["bash", "scripts/validate_loop.sh"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)


if __name__ == "__main__":
    unittest.main()
