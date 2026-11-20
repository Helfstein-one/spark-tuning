"""Tier 4 E2E Tests — Real-World End-to-End Application Scenarios (T4-01..07)."""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from examples.anti_patterns.corpus import AntiPatternCorpus
from spark_tuning.adapters import (
    FixtureExperimentRunner,
    JsonArtifactStore,
    SparkEventLogParser,
    SparkSubmitExperimentRunner,
    load_spec,
)
from spark_tuning.core.prioritization import HypothesisPrioritizer
from spark_tuning.core.registry import ExperimentRegistry
from spark_tuning.diagnostics.engine import BottleneckCategory, BottleneckDiagnosisEngine
from spark_tuning.engine import LoopEngine
from spark_tuning.evaluation.pareto import ParetoDecisionEngine
from spark_tuning.evaluator import evaluate
from spark_tuning.models import Decision, Metrics, RunSpec, RunState
from spark_tuning.reporting.report_generator import OptimizationReportGenerator
from spark_tuning.safety.quality_gate import DataQualityGate, QualityCheckStatus
from spark_tuning.safety.sanitizer import ArtifactSanitizer
from e2e_tests.helpers import (
    EVENT_LOGS_DIR,
    REPO_ROOT,
    SECRETS_DIR,
    SPECS_DIR,
    FaultInjectingRunner,
    SimulatedProcessCrash,
    assert_no_raw_secrets,
    get_parse_diagnostics,
    get_unassessed_guardrails,
    make_temp_spark_submit,
    run_cli,
)


class TestTier4Scenarios(unittest.TestCase):
    """7 realistic end-to-end Spark tuning scenarios across F1–F7."""

    def test_t4_01_skewed_join_tuning_via_cli(self) -> None:
        """T4-01: Skewed join tuning via CLI diagnoses SPILL, reorders hypotheses, parses Spark 3 log, accepts."""
        from spark_tuning.models import LoopPhase

        spec_path = SPECS_DIR / "skew_join_spec.json"
        spec = load_spec(spec_path)
        diag = BottleneckDiagnosisEngine.diagnose(spec.baseline)
        self.assertIn(diag.primary_cause, (BottleneckCategory.SPILL, BottleneckCategory.SKEW))

        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(["run", str(spec_path), "--artifacts", tmp])
            self.assertEqual(
                proc.returncode,
                0,
                f"CLI failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
            )
            out = json.loads(proc.stdout)
            self.assertEqual(out["decision"], "accept")
            self.assertEqual(out["state"], "pr_ready")
            # Cosmetic hypothesis was listed first in JSON, skew-join-hint must have run first
            self.assertEqual(out["iterations"][0]["hypothesis"], "skew-join-hint")
            self.assertEqual(out["iterations"][0]["metrics"]["duration_seconds"], 221.0)
            self.assertEqual(out["iterations"][0]["metrics"]["spill_disk_bytes"], 9448928051)

            artifact = json.loads((Path(tmp) / "e2e-skew-join-tuning.json").read_text(encoding="utf-8"))
            phases = (
                artifact.get("phases")
                or artifact.get("phase_trace")
                or out.get("phases")
                or out.get("phase_trace")
            )
            self.assertTrue(phases, "Artifact must record all 8 LoopPhase states")
            phase_values = [p["phase"] if isinstance(p, dict) else p for p in phases]
            for expected in LoopPhase:
                self.assertIn(expected.value, [str(v.value if hasattr(v, "value") else v) for v in phase_values])
            discarded = (
                artifact.get("discarded_hypotheses")
                or artifact.get("rejected_hypotheses")
                or artifact.get("discard_reasons")
                or out.get("discarded_hypotheses")
                or out.get("rejected_hypotheses")
                or out.get("discard_reasons")
            )
            self.assertTrue(discarded, "Discarded cosmetic hypothesis must have documented reason")

    def test_t4_02_gc_spill_loop_rollback_then_second_hypothesis_accept(self) -> None:
        """T4-02: GC thrashing loop rolls back unsafe offheap, accepts Kryo+G1GC, updates registry & report."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = load_spec(SPECS_DIR / "gc_rollback_then_accept_spec.json")
        diag = BottleneckDiagnosisEngine.diagnose(spec.baseline)
        self.assertEqual(diag.primary_cause, BottleneckCategory.MEMORY_GC)

        registry = ExperimentRegistry()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = LoopStateStore(tmp_path / "state.db")
            artifacts = JsonArtifactStore(tmp_path / "artifacts")
            result = LoopEngine(
                FixtureExperimentRunner(),
                artifacts,
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
                state_store=store,
            ).run(spec)

            self.assertEqual(result.decision, Decision.ACCEPT)
            self.assertEqual(result.state, RunState.PR_READY)
            self.assertEqual(
                [it.hypothesis for it in result.iterations],
                ["aggressive-offheap", "kryo-and-g1gc"],
            )
            self.assertEqual(result.iterations[0].decision, Decision.ROLLBACK)
            self.assertEqual(result.iterations[1].decision, Decision.ACCEPT)

            report_md = OptimizationReportGenerator.generate_markdown(result, spec.baseline)
            self.assertIn("aggressive-offheap", report_md)
            self.assertIn("kryo-and-g1gc", report_md)
            self.assertIn("25.0", report_md)

            # Follow-up run on same workload skips previously rolled-back aggressive-offheap
            spec_followup = RunSpec(
                run_id="t4-02-followup",
                skill=spec.skill,
                objective=spec.objective,
                baseline=spec.baseline,
                hypotheses=spec.hypotheses,
            )
            runner2 = FaultInjectingRunner()
            res2 = LoopEngine(
                runner2,
                artifacts,
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
            ).run(spec_followup)
            self.assertEqual(res2.decision, Decision.ACCEPT)
            self.assertEqual([c[2] for c in runner2.calls], ["kryo-and-g1gc"])

    def test_t4_03_truncated_corrupt_event_log_blocks_silent_accept(self) -> None:
        """T4-03: Corrupt/truncated Spark log flags ParseDiagnostics incomplete and blocks promotion."""
        truncated_path = EVENT_LOGS_DIR / "spark3_truncated.jsonl"
        parsed = SparkEventLogParser.parse_file(truncated_path)
        diag = get_parse_diagnostics(parsed)
        self.assertIsNotNone(diag)
        self.assertGreaterEqual(diag.invalid_json_lines, 2)
        self.assertTrue(diag.truncated)
        self.assertTrue(diag.missing_app_end)
        self.assertFalse(diag.is_complete)

        missing_tm = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark3_missing_task_metrics.jsonl")
        self.assertIsNone(missing_tm.spill_disk_bytes)
        self.assertIsNone(missing_tm.gc_seconds)

        spec = RunSpec(
            run_id="t4-03",
            skill="partition-sizing",
            objective="block truncated and missing-metric logs",
            baseline=Metrics(
                duration_seconds=300.0,
                cost_usd=15.0,
                spill_disk_bytes=10_000_000,
                task_p95_seconds=60.0,
            ),
            hypotheses=(
                {"id": "trunc-cand", "event_log": str(truncated_path)},
                {
                    "id": "no-task-metrics-cand",
                    "event_log": str(EVENT_LOGS_DIR / "spark3_missing_task_metrics.jsonl"),
                },
            ),
        )
        ev_trunc = evaluate(spec, parsed)
        self.assertNotEqual(ev_trunc.decision, Decision.ACCEPT)
        ev_missing = evaluate(spec, missing_tm)
        self.assertNotEqual(ev_missing.decision, Decision.ACCEPT)
        self.assertTrue(len(get_unassessed_guardrails(ev_missing)) > 0)

        with tempfile.TemporaryDirectory() as tmp:
            res = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
            ).run(spec)
            self.assertEqual(res.state, RunState.BLOCKED)
            self.assertNotEqual(res.decision, Decision.ACCEPT)

    def test_t4_04_sandbox_violation_and_secret_redaction_audit_trail(self) -> None:
        """T4-04: Valid sandboxed run redacts all synthetic secrets; traversal/prod master/URI rejected."""
        from spark_tuning.safety.sandbox import SandboxPolicy, SandboxViolationError

        log_lines = (EVENT_LOGS_DIR / "spark3_complete.jsonl").read_text(encoding="utf-8").splitlines()
        raw_corpus = (SECRETS_DIR / "anonymized_payload.txt").read_text(encoding="utf-8")
        secrets_list = [
            "AKIAIOSFODNN7EXAMPLE",
            "gho_exampleSyntheticToken1234567890abcdef",
            "synthetic_bearer_secret_token_98765",
            "deadbeefcafebabe1234567890",
            "alice.synthetic@example.com",
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "tune.py"
            job.write_text("print('tuned')\n", encoding="utf-8")
            artifacts_dir = tmp_path / "artifacts"

            spark_home = make_temp_spark_submit(
                tmp_path,
                event_log_lines=log_lines,
                stdout=raw_corpus,
                stderr="error with alice.synthetic@example.com",
            )
            policy = SandboxPolicy(
                allowed_roots=[jobs_dir],
                block_prod_masters=True,
                allow_network=False,
                allowed_env_vars={"SPARK_SQL_OPT"},
            )
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=artifacts_dir,
                sandbox_policy=policy,
            )
            spec = RunSpec(
                run_id="t4-04",
                skill="partition-sizing",
                objective="sandbox + sanitizer audit",
                baseline=Metrics(duration_seconds=340.0, cost_usd=17.0),
            )

            # Part A: Valid run with secrets in host env, args, stdout, stderr
            old_gh = os.environ.get("GITHUB_TOKEN")
            try:
                os.environ["GITHUB_TOKEN"] = "gho_exampleSyntheticToken1234567890abcdef"
                metrics = runner.run(
                    spec,
                    1,
                    {
                        "id": "valid-audit",
                        "app_resource": str(job),
                        "master": "local[2]",
                        "spark_conf": {"spark.sql.shuffle.partitions": "400"},
                        "app_args": ["--email", "alice.synthetic@example.com"],
                        "env": {"SPARK_SQL_OPT": "true"},
                    },
                )
            finally:
                if old_gh is None:
                    os.environ.pop("GITHUB_TOKEN", None)
                else:
                    os.environ["GITHUB_TOKEN"] = old_gh

            self.assertEqual(metrics.duration_seconds, 221.0)
            run_dir = artifacts_dir / "t4-04" / "valid-audit"
            meta = json.loads((run_dir / "run_metadata.json").read_text(encoding="utf-8"))
            cmd = meta.get("command", [])
            has_conf_in_cmd = any("spark.sql.shuffle.partitions=400" in str(arg) for arg in cmd)
            has_conf_in_dict = meta.get("spark_conf", {}).get("spark.sql.shuffle.partitions") == "400"
            self.assertTrue(
                has_conf_in_cmd or has_conf_in_dict,
                f"Expected spark.sql.shuffle.partitions=400 in command or spark_conf, got {meta}",
            )
            for fname in ("run_metadata.json", "stdout.log", "stderr.log"):
                assert_no_raw_secrets(
                    self,
                    (run_dir / fname).read_text(encoding="utf-8"),
                    secrets_list,
                )

            # Verify RedactionReport directly on corpus
            _, report = ArtifactSanitizer.sanitize_with_report({"raw": raw_corpus})
            self.assertGreaterEqual(report.total_redactions, 5)

            # Part B: Sandbox violations rejected before spawning subprocess
            for bad_hyp in (
                {"id": "bad-trav", "app_resource": "../../etc/passwd"},
                {"id": "bad-master", "app_resource": str(job), "master": "yarn"},
                {
                    "id": "bad-uri",
                    "app_resource": str(job),
                    "master": "local[2]",
                    "app_args": ["s3://prod-data/warehouse"],
                },
            ):
                with self.assertRaises(SandboxViolationError) as ctx:
                    runner.run(spec, 2, bad_hyp)
                self.assertTrue(bool(ctx.exception.code))
                self.assertTrue(bool(ctx.exception.field))

    def test_t4_05_simulated_mid_loop_kill_and_idempotent_resume(self) -> None:
        """T4-05: Mid-loop crash recovers cleanly via LoopEngine.resume without re-running iter 1."""
        from spark_tuning.persistence.store import LoopStateStore

        spec = RunSpec(
            run_id="t4-05",
            skill="partition-sizing",
            objective="crash recovery",
            baseline=Metrics(
                duration_seconds=300.0,
                cost_usd=15.0,
                spill_disk_bytes=10_000_000,
                task_p95_seconds=80.0,
            ),
            hypotheses=(
                {
                    "id": "h1-rollback",
                    "target_category": "spill",
                    "expected_impact": 0.95,
                    "confidence": 0.95,
                    "reversibility": 0.95,
                    "risk": 0.05,
                    "estimated_cost_usd": 4.0,
                    "metrics": {
                        "duration_seconds": 280.0,
                        "cost_usd": 14.0,
                        "spill_disk_bytes": 8_000_000,
                        "task_p95_seconds": 75.0,
                        "correctness": False,
                    },
                },
                {
                    "id": "h2-accept",
                    "target_category": "spill",
                    "expected_impact": 0.80,
                    "confidence": 0.85,
                    "reversibility": 0.90,
                    "risk": 0.10,
                    "estimated_cost_usd": 6.0,
                    "metrics": {
                        "duration_seconds": 180.0,
                        "cost_usd": 9.0,
                        "spill_disk_bytes": 1_500_000,
                        "task_p95_seconds": 45.0,
                        "correctness": True,
                    },
                },
                {
                    "id": "h3-unused",
                    "target_category": "io",
                    "expected_impact": 0.2,
                    "confidence": 0.5,
                    "reversibility": 0.9,
                    "risk": 0.2,
                    "estimated_cost_usd": 10.0,
                    "metrics": {"duration_seconds": 290.0, "cost_usd": 14.5},
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = LoopStateStore(tmp_path / "state.db")
            artifacts = JsonArtifactStore(tmp_path / "artifacts")
            registry = ExperimentRegistry()

            crash_runner = FaultInjectingRunner(
                fail_on_calls={2},
                exception_factory=lambda call_idx, hyp_id: SimulatedProcessCrash(
                    f"Simulated crash at call={call_idx}, hypothesis={hyp_id}"
                ),
            )
            with self.assertRaises(SimulatedProcessCrash):
                LoopEngine(
                    crash_runner,
                    artifacts,
                    diagnosis_engine=BottleneckDiagnosisEngine,
                    prioritizer=HypothesisPrioritizer,
                    registry=registry,
                    state_store=store,
                ).run(spec)
            self.assertEqual([c[2] for c in crash_runner.calls], ["h1-rollback", "h2-accept"])

            resume_runner = FaultInjectingRunner()
            resumed = LoopEngine(
                resume_runner,
                artifacts,
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
                state_store=store,
            ).resume("t4-05", spec)
            self.assertEqual(resumed.decision, Decision.ACCEPT)
            self.assertEqual(resumed.state, RunState.PR_READY)
            self.assertEqual([c[2] for c in resume_runner.calls], ["h2-accept"])

            # Second resume is a 0-execution idempotent no-op
            noop_runner = FaultInjectingRunner()
            resumed_again = LoopEngine(
                noop_runner,
                artifacts,
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
                state_store=store,
            ).resume("t4-05", spec)
            self.assertEqual(len(noop_runner.calls), 0)
            self.assertEqual(resumed_again.decision, Decision.ACCEPT)

    def test_t4_06_multi_iteration_regression_guard_and_budget_stopping(self) -> None:
        """T4-06: Budget cap stops loop at $52 > $50, Pareto avoids None->0, DataQualityGate blocks unknown."""
        baseline = Metrics(
            duration_seconds=300.0,
            cost_usd=20.0,
            spill_disk_bytes=10_000_000,
            gc_seconds=25.0,
            task_p95_seconds=50.0,
        )
        spec = RunSpec(
            run_id="t4-06",
            skill="partition-sizing",
            objective="budget + regression guard",
            baseline=baseline,
            max_iterations=4,
            max_total_cost_usd=50.0,
            hypotheses=(
                {
                    "id": "h1-p95-regress",
                    "metrics": {
                        "duration_seconds": 250.0,
                        "cost_usd": 18.0,
                        "spill_disk_bytes": 5_000_000,
                        "gc_seconds": 15.0,
                        "task_p95_seconds": 67.5,  # +35% > 20% -> ROLLBACK
                    },
                },
                {
                    "id": "h2-marginal",
                    "metrics": {
                        "duration_seconds": 288.0,  # -4% < 10% -> INCONCLUSIVE
                        "cost_usd": 19.0,  # -5% < 10% -> INCONCLUSIVE
                        "spill_disk_bytes": 9_000_000,
                        "gc_seconds": 22.0,
                        "task_p95_seconds": 48.0,
                    },
                },
                {
                    "id": "h3-over-budget",
                    "metrics": {
                        "duration_seconds": 150.0,
                        "cost_usd": 18.0,  # 18 + 19 + 18 = 55 > 50 -> BLOCKED
                        "spill_disk_bytes": 2_000_000,
                        "gc_seconds": 10.0,
                        "task_p95_seconds": 30.0,
                    },
                },
                {
                    "id": "h4-unreached",
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 5.0,
                        "spill_disk_bytes": 1_000_000,
                        "gc_seconds": 5.0,
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
            self.assertEqual(len(runner.calls), 3)
            self.assertIn("cost", res.reason.lower())
            self.assertNotIn("h4-unreached", [c[2] for c in runner.calls])

        # Verify Pareto does not treat missing spill/gc as 0 win
        incomplete_cand = Metrics(duration_seconds=300.0, cost_usd=20.0, spill_disk_bytes=None, gc_seconds=None)
        self.assertFalse(ParetoDecisionEngine.dominates(incomplete_cand, baseline))

        # Verify DataQualityGate blocks missing checksum/row_count
        dq = DataQualityGate.evaluate(
            {"row_count": 1000, "checksum": "abc", "schema": ["id"]},
            {"row_count": 1000, "checksum": None, "schema": ["id"]},
        )
        self.assertFalse(dq.passed)
        self.assertTrue(any(c.status == QualityCheckStatus.UNKNOWN for c in dq.checks))

    def test_t4_07_cross_version_spark2_3_4_and_repo_gate(self) -> None:
        """T4-07: Cross-version Spark 2/3/4 logs parse with major versions and repo gates pass."""
        m2 = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark2_complete.jsonl")
        m3 = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark3_complete.jsonl")
        m4 = SparkEventLogParser.parse_file(EVENT_LOGS_DIR / "spark4_complete.jsonl")
        self.assertEqual(get_parse_diagnostics(m2).spark_major, 2)
        self.assertEqual(get_parse_diagnostics(m3).spark_major, 3)
        self.assertEqual(get_parse_diagnostics(m4).spark_major, 4)

        clean_env = os.environ.copy()
        clean_env.pop("PYTHONPATH", None)
        disc_proc = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-q"],
            cwd=str(REPO_ROOT),
            env=clean_env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(disc_proc.returncode, 0, disc_proc.stderr)

        gate_proc = subprocess.run(
            ["bash", "scripts/validate_loop.sh"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        self.assertEqual(gate_proc.returncode, 0, gate_proc.stderr)

        for json_path in glob.glob(str(REPO_ROOT / "**" / "*.json"), recursive=True):
            with open(json_path, "r", encoding="utf-8") as fh:
                json.load(fh)


if __name__ == "__main__":
    unittest.main()
