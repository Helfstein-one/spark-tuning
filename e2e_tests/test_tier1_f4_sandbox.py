"""Tier 1 E2E Tests — F4 (#37): Secure Runner Sandbox Policy."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from spark_tuning.adapters import FixtureExperimentRunner, SparkSubmitExperimentRunner
from spark_tuning.models import Metrics, RunSpec
from e2e_tests.helpers import EVENT_LOGS_DIR, assert_no_raw_secrets, make_temp_spark_submit


class TestTier1F4Sandbox(unittest.TestCase):
    """T1-F4-01..05: SandboxPolicy validation, sandboxed spark-submit, env isolation, limits, network."""

    def _base_spec(self, run_id: str = "t1-f4-run") -> RunSpec:
        return RunSpec(
            run_id=run_id,
            skill="partition-sizing",
            objective="sandboxed execution",
            baseline=Metrics(
                duration_seconds=300.0,
                cost_usd=15.0,
                spill_disk_bytes=1000000,
                task_p95_seconds=100.0,
            ),
        )

    def test_t1_f4_01_valid_sandboxed_submission_passes_validate_run(self) -> None:
        """T1-F4-01: Allowed app_resource and local master pass SandboxPolicy.validate_run."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        with tempfile.TemporaryDirectory() as tmp:
            jobs_dir = Path(tmp) / "jobs"
            jobs_dir.mkdir()
            job_file = jobs_dir / "tune_job.py"
            job_file.write_text("print('spark job')\n", encoding="utf-8")

            policy = SandboxPolicy(allowed_roots=[jobs_dir], allow_network=False)
            submission = policy.validate_run(
                spec=self._base_spec(),
                iteration=1,
                hypothesis={
                    "id": "hyp-valid",
                    "app_resource": str(job_file),
                    "master": "local[2]",
                    "spark_conf": {"spark.sql.shuffle.partitions": "64"},
                    "env": {"SPARK_USER_OPT": "enabled"},
                },
            )
            self.assertEqual(Path(submission.app_resource).resolve(), job_file.resolve())
            self.assertEqual(submission.master, "local[2]")

    def test_t1_f4_02_spark_submit_runner_executes_valid_sandboxed_mock(self) -> None:
        """T1-F4-02: SparkSubmitExperimentRunner runs sandboxed mock spark-submit and parses Metrics."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        log_lines = (EVENT_LOGS_DIR / "spark3_complete.jsonl").read_text(encoding="utf-8").splitlines()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job_file = jobs_dir / "job.py"
            job_file.write_text("print('ok')\n", encoding="utf-8")
            artifacts_dir = tmp_path / "artifacts"

            spark_home = make_temp_spark_submit(
                tmp_path,
                event_log_lines=log_lines,
                stdout="job completed\n",
                stderr="",
            )
            policy = SandboxPolicy(allowed_roots=[jobs_dir])
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=artifacts_dir,
                cost_per_second=0.05,
                sandbox_policy=policy,
            )
            metrics = runner.run(
                self._base_spec("t1-f4-02"),
                1,
                {"id": "hyp-1", "app_resource": str(job_file), "master": "local[2]"},
            )
            self.assertEqual(metrics.duration_seconds, 221.0)
            self.assertEqual(metrics.cost_usd, 11.05)

            run_dir = artifacts_dir / "t1-f4-02" / "hyp-1"
            self.assertTrue((run_dir / "run_metadata.json").exists())
            self.assertTrue((run_dir / "stdout.log").exists())
            self.assertTrue((run_dir / "stderr.log").exists())

    def test_t1_f4_03_env_allowlist_and_sanitizer_filter_host_secrets(self) -> None:
        """T1-F4-03: Host os.environ secrets do not leak to child subprocess or run_metadata.json."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        log_lines = (EVENT_LOGS_DIR / "spark3_complete.jsonl").read_text(encoding="utf-8").splitlines()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job_file = jobs_dir / "job.py"
            job_file.write_text("print('ok')\n", encoding="utf-8")
            artifacts_dir = tmp_path / "artifacts"
            captured_env_file = tmp_path / "child_env.json"

            spark_home = make_temp_spark_submit(
                tmp_path,
                event_log_lines=log_lines,
                record_env_path=captured_env_file,
            )
            policy = SandboxPolicy(
                allowed_roots=[jobs_dir],
                allowed_env_vars={"SPARK_USER_OPT", "DB_PASS"},
            )
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=artifacts_dir,
                sandbox_policy=policy,
            )

            host_secret = "super_host_aws_secret_value_999"
            host_gh = "gho_hostSecretToken1234567890abcdef"
            hyp_pass = "inline_db_password_secret_777"
            old_aws = os.environ.get("AWS_SECRET_ACCESS_KEY")
            old_gh = os.environ.get("GITHUB_TOKEN")
            try:
                os.environ["AWS_SECRET_ACCESS_KEY"] = host_secret
                os.environ["GITHUB_TOKEN"] = host_gh
                runner.run(
                    self._base_spec("t1-f4-03"),
                    1,
                    {
                        "id": "hyp-env",
                        "app_resource": str(job_file),
                        "env": {"SPARK_USER_OPT": "1", "DB_PASS": hyp_pass},
                    },
                )
            finally:
                if old_aws is None:
                    os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
                else:
                    os.environ["AWS_SECRET_ACCESS_KEY"] = old_aws
                if old_gh is None:
                    os.environ.pop("GITHUB_TOKEN", None)
                else:
                    os.environ["GITHUB_TOKEN"] = old_gh

            child_env = json.loads(captured_env_file.read_text(encoding="utf-8"))
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", child_env)
            self.assertNotIn("GITHUB_TOKEN", child_env)
            self.assertEqual(child_env.get("SPARK_USER_OPT"), "1")
            self.assertEqual(child_env.get("SPARK_RUN_ID"), "t1-f4-03")

            metadata_text = (artifacts_dir / "t1-f4-03" / "hyp-env" / "run_metadata.json").read_text(
                encoding="utf-8"
            )
            assert_no_raw_secrets(self, metadata_text, [host_secret, host_gh, hyp_pass])

    def test_t1_f4_04_resource_limit_flags_and_timeout_policy_propagation(self) -> None:
        """T1-F4-04: SandboxPolicy enforces and propagates CPU/memory/timeout limits."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        with tempfile.TemporaryDirectory() as tmp:
            jobs_dir = Path(tmp) / "jobs"
            jobs_dir.mkdir()
            job_file = jobs_dir / "job.py"
            job_file.write_text("print('ok')\n", encoding="utf-8")

            policy = SandboxPolicy(
                allowed_roots=[jobs_dir],
                max_driver_memory_mb=2048,
                max_executor_memory_mb=4096,
                max_cores=4,
                timeout_seconds=30.0,
            )
            submission = policy.validate_run(
                spec=self._base_spec("t1-f4-04"),
                iteration=1,
                hypothesis={
                    "id": "hyp-limits",
                    "app_resource": str(job_file),
                    "spark_conf": {
                        "spark.driver.memory": "1g",
                        "spark.executor.memory": "2g",
                        "spark.executor.cores": "2",
                    },
                },
            )
            self.assertLessEqual(submission.timeout_seconds, 30.0)
            self.assertIn("spark.driver.memory", submission.spark_conf)

    def test_t1_f4_05_network_isolation_configuration_and_fixture_runner(self) -> None:
        """T1-F4-05: Network isolation configures local binding and FixtureExperimentRunner handles both modes."""
        from spark_tuning.safety.sandbox import SandboxPolicy

        with tempfile.TemporaryDirectory() as tmp:
            jobs_dir = Path(tmp) / "jobs"
            jobs_dir.mkdir()
            job_file = jobs_dir / "job.py"
            job_file.write_text("print('ok')\n", encoding="utf-8")

            policy = SandboxPolicy(allowed_roots=[jobs_dir], allow_network=False)
            submission = policy.validate_run(
                spec=self._base_spec("t1-f4-05"),
                iteration=1,
                hypothesis={"id": "hyp-net", "app_resource": str(job_file), "master": "local[*]"},
            )
            self.assertTrue(
                submission.spark_conf.get("spark.driver.host") == "127.0.0.1"
                or submission.spark_conf.get("spark.driver.bindAddress") == "127.0.0.1",
                f"Expected local loopback driver binding in spark_conf, got {submission.spark_conf}",
            )

        # Also validate FixtureExperimentRunner dual mode (event_log + inline metrics)
        spec = self._base_spec("t1-f4-05-fix")
        fixture_runner = FixtureExperimentRunner()
        m_log = fixture_runner.run(
            spec, 1, {"id": "h1", "event_log": str(EVENT_LOGS_DIR / "spark3_complete.jsonl")}
        )
        m_inline = fixture_runner.run(
            spec, 2, {"id": "h2", "metrics": {"duration_seconds": 50.0, "cost_usd": 2.5}}
        )
        self.assertEqual(m_log.duration_seconds, 221.0)
        self.assertEqual(m_inline.duration_seconds, 50.0)


if __name__ == "__main__":
    unittest.main()
