"""Unit tests for Issue #37: SandboxPolicy, SandboxViolationError, and SparkSubmitExperimentRunner isolation."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from spark_tuning.adapters import SparkSubmitExperimentRunner
from spark_tuning.models import Metrics, RunSpec
from spark_tuning.safety import SandboxPolicy, SandboxViolationError, SanitizedSubmission


class SandboxPolicyTests(unittest.TestCase):
    def _spec(self, run_id: str = "sandbox-run") -> RunSpec:
        return RunSpec(
            run_id=run_id,
            skill="partition-sizing",
            objective="sandbox validation",
            baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
        )

    def test_valid_submission_and_canonical_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs_dir = Path(tmp) / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "tune.py"
            job.write_text("print('ok')\n", encoding="utf-8")

            policy = SandboxPolicy(allowed_roots=[jobs_dir], allow_network=False)
            submission = policy.validate_run(
                spec=self._spec(),
                iteration=1,
                hypothesis={
                    "id": "h-valid",
                    "app_resource": str(job),
                    "master": "local[2]",
                    "spark_conf": {"spark.sql.shuffle.partitions": "64"},
                    "app_args": ["--input", "local_table"],
                    "env": {"SPARK_USER_OPT": "enabled"},
                },
            )
            self.assertIsInstance(submission, SanitizedSubmission)
            self.assertEqual(Path(submission.app_resource), job.resolve())
            self.assertEqual(submission.master, "local[2]")
            self.assertEqual(submission.deploy_mode, "client")
            self.assertEqual(submission.app_args, ("--input", "local_table"))
            self.assertEqual(submission.spark_conf["spark.sql.shuffle.partitions"], "64")
            self.assertEqual(submission.env["SPARK_RUN_ID"], "sandbox-run")

    def test_path_traversal_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs_dir = Path(tmp) / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs_dir])

            for bad_path in ("../../etc/passwd", str(jobs_dir / ".." / "outside" / "job.py")):
                with self.assertRaises(SandboxViolationError) as ctx:
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h-trav", "app_resource": bad_path},
                    )
                self.assertEqual(ctx.exception.code, "PATH_TRAVERSAL")
                self.assertEqual(ctx.exception.field, "app_resource")

            with self.assertRaises(SandboxViolationError) as ctx_run:
                policy.validate_run(self._spec("../escape"), 1, {"id": "h1", "app_resource": str(job)})
            self.assertEqual(ctx_run.exception.code, "PATH_TRAVERSAL")

            with self.assertRaises(SandboxViolationError) as ctx_hyp:
                policy.validate_run(self._spec(), 1, {"id": "../../bad", "app_resource": str(job)})
            self.assertEqual(ctx_hyp.exception.code, "PATH_TRAVERSAL")

            with self.assertRaises(SandboxViolationError) as ctx_arg:
                policy.validate_run(
                    self._spec(),
                    1,
                    {"id": "h1", "app_resource": str(job), "app_args": ["../../etc/shadow"]},
                )
            self.assertEqual(ctx_arg.exception.code, "PATH_TRAVERSAL")

    def test_symlink_escape_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            allowed = tmp_path / "allowed"
            outside = tmp_path / "outside"
            allowed.mkdir()
            outside.mkdir()
            secret = outside / "secret.py"
            secret.write_text("print('secret')\n", encoding="utf-8")

            link_file = allowed / "link.py"
            link_file.symlink_to(secret)
            link_dir = allowed / "sub"
            link_dir.symlink_to(outside)

            policy = SandboxPolicy(allowed_roots=[allowed])
            for target in (link_file, link_dir / "secret.py"):
                with self.assertRaises(SandboxViolationError) as ctx:
                    policy.validate_run(self._spec(), 1, {"id": "h-sym", "app_resource": str(target)})
                self.assertEqual(ctx.exception.code, "SYMLINK_ESCAPE")

    def test_empty_allowed_roots_and_default_system_dir_protection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            job = Path(tmp) / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")

            closed_policy = SandboxPolicy(allowed_roots=[])
            with self.assertRaises(SandboxViolationError):
                closed_policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job)})

            default_policy = SandboxPolicy()
            with self.assertRaises(SandboxViolationError):
                default_policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": "/etc/passwd"})

            # Dangerous script extensions blocked even under default policy
            with self.assertRaises(SandboxViolationError):
                default_policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": "run.sh"})

            # Non-existent relative "job.py" passes default policy so missing spark-submit raises FileNotFoundError
            sub = default_policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": "job.py"})
            self.assertTrue(sub.app_resource.endswith("job.py"))

    def test_forbidden_masters_and_deploy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs], max_cores=4, block_prod_masters=True)

            for bad_master in ("yarn", "yarn-cluster", "k8s://https://prod.k8s:6443", "mesos://host:5050", "spark://prod:7077"):
                with self.assertRaises(SandboxViolationError) as ctx:
                    policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job), "master": bad_master})
                self.assertEqual(ctx.exception.field, "master")

            with self.assertRaises(SandboxViolationError):
                policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job), "master": "local[16]"})

            with self.assertRaises(SandboxViolationError):
                policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job), "deploy_mode": "cluster"})

    def test_disallowed_uri_schemes_and_prod_uris(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs])

            for bad_uri in ("s3://prod-bucket/job.py", "s3a://prod-warehouse/table", "https://evil.example/a.jar"):
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": bad_uri})
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job), "app_args": [bad_uri]})
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h1", "app_resource": str(job), "spark_conf": {"spark.jars": bad_uri}},
                    )

    def test_shell_metacharacters_jvm_agents_and_flag_smuggling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs])

            for bad_arg in ("$(whoami)", "`id`", "a; rm -rf /", "a | cat", "line1\nline2", "--conf", "--master=yarn"):
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job), "app_args": [bad_arg]})

            for bad_opts in ("-Dfoo=$(whoami)", "-javaagent:/tmp/evil.jar", "-XX:OnOutOfMemoryError=sh"):
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h1", "app_resource": str(job), "spark_conf": {"spark.driver.extraJavaOptions": bad_opts}},
                    )

    def test_blocked_spark_conf_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs])

            for bad_key in (
                "spark.yarn.queue",
                "spark.kubernetes.namespace",
                "spark.hadoop.fs.s3a.secret.key",
                "non_spark_key",
            ):
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h1", "app_resource": str(job), "spark_conf": {bad_key: "val"}},
                    )

    def test_memory_cores_timeout_parsing_and_limits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(
                allowed_roots=[jobs],
                max_driver_memory_mb=2048,
                max_executor_memory_mb=4096,
                max_cores=4,
                timeout_seconds=60.0,
            )

            ok = policy.validate_run(
                self._spec(),
                1,
                {
                    "id": "h-ok",
                    "app_resource": str(job),
                    "spark_conf": {
                        "spark.driver.memory": "1g",
                        "spark.executor.memory": "2048m",
                        "spark.executor.cores": "2",
                    },
                    "timeout_seconds": 30.0,
                },
            )
            self.assertEqual(ok.timeout_seconds, 30.0)

            for bad_mem in ("0g", "-1g", "abc", "64g"):
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h1", "app_resource": str(job), "spark_conf": {"spark.driver.memory": bad_mem}},
                    )

            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    self._spec(),
                    1,
                    {"id": "h1", "app_resource": str(job), "spark_conf": {"spark.executor.cores": "16"}},
                )

            for bad_timeout in (0, -5, True, 99999):
                with self.assertRaises(SandboxViolationError):
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h1", "app_resource": str(job), "timeout_seconds": bad_timeout},
                    )

    def test_network_isolation_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs], allow_network=False)

            sub = policy.validate_run(self._spec(), 1, {"id": "h1", "app_resource": str(job)})
            self.assertEqual(sub.spark_conf["spark.driver.host"], "127.0.0.1")
            self.assertEqual(sub.spark_conf["spark.driver.bindAddress"], "127.0.0.1")

            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    self._spec(),
                    1,
                    {"id": "h1", "app_resource": str(job), "spark_conf": {"spark.driver.host": "10.0.0.5"}},
                )

            with self.assertRaises(SandboxViolationError):
                policy.validate_run(
                    self._spec(),
                    1,
                    {"id": "h1", "app_resource": str(job), "spark_conf": {"spark.jars.packages": "org.example:pkg:1.0"}},
                )

    def test_env_allowlist_filtering_and_forbidden_loader_vars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs], allowed_env_vars={"SPARK_USER_OPT", "DB_PASS"})

            old_aws = os.environ.get("AWS_SECRET_ACCESS_KEY")
            try:
                os.environ["AWS_SECRET_ACCESS_KEY"] = "host_secret_val"
                sub = policy.validate_run(
                    self._spec("env-run"),
                    1,
                    {
                        "id": "h-env",
                        "app_resource": str(job),
                        "env": {
                            "SPARK_USER_OPT": "1",
                            "DB_PASS": "inline_db_secret",
                            "UNLISTED_VAR": "dropped",
                        },
                    },
                )
            finally:
                if old_aws is None:
                    os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
                else:
                    os.environ["AWS_SECRET_ACCESS_KEY"] = old_aws

            self.assertNotIn("AWS_SECRET_ACCESS_KEY", sub.env)
            self.assertNotIn("UNLISTED_VAR", sub.env)
            self.assertEqual(sub.env["SPARK_USER_OPT"], "1")
            self.assertEqual(sub.env["DB_PASS"], "inline_db_secret")
            self.assertEqual(sub.env["SPARK_RUN_ID"], "env-run")
            self.assertNotIn("inline_db_secret", json.dumps(sub.sanitized_env))

            for bad_var in ("LD_PRELOAD", "DYLD_INSERT_LIBRARIES"):
                with self.assertRaises(SandboxViolationError) as ctx:
                    policy.validate_run(
                        self._spec(),
                        1,
                        {"id": "h-bad-env", "app_resource": str(job), "env": {bad_var: "/tmp/evil.so"}},
                    )
                self.assertEqual(ctx.exception.code, "FORBIDDEN_ENV_VAR")

    def test_sandbox_violation_error_redacts_secrets(self) -> None:
        raw_sig = "deadbeefcafebabe1234567890"
        raw_pw = "superSecretDbPass999"
        with tempfile.TemporaryDirectory() as tmp:
            jobs = Path(tmp) / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")
            policy = SandboxPolicy(allowed_roots=[jobs])

            with self.assertRaises(SandboxViolationError) as ctx:
                policy.validate_run(
                    self._spec(),
                    1,
                    {
                        "id": "h-leak",
                        "app_resource": str(job),
                        "app_args": [f"s3://prod-bucket/path?X-Amz-Signature={raw_sig}&password={raw_pw}"],
                    },
                )
            combined = f"{ctx.exception} {ctx.exception.detail}"
            self.assertNotIn(raw_sig, combined)
            self.assertNotIn(raw_pw, combined)

    def test_runner_validates_before_subprocess_and_handles_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs = tmp_path / "jobs"
            jobs.mkdir()
            job = jobs / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")

            spark_home = tmp_path / "spark"
            bin_dir = spark_home / "bin"
            bin_dir.mkdir(parents=True)
            submit = bin_dir / "spark-submit"
            submit.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            os.chmod(submit, os.stat(submit).st_mode | stat.S_IXUSR)

            policy = SandboxPolicy(allowed_roots=[jobs], timeout_seconds=1.0)
            artifacts_dir = tmp_path / "artifacts"
            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=artifacts_dir,
                sandbox_policy=policy,
            )

            # Violation raises BEFORE subprocess.run or directory creation
            with patch("spark_tuning.adapters.subprocess.run") as mock_run:
                with self.assertRaises(SandboxViolationError):
                    runner.run(self._spec("blocked-run"), 1, {"id": "h-bad", "app_resource": "../../etc/passwd"})
                mock_run.assert_not_called()
            self.assertFalse((artifacts_dir / "blocked-run").exists())

            # TimeoutExpired saves sanitized partial logs and re-raises
            raw_aws = "AKIAIOSFODNN7EXAMPLE"
            timeout_exc = subprocess.TimeoutExpired(
                cmd=[str(submit)],
                timeout=1.0,
                output=f"partial out {raw_aws}".encode("utf-8"),
                stderr=b"partial err alice@example.com",
            )
            with patch("spark_tuning.adapters.subprocess.run", side_effect=timeout_exc):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.run(self._spec("timeout-run"), 1, {"id": "h-slow", "app_resource": str(job)})

            run_dir = artifacts_dir / "timeout-run" / "h-slow"
            stdout_text = (run_dir / "stdout.log").read_text(encoding="utf-8")
            stderr_text = (run_dir / "stderr.log").read_text(encoding="utf-8")
            self.assertNotIn(raw_aws, stdout_text)
            self.assertNotIn("alice@example.com", stderr_text)
            self.assertIn("[REDACTED:AWS_KEY]", stdout_text)
            self.assertIn("[REDACTED:EMAIL]", stderr_text)


if __name__ == "__main__":
    unittest.main()
