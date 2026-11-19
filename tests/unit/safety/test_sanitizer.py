"""Unit tests for Issue #54: ArtifactSanitizer hardening & RedactionReport."""

from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from spark_tuning.adapters import JsonArtifactStore, SparkSubmitExperimentRunner
from spark_tuning.models import Decision, Metrics, RunSpec, RunState
from spark_tuning.safety import ArtifactSanitizer, RedactionReport


class ArtifactSanitizerTests(unittest.TestCase):
    def test_structured_detectors_in_text(self) -> None:
        raw_jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiJzeW50aGV0aWMifQ."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        raw_bearer = "syntheticBearerToken_987654321"
        raw_basic = "c3ludGhldGljOnBhc3N3b3Jk"
        raw_gho = "gho_syntheticToken1234567890abcdefghij"
        raw_ghp = "ghp_syntheticToken1234567890abcdefghij"
        raw_pat = "github_pat_11ABCDEFG_syntheticToken1234567890"
        raw_aws = "AKIAIOSFODNN7EXAMPLE"
        raw_asia = "ASIAIOSFODNN7EXAMPLE"
        raw_gcp = "AIzaSySyntheticTestKey0000000000000000000"
        raw_azure = "FakeAzureAccountKeyValue123456=="
        raw_s3_sig = "deadbeefcafebabe1234567890"
        raw_gcs_sig = "abc123def456signature"
        raw_sas_sig = "FakeAzureSasSig999%3D"
        raw_pem_body = "MIIEowIBAAKCAQEAtestSyntheticKeyData0000000000000000000000000000"
        raw_email = "alice.synthetic@example.com"
        raw_posix = "/Users/alice/projects/spark/job.py"
        raw_linux = "/home/bob/secrets/creds.txt"
        raw_win = r"C:\Users\charlie\AppData\token.txt"

        corpus = "\n".join(
            [
                f"jwt={raw_jwt}",
                f"Authorization: Bearer {raw_bearer}",
                f"Authorization: Basic {raw_basic}",
                f"gh1={raw_gho} gh2={raw_ghp} gh3={raw_pat}",
                f"aws1={raw_aws} aws2={raw_asia}",
                f"gcp={raw_gcp}",
                f"DefaultEndpointsProtocol=https;AccountKey={raw_azure};EndpointSuffix=core.windows.net",
                f"https://example-bucket.s3.amazonaws.com/data.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature={raw_s3_sig}",
                f"https://storage.googleapis.com/example-bucket/obj?X-Goog-Signature={raw_gcs_sig}",
                f"https://example.blob.core.windows.net/c/b?sv=2022-11-02&sig={raw_sas_sig}",
                "PEM_KEY=-----BEGIN RSA PRIVATE KEY-----",
                raw_pem_body,
                "-----END RSA PRIVATE KEY-----",
                f"contact={raw_email}",
                f"mac={raw_posix} linux={raw_linux} win={raw_win}",
                '{"spark.custom.secret":"opaqueSecretValue42"}',
                "hypothesis=repartition-by-key failed because key skew remained",
            ]
        )

        sanitized = ArtifactSanitizer.sanitize_text(corpus)

        for secret in (
            raw_jwt,
            raw_bearer,
            raw_basic,
            raw_gho,
            raw_ghp,
            raw_pat,
            raw_aws,
            raw_asia,
            raw_gcp,
            raw_azure,
            raw_s3_sig,
            raw_gcs_sig,
            raw_sas_sig,
            raw_pem_body,
            raw_email,
            "/Users/alice/",
            "/home/bob/",
            r"C:\Users\charlie",
            "opaqueSecretValue42",
        ):
            self.assertNotIn(secret, sanitized, f"Secret leaked in output: {secret}")

        # Signed URL host/bucket and normal prose remain intact
        self.assertIn("example-bucket.s3.amazonaws.com/data.parquet?[REDACTED:SIGNED_URL]", sanitized)
        self.assertIn("storage.googleapis.com/example-bucket/obj?[REDACTED:SIGNED_URL]", sanitized)
        self.assertIn("repartition-by-key failed because key skew remained", sanitized)
        self.assertIn('{"spark.custom.secret":"[REDACTED:SECRET]"}', sanitized)

    def test_no_double_redaction_and_report_invariant(self) -> None:
        raw_jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiIxMjMifQ."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        payload = {
            "line": (
                f"token={raw_jwt} "
                f"Authorization: Bearer {raw_jwt} "
                "AWS_KEY=AKIAIOSFODNN7EXAMPLE "
                "PEM_KEY=-----BEGIN PRIVATE KEY-----\n"
                "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSj\n"
                "-----END PRIVATE KEY-----"
            )
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(payload)
        self.assertIsInstance(report, RedactionReport)
        self.assertEqual(sum(report.categories.values()), report.total_redactions)
        self.assertGreaterEqual(report.total_redactions, 4)
        self.assertIn("pem_key", report.categories)
        self.assertIn("jwt", report.categories)
        self.assertIn("aws_key", report.categories)
        self.assertNotIn("[REDACTED:SECRET]", sanitized["line"])

    def test_recursive_dict_list_tuple_and_scalar_preservation(self) -> None:
        payload = {
            "env": [{"Authorization": "Bearer secret_bearer_token_12345"}],
            "args": ("--email", "ops.synthetic@example.com"),
            "nested": (["ok", {"api_key": "opaque_key_999"}],),
            "counts": (1, 2.5, True, False, None, ""),
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(payload)

        self.assertIsInstance(sanitized["env"], list)
        self.assertIsInstance(sanitized["args"], tuple)
        self.assertIsInstance(sanitized["nested"], tuple)
        self.assertIsInstance(sanitized["nested"][0], list)
        self.assertEqual(sanitized["counts"], (1, 2.5, True, False, None, ""))
        self.assertEqual(sum(report.categories.values()), report.total_redactions)
        self.assertGreaterEqual(report.total_redactions, 3)
        self.assertIn("env[0].Authorization", report.redacted_paths)
        self.assertIn("args[1]", report.redacted_paths)
        self.assertIn("nested[0][1].api_key", report.redacted_paths)

        for clean in ("", {}, [], (), None, 42, True, 3.14):
            clean_out, clean_rep = ArtifactSanitizer.sanitize_with_report(clean)
            self.assertEqual(clean_out, clean)
            self.assertEqual(clean_rep.total_redactions, 0)
            self.assertEqual(clean_rep.redacted_paths, ())
            self.assertEqual(clean_rep.categories, {})

    def test_cyclic_object_graphs_terminate_and_dag_reports_all_paths(self) -> None:
        secret = "gho_cycleTestSecret1234567890abcdef"
        d: dict = {"token": secret}
        d["self"] = d

        lst: list = [secret]
        lst.append(lst)
        d["lst"] = lst

        sanitized, report = ArtifactSanitizer.sanitize_with_report(d)
        self.assertIs(sanitized["self"], sanitized)
        self.assertIs(sanitized["lst"][1], sanitized["lst"])
        self.assertNotIn(secret, repr(sanitized))
        self.assertGreaterEqual(report.total_redactions, 2)

        shared = {"token": secret}
        dag = {"first": shared, "second": shared}
        dag_out, dag_report = ArtifactSanitizer.sanitize_with_report(dag)
        self.assertIn("first.token", dag_report.redacted_paths)
        self.assertIn("second.token", dag_report.redacted_paths)
        self.assertEqual(dag_report.total_redactions, 2)
        self.assertNotIn(secret, json.dumps(dag_out))

    def test_sensitive_keys_and_allowlist_semantics(self) -> None:
        payload = {
            "X-Api-Key": "val1",
            "SECRET_VALUE": "val2",
            "PassWord": "val3",
            "Access_Token": "val4",
            "privateKey": "val5",
            "auth": {"jwt": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sigValueHere1234"},
            "author": "benign-author-name",
            "bypass_cache": True,
            "spark.sql.shuffle.partitions": "200",
            "spark.app.name": "job-with-AKIAIOSFODNN7EXAMPLE",
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(
            payload,
            allowlist={"spark.sql.shuffle.partitions", "spark.app.name"},
        )

        for secret in ("val1", "val2", "val3", "val4", "val5", "AKIAIOSFODNN7EXAMPLE", "eyJhbGciOi"):
            self.assertNotIn(secret, json.dumps(sanitized))

        self.assertEqual(sanitized["author"], "benign-author-name")
        self.assertTrue(sanitized["bypass_cache"])
        self.assertEqual(sanitized["spark.sql.shuffle.partitions"], "200")
        self.assertIn("[REDACTED:AWS_KEY]", sanitized["spark.app.name"])
        self.assertIn("auth.jwt", report.redacted_paths)
        self.assertEqual(sum(report.categories.values()), report.total_redactions)

    def test_secret_in_dict_key_does_not_leak_into_report_paths(self) -> None:
        raw_email = "leak-key@example.com"
        payload = {f"user_{raw_email}": "AKIAIOSFODNN7EXAMPLE"}
        sanitized, report = ArtifactSanitizer.sanitize_with_report(payload)
        self.assertNotIn(raw_email, json.dumps(sanitized))
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", json.dumps(sanitized))
        for path in report.redacted_paths:
            self.assertNotIn(raw_email, path)

    def test_enum_path_and_json_artifact_store_integration(self) -> None:
        raw_secret = "gho_storeTestSecret1234567890abcdef"
        payload = {
            "run_id": "run-enum",
            "state": RunState.PR_READY,
            "decision": Decision.ACCEPT,
            "path": Path("/Users/alice/work/job.py"),
            "reason": "Selected repartition-by-key",
            "hypothesis": {"env": {"DB_PASS": raw_secret}},
        }
        original_env_val = payload["hypothesis"]["env"]["DB_PASS"]

        with tempfile.TemporaryDirectory() as tmp:
            store = JsonArtifactStore(tmp)
            store.save("run-enum", payload)
            # Caller dict was not mutated
            self.assertEqual(payload["hypothesis"]["env"]["DB_PASS"], original_env_val)

            raw_text = (Path(tmp) / "run-enum.json").read_text(encoding="utf-8")
            self.assertNotIn(raw_secret, raw_text)
            self.assertNotIn("/Users/alice/", raw_text)
            loaded = json.loads(raw_text)
            self.assertEqual(loaded["state"], "pr_ready")
            self.assertEqual(loaded["decision"], "accept")
            self.assertEqual(loaded["reason"], "Selected repartition-by-key")

    def test_spark_submit_runner_sanitizes_metadata_logs_and_errors(self) -> None:
        raw_aws = "AKIAIOSFODNN7EXAMPLE"
        raw_email = "runner.secret@example.com"
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            jobs_dir = tmp_path / "jobs"
            jobs_dir.mkdir()
            job = jobs_dir / "job.py"
            job.write_text("print('ok')\n", encoding="utf-8")

            spark_home = tmp_path / "spark"
            bin_dir = spark_home / "bin"
            bin_dir.mkdir(parents=True)
            submit = bin_dir / "spark-submit"
            submit.write_text(
                "#!/bin/sh\n"
                f'echo "stdout secret={raw_aws}"\n'
                f'echo "stderr contact={raw_email}" >&2\n'
                "exit 1\n",
                encoding="utf-8",
            )
            os.chmod(submit, os.stat(submit).st_mode | stat.S_IXUSR)

            runner = SparkSubmitExperimentRunner(
                spark_home=str(spark_home),
                artifacts_dir=tmp_path / "artifacts",
            )
            spec = RunSpec(
                run_id="san-runner",
                skill="partition-sizing",
                objective="test",
                baseline=Metrics(duration_seconds=100.0, cost_usd=5.0),
            )
            with self.assertRaises(RuntimeError) as ctx:
                runner.run(
                    spec,
                    1,
                    {
                        "id": "h-fail",
                        "app_resource": str(job),
                        "app_args": ["--owner", raw_email],
                    },
                )
            self.assertNotIn(raw_email, str(ctx.exception))

            run_dir = tmp_path / "artifacts" / "san-runner" / "h-fail"
            meta_text = (run_dir / "run_metadata.json").read_text(encoding="utf-8")
            stdout_text = (run_dir / "stdout.log").read_text(encoding="utf-8")
            stderr_text = (run_dir / "stderr.log").read_text(encoding="utf-8")
            self.assertNotIn(raw_email, meta_text)
            self.assertNotIn(raw_aws, stdout_text)
            self.assertNotIn(raw_email, stderr_text)
            self.assertIn("redaction_report", json.loads(meta_text))


if __name__ == "__main__":
    unittest.main()
