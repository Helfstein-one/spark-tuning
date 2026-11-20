"""Tier 1 E2E Tests — F3 (#54): Structured Secret & PII Sanitizer."""

from __future__ import annotations

import unittest

from spark_tuning.safety.sanitizer import ArtifactSanitizer
from e2e_tests.helpers import SECRETS_DIR, assert_no_raw_secrets


class TestTier1F3Sanitizer(unittest.TestCase):
    """T1-F3-01..05: Structured secret detection, signed URLs/PEMs, recursive traversal, allowlist, report."""

    def test_t1_f3_01_redact_structured_tokens_and_auth_headers_in_text(self) -> None:
        """T1-F3-01: JWT, Bearer, Basic, GitHub, and AWS tokens are replaced with typed redaction markers."""
        raw_jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IlRlc3QifQ."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        raw_bearer = "synthetic_bearer_token_abcdef123456"
        raw_basic = "c3ludGhldGljOnBhc3N3b3Jk"
        raw_gh = "gho_exampleSyntheticToken1234567890abcdef"
        raw_aws = "AKIAIOSFODNN7EXAMPLE"
        text = (
            f"jwt={raw_jwt} Authorization: Bearer {raw_bearer} "
            f"Authorization: Basic {raw_basic} gh={raw_gh} aws={raw_aws}"
        )

        sanitized = ArtifactSanitizer.sanitize_text(text)
        assert_no_raw_secrets(
            self,
            sanitized,
            [raw_jwt, raw_bearer, raw_basic, raw_gh, raw_aws],
        )
        self.assertIn("REDACTED", sanitized)

    def test_t1_f3_02_redact_signed_cloud_urls_and_pem_private_keys(self) -> None:
        """T1-F3-02: Signed S3/GCS/Azure SAS URLs and PEM private keys are redacted cleanly."""
        payload_text = (SECRETS_DIR / "anonymized_payload.txt").read_text(encoding="utf-8")
        sanitized = ArtifactSanitizer.sanitize_text(payload_text)

        assert_no_raw_secrets(
            self,
            sanitized,
            [
                "deadbeefcafebabe1234567890",
                "abc123def456",
                "FakeAzureSasSignatureValue12345%3D",
                "MIIEowIBAAKCAQEAtestSyntheticKeyData0000000000000000000000000000",
                "AKIAIOSFODNN7EXAMPLE",
                "gho_exampleSyntheticToken1234567890abcdef",
            ],
        )
        self.assertIn("example-bucket.s3.amazonaws.com", sanitized)
        self.assertIn("REDACTED", sanitized)

    def test_t1_f3_03_recursive_sanitization_of_nested_dict_list_tuple(self) -> None:
        """T1-F3-03: Nested dict/list/tuple payloads redact secrets, email PII, and local user paths."""
        payload = {
            "env": [{"Authorization": "Bearer super_secret_bearer_xyz"}],
            "args": ("--email", "alice.synthetic@example.com"),
            "path": "/Users/testuser/projects/spark-job/secret_runner.py",
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(payload)

        assert_no_raw_secrets(
            self,
            sanitized,
            [
                "super_secret_bearer_xyz",
                "alice.synthetic@example.com",
                "/Users/testuser/",
            ],
        )
        self.assertIsInstance(sanitized["args"], tuple)
        self.assertIsInstance(sanitized["env"], list)
        self.assertGreaterEqual(report.total_redactions, 3)

    def test_t1_f3_04_safe_allowlist_preserves_benign_spark_config_keys(self) -> None:
        """T1-F3-04: Safe allowlist preserves benign Spark keys while still redacting non-allowlisted secrets."""
        payload = {
            "spark.sql.shuffle.partitions": "200",
            "spark.app.name": "etl-job",
            "api_token": "tok_secret_value_12345",
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(
            payload,
            allowlist={"spark.sql.shuffle.partitions", "spark.app.name"},
        )
        self.assertEqual(sanitized["spark.sql.shuffle.partitions"], "200")
        self.assertEqual(sanitized["spark.app.name"], "etl-job")
        assert_no_raw_secrets(self, sanitized, ["tok_secret_value_12345"])
        self.assertGreaterEqual(report.total_redactions, 1)

        # Backward-compatible sanitize_dict still functions
        compat = ArtifactSanitizer.sanitize_dict(payload)
        self.assertEqual(compat["spark.sql.shuffle.partitions"], "200")
        assert_no_raw_secrets(self, compat, ["tok_secret_value_12345"])

    def test_t1_f3_05_sanitize_with_report_returns_accurate_redaction_report(self) -> None:
        """T1-F3-05: sanitize_with_report returns accurate total_redactions, redacted_paths, and categories."""
        from spark_tuning.safety.sanitizer import RedactionReport

        raw_jwt = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiIxMjM0NSJ9."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        payload = {
            "auth": {"jwt": raw_jwt},
            "owner_email": "ops@example.com",
            "aws": "AKIAIOSFODNN7EXAMPLE",
        }
        sanitized, report = ArtifactSanitizer.sanitize_with_report(payload)

        self.assertIsInstance(report, RedactionReport)
        self.assertGreaterEqual(report.total_redactions, 3)
        self.assertGreaterEqual(len(report.redacted_paths), 3)
        self.assertIsInstance(report.categories, dict)
        self.assertEqual(sum(report.categories.values()), report.total_redactions)
        assert_no_raw_secrets(
            self,
            sanitized,
            [raw_jwt, "ops@example.com", "AKIAIOSFODNN7EXAMPLE"],
        )


if __name__ == "__main__":
    unittest.main()
