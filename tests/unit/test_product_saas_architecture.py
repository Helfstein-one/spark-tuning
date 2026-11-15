"""Unit tests for product SaaS architecture: Scanner, Daemon, Knowledge Engine and Telemetry."""

import tempfile
import unittest
from pathlib import Path
from spark_tuning.daemon.scanner import MultiTenantRepoScanner, TargetRepository
from spark_tuning.daemon.worker import AutonomousTuningDaemon, TuningEvent
from spark_tuning.knowledge.engine import CrossRepoKnowledgeEngine
from spark_tuning.reporting.telemetry import TelemetryControlPlane


class ProductSaaSArchitectureTests(unittest.TestCase):
    def test_multi_tenant_scanner(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_path = Path(tmp_dir) / "client_repo"
            repo_path.mkdir()
            (repo_path / "job.py").write_text("from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()")
            (repo_path / "query.sql").write_text("SELECT count(*) FROM table")
            (repo_path / "spark-defaults.conf").write_text("spark.executor.memory 4g")

            scanner = MultiTenantRepoScanner()
            repo = TargetRepository(tenant_id="client_corp_1", repo_url="https://github.com/client/repo.git")
            artifacts = scanner.scan_local_target(repo, repo_path)

            self.assertIn("job.py", artifacts.pyspark_files)
            self.assertIn("query.sql", artifacts.sql_files)
            self.assertIn("spark-defaults.conf", artifacts.spark_conf_files)

    def test_cross_repo_knowledge_sharing(self):
        engine = CrossRepoKnowledgeEngine()
        code_sample = "df.groupBy('user_id').agg(count('transaction'))"

        # Repo A learns and registers successful tuning pattern
        engine.register_success(code_sample, skill_name="partition-sizing", speedup_ratio=0.75, source_repo="repo_alpha")

        # Repo B queries for identical code transformation
        known = engine.query_prior_knowledge(code_sample)
        self.assertIsNotNone(known)
        self.assertEqual(known.target_skill, "partition-sizing")
        self.assertEqual(known.discovered_in_repo, "repo_alpha")

    def test_autonomous_daemon_priority_queue(self):
        daemon = AutonomousTuningDaemon()
        repo1 = TargetRepository(tenant_id="tenant_1", repo_url="url1")
        repo2 = TargetRepository(tenant_id="tenant_2", repo_url="url2")

        # Low priority (10) vs High priority (1)
        daemon.dispatch_event(TuningEvent(event_id="e_low", event_type="schedule", repository=repo1, priority=10))
        daemon.dispatch_event(TuningEvent(event_id="e_urgent", event_type="pr_opened", repository=repo2, priority=1))

        # First processed must be e_urgent
        processed = daemon.process_next_sync()
        self.assertIsNotNone(processed)
        self.assertEqual(processed.event_id, "e_urgent")

    def test_telemetry_control_plane_roi(self):
        plane = TelemetryControlPlane()
        plane.record_tuning_run(
            tenant_id="fintech_tenant",
            baseline_cost_usd=100.0,
            tuned_cost_usd=25.0,
            baseline_duration_sec=7200.0,
            tuned_duration_sec=1800.0,
            data_volume_gb=2048.0,
        )

        roi = plane.get_tenant_roi("fintech_tenant")
        self.assertEqual(roi.total_jobs_tuned, 1)
        self.assertEqual(roi.total_dollars_saved_usd, 75.0)
        self.assertEqual(roi.total_hours_saved, 1.5)
        self.assertEqual(roi.total_terabytes_optimized, 2.0)
        self.assertEqual(roi.overall_roi_multiplier, 3.0)  # $75 saved / $25 incurred


if __name__ == "__main__":
    unittest.main()
