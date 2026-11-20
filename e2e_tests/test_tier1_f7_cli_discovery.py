"""Tier 1 E2E Tests — F7: CLI, Spec Contract, Repo Gate & tests/ Discovery."""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from e2e_tests.helpers import REPO_ROOT, run_cli


class TestTier1F7CliDiscovery(unittest.TestCase):
    """T1-F7-01..05: Bare test discovery, CLI exit codes, validate_loop.sh, CI JSON gate."""

    def test_t1_f7_01_bare_unittest_discover_tests_without_pythonpath(self) -> None:
        """T1-F7-01: `python3 -m unittest discover -s tests` works without PYTHONPATH=src."""
        clean_env = os.environ.copy()
        clean_env.pop("PYTHONPATH", None)
        proc = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-q"],
            cwd=str(REPO_ROOT),
            env=clean_env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"Bare unittest discovery failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )

    def test_t1_f7_02_cli_run_exits_zero_on_accept_and_writes_artifact(self) -> None:
        """T1-F7-02: `spark_tuning.cli run examples/runs/demo.json` exits 0 and writes artifact JSON."""
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(["run", "examples/runs/demo.json", "--artifacts", tmp])
            self.assertEqual(
                proc.returncode,
                0,
                f"Expected exit 0, got {proc.returncode}:\nSTDERR:\n{proc.stderr}",
            )
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["run_id"], "demo-join-strategy")
            self.assertEqual(payload["decision"], "accept")

            artifact_path = Path(tmp) / "demo-join-strategy.json"
            self.assertTrue(artifact_path.exists())
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(artifact["run_id"], "demo-join-strategy")

    def test_t1_f7_03_cli_run_exits_two_on_non_accept_and_persists_artifact(self) -> None:
        """T1-F7-03: `spark_tuning.cli run` exits 2 on non-accept decision and writes artifact."""
        spec_payload = {
            "run_id": "t1-f7-03-blocked",
            "skill": "partition-sizing",
            "objective": "reject insufficient improvement",
            "baseline": {
                "duration_seconds": 100.0,
                "cost_usd": 10.0,
                "spill_disk_bytes": 1000,
                "task_p95_seconds": 10.0,
            },
            "hypotheses": [
                {
                    "id": "no-gain",
                    "metrics": {
                        "duration_seconds": 99.0,
                        "cost_usd": 9.9,
                        "spill_disk_bytes": 1000,
                        "task_p95_seconds": 10.0,
                    },
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "spec.json"
            spec_path.write_text(json.dumps(spec_payload), encoding="utf-8")
            artifacts_dir = Path(tmp) / "artifacts"

            proc = run_cli(["run", str(spec_path), "--artifacts", str(artifacts_dir)])
            self.assertEqual(proc.returncode, 2, f"Expected exit 2, got {proc.returncode}")
            out = json.loads(proc.stdout)
            self.assertNotEqual(out["decision"], "accept")
            self.assertTrue((artifacts_dir / "t1-f7-03-blocked.json").exists())

    def test_t1_f7_04_validate_loop_script_exits_zero(self) -> None:
        """T1-F7-04: `bash scripts/validate_loop.sh` exits 0 with `loop contract valid`."""
        proc = subprocess.run(
            ["bash", "scripts/validate_loop.sh"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"validate_loop.sh failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertIn("loop contract valid", proc.stdout)

    def test_t1_f7_05_ci_json_schema_gate_parses_all_repo_json_files(self) -> None:
        """T1-F7-05: All **/*.json files across repo (including e2e_tests/) parse cleanly."""
        json_files = sorted(glob.glob(str(REPO_ROOT / "**" / "*.json"), recursive=True))
        self.assertGreater(len(json_files), 0)
        for path in json_files:
            with open(path, "r", encoding="utf-8") as fh:
                json.load(fh)


if __name__ == "__main__":
    unittest.main()
