"""Shared opaque-box test utilities for the spark-tuning E2E suite."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

from spark_tuning.adapters import FixtureExperimentRunner
from spark_tuning.models import Metrics, RunSpec

REPO_ROOT = Path(__file__).resolve().parent.parent
E2E_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = E2E_DIR / "fixtures"
SPECS_DIR = FIXTURES_DIR / "specs"
EVENT_LOGS_DIR = FIXTURES_DIR / "event_logs"
SECRETS_DIR = FIXTURES_DIR / "secrets"


def make_temp_spark_submit(
    tmp_dir: str | Path,
    *,
    event_log_lines: Sequence[str] | str = (),
    exit_code: int = 0,
    sleep_seconds: float = 0.0,
    stdout: str = "",
    stderr: str = "",
    record_env_path: str | Path | None = None,
    record_args_path: str | Path | None = None,
    spark_home_name: str = "spark_home",
) -> Path:
    """Create an executable mock spark-submit inside a temporary SPARK_HOME.

    The generated script inspects `--conf spark.eventLog.dir=...` from argv,
    optionally records received environment/arguments, writes stdout/stderr,
    emits deterministic event log lines, and exits with `exit_code`.
    Returns the temporary `SPARK_HOME` Path.
    """
    spark_home = Path(tmp_dir) / spark_home_name
    bin_dir = spark_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    submit_path = bin_dir / "spark-submit"

    if isinstance(event_log_lines, str):
        log_payload = event_log_lines
    else:
        log_payload = "\n".join(event_log_lines) + ("\n" if event_log_lines else "")

    payload_file = spark_home / "_mock_event_log.jsonl"
    payload_file.write_text(log_payload, encoding="utf-8")

    config = {
        "payload_file": str(payload_file),
        "exit_code": int(exit_code),
        "sleep_seconds": float(sleep_seconds),
        "stdout": stdout,
        "stderr": stderr,
        "record_env_path": str(record_env_path) if record_env_path else None,
        "record_args_path": str(record_args_path) if record_args_path else None,
    }
    config_file = spark_home / "_mock_config.json"
    config_file.write_text(json.dumps(config, indent=2), encoding="utf-8")

    script = f"""#!{sys.executable}
import json
import os
import sys
import time
from pathlib import Path

cfg = json.loads(Path({repr(str(config_file))}).read_text(encoding="utf-8"))

if cfg["record_env_path"]:
    Path(cfg["record_env_path"]).write_text(
        json.dumps(dict(os.environ), indent=2), encoding="utf-8"
    )

if cfg["record_args_path"]:
    Path(cfg["record_args_path"]).write_text(
        json.dumps(sys.argv, indent=2), encoding="utf-8"
    )

if cfg["sleep_seconds"] > 0:
    time.sleep(cfg["sleep_seconds"])

event_log_dir = None
argv = sys.argv[1:]
for idx, arg in enumerate(argv):
    if arg.startswith("--conf="):
        kv = arg.split("=", 1)[1]
        if kv.startswith("spark.eventLog.dir="):
            event_log_dir = kv.split("=", 1)[1]
    elif arg == "--conf" and idx + 1 < len(argv):
        kv = argv[idx + 1]
        if kv.startswith("spark.eventLog.dir="):
            event_log_dir = kv.split("=", 1)[1]
    elif arg.startswith("spark.eventLog.dir="):
        event_log_dir = arg.split("=", 1)[1]

payload = Path(cfg["payload_file"]).read_text(encoding="utf-8")
if event_log_dir and payload:
    out_dir = Path(event_log_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "app-e2e-mock.jsonl").write_text(payload, encoding="utf-8")

if cfg["stdout"]:
    sys.stdout.write(cfg["stdout"])
    sys.stdout.flush()

if cfg["stderr"]:
    sys.stderr.write(cfg["stderr"])
    sys.stderr.flush()

raise SystemExit(cfg["exit_code"])
"""
    submit_path.write_text(script, encoding="utf-8")
    submit_path.chmod(submit_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return spark_home


def _flatten_strings(obj: Any, seen: set[int] | None = None) -> list[str]:
    if obj is None:
        return []
    if isinstance(obj, str):
        return [obj]
    if isinstance(obj, Path):
        if obj.exists() and obj.is_file():
            return [obj.read_text(encoding="utf-8", errors="replace")]
        return [str(obj)]
    if seen is None:
        seen = set()
    if isinstance(obj, dict):
        obj_id = id(obj)
        if obj_id in seen:
            return []
        seen.add(obj_id)
        parts: list[str] = []
        for k, v in obj.items():
            parts.extend(_flatten_strings(k, seen))
            parts.extend(_flatten_strings(v, seen))
        return parts
    if isinstance(obj, (list, tuple, set)):
        obj_id = id(obj)
        if obj_id in seen:
            return []
        seen.add(obj_id)
        parts = []
        for item in obj:
            parts.extend(_flatten_strings(item, seen))
        return parts
    return [str(obj)]


def assert_no_raw_secrets(
    testcase: Any,
    text_or_obj: Any,
    secrets: Iterable[str],
) -> None:
    """Assert that none of the raw secret strings appear in `text_or_obj`."""
    haystack = "\n".join(_flatten_strings(text_or_obj))
    for secret in secrets:
        testcase.assertNotIn(
            secret,
            haystack,
            f"Raw secret leaked in output/artifact: {secret!r}",
        )


def run_cli(
    args: Sequence[str],
    *,
    cwd: str | Path = REPO_ROOT,
    env: dict[str, str] | None = None,
    timeout: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    """Run `python3 -m spark_tuning.cli ...` in an isolated subprocess."""
    proc_env = os.environ.copy() if env is None else dict(env)
    src_path = str(REPO_ROOT / "src")
    existing_pp = proc_env.get("PYTHONPATH", "")
    proc_env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{existing_pp}" if existing_pp else src_path
    )
    return subprocess.run(
        [sys.executable, "-m", "spark_tuning.cli", *args],
        cwd=str(cwd),
        env=proc_env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


class SimulatedProcessCrash(BaseException):
    """Uncatchable crash sentinel for simulating mid-loop process kills."""


class FaultInjectingRunner:
    """ExperimentRunner wrapper recording calls and injecting deterministic crashes."""

    def __init__(
        self,
        inner: Any = None,
        *,
        fail_on_calls: set[int] | None = None,
        fail_on_hypothesis_ids: set[str] | None = None,
        exception_factory: Any = None,
        fail_after_delegate: bool = False,
    ) -> None:
        self.inner = inner or FixtureExperimentRunner()
        self.fail_on_calls = set(fail_on_calls or ())
        self.fail_on_hypothesis_ids = set(fail_on_hypothesis_ids or ())
        self.exception_factory = exception_factory or (
            lambda call_idx, hyp_id: RuntimeError(
                f"Injected runner crash at call={call_idx}, hypothesis={hyp_id}"
            )
        )
        self.fail_after_delegate = fail_after_delegate
        self.calls: list[tuple[str, int, str]] = []
        self._call_count = 0

    def run(self, spec: RunSpec, iteration: int, hypothesis: dict[str, Any]) -> Metrics:
        self._call_count += 1
        hyp_id = str(hypothesis.get("id", f"iter_{iteration}"))
        self.calls.append((spec.run_id, iteration, hyp_id))

        should_fail = (
            self._call_count in self.fail_on_calls
            or hyp_id in self.fail_on_hypothesis_ids
        )
        if should_fail and not self.fail_after_delegate:
            raise self.exception_factory(self._call_count, hyp_id)

        metrics = self.inner.run(spec, iteration, hypothesis)
        if should_fail and self.fail_after_delegate:
            raise self.exception_factory(self._call_count, hyp_id)
        return metrics


def get_parse_diagnostics(metrics: Metrics) -> Any:
    """Retrieve public ParseDiagnostics attached to parsed Metrics."""
    return getattr(metrics, "parse_diagnostics", None) or getattr(
        metrics, "diagnostics", None
    )


def get_unassessed_guardrails(evaluation: Any) -> tuple[str, ...]:
    """Retrieve public unassessed_guardrails from an Evaluation."""
    val = getattr(evaluation, "unassessed_guardrails", None)
    if val is None:
        raise AssertionError(
            "Evaluation missing required 'unassessed_guardrails' attribute per PROJECT.md"
        )
    return tuple(val)
