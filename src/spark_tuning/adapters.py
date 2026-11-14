"""Event log parser and Spark runner adapters."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .models import Metrics, RunSpec


class JsonArtifactStore:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(self, run_id: str, payload: dict) -> None:
        target = self.directory / f"{run_id}.json"
        target.write_text(json.dumps(payload, indent=2, default=str) + "\n")


class SparkEventLogParser:
    """Parses JSON-lines Spark event logs to extract execution metrics without turning missing metrics into zeros."""

    @classmethod
    def parse_file(cls, log_path: Path | str, cost_per_second: float = 0.0) -> Metrics:
        path = Path(log_path)
        if not path.exists():
            raise FileNotFoundError(f"Event log file not found: {path}")

        app_start_time: int | None = None
        app_end_time: int | None = None
        stages_count = 0
        task_durations_ms: list[int] = []
        task_retries = 0
        failures = 0

        # Optional task metrics accumulators (None if not present in any task)
        has_task_metrics = False
        gc_time_ms_total = 0
        shuffle_write_bytes_total = 0
        shuffle_read_bytes_total = 0
        bytes_read_total = 0
        bytes_written_total = 0
        spill_disk_bytes_total = 0
        spill_mem_bytes_total = 0

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                event_type = event.get("Event")

                if event_type == "SparkListenerApplicationStart":
                    app_start_time = event.get("Timestamp")
                elif event_type == "SparkListenerApplicationEnd":
                    app_end_time = event.get("Timestamp")
                elif event_type == "SparkListenerStageCompleted":
                    stages_count += 1
                    stage_info = event.get("Stage Info", {})
                    if stage_info.get("Failure Reason"):
                        failures += 1
                elif event_type == "SparkListenerTaskEnd":
                    task_info = event.get("Task Info", {})
                    if task_info.get("Attempt", 0) > 0:
                        task_retries += 1
                    if task_info.get("Failed", False):
                        failures += 1

                    launch_time = task_info.get("Launch Time")
                    finish_time = task_info.get("Finish Time")
                    if launch_time and finish_time and finish_time >= launch_time:
                        task_durations_ms.append(finish_time - launch_time)

                    task_metrics = event.get("Task Metrics")
                    if task_metrics:
                        has_task_metrics = True
                        gc_time_ms_total += task_metrics.get("JVM GC Time", 0)
                        spill_disk_bytes_total += task_metrics.get("Disk Bytes Spilled", 0)
                        spill_mem_bytes_total += task_metrics.get("Memory Bytes Spilled", 0)

                        in_metrics = task_metrics.get("Input Metrics", {})
                        bytes_read_total += in_metrics.get("Bytes Read", 0)

                        out_metrics = task_metrics.get("Output Metrics", {})
                        bytes_written_total += out_metrics.get("Bytes Written", 0)

                        shuf_read = task_metrics.get("Shuffle Read Metrics", {})
                        shuffle_read_bytes_total += shuf_read.get("Total Bytes Read", 0)

                        shuf_write = task_metrics.get("Shuffle Write Metrics", {})
                        shuffle_write_bytes_total += shuf_write.get("Shuffle Bytes Written", 0)

        duration_seconds = 0.0
        if app_start_time and app_end_time:
            duration_seconds = max(0.0, (app_end_time - app_start_time) / 1000.0)

        task_p95_seconds: float | None = None
        if task_durations_ms:
            task_durations_ms.sort()
            idx = int(0.95 * (len(task_durations_ms) - 1))
            task_p95_seconds = task_durations_ms[idx] / 1000.0

        cost_usd = round(duration_seconds * cost_per_second, 4)

        return Metrics(
            duration_seconds=duration_seconds,
            cost_usd=cost_usd,
            shuffle_write_bytes=shuffle_write_bytes_total if has_task_metrics else None,
            spill_disk_bytes=spill_disk_bytes_total if has_task_metrics else None,
            gc_seconds=round(gc_time_ms_total / 1000.0, 4) if has_task_metrics else None,
            task_p95_seconds=task_p95_seconds,
            failures=failures,
            correctness=(failures == 0),
            stages_count=stages_count,
            tasks_count=len(task_durations_ms),
            task_retries=task_retries,
            bytes_read=bytes_read_total if has_task_metrics else None,
            bytes_written=bytes_written_total if has_task_metrics else None,
            shuffle_read_bytes=shuffle_read_bytes_total if has_task_metrics else None,
            spill_memory_bytes=spill_mem_bytes_total if has_task_metrics else None,
        )


class FixtureExperimentRunner:
    """Deterministic runner for CI and local loop development."""

    def run(self, spec: RunSpec, iteration: int, hypothesis: dict) -> Metrics:
        fixture = hypothesis.get("metrics")
        if not fixture:
            event_log = hypothesis.get("event_log")
            if event_log:
                return SparkEventLogParser.parse_file(event_log)
            raise ValueError(f"hypothesis {hypothesis.get('id', iteration)} has no metrics or event_log fixture")
        return Metrics(**fixture)


class SparkSubmitExperimentRunner:
    """Production ExperimentRunner executing spark-submit, tracking metadata and extracting metrics."""

    def __init__(
        self,
        spark_home: str | None = None,
        artifacts_dir: str | Path = "./artifacts",
        cost_per_second: float = 0.0,
    ):
        self.spark_home = spark_home or os.environ.get("SPARK_HOME")
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.cost_per_second = cost_per_second

    def _resolve_spark_submit(self) -> str:
        if self.spark_home:
            bin_path = Path(self.spark_home) / "bin" / "spark-submit"
            if bin_path.exists():
                return str(bin_path.resolve())
        system_submit = shutil.which("spark-submit")
        if system_submit:
            return system_submit
        raise FileNotFoundError(
            "spark-submit executable not found. Configure SPARK_HOME or add spark-submit to PATH."
        )

    def _get_git_commit(self) -> str | None:
        try:
            res = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return res.stdout.strip()
        except (subprocess.SubprocessError, OSError):
            return None

    def run(self, spec: RunSpec, iteration: int, hypothesis: dict) -> Metrics:
        hypothesis_id = hypothesis.get("id", f"iter_{iteration}")
        run_artifacts_dir = self.artifacts_dir / spec.run_id / hypothesis_id
        run_artifacts_dir.mkdir(parents=True, exist_ok=True)
        event_log_dir = run_artifacts_dir / "event_logs"
        event_log_dir.mkdir(parents=True, exist_ok=True)

        app_resource = hypothesis.get("app_resource") or hypothesis.get("script")
        if not app_resource:
            raise ValueError(f"Hypothesis {hypothesis_id} requires 'app_resource' or 'script'")

        spark_submit_bin = self._resolve_spark_submit()
        master = hypothesis.get("master", "local[*]")
        deploy_mode = hypothesis.get("deploy_mode", "client")

        cmd = [spark_submit_bin, "--master", master, "--deploy-mode", deploy_mode]

        # Enforce event logging
        spark_conf = dict(hypothesis.get("spark_conf", {}))
        spark_conf["spark.eventLog.enabled"] = "true"
        spark_conf["spark.eventLog.dir"] = str(event_log_dir.resolve())

        for k, v in spark_conf.items():
            cmd.extend(["--conf", f"{k}={v}"])

        main_class = hypothesis.get("main_class")
        if main_class:
            cmd.extend(["--class", main_class])

        cmd.append(str(app_resource))
        app_args = hypothesis.get("app_args", [])
        cmd.extend([str(a) for a in app_args])

        env = os.environ.copy()
        env.update(hypothesis.get("env", {}))
        env["SPARK_RUN_ID"] = spec.run_id

        # Record command execution metadata artifact
        run_metadata = {
            "run_id": spec.run_id,
            "iteration": iteration,
            "hypothesis_id": hypothesis_id,
            "command": cmd,
            "environment": hypothesis.get("env", {}),
            "git_commit": self._get_git_commit(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        (run_artifacts_dir / "run_metadata.json").write_text(json.dumps(run_metadata, indent=2))

        timeout = hypothesis.get("timeout_seconds")
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout,
            check=False,
        )

        # Save stdout/stderr output
        (run_artifacts_dir / "stdout.log").write_text(process.stdout)
        (run_artifacts_dir / "stderr.log").write_text(process.stderr)

        if process.returncode != 0:
            raise RuntimeError(
                f"spark-submit failed with code {process.returncode}:\n{process.stderr[-1000:]}"
            )

        # Look for event log
        found_logs = list(event_log_dir.glob("*"))
        if not found_logs:
            raise FileNotFoundError(f"No Spark event log generated in {event_log_dir}")

        target_log = found_logs[0]
        return SparkEventLogParser.parse_file(target_log, cost_per_second=self.cost_per_second)


def load_spec(path: str | Path) -> RunSpec:
    data = json.loads(Path(path).read_text())
    baseline = Metrics(**data.pop("baseline"))
    hypotheses = tuple(data.pop("hypotheses", ()))
    return RunSpec(baseline=baseline, hypotheses=hypotheses, **data)


def metrics_dict(metrics: Metrics) -> dict:
    return asdict(metrics)
