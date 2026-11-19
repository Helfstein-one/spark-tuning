"""Event log parser and Spark runner adapters."""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import CANONICAL_UNITS, Metrics, MetricValue, ParseDiagnostics, RunSpec


class JsonArtifactStore:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(self, run_id: str, payload: dict) -> None:
        target = self.directory / f"{run_id}.json"
        target.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def _first_number(mapping: Any, *keys: str) -> int | float | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            val = mapping[key]
            if (
                val is not None
                and not isinstance(val, bool)
                and isinstance(val, (int, float))
                and math.isfinite(float(val))
            ):
                return val
    return None


def _extract_nonneg_number(
    mapping: Any,
    *keys: str,
) -> tuple[int | float | None, tuple[str, int | float] | None]:
    if not isinstance(mapping, dict):
        return None, None
    for key in keys:
        if key in mapping:
            val = mapping[key]
            if (
                val is not None
                and not isinstance(val, bool)
                and isinstance(val, (int, float))
                and math.isfinite(float(val))
            ):
                if float(val) < 0.0:
                    return None, (key, val)
                return val, None
    return None, None


def _add_opt_int(total: int | None, delta: int | float | None) -> int | None:
    if delta is None:
        return total
    return (total or 0) + int(delta)


def _add_opt_float(total: float | None, delta: int | float | None) -> float | None:
    if delta is None:
        return total
    return (total or 0.0) + float(delta)


class SparkEventLogParser:
    """Parses JSON-lines Spark event logs with version/schema detection and parse diagnostics."""

    KNOWN_SPARK_EVENTS: frozenset[str] = frozenset(
        {
            "SparkListenerLogStart",
            "SparkListenerApplicationStart",
            "SparkListenerApplicationEnd",
            "SparkListenerStageSubmitted",
            "SparkListenerStageCompleted",
            "SparkListenerTaskStart",
            "SparkListenerTaskGettingResult",
            "SparkListenerTaskEnd",
            "SparkListenerJobStart",
            "SparkListenerJobEnd",
            "SparkListenerEnvironmentUpdate",
            "SparkListenerBlockManagerAdded",
            "SparkListenerBlockManagerRemoved",
            "SparkListenerExecutorAdded",
            "SparkListenerExecutorRemoved",
            "SparkListenerExecutorMetricsUpdate",
            "SparkListenerResourceProfileAdded",
            "SparkListenerUnpersistRDD",
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
            "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
            "org.apache.spark.sql.execution.ui.SparkListenerAdaptiveExecutionUpdate",
        }
    )

    @classmethod
    def parse_with_diagnostics(
        cls,
        log_path: Path | str,
        cost_per_second: float = 0.0,
    ) -> tuple[Metrics, ParseDiagnostics]:
        path = Path(log_path)
        if not path.exists():
            raise FileNotFoundError(f"Event log file not found: {path}")

        spark_version: str | None = None
        spark_major: int | None = None
        saw_2x_marker = False
        saw_3x_marker = False
        saw_4x_marker = False

        app_start_time: int | float | None = None
        app_end_time: int | float | None = None
        stages_submitted = 0
        stages_count = 0
        tasks_ended = 0
        task_durations_ms: list[float] = []
        task_retries = 0
        failures = 0

        total_lines = 0
        valid_events = 0
        invalid_json_lines = 0
        unknown_events = 0
        last_nonempty_line_invalid = False
        has_corrupted_metrics = False
        corrupted_metrics: set[str] = set()
        warnings: list[str] = []

        # Absent task metric categories stay None rather than silently coercing to 0
        has_any_task_metrics = False
        gc_time_ms_total: float | None = None
        shuffle_write_bytes_total: int | None = None
        shuffle_read_bytes_total: int | None = None
        bytes_read_total: int | None = None
        bytes_written_total: int | None = None
        spill_disk_bytes_total: int | None = None
        spill_mem_bytes_total: int | None = None

        with open(path, "rb") as f:
            for line_no, raw_bytes in enumerate(f, start=1):
                stripped_bytes = raw_bytes.strip()
                if not stripped_bytes:
                    continue
                total_lines += 1

                try:
                    line = stripped_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    invalid_json_lines += 1
                    last_nonempty_line_invalid = True
                    warnings.append(f"Line {line_no}: invalid UTF-8 bytes ({exc.reason})")
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    invalid_json_lines += 1
                    last_nonempty_line_invalid = True
                    warnings.append(f"Line {line_no}: invalid JSON ({exc.msg})")
                    continue

                if not isinstance(event, dict):
                    invalid_json_lines += 1
                    last_nonempty_line_invalid = True
                    warnings.append(
                        f"Line {line_no}: expected JSON object, got {type(event).__name__}"
                    )
                    continue

                last_nonempty_line_invalid = False
                valid_events += 1

                event_type = event.get("Event")
                if not isinstance(event_type, str) or not event_type:
                    unknown_events += 1
                    warnings.append(f"Line {line_no}: missing or non-string 'Event' field")
                    continue

                if event_type not in cls.KNOWN_SPARK_EVENTS:
                    unknown_events += 1
                    warnings.append(f"Line {line_no}: unknown Spark event '{event_type}'")
                    continue

                if event_type == "SparkListenerLogStart":
                    sv = event.get("Spark Version") or event.get("sparkVersion")
                    if isinstance(sv, str) and sv.strip():
                        spark_version = sv.strip()
                        match = re.match(r"^v?(\d+)", spark_version)
                        if match:
                            spark_major = int(match.group(1))
                elif event_type == "SparkListenerEnvironmentUpdate":
                    spark_props = event.get("Spark Properties")
                    if isinstance(spark_props, dict) and spark_version is None:
                        sv = spark_props.get("spark.version")
                        if isinstance(sv, str) and sv.strip():
                            spark_version = sv.strip()
                            match = re.match(r"^v?(\d+)", spark_version)
                            if match:
                                spark_major = int(match.group(1))
                            if spark_version.startswith("4."):
                                saw_4x_marker = True
                elif event_type == "SparkListenerResourceProfileAdded":
                    saw_3x_marker = True
                elif event_type == "SparkListenerExecutorAdded":
                    exec_info = event.get("Executor Info")
                    if isinstance(exec_info, dict) and "Resource Profile Id" in exec_info:
                        saw_3x_marker = True
                elif event_type == "SparkListenerApplicationStart":
                    ts, neg_ts = _extract_nonneg_number(event, "Timestamp", "Time")
                    if neg_ts is not None:
                        has_corrupted_metrics = True
                        corrupted_metrics.update(("duration_seconds", "cost_usd"))
                        warnings.append(
                            f"Line {line_no}: negative timestamp {neg_ts[0]}={neg_ts[1]} in SparkListenerApplicationStart"
                        )
                    elif ts is not None:
                        app_start_time = ts
                    if "Timestamp" not in event and "Time" in event:
                        saw_4x_marker = True
                elif event_type == "SparkListenerApplicationEnd":
                    ts, neg_ts = _extract_nonneg_number(event, "Timestamp", "Time")
                    if neg_ts is not None:
                        has_corrupted_metrics = True
                        corrupted_metrics.update(("duration_seconds", "cost_usd"))
                        warnings.append(
                            f"Line {line_no}: negative timestamp {neg_ts[0]}={neg_ts[1]} in SparkListenerApplicationEnd"
                        )
                    elif ts is not None:
                        app_end_time = ts
                    if "Timestamp" not in event and "Time" in event:
                        saw_4x_marker = True
                    exit_code = _first_number(event, "ExitCode")
                    if exit_code is not None and exit_code != 0:
                        failures += 1
                        warnings.append(
                            f"Line {line_no}: SparkListenerApplicationEnd reported ExitCode={exit_code}"
                        )
                elif event_type == "SparkListenerJobEnd":
                    job_result = event.get("Job Result")
                    if isinstance(job_result, dict) and "Result" in job_result:
                        result_val = job_result.get("Result")
                        if result_val != "JobSucceeded":
                            failures += 1
                            warnings.append(
                                f"Line {line_no}: SparkListenerJobEnd reported Result={result_val!r}"
                            )
                elif event_type == "SparkListenerStageSubmitted":
                    stages_submitted += 1
                    stage_info = event.get("Stage Info")
                    if isinstance(stage_info, dict) and "Resource Profile Id" in stage_info:
                        saw_3x_marker = True
                elif event_type == "SparkListenerStageCompleted":
                    stages_count += 1
                    stage_info = event.get("Stage Info")
                    if isinstance(stage_info, dict):
                        if "Resource Profile Id" in stage_info:
                            saw_3x_marker = True
                        if stage_info.get("Failure Reason"):
                            failures += 1
                elif event_type == "SparkListenerTaskEnd":
                    tasks_ended += 1
                    task_info = event.get("Task Info")
                    if isinstance(task_info, dict):
                        if "Partition Id" in task_info:
                            saw_3x_marker = True
                        attempt, neg_attempt = _extract_nonneg_number(task_info, "Attempt")
                        if neg_attempt is not None:
                            has_corrupted_metrics = True
                            corrupted_metrics.add("task_retries")
                            warnings.append(
                                f"Line {line_no}: negative task Attempt={neg_attempt[1]} ignored"
                            )
                        elif attempt is not None and attempt > 0:
                            task_retries += 1
                        if task_info.get("Failed") is True:
                            failures += 1

                        launch_time, neg_launch = _extract_nonneg_number(task_info, "Launch Time")
                        finish_time, neg_finish = _extract_nonneg_number(task_info, "Finish Time")
                        if neg_launch is not None:
                            has_corrupted_metrics = True
                            corrupted_metrics.add("task_p95_seconds")
                            warnings.append(
                                f"Line {line_no}: negative task Launch Time={neg_launch[1]} ignored"
                            )
                        if neg_finish is not None:
                            has_corrupted_metrics = True
                            corrupted_metrics.add("task_p95_seconds")
                            warnings.append(
                                f"Line {line_no}: negative task Finish Time={neg_finish[1]} ignored"
                            )
                        if (
                            neg_launch is None
                            and neg_finish is None
                            and launch_time is not None
                            and finish_time is not None
                            and finish_time >= launch_time
                        ):
                            task_durations_ms.append(float(finish_time - launch_time))

                    if isinstance(event.get("Task Executor Metrics"), dict):
                        saw_3x_marker = True

                    task_metrics = event.get("Task Metrics")
                    if isinstance(task_metrics, dict) and task_metrics:
                        has_any_task_metrics = True

                        def _take_nonneg(container: dict[str, Any], metric_name: str, *keys: str) -> int | float | None:
                            nonlocal has_corrupted_metrics
                            val, neg = _extract_nonneg_number(container, *keys)
                            if neg is not None:
                                has_corrupted_metrics = True
                                corrupted_metrics.add(metric_name)
                                warnings.append(
                                    f"Line {line_no}: negative metric '{neg[0]}'={neg[1]} ignored"
                                )
                            return val

                        gc_time_ms_total = _add_opt_float(
                            gc_time_ms_total, _take_nonneg(task_metrics, "gc_seconds", "JVM GC Time")
                        )
                        spill_disk_bytes_total = _add_opt_int(
                            spill_disk_bytes_total, _take_nonneg(task_metrics, "spill_disk_bytes", "Disk Bytes Spilled")
                        )
                        spill_mem_bytes_total = _add_opt_int(
                            spill_mem_bytes_total, _take_nonneg(task_metrics, "spill_memory_bytes", "Memory Bytes Spilled")
                        )

                        in_metrics = task_metrics.get("Input Metrics")
                        if isinstance(in_metrics, dict):
                            bytes_read_total = _add_opt_int(
                                bytes_read_total, _take_nonneg(in_metrics, "bytes_read", "Bytes Read")
                            )

                        out_metrics = task_metrics.get("Output Metrics")
                        if isinstance(out_metrics, dict):
                            bytes_written_total = _add_opt_int(
                                bytes_written_total, _take_nonneg(out_metrics, "bytes_written", "Bytes Written")
                            )

                        shuf_read = task_metrics.get("Shuffle Read Metrics")
                        if isinstance(shuf_read, dict):
                            if (
                                "Remote Merged Blocks Fetched" in shuf_read
                                or "Merged Fetch Fallback Count" in shuf_read
                            ):
                                saw_3x_marker = True
                            if "Total Bytes Read" in shuf_read:
                                total_read = _take_nonneg(shuf_read, "shuffle_read_bytes", "Total Bytes Read")
                                shuffle_read_bytes_total = _add_opt_int(
                                    shuffle_read_bytes_total, total_read
                                )
                            else:
                                remote_read = _take_nonneg(shuf_read, "shuffle_read_bytes", "Remote Bytes Read")
                                local_read = _take_nonneg(shuf_read, "shuffle_read_bytes", "Local Bytes Read")
                                if remote_read is not None or local_read is not None:
                                    saw_2x_marker = True
                                    shuffle_read_bytes_total = _add_opt_int(
                                        shuffle_read_bytes_total,
                                        (remote_read or 0) + (local_read or 0),
                                    )

                        shuf_write = task_metrics.get("Shuffle Write Metrics")
                        if isinstance(shuf_write, dict):
                            if "Shuffle Bytes Written" in shuf_write:
                                sw = _take_nonneg(shuf_write, "shuffle_write_bytes", "Shuffle Bytes Written")
                                shuffle_write_bytes_total = _add_opt_int(
                                    shuffle_write_bytes_total, sw
                                )
                            else:
                                legacy_sw = _take_nonneg(shuf_write, "shuffle_write_bytes", "Bytes Written")
                                if legacy_sw is not None:
                                    saw_2x_marker = True
                                    shuffle_write_bytes_total = _add_opt_int(
                                        shuffle_write_bytes_total, legacy_sw
                                    )

        if spark_major is None:
            if saw_4x_marker:
                spark_major = 4
            elif saw_3x_marker:
                spark_major = 3
            elif saw_2x_marker:
                spark_major = 2
            if spark_major is not None:
                warnings.append(
                    f"SparkListenerLogStart absent; inferred Spark major version {spark_major}.x from event schema"
                )

        missing_app_start = app_start_time is None
        missing_app_end = app_end_time is None
        truncated = (
            last_nonempty_line_invalid
            or (valid_events > 0 and missing_app_end)
            or (stages_submitted > stages_count and missing_app_end)
        )

        if total_lines == 0:
            warnings.append("Event log is empty")
        else:
            if missing_app_start:
                warnings.append("Missing SparkListenerApplicationStart event")
            if missing_app_end:
                warnings.append("Missing SparkListenerApplicationEnd event")
            if last_nonempty_line_invalid:
                warnings.append("Event log appears truncated at last non-empty line")

        duration_seconds: float | None = None
        if app_start_time is not None and app_end_time is not None:
            if app_end_time >= app_start_time:
                duration_seconds = (app_end_time - app_start_time) / 1000.0
            else:
                warnings.append(
                    f"ApplicationEnd ({app_end_time}) precedes ApplicationStart ({app_start_time})"
                )

        task_p95_seconds: float | None = None
        if task_durations_ms:
            task_durations_ms.sort()
            idx = int(0.95 * (len(task_durations_ms) - 1))
            task_p95_seconds = task_durations_ms[idx] / 1000.0

        cost_usd: float | None = (
            round(duration_seconds * cost_per_second, 4)
            if duration_seconds is not None
            else None
        )

        if total_lines == 0 or valid_events == 0:
            completeness_score = 0.0
        else:
            valid_ratio = valid_events / total_lines
            recognized_ratio = (valid_events - unknown_events) / valid_events
            lifecycle = 1.0
            if missing_app_start:
                lifecycle -= 0.30
            if missing_app_end:
                lifecycle -= 0.30
            if last_nonempty_line_invalid:
                lifecycle -= 0.20
            if tasks_ended > 0 and not has_any_task_metrics:
                lifecycle -= 0.10
            if has_corrupted_metrics:
                lifecycle -= 0.20
            lifecycle = max(0.0, lifecycle)
            completeness_score = round(
                max(0.0, min(1.0, valid_ratio * recognized_ratio * lifecycle)), 4
            )

        is_complete = (
            valid_events > 0
            and invalid_json_lines == 0
            and not truncated
            and not missing_app_start
            and not missing_app_end
            and not has_corrupted_metrics
            and duration_seconds is not None
        )

        if failures > 0:
            correctness: bool | None = False
        elif not is_complete:
            correctness = None
        else:
            correctness = True

        diagnostics = ParseDiagnostics(
            spark_version=spark_version,
            spark_major=spark_major,
            total_lines=total_lines,
            valid_events=valid_events,
            invalid_json_lines=invalid_json_lines,
            unknown_events=unknown_events,
            truncated=truncated,
            missing_app_start=missing_app_start,
            missing_app_end=missing_app_end,
            completeness_score=completeness_score,
            is_complete=is_complete,
            warnings=tuple(dict.fromkeys(warnings)),
        )

        source_name = f"spark_event_log:{path.name}"
        metric_values = {
            name: MetricValue.invalid(
                unit=CANONICAL_UNITS.get(name, ""),
                source=source_name,
                quality="corrupted",
            )
            for name in sorted(corrupted_metrics)
        }

        metrics = Metrics(
            duration_seconds=duration_seconds,
            cost_usd=cost_usd,
            shuffle_write_bytes=shuffle_write_bytes_total,
            spill_disk_bytes=spill_disk_bytes_total,
            gc_seconds=round(gc_time_ms_total / 1000.0, 4) if gc_time_ms_total is not None else None,
            task_p95_seconds=task_p95_seconds,
            failures=failures,
            correctness=correctness,
            stages_count=stages_count,
            tasks_count=tasks_ended,
            task_retries=task_retries,
            bytes_read=bytes_read_total,
            bytes_written=bytes_written_total,
            shuffle_read_bytes=shuffle_read_bytes_total,
            spill_memory_bytes=spill_mem_bytes_total,
            source=source_name,
            quality="complete" if is_complete else "incomplete",
            metric_values=metric_values,
            parse_diagnostics=diagnostics,
        )
        return metrics, diagnostics

    @classmethod
    def parse_file(cls, log_path: Path | str, cost_per_second: float = 0.0) -> Metrics:
        metrics, _ = cls.parse_with_diagnostics(log_path, cost_per_second=cost_per_second)
        return metrics


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
