from pathlib import Path

import pytest

from spark_tuning.adapters import (
    FixtureExperimentRunner,
    SparkEventLogParser,
    SparkSubmitExperimentRunner,
)
from spark_tuning.models import Metrics, RunSpec

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_event_log.json"


def test_spark_event_log_parser():
    metrics = SparkEventLogParser.parse_file(FIXTURE_PATH, cost_per_second=0.01)

    assert metrics.duration_seconds == 3.0
    assert metrics.cost_usd == 0.03
    assert metrics.stages_count == 1
    assert metrics.tasks_count == 2
    assert metrics.task_retries == 0
    assert metrics.failures == 0
    assert metrics.correctness is True

    # Detailed metrics
    assert metrics.bytes_read == 6144
    assert metrics.bytes_written == 3072
    assert metrics.shuffle_read_bytes == 1200
    assert metrics.shuffle_write_bytes == 1400
    assert metrics.spill_disk_bytes == 512
    assert metrics.spill_memory_bytes == 1024
    assert metrics.gc_seconds == 0.11


def test_fixture_runner_with_event_log():
    runner = FixtureExperimentRunner()
    spec = RunSpec(
        run_id="test_run",
        skill="partition-sizing",
        objective="reduce spill",
        baseline=Metrics(duration_seconds=10.0, cost_usd=1.0),
    )
    hypothesis = {
        "id": "hyp_event_log",
        "event_log": str(FIXTURE_PATH),
    }

    metrics = runner.run(spec, 1, hypothesis)
    assert metrics.duration_seconds == 3.0
    assert metrics.shuffle_write_bytes == 1400


def test_spark_submit_runner_missing_executable(tmp_path):
    runner = SparkSubmitExperimentRunner(
        spark_home="/invalid/spark/home",
        artifacts_dir=tmp_path,
    )
    spec = RunSpec(
        run_id="test_missing_bin",
        skill="partition-sizing",
        objective="test",
        baseline=Metrics(duration_seconds=10.0, cost_usd=1.0),
    )
    hypothesis = {
        "id": "hyp_missing",
        "app_resource": "job.py",
    }

    with pytest.raises(FileNotFoundError, match="spark-submit executable not found"):
        runner.run(spec, 1, hypothesis)
