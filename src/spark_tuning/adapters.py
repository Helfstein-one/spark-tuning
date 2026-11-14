from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .models import Metrics, RunSpec


class JsonArtifactStore:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(self, run_id: str, payload: dict) -> None:
        target = self.directory / f"{run_id}.json"
        target.write_text(json.dumps(payload, indent=2, default=str) + "\n")


class FixtureExperimentRunner:
    """Deterministic runner for CI and local loop development.

    A production adapter should execute Spark and collect event-log metrics.
    """

    def run(self, spec: RunSpec, iteration: int, hypothesis: dict) -> Metrics:
        fixture = hypothesis.get("metrics")
        if not fixture:
            raise ValueError(f"hypothesis {hypothesis.get('id', iteration)} has no metrics fixture")
        return Metrics(**fixture)


def load_spec(path: str | Path) -> RunSpec:
    data = json.loads(Path(path).read_text())
    baseline = Metrics(**data.pop("baseline"))
    hypotheses = tuple(data.pop("hypotheses", ()))
    return RunSpec(baseline=baseline, hypotheses=hypotheses, **data)


def metrics_dict(metrics: Metrics) -> dict:
    return asdict(metrics)
