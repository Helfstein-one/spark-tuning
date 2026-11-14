from __future__ import annotations

from typing import Protocol

from .models import Metrics, RunSpec


class ExperimentRunner(Protocol):
    def run(self, spec: RunSpec, iteration: int, hypothesis: dict) -> Metrics:
        """Run one isolated candidate experiment."""


class ArtifactStore(Protocol):
    def save(self, run_id: str, payload: dict) -> None:
        """Persist a run result without replacing prior evidence."""
