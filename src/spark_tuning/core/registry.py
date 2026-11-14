"""Experiment registry and recommendation memory preventing redundant trials."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from spark_tuning.models import Decision, Metrics


@dataclass
class HistoricExperimentRecord:
    workload_fingerprint: str
    hypothesis_id: str
    decision: Decision
    metrics: Metrics
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ExperimentRegistry:
    """Stores past optimization trial results to reuse knowledge and demote rejected configs."""

    def __init__(self):
        self.records: List[HistoricExperimentRecord] = []

    def record_experiment(self, record: HistoricExperimentRecord) -> None:
        self.records.append(record)

    def is_previously_rejected(self, workload_fingerprint: str, hypothesis_id: str) -> bool:
        return any(
            r.workload_fingerprint == workload_fingerprint
            and r.hypothesis_id == hypothesis_id
            and r.decision in (Decision.REJECT, Decision.ROLLBACK)
            for r in self.records
        )

    def get_past_wins(self, workload_fingerprint: str) -> List[HistoricExperimentRecord]:
        return [
            r for r in self.records
            if r.workload_fingerprint == workload_fingerprint and r.decision == Decision.ACCEPT
        ]
