"""Resilience and chaos benchmark suite evaluating recovery under failures."""

from __future__ import annotations

from dataclasses import dataclass
from spark_tuning.models import Metrics


@dataclass
class ChaosScenarioResult:
    scenario_name: str
    recovered_successfully: bool
    recovery_duration_seconds: float
    slo_violated: bool
    data_loss_detected: bool


class ChaosBenchmarkSuite:
    """Evaluates job tolerance to spot interruptions, task failures, and storage latency."""

    @classmethod
    def evaluate_spot_interruption(
        cls,
        interruption_metrics: Metrics,
        max_allowed_recovery_seconds: float = 60.0,
    ) -> ChaosScenarioResult:
        recovered = interruption_metrics.correctness and (interruption_metrics.failures == 0)
        recovery_time = interruption_metrics.duration_seconds
        slo_violated = recovery_time > max_allowed_recovery_seconds

        return ChaosScenarioResult(
            scenario_name="spot_executor_interruption",
            recovered_successfully=recovered,
            recovery_duration_seconds=recovery_time,
            slo_violated=slo_violated,
            data_loss_detected=not interruption_metrics.correctness,
        )
