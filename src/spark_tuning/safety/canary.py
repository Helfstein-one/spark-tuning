"""Canary and shadow validation gate for production promotions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any
from spark_tuning.models import Metrics


@dataclass
class CanaryValidationResult:
    promotion_approved: bool
    canary_sample_percentage: float
    error_rate: float
    latency_delta_ratio: float
    rollback_required: bool
    reason: str


class CanaryValidator:
    """Validates shadow or small-slice canary execution before 100% traffic rollout."""

    @classmethod
    def evaluate_canary(
        cls,
        canary_metrics: Metrics,
        baseline_metrics: Metrics,
        sample_percentage: float = 5.0,
        max_allowed_error_rate: float = 0.0,
    ) -> CanaryValidationResult:
        error_rate = 1.0 if (canary_metrics.failures > 0 or not canary_metrics.correctness) else 0.0
        
        latency_delta = 0.0
        if baseline_metrics.duration_seconds > 0:
            latency_delta = (canary_metrics.duration_seconds - baseline_metrics.duration_seconds) / baseline_metrics.duration_seconds

        rollback = (error_rate > max_allowed_error_rate) or (latency_delta > 0.20)
        approved = not rollback

        reason = "Canary validation passed with zero errors"
        if error_rate > max_allowed_error_rate:
            reason = f"Canary failed: observed error rate {error_rate} > {max_allowed_error_rate}"
        elif latency_delta > 0.20:
            reason = f"Canary regression: latency increased by {round(latency_delta*100, 1)}%"

        return CanaryValidationResult(
            promotion_approved=approved,
            canary_sample_percentage=sample_percentage,
            error_rate=error_rate,
            latency_delta_ratio=round(latency_delta, 4),
            rollback_required=rollback,
            reason=reason,
        )
