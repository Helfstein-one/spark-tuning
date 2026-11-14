"""Data, schema and workload distribution drift detection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class DriftReport:
    drift_detected: bool
    schema_drift: bool
    volume_drift_ratio: float
    confidence_penalty: float
    message: str


class DriftDetector:
    """Detects shifts in volume, schema, or cardinality that invalidate tuning hypotheses."""

    @classmethod
    def detect_drift(
        cls,
        baseline_profile: Dict[str, Any],
        current_profile: Dict[str, Any],
        volume_threshold_ratio: float = 0.50,  # 50% shift in rows triggers drift
    ) -> DriftReport:
        schema_drift = baseline_profile.get("schema") != current_profile.get("schema")
        
        base_rows = baseline_profile.get("row_count", 0)
        curr_rows = current_profile.get("row_count", 0)
        
        volume_ratio = 0.0
        if base_rows > 0:
            volume_ratio = abs(curr_rows - base_rows) / base_rows

        vol_drift = volume_ratio > volume_threshold_ratio
        drift_detected = schema_drift or vol_drift

        confidence_penalty = 0.0
        if schema_drift:
            confidence_penalty += 0.50
        if vol_drift:
            confidence_penalty += 0.30

        msg = "No significant drift detected"
        if schema_drift and vol_drift:
            msg = "Critical: Both schema and dataset volume drifted significantly"
        elif schema_drift:
            msg = "Schema structure has changed since baseline"
        elif vol_drift:
            msg = f"Volume drifted by {round(volume_ratio*100, 1)}% exceeding threshold"

        return DriftReport(
            drift_detected=drift_detected,
            schema_drift=schema_drift,
            volume_drift_ratio=round(volume_ratio, 4),
            confidence_penalty=min(1.0, confidence_penalty),
            message=msg,
        )
