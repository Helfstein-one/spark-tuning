"""Headless control plane and ROI telemetry calculator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class TenantRoiSummary:
    tenant_id: str
    total_jobs_tuned: int
    total_hours_saved: float
    total_dollars_saved_usd: float
    total_terabytes_optimized: float
    overall_roi_multiplier: float


class TelemetryControlPlane:
    """Aggregates multi-tenant performance metrics and ROI for commercial SaaS reporting."""

    def __init__(self):
        self.tenant_records: Dict[str, List[Dict[str, float]]] = {}

    def record_tuning_run(
        self,
        tenant_id: str,
        baseline_cost_usd: float,
        tuned_cost_usd: float,
        baseline_duration_sec: float,
        tuned_duration_sec: float,
        data_volume_gb: float,
    ) -> None:
        if tenant_id not in self.tenant_records:
            self.tenant_records[tenant_id] = []

        self.tenant_records[tenant_id].append({
            "dollars_saved": max(0.0, baseline_cost_usd - tuned_cost_usd),
            "hours_saved": max(0.0, (baseline_duration_sec - tuned_duration_sec) / 3600.0),
            "tb_optimized": data_volume_gb / 1024.0,
            "cost_incurred": tuned_cost_usd,
        })

    def get_tenant_roi(self, tenant_id: str) -> TenantRoiSummary:
        records = self.tenant_records.get(tenant_id, [])
        if not records:
            return TenantRoiSummary(
                tenant_id=tenant_id,
                total_jobs_tuned=0,
                total_hours_saved=0.0,
                total_dollars_saved_usd=0.0,
                total_terabytes_optimized=0.0,
                overall_roi_multiplier=1.0,
            )

        dollars_saved = sum(r["dollars_saved"] for r in records)
        hours_saved = sum(r["hours_saved"] for r in records)
        tb_opt = sum(r["tb_optimized"] for r in records)
        cost_incurred = sum(r["cost_incurred"] for r in records)

        roi = round((dollars_saved / cost_incurred), 2) if cost_incurred > 0 else 1.0

        return TenantRoiSummary(
            tenant_id=tenant_id,
            total_jobs_tuned=len(records),
            total_hours_saved=round(hours_saved, 2),
            total_dollars_saved_usd=round(dollars_saved, 2),
            total_terabytes_optimized=round(tb_opt, 2),
            overall_roi_multiplier=roi,
        )
