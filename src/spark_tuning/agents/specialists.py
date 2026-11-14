"""Specialized agents for Storage/IO and Shuffle/Skew optimization."""

from __future__ import annotations

from typing import Optional
from spark_tuning.models import Metrics
from spark_tuning.diagnostics.engine import BottleneckCategory, Diagnosis
from spark_tuning.agents.base import AgentProposal, TuningSpecialistAgent


class StorageSpecialistAgent(TuningSpecialistAgent):
    """Specialist in I/O layout, ZSTD compression, and small files elimination."""

    @property
    def name(self) -> str:
        return "StorageIOExpert"

    def evaluate(self, metrics: Metrics, diagnosis: Diagnosis) -> Optional[AgentProposal]:
        if (
            metrics.bytes_read is not None
            and metrics.bytes_read > 50 * 1024 * 1024 * 1024
            or diagnosis.primary_cause in (BottleneckCategory.IO, BottleneckCategory.SMALL_FILES)
        ):
            return AgentProposal(
                agent_name=self.name,
                target_category=BottleneckCategory.IO,
                hypothesis_id="hyp_storage_zstd_pushdown",
                description="Apply ZSTD compression, column pruning and predicate pushdown filters",
                estimated_impact=0.85,
                confidence=0.90,
                risk=0.10,
                estimated_cost_usd=5.0,
                spark_conf_overrides={
                    "spark.sql.parquet.compression.codec": "zstd",
                    "spark.sql.parquet.filterPushdown": "true",
                },
            )
        return None


class ShuffleSpecialistAgent(TuningSpecialistAgent):
    """Specialist in partition sizing, broadcast hints, and data skew mitigation."""

    @property
    def name(self) -> str:
        return "ShuffleSkewExpert"

    def evaluate(self, metrics: Metrics, diagnosis: Diagnosis) -> Optional[AgentProposal]:
        has_spill = (metrics.spill_disk_bytes is not None and metrics.spill_disk_bytes > 0)
        has_skew = (
            metrics.task_p95_seconds is not None
            and metrics.duration_seconds > 0
            and metrics.tasks_count is not None
            and metrics.tasks_count > 1
            and (metrics.task_p95_seconds / (metrics.duration_seconds / metrics.tasks_count)) > 2.5
        )

        if has_spill or has_skew or diagnosis.primary_cause in (BottleneckCategory.SPILL, BottleneckCategory.SKEW, BottleneckCategory.SHUFFLE):
            return AgentProposal(
                agent_name=self.name,
                target_category=BottleneckCategory.SPILL if has_spill else BottleneckCategory.SKEW,
                hypothesis_id="hyp_shuffle_adaptive_partitioning",
                description="Enable Adaptive Query Execution (AQE) partition coalescing and skew join handling",
                estimated_impact=0.88,
                confidence=0.92,
                risk=0.15,
                estimated_cost_usd=8.0,
                spark_conf_overrides={
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                },
            )
        return None
