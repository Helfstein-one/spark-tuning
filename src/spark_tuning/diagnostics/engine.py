"""Automatic bottleneck diagnosis engine classifying Spark workloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from spark_tuning.models import Metrics


class BottleneckCategory(str, Enum):
    CPU = "cpu"
    MEMORY_GC = "memory_gc"
    IO = "io"
    SHUFFLE = "shuffle"
    SPILL = "spill"
    SKEW = "skew"
    DRIVER = "driver"
    PYTHON_JVM = "python_jvm"
    SMALL_FILES = "small_files"
    PROVISIONING = "provisioning"
    UNKNOWN = "unknown"


@dataclass
class DiagnosisEvidence:
    metric: str
    observed_value: Any
    threshold: Any
    description: str


@dataclass
class Diagnosis:
    primary_cause: BottleneckCategory
    secondary_causes: list[BottleneckCategory] = field(default_factory=list)
    confidence: float = 0.0
    evidences: list[DiagnosisEvidence] = field(default_factory=list)
    recommended_experiments: list[str] = field(default_factory=list)


class BottleneckDiagnosisEngine:
    """Classifies primary and secondary bottlenecks based on execution metrics."""

    @classmethod
    def diagnose(cls, metrics: Metrics) -> Diagnosis:
        evidences: list[DiagnosisEvidence] = []
        candidates: list[tuple[BottleneckCategory, float]] = []

        # 1. Spill Check
        if metrics.spill_disk_bytes is not None and metrics.spill_disk_bytes > 0:
            evidences.append(
                DiagnosisEvidence(
                    metric="spill_disk_bytes",
                    observed_value=metrics.spill_disk_bytes,
                    threshold=0,
                    description=f"Workload spilled {metrics.spill_disk_bytes} bytes to disk",
                )
            )
            candidates.append((BottleneckCategory.SPILL, 0.90))

        # 2. GC Check (> 10% of total duration)
        if metrics.gc_seconds is not None and metrics.duration_seconds > 0:
            gc_ratio = metrics.gc_seconds / metrics.duration_seconds
            if gc_ratio > 0.10:
                evidences.append(
                    DiagnosisEvidence(
                        metric="gc_seconds",
                        observed_value=metrics.gc_seconds,
                        threshold=round(metrics.duration_seconds * 0.10, 2),
                        description=f"GC time accounts for {round(gc_ratio * 100, 1)}% of duration (>10%)",
                    )
                )
                candidates.append((BottleneckCategory.MEMORY_GC, min(0.95, 0.5 + gc_ratio)))

        # 3. Skew Check (p95 task duration vs average task duration)
        if (
            metrics.task_p95_seconds is not None
            and metrics.tasks_count is not None
            and metrics.tasks_count > 1
            and metrics.duration_seconds > 0
        ):
            avg_task_duration = metrics.duration_seconds / metrics.tasks_count
            if avg_task_duration > 0 and (metrics.task_p95_seconds / avg_task_duration) > 3.0:
                evidences.append(
                    DiagnosisEvidence(
                        metric="task_p95_seconds",
                        observed_value=metrics.task_p95_seconds,
                        threshold=round(avg_task_duration * 3.0, 2),
                        description="Task p95 is > 3x average task duration indicating task skew",
                    )
                )
                candidates.append((BottleneckCategory.SKEW, 0.85))

        # 4. Shuffle Check
        if metrics.shuffle_write_bytes is not None and metrics.shuffle_write_bytes > 10 * 1024 * 1024 * 1024:
            evidences.append(
                DiagnosisEvidence(
                    metric="shuffle_write_bytes",
                    observed_value=metrics.shuffle_write_bytes,
                    threshold=10 * 1024 * 1024 * 1024,
                    description=f"High shuffle write volume: {metrics.shuffle_write_bytes} bytes",
                )
            )
            candidates.append((BottleneckCategory.SHUFFLE, 0.75))

        # 5. Missing metrics / Unknown
        if not candidates:
            return Diagnosis(
                primary_cause=BottleneckCategory.UNKNOWN,
                secondary_causes=[],
                confidence=0.0,
                evidences=[],
                recommended_experiments=["collect_detailed_event_log", "inspect_spark_ui"],
            )

        # Sort by confidence descending
        candidates.sort(key=lambda x: x[1], reverse=True)
        primary_cause, primary_conf = candidates[0]
        secondary_causes = [c[0] for c in candidates[1:]]

        # Determine recommendations based on primary cause
        recommendations = []
        if primary_cause == BottleneckCategory.SPILL:
            recommendations = ["partition-sizing", "jvm-gc-memory", "spark.sql.shuffle.partitions"]
        elif primary_cause == BottleneckCategory.MEMORY_GC:
            recommendations = ["jvm-gc-memory", "off-heap-memory", "kryo-serializer"]
        elif primary_cause == BottleneckCategory.SKEW:
            recommendations = ["skew-join-hint", "salting", "adaptive-query-execution"]
        elif primary_cause == BottleneckCategory.SHUFFLE:
            recommendations = ["join-strategy", "broadcast-hash-join", "column-pruning"]

        return Diagnosis(
            primary_cause=primary_cause,
            secondary_causes=secondary_causes,
            confidence=primary_conf,
            evidences=evidences,
            recommended_experiments=recommendations,
        )
