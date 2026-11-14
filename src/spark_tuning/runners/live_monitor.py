"""Spark REST API live monitor with Early-Abort capabilities to prevent expensive runs."""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class EarlyAbortDecision:
    should_abort: bool
    reason: str
    observed_spill_bytes: int
    observed_gc_ms: int


class SparkLiveMonitor:
    """Monitors live Spark stage execution via the Spark Driver REST API (:4040)."""

    def __init__(
        self,
        spark_ui_url: str = "http://localhost:4040",
        max_allowed_spill_bytes: int = 10 * 1024 * 1024 * 1024,  # 10GB
        max_gc_duration_ratio: float = 0.25,  # 25% of runtime in GC
    ):
        self.spark_ui_url = spark_ui_url.rstrip("/")
        self.max_allowed_spill_bytes = max_allowed_spill_bytes
        self.max_gc_duration_ratio = max_gc_duration_ratio

    def evaluate_live_stage_metrics(
        self,
        stage_metrics: Dict[str, Any],
        elapsed_seconds: float,
    ) -> EarlyAbortDecision:
        spill_bytes = (
            stage_metrics.get("diskBytesSpilled", 0)
            + stage_metrics.get("memoryBytesSpilled", 0)
        )
        jvm_gc_ms = stage_metrics.get("jvmGcTime", 0)
        jvm_gc_sec = jvm_gc_ms / 1000.0

        # Check for catastrophic disk spill early in the run
        if spill_bytes > self.max_allowed_spill_bytes:
            return EarlyAbortDecision(
                should_abort=True,
                reason=(
                    f"Catastrophic disk spill detected: {spill_bytes} bytes "
                    f"> {self.max_allowed_spill_bytes} bytes limit."
                ),
                observed_spill_bytes=spill_bytes,
                observed_gc_ms=jvm_gc_ms,
            )

        # Check for GC death spiral (> 25% of elapsed time spent in garbage collection)
        if elapsed_seconds > 10.0:
            gc_ratio = jvm_gc_sec / elapsed_seconds
            if gc_ratio > self.max_gc_duration_ratio:
                return EarlyAbortDecision(
                    should_abort=True,
                    reason=(
                        f"GC death spiral: {round(gc_ratio * 100, 1)}% of execution "
                        f"spent in JVM GC (> {int(self.max_gc_duration_ratio * 100)}% limit)."
                    ),
                    observed_spill_bytes=spill_bytes,
                    observed_gc_ms=jvm_gc_ms,
                )

        return EarlyAbortDecision(
            should_abort=False,
            reason="Stage metrics within acceptable operational guardrails.",
            observed_spill_bytes=spill_bytes,
            observed_gc_ms=jvm_gc_ms,
        )
