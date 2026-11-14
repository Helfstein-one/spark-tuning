"""Explainable optimization reporting generator."""

from __future__ import annotations

from typing import Any, Dict
from spark_tuning.models import LoopResult, Metrics


class OptimizationReportGenerator:
    """Generates explainable Markdown and JSON summaries for Pull Requests and dashboards."""

    @classmethod
    def generate_markdown(cls, result: LoopResult, baseline: Metrics) -> str:
        lines = [
            f"# Spark Optimization Report ({result.run_id})",
            "",
            f"- **Status:** `{result.state.value}`",
            f"- **Final Decision:** `{result.decision.value}`",
            f"- **Reason:** {result.reason}",
            "",
            "## Baseline vs Candidate Iterations",
            "",
            "| Iteration | Hypothesis | Duration (s) | Cost ($) | Spill Disk (B) | GC (s) | Decision | Reason |",
            "|---|---|---|---|---|---|---|---|",
            f"| 0 (Baseline) | - | {baseline.duration_seconds} | {baseline.cost_usd} | {baseline.spill_disk_bytes or 0} | {baseline.gc_seconds or 0.0} | BASELINE | - |",
        ]

        for it in result.iterations:
            m = it.metrics
            dur = m.duration_seconds if m else "-"
            cost = m.cost_usd if m else "-"
            spill = (m.spill_disk_bytes or 0) if m else "-"
            gc = (m.gc_seconds or 0.0) if m else "-"
            dec = it.decision.value if it.decision else "-"
            lines.append(f"| {it.number} | {it.hypothesis} | {dur} | {cost} | {spill} | {gc} | {dec} | {it.reason} |")

        return "\n".join(lines) + "\n"
