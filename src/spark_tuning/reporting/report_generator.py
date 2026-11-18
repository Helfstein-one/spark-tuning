"""Explainable optimization reporting generator."""

from __future__ import annotations

from spark_tuning.models import LoopResult, Metrics


def _fmt_metric(metrics: Metrics, field_name: str) -> str:
    if hasattr(metrics, "get_metric"):
        mv = metrics.get_metric(field_name)
        if not mv.is_usable:
            return "unknown"
        return str(mv.value)
    val = getattr(metrics, field_name, None)
    return "unknown" if val is None else str(val)


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
            (
                f"| 0 (Baseline) | - | {_fmt_metric(baseline, 'duration_seconds')} "
                f"| {_fmt_metric(baseline, 'cost_usd')} "
                f"| {_fmt_metric(baseline, 'spill_disk_bytes')} "
                f"| {_fmt_metric(baseline, 'gc_seconds')} | BASELINE | - |"
            ),
        ]

        for it in result.iterations:
            m = it.metrics
            dur = _fmt_metric(m, "duration_seconds") if m else "-"
            cost = _fmt_metric(m, "cost_usd") if m else "-"
            spill = _fmt_metric(m, "spill_disk_bytes") if m else "-"
            gc = _fmt_metric(m, "gc_seconds") if m else "-"
            dec = it.decision.value if it.decision else "-"
            lines.append(f"| {it.number} | {it.hypothesis} | {dur} | {cost} | {spill} | {gc} | {dec} | {it.reason} |")

        return "\n".join(lines) + "\n"
