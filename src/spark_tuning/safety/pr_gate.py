"""Automated branch and PR safety validation."""

from __future__ import annotations

from dataclasses import dataclass
from spark_tuning.models import Decision, LoopResult, RunState


@dataclass
class PullRequestSpec:
    title: str
    body: str
    head_branch: str
    base_branch: str = "main"


class PRAutomationGate:
    """Prepares Pull Request content and ensures gate safety (never opens PR on red gate)."""

    @classmethod
    def can_open_pr(cls, result: LoopResult) -> tuple[bool, str]:
        if result.state != RunState.PR_READY or result.decision != Decision.ACCEPT:
            return False, f"Cannot open PR: Loop state is '{result.state.value}' and decision is '{result.decision.value}'"
        return True, "Eligible for PR creation"

    @classmethod
    def build_pr_spec(cls, result: LoopResult, skill_name: str) -> PullRequestSpec:
        allowed, reason = cls.can_open_pr(result)
        if not allowed:
            raise PermissionError(reason)

        title = f"perf({skill_name}): verified tuning optimization ({result.run_id})"
        body = (
            f"## Summary\n\n"
            f"Automated Spark performance tuning loop completed successfully for `{skill_name}`.\n\n"
            f"- **Run ID:** {result.run_id}\n"
            f"- **Decision:** {result.decision.value}\n"
            f"- **Reason:** {result.reason}\n"
            f"- **Iterations executed:** {len(result.iterations)}\n"
        )

        return PullRequestSpec(
            title=title,
            body=body,
            head_branch=f"tune/{skill_name}/{result.run_id}",
            base_branch="main",
        )
