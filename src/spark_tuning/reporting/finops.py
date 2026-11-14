"""FinOps cost attribution and budget guardrails engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class BudgetGuardrail:
    max_budget_usd: float
    alert_threshold_ratio: float = 0.80  # Alert at 80% of budget


@dataclass
class FinOpsReport:
    total_cost_usd: float
    budget_limit_usd: float
    budget_exceeded: bool
    budget_alert: bool
    cost_per_million_rows: Optional[float]
    details: str


class FinOpsEngine:
    """Tracks cloud costs and enforces budget guardrails."""

    @classmethod
    def evaluate_cost(
        cls,
        current_cost_usd: float,
        budget: BudgetGuardrail,
        row_count: Optional[int] = None,
    ) -> FinOpsReport:
        exceeded = current_cost_usd > budget.max_budget_usd
        alert = current_cost_usd >= (budget.max_budget_usd * budget.alert_threshold_ratio)

        cost_per_mil = None
        if row_count and row_count > 0:
            cost_per_mil = round((current_cost_usd / row_count) * 1_000_000, 4)

        details = "Within budget"
        if exceeded:
            details = f"Budget exceeded: ${current_cost_usd:.2f} > ${budget.max_budget_usd:.2f}"
        elif alert:
            details = f"Budget warning: ${current_cost_usd:.2f} reached {int(budget.alert_threshold_ratio*100)}% of limit"

        return FinOpsReport(
            total_cost_usd=round(current_cost_usd, 4),
            budget_limit_usd=budget.max_budget_usd,
            budget_exceeded=exceeded,
            budget_alert=alert,
            cost_per_million_rows=cost_per_mil,
            details=details,
        )
