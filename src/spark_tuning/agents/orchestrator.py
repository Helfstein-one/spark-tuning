"""FinOps Arbiter and Multi-Agent Orchestrator."""

from __future__ import annotations

from typing import List, Optional
from spark_tuning.models import Metrics
from spark_tuning.diagnostics.engine import Diagnosis
from spark_tuning.reporting.finops import BudgetGuardrail
from spark_tuning.agents.base import AgentProposal, TuningSpecialistAgent


class FinOpsArbiterAgent:
    """Arbiter with budget veto power preventing expensive trials."""

    def __init__(self, budget: BudgetGuardrail):
        self.budget = budget

    def review_proposal(self, proposal: AgentProposal, current_spent_usd: float) -> tuple[bool, str]:
        projected_cost = current_spent_usd + proposal.estimated_cost_usd
        if projected_cost > self.budget.max_budget_usd:
            return False, f"VETO by FinOpsArbiter: projected cost ${projected_cost:.2f} exceeds budget ${self.budget.max_budget_usd:.2f}"
        return True, "Approved by FinOpsArbiter within budget guardrails"


class MultiAgentTuningOrchestrator:
    """Orchestrates consensus among specialized agents and FinOps arbiter."""

    def __init__(
        self,
        specialists: List[TuningSpecialistAgent],
        arbiter: FinOpsArbiterAgent,
    ):
        self.specialists = specialists
        self.arbiter = arbiter

    def deliberate(
        self,
        metrics: Metrics,
        diagnosis: Diagnosis,
        current_spent_usd: float = 0.0,
    ) -> List[AgentProposal]:
        proposals: List[AgentProposal] = []

        # 1. Collect domain proposals from all specialists
        for specialist in self.specialists:
            prop = specialist.evaluate(metrics, diagnosis)
            if prop:
                # 2. Subject to FinOps Arbiter veto
                approved, reason = self.arbiter.review_proposal(prop, current_spent_usd)
                if approved:
                    proposals.append(prop)

        # 3. Sort by estimated impact * confidence descending
        proposals.sort(key=lambda p: (p.estimated_impact * p.confidence), reverse=True)
        return proposals
