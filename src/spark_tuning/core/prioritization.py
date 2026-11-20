"""Hypothesis prioritization engine ranking candidate tuning experiments."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from spark_tuning.diagnostics.engine import BottleneckCategory, Diagnosis


@dataclass
class HypothesisProposal:
    id: str
    target_category: BottleneckCategory
    description: str = ""
    expected_impact: float = 0.5  # 0.0 - 1.0
    confidence: float = 0.5       # 0.0 - 1.0
    reversibility: float = 0.9    # 0.0 - 1.0 (higher is better)
    risk: float = 0.2             # 0.0 - 1.0 (lower is better)
    estimated_cost_usd: float = 1.0


@dataclass
class PrioritizedHypothesis:
    hypothesis: HypothesisProposal
    priority_score: float
    rank: int


class HypothesisPrioritizer:
    """Ranks hypotheses prioritizing primary bottleneck causes over cosmetic parameter adjustments."""

    @classmethod
    def rank_hypotheses(
        cls,
        proposals: list[HypothesisProposal],
        diagnosis: Diagnosis,
        rejected_hypothesis_ids: set[str] | None = None,
    ) -> list[PrioritizedHypothesis]:
        rejected = rejected_hypothesis_ids or set()
        scored: list[tuple[HypothesisProposal, float]] = []

        for p in proposals:
            # Skip previously rejected hypotheses unless new evidence is provided
            if p.id in rejected:
                continue

            # Numerator: impact * confidence * reversibility
            # Denominator: cost * risk (guarded against <= 0 or division by zero)
            impact = max(0.001, float(p.expected_impact))
            confidence = max(0.001, float(p.confidence))
            reversibility = max(0.001, float(p.reversibility))
            risk = max(0.01, float(p.risk))
            cost = max(0.01, float(p.estimated_cost_usd))

            raw_score = (impact * confidence * reversibility) / (cost * risk)

            # Boost if hypothesis directly targets primary bottleneck cause
            if p.target_category == diagnosis.primary_cause:
                raw_score *= 2.5
            elif p.target_category in diagnosis.secondary_causes:
                raw_score *= 1.5

            scored.append((p, round(raw_score, 4)))

        # Deterministic sorting: highest score first, then lowest cost, then id
        scored.sort(key=lambda x: (-x[1], x[0].estimated_cost_usd, x[0].id))

        result: list[PrioritizedHypothesis] = []
        for rank, (p, score) in enumerate(scored, start=1):
            result.append(PrioritizedHypothesis(hypothesis=p, priority_score=score, rank=rank))

        return result
