"""Base protocol and models for specialized tuning agents."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
from spark_tuning.models import Metrics
from spark_tuning.diagnostics.engine import BottleneckCategory, Diagnosis


@dataclass
class AgentProposal:
    agent_name: str
    target_category: BottleneckCategory
    hypothesis_id: str
    description: str
    estimated_impact: float      # 0.0 - 1.0
    confidence: float            # 0.0 - 1.0
    risk: float                  # 0.0 - 1.0
    estimated_cost_usd: float
    spark_conf_overrides: dict[str, str] = field(default_factory=dict)


class TuningSpecialistAgent(ABC):
    """Abstract base class for specialized domain agents."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name / Role of the agent."""
        pass

    @abstractmethod
    def evaluate(self, metrics: Metrics, diagnosis: Diagnosis) -> Optional[AgentProposal]:
        """Evaluates metrics & diagnosis to propose a domain-specific optimization."""
        pass
