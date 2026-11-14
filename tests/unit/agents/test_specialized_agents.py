"""Unit tests for Phase 1 Multi-Agent Specialization."""

import unittest
from spark_tuning.models import Metrics
from spark_tuning.diagnostics.engine import BottleneckCategory, Diagnosis
from spark_tuning.reporting.finops import BudgetGuardrail
from spark_tuning.agents import (
    StorageSpecialistAgent,
    ShuffleSpecialistAgent,
    FinOpsArbiterAgent,
    MultiAgentTuningOrchestrator,
)


class MultiAgentSpecializationTests(unittest.TestCase):
    def test_shuffle_specialist_proposes_adaptive_partitioning_on_spill(self):
        agent = ShuffleSpecialistAgent()
        metrics = Metrics(duration_seconds=120.0, cost_usd=6.0, spill_disk_bytes=10 * 1024 * 1024 * 1024)
        diag = Diagnosis(primary_cause=BottleneckCategory.SPILL, confidence=0.9)

        proposal = agent.evaluate(metrics, diag)
        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.agent_name, "ShuffleSkewExpert")
        self.assertEqual(proposal.spark_conf_overrides["spark.sql.adaptive.enabled"], "true")

    def test_finops_arbiter_vetoes_expensive_proposals(self):
        budget = BudgetGuardrail(max_budget_usd=20.0)
        arbiter = FinOpsArbiterAgent(budget=budget)

        specialist = ShuffleSpecialistAgent()
        metrics = Metrics(duration_seconds=120.0, cost_usd=6.0, spill_disk_bytes=5 * 1024 * 1024 * 1024)
        diag = Diagnosis(primary_cause=BottleneckCategory.SPILL, confidence=0.9)
        prop = specialist.evaluate(metrics, diag)

        # Spending $15 with a proposal of $8 exceeds $20 limit -> Veto
        approved, reason = arbiter.review_proposal(prop, current_spent_usd=15.0)
        self.assertFalse(approved)
        self.assertIn("VETO", reason)

        # Spending $5 with a proposal of $8 is within $20 -> Approved
        approved_ok, _ = arbiter.review_proposal(prop, current_spent_usd=5.0)
        self.assertTrue(approved_ok)

    def test_orchestrator_consensus_and_ranking(self):
        budget = BudgetGuardrail(max_budget_usd=50.0)
        arbiter = FinOpsArbiterAgent(budget=budget)
        orchestrator = MultiAgentTuningOrchestrator(
            specialists=[StorageSpecialistAgent(), ShuffleSpecialistAgent()],
            arbiter=arbiter,
        )

        # Workload having both high I/O and Disk Spill
        metrics = Metrics(
            duration_seconds=200.0,
            cost_usd=10.0,
            bytes_read=80 * 1024 * 1024 * 1024,
            spill_disk_bytes=15 * 1024 * 1024 * 1024,
        )
        diag = Diagnosis(primary_cause=BottleneckCategory.SPILL, secondary_causes=[BottleneckCategory.IO], confidence=0.92)

        proposals = orchestrator.deliberate(metrics, diag, current_spent_usd=10.0)
        self.assertEqual(len(proposals), 2)
        # Higher score (Shuffle: 0.88 * 0.92 = 0.8096 vs Storage: 0.85 * 0.90 = 0.765) ranked first
        self.assertEqual(proposals[0].agent_name, "ShuffleSkewExpert")
        self.assertEqual(proposals[1].agent_name, "StorageIOExpert")


if __name__ == "__main__":
    unittest.main()
