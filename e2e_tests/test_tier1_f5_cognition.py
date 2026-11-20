"""Tier 1 E2E Tests — F5 (#36): O-C-H-E-M-V-D-L Cognitive Loop Integration."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from examples.anti_patterns.corpus import AntiPatternCorpus
from spark_tuning.adapters import FixtureExperimentRunner, JsonArtifactStore
from spark_tuning.core.prioritization import HypothesisPrioritizer, HypothesisProposal
from spark_tuning.core.registry import ExperimentRegistry, HistoricExperimentRecord
from spark_tuning.diagnostics.engine import BottleneckCategory, BottleneckDiagnosisEngine
from spark_tuning.engine import LoopEngine
from spark_tuning.models import Decision, Metrics, RunSpec, RunState


class TestTier1F5Cognition(unittest.TestCase):
    """T1-F5-01..06: O-C-H-E-M-V-D-L phases, diagnosis-driven selection, aligned formula, registry, explanations."""

    def test_t1_f5_01_executes_all_o_c_h_e_m_v_d_l_phases(self) -> None:
        """T1-F5-01: LoopResult records ordered LoopPhase transitions OBSERVE..LEARN."""
        from spark_tuning.models import LoopPhase

        expected_phases = [
            LoopPhase.OBSERVE,
            LoopPhase.CLASSIFY,
            LoopPhase.HYPOTHESIZE,
            LoopPhase.EXPERIMENT,
            LoopPhase.MEASURE,
            LoopPhase.VERIFY,
            LoopPhase.DECIDE,
            LoopPhase.LEARN,
        ]
        spec = RunSpec(
            run_id="t1-f5-01",
            skill="partition-sizing",
            objective="verify phase trace",
            baseline=Metrics(
                duration_seconds=200.0,
                cost_usd=10.0,
                spill_disk_bytes=10_000_000,
                task_p95_seconds=80.0,
            ),
            hypotheses=(
                {
                    "id": "fix-spill",
                    "target_category": "spill",
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 6.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 40.0,
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            engine = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=ExperimentRegistry(),
            )
            result = engine.run(spec)
            self.assertEqual(result.decision, Decision.ACCEPT)
            self.assertEqual(result.state, RunState.PR_READY)

            phases = getattr(result, "phases", None) or getattr(result, "phase_trace", None)
            self.assertIsNotNone(phases, "LoopResult must record LoopPhase transitions")
            recorded = [p.phase if hasattr(p, "phase") else p for p in phases]
            for expected in expected_phases:
                self.assertIn(expected, recorded)
            positions = [recorded.index(p) for p in expected_phases]
            self.assertEqual(positions, sorted(positions))

    def test_t1_f5_02_diagnosis_drives_hypothesis_prioritization_order(self) -> None:
        """T1-F5-02: BottleneckDiagnosisEngine SPILL classification prioritizes SPILL hypothesis first."""
        scenario = AntiPatternCorpus.get_skew_spill_scenario()
        spec = RunSpec(
            run_id="t1-f5-02",
            skill="partition-sizing",
            objective="diagnose spill and reorder hypotheses",
            baseline=scenario.bad_metrics,
            hypotheses=(
                {
                    "id": "cosmetic-driver-log",
                    "target_category": "driver",
                    "expected_impact": 0.2,
                    "confidence": 0.5,
                    "reversibility": 0.9,
                    "risk": 0.2,
                    "estimated_cost_usd": 16.0,
                    "metrics": {
                        "duration_seconds": 335.0,
                        "cost_usd": 16.8,
                        "spill_disk_bytes": scenario.bad_metrics.spill_disk_bytes,
                        "task_p95_seconds": 178.0,
                    },
                },
                {
                    "id": "repartition-spill-fix",
                    "target_category": "spill",
                    "expected_impact": 0.9,
                    "confidence": 0.9,
                    "reversibility": 0.95,
                    "risk": 0.1,
                    "estimated_cost_usd": 8.0,
                    "metrics": {
                        "duration_seconds": 200.0,
                        "cost_usd": 10.0,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 80.0,
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            engine = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=ExperimentRegistry(),
            )
            result = engine.run(spec)
            self.assertEqual(result.decision, Decision.ACCEPT)
            self.assertEqual(len(result.iterations), 1)
            self.assertEqual(result.iterations[0].hypothesis, "repartition-spill-fix")

    def test_t1_f5_03_hypothesis_prioritizer_aligned_formula(self) -> None:
        """T1-F5-03: HypothesisPrioritizer implements impact * confidence * reversibility / (cost * risk)."""
        diagnosis = BottleneckDiagnosisEngine.diagnose(
            Metrics(duration_seconds=100.0, cost_usd=10.0, spill_disk_bytes=10_000)
        )
        p_high_rev_low_cost = HypothesisProposal(
            id="h-best",
            target_category=BottleneckCategory.SPILL,
            description="High reversibility, low cost",
            expected_impact=0.8,
            confidence=0.9,
            reversibility=1.0,
            risk=0.2,
            estimated_cost_usd=2.0,
        )
        p_low_rev_high_cost = HypothesisProposal(
            id="h-worse",
            target_category=BottleneckCategory.SPILL,
            description="Low reversibility, high cost",
            expected_impact=0.8,
            confidence=0.9,
            reversibility=0.2,
            risk=0.2,
            estimated_cost_usd=20.0,
        )

        ranked = HypothesisPrioritizer.rank_hypotheses(
            [p_low_rev_high_cost, p_high_rev_low_cost],
            diagnosis,
        )
        self.assertEqual([r.hypothesis.id for r in ranked], ["h-best", "h-worse"])
        self.assertGreater(ranked[0].priority_score, ranked[1].priority_score)

    def test_t1_f5_04_registry_suppresses_previously_rejected_and_learns(self) -> None:
        """T1-F5-04: ExperimentRegistry suppresses previously rejected hyp_A and learns from completed run."""
        baseline = Metrics(
            duration_seconds=200.0,
            cost_usd=10.0,
            spill_disk_bytes=5_000_000,
            task_p95_seconds=60.0,
        )
        registry = ExperimentRegistry()
        spec = RunSpec(
            run_id="t1-f5-04",
            skill="partition-sizing",
            objective="skip prior failures",
            baseline=baseline,
            hypotheses=(
                {
                    "id": "hyp_A",
                    "target_category": "spill",
                    "expected_impact": 0.95,
                    "confidence": 0.95,
                    "reversibility": 0.95,
                    "risk": 0.05,
                    "estimated_cost_usd": 2.0,
                    "metrics": {
                        "duration_seconds": 190.0,
                        "cost_usd": 9.5,
                        "spill_disk_bytes": 4_000_000,
                        "task_p95_seconds": 55.0,
                        "correctness": False,
                    },
                },
                {
                    "id": "hyp_B",
                    "target_category": "spill",
                    "expected_impact": 0.70,
                    "confidence": 0.80,
                    "reversibility": 0.90,
                    "risk": 0.15,
                    "estimated_cost_usd": 5.0,
                    "metrics": {
                        "duration_seconds": 130.0,
                        "cost_usd": 6.5,
                        "spill_disk_bytes": 1_000_000,
                        "task_p95_seconds": 40.0,
                        "correctness": True,
                    },
                },
            ),
        )
        # First figure out what fingerprint the engine uses or pre-seed common fingerprints
        # Run 1 executes hyp_A (ROLLBACK -> recorded in registry) then hyp_B (ACCEPT -> recorded in registry)
        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            engine = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp1),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
            )
            r1 = engine.run(spec)
            self.assertEqual([it.hypothesis for it in r1.iterations], ["hyp_A", "hyp_B"])
            self.assertGreaterEqual(len(registry.records), 2)

            # Second run with same workload & registry must suppress hyp_A and execute hyp_B directly
            spec2 = RunSpec(
                run_id="t1-f5-04-repeat",
                skill=spec.skill,
                objective=spec.objective,
                baseline=spec.baseline,
                hypotheses=spec.hypotheses,
            )
            engine2 = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp2),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=registry,
            )
            r2 = engine2.run(spec2)
            self.assertEqual(r2.decision, Decision.ACCEPT)
            self.assertEqual([it.hypothesis for it in r2.iterations], ["hyp_B"])

    def test_t1_f5_05_decision_and_discarded_hypotheses_explanations_persisted(self) -> None:
        """T1-F5-05: LoopResult and artifact JSON persist decision justification and discarded-hypothesis reasons."""
        baseline = Metrics(
            duration_seconds=200.0,
            cost_usd=10.0,
            spill_disk_bytes=5_000_000,
            task_p95_seconds=60.0,
        )
        spec = RunSpec(
            run_id="t1-f5-05",
            skill="partition-sizing",
            objective="explain selection and discards",
            baseline=baseline,
            hypotheses=(
                {
                    "id": "h-winner",
                    "target_category": "spill",
                    "expected_impact": 0.9,
                    "confidence": 0.9,
                    "reversibility": 0.9,
                    "risk": 0.1,
                    "estimated_cost_usd": 4.0,
                    "metrics": {
                        "duration_seconds": 120.0,
                        "cost_usd": 6.0,
                        "spill_disk_bytes": 500_000,
                        "task_p95_seconds": 35.0,
                    },
                },
                {
                    "id": "h-unchosen-1",
                    "target_category": "driver",
                    "expected_impact": 0.2,
                    "confidence": 0.5,
                    "reversibility": 0.8,
                    "risk": 0.3,
                    "estimated_cost_usd": 9.0,
                    "metrics": {"duration_seconds": 195.0, "cost_usd": 9.8},
                },
                {
                    "id": "h-unchosen-2",
                    "target_category": "io",
                    "expected_impact": 0.3,
                    "confidence": 0.4,
                    "reversibility": 0.8,
                    "risk": 0.3,
                    "estimated_cost_usd": 8.0,
                    "metrics": {"duration_seconds": 190.0, "cost_usd": 9.5},
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            engine = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
                registry=ExperimentRegistry(),
            )
            result = engine.run(spec)
            self.assertEqual(result.decision, Decision.ACCEPT)
            self.assertTrue(bool(result.reason))

            artifact = json.loads((Path(tmp) / "t1-f5-05.json").read_text(encoding="utf-8"))
            discarded = (
                getattr(result, "discarded_hypotheses", None)
                or getattr(result, "rejected_hypotheses", None)
                or getattr(result, "discard_reasons", None)
                or artifact.get("discarded_hypotheses")
                or artifact.get("rejected_hypotheses")
                or artifact.get("discard_reasons")
            )
            self.assertTrue(discarded, "Discarded/unchosen hypotheses reasons must be recorded")

    def test_t1_f5_06_deterministic_hypothesis_selection_across_repeated_runs(self) -> None:
        """T1-F5-06: Identical baseline and shuffled input hypotheses yield identical execution order."""
        baseline = Metrics(
            duration_seconds=200.0,
            cost_usd=10.0,
            spill_disk_bytes=5_000_000,
            task_p95_seconds=60.0,
        )
        h_a = {
            "id": "alpha",
            "target_category": "spill",
            "expected_impact": 0.7,
            "confidence": 0.8,
            "reversibility": 0.9,
            "risk": 0.2,
            "estimated_cost_usd": 5.0,
            "metrics": {
                "duration_seconds": 195.0,
                "cost_usd": 9.8,
                "spill_disk_bytes": 5_000_000,
                "task_p95_seconds": 60.0,
            },
        }
        h_b = {
            "id": "beta",
            "target_category": "spill",
            "expected_impact": 0.7,
            "confidence": 0.8,
            "reversibility": 0.9,
            "risk": 0.2,
            "estimated_cost_usd": 5.0,
            "metrics": {
                "duration_seconds": 120.0,
                "cost_usd": 6.0,
                "spill_disk_bytes": 1_000_000,
                "task_p95_seconds": 40.0,
            },
        }
        spec1 = RunSpec(
            run_id="t1-f5-06-a",
            skill="partition-sizing",
            objective="determinism",
            baseline=baseline,
            hypotheses=(h_a, h_b),
        )
        spec2 = RunSpec(
            run_id="t1-f5-06-b",
            skill="partition-sizing",
            objective="determinism",
            baseline=baseline,
            hypotheses=(h_b, h_a),
        )
        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            r1 = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp1),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
            ).run(spec1)
            r2 = LoopEngine(
                FixtureExperimentRunner(),
                JsonArtifactStore(tmp2),
                diagnosis_engine=BottleneckDiagnosisEngine,
                prioritizer=HypothesisPrioritizer,
            ).run(spec2)

            self.assertEqual(
                [it.hypothesis for it in r1.iterations],
                [it.hypothesis for it in r2.iterations],
            )
            self.assertEqual(r1.decision, r2.decision)


if __name__ == "__main__":
    unittest.main()
