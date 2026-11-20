from __future__ import annotations

from typing import Any

from .adapters import metrics_dict
from .core.prioritization import HypothesisPrioritizer, HypothesisProposal
from .core.registry import ExperimentRegistry, HistoricExperimentRecord
from .diagnostics.engine import BottleneckCategory, BottleneckDiagnosisEngine
from .evaluator import evaluate
from .models import Decision, Iteration, LoopPhase, LoopResult, RunSpec, RunState
from .persistence.store import LoopStateStore
from .ports import ArtifactStore, ExperimentRunner
from .safety.sanitizer import ArtifactSanitizer


class LoopEngine:
    def __init__(
        self,
        runner: ExperimentRunner,
        artifacts: ArtifactStore,
        *,
        diagnosis_engine: Any = None,
        prioritizer: Any = None,
        registry: ExperimentRegistry | None = None,
        state_store: LoopStateStore | None = None,
    ):
        self.runner = runner
        self.artifacts = artifacts
        self.diagnosis_engine = diagnosis_engine or BottleneckDiagnosisEngine
        self.prioritizer = prioritizer or HypothesisPrioritizer
        self.registry = registry
        self.state_store = state_store

    def run(self, spec: RunSpec) -> LoopResult:
        lease_owner = f"worker-{spec.run_id}"
        if self.state_store:
            self.state_store.init_run(spec)
            self.state_store.acquire_lease(spec.run_id, owner=lease_owner, ttl_seconds=300.0)

        phases: list[LoopPhase] = [LoopPhase.OBSERVE]
        phase_trace: list[LoopPhase] = [LoopPhase.OBSERVE]

        iterations: list[Iteration] = []
        total_cost = 0.0

        if not spec.hypotheses:
            res = self._finish(
                spec,
                RunState.BLOCKED,
                Decision.BLOCKED,
                iterations,
                "no hypotheses",
                phases=phases,
                phase_trace=phase_trace,
            )
            if self.state_store:
                self.state_store.release_lease(spec.run_id, owner=lease_owner)
            return res

        # CLASSIFY
        phases.append(LoopPhase.CLASSIFY)
        phase_trace.append(LoopPhase.CLASSIFY)
        diagnosis = self.diagnosis_engine.diagnose(spec.baseline)

        # HYPOTHESIZE
        phases.append(LoopPhase.HYPOTHESIZE)
        phase_trace.append(LoopPhase.HYPOTHESIZE)

        # Check rejected hypotheses in registry
        workload_fp = spec.baseline.workload_id or spec.skill
        proposals: list[HypothesisProposal] = []
        hyp_map: dict[str, dict[str, Any]] = {}
        rejected_ids: set[str] = set()

        for h in spec.hypotheses:
            hid = str(h.get("id", ""))
            hyp_map[hid] = h
            if self.registry and self.registry.is_previously_rejected(workload_fp, hid):
                rejected_ids.add(hid)
            target_cat = h.get("target_category", BottleneckCategory.UNKNOWN)
            if isinstance(target_cat, str):
                try:
                    target_cat = BottleneckCategory(target_cat)
                except ValueError:
                    target_cat = BottleneckCategory.UNKNOWN

            proposals.append(
                HypothesisProposal(
                    id=hid,
                    target_category=target_cat,
                    description=h.get("description", ""),
                    expected_impact=h.get("expected_impact", 0.5),
                    confidence=h.get("confidence", 0.5),
                    reversibility=h.get("reversibility", 0.9),
                    risk=h.get("risk", 0.2),
                    estimated_cost_usd=h.get("estimated_cost_usd", 1.0),
                )
            )

        ranked = self.prioritizer.rank_hypotheses(proposals, diagnosis, rejected_hypothesis_ids=rejected_ids)

        discarded_reasons: dict[str, str] = {}
        for r_id in rejected_ids:
            discarded_reasons[r_id] = "previously rejected in experiment registry"

        if not ranked:
            res = self._finish(
                spec,
                RunState.BLOCKED,
                Decision.BLOCKED,
                iterations,
                "all candidate hypotheses rejected or excluded by prior knowledge",
                phases=phases,
                phase_trace=phase_trace,
                discarded_hypotheses=discarded_reasons,
            )
            if self.state_store:
                self.state_store.release_lease(spec.run_id, owner=lease_owner)
            return res

        # Record why other hypotheses were not prioritized first
        top_id = ranked[0].hypothesis.id
        for rank_item in ranked[1:]:
            h_obj = rank_item.hypothesis
            discarded_reasons[h_obj.id] = (
                f"ranked lower (rank {rank_item.rank}, score {rank_item.priority_score}) "
                f"targeting {h_obj.target_category.value} compared to top candidate {top_id}"
            )

        # EXPERIMENT & EXECUTION LOOP
        phases.append(LoopPhase.EXPERIMENT)
        phase_trace.append(LoopPhase.EXPERIMENT)

        terminal_result: LoopResult | None = None

        for number, ranked_item in enumerate(ranked[: max(0, spec.max_iterations)], start=1):
            h_obj = ranked_item.hypothesis
            hypothesis = hyp_map[h_obj.id]
            iteration = Iteration(number=number, hypothesis=h_obj.id)

            try:
                metrics = self.runner.run(spec, number, hypothesis)
            except (RuntimeError, ValueError, OSError) as exc:
                iteration.decision = Decision.ROLLBACK
                iteration.reason = f"experiment failed: {exc}"
                iterations.append(iteration)
                if self.state_store:
                    self.state_store.save_iteration(spec.run_id, iteration)
                terminal_result = self._finish(
                    spec,
                    RunState.ROLLBACK,
                    Decision.ROLLBACK,
                    iterations,
                    iteration.reason,
                    phases=phases,
                    phase_trace=phase_trace,
                    discarded_hypotheses=discarded_reasons,
                )
                break

            iteration.metrics = metrics
            if metrics.cost_usd is not None:
                total_cost += metrics.cost_usd

            # MEASURE
            if LoopPhase.MEASURE not in phases:
                phases.append(LoopPhase.MEASURE)
            phase_trace.append(LoopPhase.MEASURE)

            # VERIFY
            if LoopPhase.VERIFY not in phases:
                phases.append(LoopPhase.VERIFY)
            phase_trace.append(LoopPhase.VERIFY)
            evaluation = evaluate(spec, metrics)
            iteration.decision = evaluation.decision
            iteration.reason = evaluation.reason
            iterations.append(iteration)

            if self.state_store:
                self.state_store.save_iteration(spec.run_id, iteration)

            # DECIDE
            if LoopPhase.DECIDE not in phases:
                phases.append(LoopPhase.DECIDE)
            phase_trace.append(LoopPhase.DECIDE)

            # Update registry memory
            if self.registry:
                self.registry.record_experiment(
                    HistoricExperimentRecord(
                        workload_fingerprint=workload_fp,
                        hypothesis_id=h_obj.id,
                        decision=evaluation.decision,
                        metrics=metrics,
                    )
                )

            if total_cost > spec.max_total_cost_usd:
                terminal_result = self._finish(
                    spec,
                    RunState.BLOCKED,
                    Decision.BLOCKED,
                    iterations,
                    "cost limit exceeded",
                    phases=phases,
                    phase_trace=phase_trace,
                    discarded_hypotheses=discarded_reasons,
                )
                break

            if evaluation.decision is Decision.ACCEPT:
                # LEARN
                phases.append(LoopPhase.LEARN)
                phase_trace.append(LoopPhase.LEARN)
                terminal_result = self._finish(
                    spec,
                    RunState.PR_READY,
                    Decision.ACCEPT,
                    iterations,
                    evaluation.reason,
                    phases=phases,
                    phase_trace=phase_trace,
                    discarded_hypotheses=discarded_reasons,
                )
                break

            if evaluation.decision is Decision.ROLLBACK:
                continue

        if terminal_result is None:
            reason = "iteration limit reached without accepted candidate"
            terminal_result = self._finish(
                spec,
                RunState.BLOCKED,
                Decision.BLOCKED,
                iterations,
                reason,
                phases=phases,
                phase_trace=phase_trace,
                discarded_hypotheses=discarded_reasons,
            )

        if self.state_store:
            self.state_store.release_lease(spec.run_id, owner=lease_owner)
        return terminal_result

    def resume(self, run_id: str, spec: RunSpec) -> LoopResult:
        if not self.state_store:
            raise ValueError("Cannot resume without a state_store")

        lease_owner = f"worker-resume-{run_id}"
        self.state_store.acquire_lease(run_id, owner=lease_owner, ttl_seconds=300.0)

        stored = self.state_store.load_run(run_id)

        # Idempotent return if already finished
        if stored.decision in (Decision.ACCEPT, Decision.BLOCKED) and stored.state in (RunState.PR_READY, RunState.ACCEPTED, RunState.BLOCKED):
            self.state_store.release_lease(run_id, owner=lease_owner)
            res = LoopResult(
                run_id=stored.run_id,
                state=stored.state,
                decision=stored.decision,
                iterations=stored.iterations,
                reason=stored.reason,
                phases=stored.phases,
                phase_trace=stored.phases,
            )
            return res

        iterations: list[Iteration] = list(stored.iterations)
        total_cost = sum(
            it.metrics.cost_usd
            for it in iterations
            if it.metrics and it.metrics.cost_usd is not None
        )
        completed_hyp_ids = {it.hypothesis for it in iterations}

        phases: list[LoopPhase] = list(stored.phases) or [LoopPhase.OBSERVE, LoopPhase.CLASSIFY, LoopPhase.HYPOTHESIZE]
        phase_trace: list[LoopPhase] = list(phases)

        diagnosis = self.diagnosis_engine.diagnose(spec.baseline)
        workload_fp = spec.baseline.workload_id or spec.skill
        hyp_map: dict[str, dict[str, Any]] = {str(h.get("id", "")): h for h in spec.hypotheses}
        proposals: list[HypothesisProposal] = []
        rejected_ids: set[str] = set()

        for h in spec.hypotheses:
            hid = str(h.get("id", ""))
            if self.registry and self.registry.is_previously_rejected(workload_fp, hid):
                rejected_ids.add(hid)
            target_cat = h.get("target_category", BottleneckCategory.UNKNOWN)
            if isinstance(target_cat, str):
                try:
                    target_cat = BottleneckCategory(target_cat)
                except ValueError:
                    target_cat = BottleneckCategory.UNKNOWN
            proposals.append(
                HypothesisProposal(
                    id=hid,
                    target_category=target_cat,
                    description=h.get("description", ""),
                    expected_impact=h.get("expected_impact", 0.5),
                    confidence=h.get("confidence", 0.5),
                    reversibility=h.get("reversibility", 0.9),
                    risk=h.get("risk", 0.2),
                    estimated_cost_usd=h.get("estimated_cost_usd", 1.0),
                )
            )

        ranked = self.prioritizer.rank_hypotheses(proposals, diagnosis, rejected_hypothesis_ids=rejected_ids)
        discarded_reasons: dict[str, str] = {rid: "previously rejected in registry" for rid in rejected_ids}

        start_num = len(iterations) + 1
        remaining_ranked = [r for r in ranked if r.hypothesis.id not in completed_hyp_ids]

        terminal_result: LoopResult | None = None

        for offset, ranked_item in enumerate(remaining_ranked[: max(0, spec.max_iterations - len(iterations))], start=0):
            number = start_num + offset
            h_obj = ranked_item.hypothesis
            hypothesis = hyp_map[h_obj.id]
            iteration = Iteration(number=number, hypothesis=h_obj.id)

            try:
                metrics = self.runner.run(spec, number, hypothesis)
            except (RuntimeError, ValueError, OSError) as exc:
                iteration.decision = Decision.ROLLBACK
                iteration.reason = f"experiment failed: {exc}"
                iterations.append(iteration)
                self.state_store.save_iteration(spec.run_id, iteration)
                terminal_result = self._finish(
                    spec,
                    RunState.ROLLBACK,
                    Decision.ROLLBACK,
                    iterations,
                    iteration.reason,
                    phases=phases,
                    phase_trace=phase_trace,
                    discarded_hypotheses=discarded_reasons,
                )
                break

            iteration.metrics = metrics
            if metrics.cost_usd is not None:
                total_cost += metrics.cost_usd

            evaluation = evaluate(spec, metrics)
            iteration.decision = evaluation.decision
            iteration.reason = evaluation.reason
            iterations.append(iteration)

            self.state_store.save_iteration(spec.run_id, iteration)

            if self.registry:
                self.registry.record_experiment(
                    HistoricExperimentRecord(
                        workload_fingerprint=workload_fp,
                        hypothesis_id=h_obj.id,
                        decision=evaluation.decision,
                        metrics=metrics,
                    )
                )

            if total_cost > spec.max_total_cost_usd:
                terminal_result = self._finish(
                    spec,
                    RunState.BLOCKED,
                    Decision.BLOCKED,
                    iterations,
                    "cost limit exceeded",
                    phases=phases,
                    phase_trace=phase_trace,
                    discarded_hypotheses=discarded_reasons,
                )
                break

            if evaluation.decision is Decision.ACCEPT:
                if LoopPhase.LEARN not in phases:
                    phases.append(LoopPhase.LEARN)
                    phase_trace.append(LoopPhase.LEARN)
                terminal_result = self._finish(
                    spec,
                    RunState.PR_READY,
                    Decision.ACCEPT,
                    iterations,
                    evaluation.reason,
                    phases=phases,
                    phase_trace=phase_trace,
                    discarded_hypotheses=discarded_reasons,
                )
                break

        if terminal_result is None:
            reason = "iteration limit reached without accepted candidate"
            terminal_result = self._finish(
                spec,
                RunState.BLOCKED,
                Decision.BLOCKED,
                iterations,
                reason,
                phases=phases,
                phase_trace=phase_trace,
                discarded_hypotheses=discarded_reasons,
            )

        self.state_store.release_lease(run_id, owner=lease_owner)
        return terminal_result

    def _finish(
        self,
        spec: RunSpec,
        state: RunState,
        decision: Decision,
        iterations: list[Iteration],
        reason: str,
        phases: list[LoopPhase] | None = None,
        phase_trace: list[LoopPhase] | None = None,
        discarded_hypotheses: dict[str, str] | None = None,
    ) -> LoopResult:
        phase_list = phases or [LoopPhase.OBSERVE, LoopPhase.CLASSIFY, LoopPhase.HYPOTHESIZE]
        trace_list = phase_trace or list(phase_list)
        disc_map = discarded_hypotheses or {}

        result = LoopResult(
            run_id=spec.run_id,
            state=state,
            decision=decision,
            iterations=iterations,
            reason=reason,
            phases=phase_list,
            phase_trace=trace_list,
            discarded_hypotheses=disc_map,
            rejected_hypotheses=disc_map,
            discard_reasons=disc_map,
        )

        artifact_payload = {
            "run_id": spec.run_id,
            "skill": spec.skill,
            "state": state.value,
            "decision": decision.value,
            "reason": reason,
            "baseline": metrics_dict(spec.baseline),
            "phases": [p.value if isinstance(p, LoopPhase) else p for p in phase_list],
            "phase_trace": [p.value if isinstance(p, LoopPhase) else p for p in trace_list],
            "discarded_hypotheses": disc_map,
            "rejected_hypotheses": disc_map,
            "discard_reasons": disc_map,
            "iterations": [
                {
                    "number": item.number,
                    "hypothesis": item.hypothesis,
                    "decision": item.decision.value if item.decision else None,
                    "reason": item.reason,
                    "metrics": metrics_dict(item.metrics) if item.metrics else None,
                }
                for item in iterations
            ],
        }

        sanitized_artifact = ArtifactSanitizer.sanitize_dict(artifact_payload)
        self.artifacts.save(spec.run_id, sanitized_artifact)

        if self.state_store:
            self.state_store.update_run_status(
                spec.run_id,
                state=state,
                decision=decision,
                reason=reason,
                phases=phase_list,
            )

        return result
