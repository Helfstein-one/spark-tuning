from __future__ import annotations

from .adapters import metrics_dict
from .evaluator import evaluate
from .models import Decision, Iteration, LoopResult, RunSpec, RunState
from .ports import ArtifactStore, ExperimentRunner


class LoopEngine:
    def __init__(self, runner: ExperimentRunner, artifacts: ArtifactStore):
        self.runner = runner
        self.artifacts = artifacts

    def run(self, spec: RunSpec) -> LoopResult:
        iterations: list[Iteration] = []
        total_cost = 0.0
        if not spec.hypotheses:
            return self._finish(spec, RunState.BLOCKED, Decision.BLOCKED, iterations, "no hypotheses")

        for number, hypothesis in enumerate(spec.hypotheses[: spec.max_iterations], start=1):
            iteration = Iteration(number=number, hypothesis=str(hypothesis.get("id", number)))
            try:
                metrics = self.runner.run(spec, number, hypothesis)
            except (RuntimeError, ValueError, OSError) as exc:
                iteration.decision = Decision.ROLLBACK
                iteration.reason = f"experiment failed: {exc}"
                iterations.append(iteration)
                return self._finish(spec, RunState.ROLLBACK, Decision.ROLLBACK, iterations, iteration.reason)

            iteration.metrics = metrics
            if metrics.cost_usd is not None:
                total_cost += metrics.cost_usd
            evaluation = evaluate(spec, metrics)
            iteration.decision = evaluation.decision
            iteration.reason = evaluation.reason
            iterations.append(iteration)

            if total_cost > spec.max_total_cost_usd:
                return self._finish(spec, RunState.BLOCKED, Decision.BLOCKED, iterations, "cost limit exceeded")
            if evaluation.decision is Decision.ACCEPT:
                return self._finish(spec, RunState.PR_READY, Decision.ACCEPT, iterations, evaluation.reason)
            if evaluation.decision is Decision.ROLLBACK:
                continue

        reason = "iteration limit reached without accepted candidate"
        return self._finish(spec, RunState.BLOCKED, Decision.BLOCKED, iterations, reason)

    def _finish(
        self,
        spec: RunSpec,
        state: RunState,
        decision: Decision,
        iterations: list[Iteration],
        reason: str,
    ) -> LoopResult:
        result = LoopResult(spec.run_id, state, decision, iterations, reason)
        self.artifacts.save(
            spec.run_id,
            {
                "run_id": spec.run_id,
                "skill": spec.skill,
                "state": state,
                "decision": decision,
                "reason": reason,
                "baseline": metrics_dict(spec.baseline),
                "iterations": [
                    {
                        "number": item.number,
                        "hypothesis": item.hypothesis,
                        "decision": item.decision,
                        "reason": item.reason,
                        "metrics": metrics_dict(item.metrics) if item.metrics else None,
                    }
                    for item in iterations
                ],
            },
        )
        return result
