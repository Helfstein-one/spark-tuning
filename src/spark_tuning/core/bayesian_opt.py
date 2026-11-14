"""Bayesian optimization and adaptive active search for Spark hyperparameters."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass
class HyperparameterSpace:
    param_name: str
    min_value: int
    max_value: int
    step: int = 1


@dataclass
class TrialObservation:
    value: int
    score: float  # Objective to minimize (e.g. duration or composite cost)


class BayesianOptimizer:
    """1D Gaussian Process surrogate with Expected Improvement (EI) for Spark hyperparameter tuning."""

    def __init__(
        self,
        space: HyperparameterSpace,
        exploration_weight: float = 1.96,
        seed: int = 42,
    ):
        self.space = space
        self.exploration_weight = exploration_weight
        self.observations: List[TrialObservation] = []
        self.rng = random.Random(seed)

    def observe(self, value: int, score: float) -> None:
        self.observations.append(TrialObservation(value=value, score=score))

    def _radial_basis_kernel(self, x1: float, x2: float, length_scale: float = 50.0) -> float:
        return math.exp(-((x1 - x2) ** 2) / (2 * (length_scale ** 2)))

    def _predict(self, x: float) -> Tuple[float, float]:
        """Simple GP regression returning predicted mean and uncertainty (variance)."""
        if not self.observations:
            return 0.0, 1.0

        weights = [self._radial_basis_kernel(x, obs.value) for obs in self.observations]
        total_weight = sum(weights)

        if total_weight < 1e-6:
            mean = sum(obs.score for obs in self.observations) / len(self.observations)
            variance = 1.0
            return mean, variance

        mean = sum(w * obs.score for w, obs in zip(weights, self.observations)) / total_weight
        min_dist = min(abs(x - obs.value) for obs in self.observations)
        normalized_dist = min(1.0, min_dist / (self.space.max_value - self.space.min_value + 1e-6))
        variance = normalized_dist

        return mean, variance

    def suggest_next(self) -> int:
        """Returns the next candidate parameter value maximizing Upper Confidence Bound / Acquisition."""
        valid_range = list(
            range(self.space.min_value, self.space.max_value + 1, self.space.step)
        )
        if not self.observations:
            # First suggestion: midpoint
            return valid_range[len(valid_range) // 2]

        observed_vals = {obs.value for obs in self.observations}
        untested = [v for v in valid_range if v not in observed_vals]

        if not untested:
            return valid_range[len(valid_range) // 2]

        # Acquisition function: Minimize Mean - (exploration_weight * uncertainty)
        best_val = untested[0]
        best_acquisition = float("inf")

        for cand in untested:
            pred_mean, uncertainty = self._predict(cand)
            acquisition_score = pred_mean - (self.exploration_weight * uncertainty * 100.0)
            if acquisition_score < best_acquisition:
                best_acquisition = acquisition_score
                best_val = cand

        return best_val
