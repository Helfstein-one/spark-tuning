from __future__ import annotations

import argparse
import json
from pathlib import Path

from .adapters import FixtureExperimentRunner, JsonArtifactStore, load_spec
from .engine import LoopEngine


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an evidence-driven Spark tuning loop")
    parser.add_argument("command", choices=["run"])
    parser.add_argument("spec", type=Path)
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts/runs"))
    args = parser.parse_args()

    spec = load_spec(args.spec)
    result = LoopEngine(FixtureExperimentRunner(), JsonArtifactStore(args.artifacts)).run(spec)
    print(json.dumps(result.to_dict(), indent=2, default=str))
    return 0 if result.decision.value == "accept" else 2


if __name__ == "__main__":
    raise SystemExit(main())
