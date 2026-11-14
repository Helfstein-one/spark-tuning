#!/usr/bin/env bash
set -euo pipefail

required=(
  README.md
  CHANGELOG.md
  docs/engineering-loop.md
  docs/pr-safety.md
  docs/skill-lifecycle.md
  templates/LOOP_RUN_TEMPLATE.md
  .github/workflows/skills-validation.yml
)

for path in "${required[@]}"; do
  test -f "$path" || { echo "missing required file: $path" >&2; exit 1; }
done

grep -q "max_iterations" templates/LOOP_RUN_TEMPLATE.md
grep -q "rollback" docs/pr-safety.md
grep -q "correctness" docs/engineering-loop.md
grep -q "PullRequest" docs/engineering-loop.md
test -f pyproject.toml
test -f src/spark_tuning/engine.py
PYTHONPATH=src python -m compileall -q src tests
PYTHONPATH=src python -m unittest discover -s tests -q

git diff --check
echo "loop contract valid"
