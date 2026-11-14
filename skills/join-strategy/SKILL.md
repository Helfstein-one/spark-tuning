# Skill: join-strategy

## Detect

Prioritize when a join stage dominates runtime, shuffle, spill or task skew.

## Hypotheses

- broadcast a validated small dimension;
- filter and project before joining;
- use sort-merge for large distributed inputs;
- salt hot keys only when skew evidence exists.

## Reference

Spark SQL, with pandas API on Spark as the tabular baseline.

## Gates

- output row count and checksum unchanged;
- zero executor OOM;
- shuffle or duration improves by at least 10%;
- p95 task regression below 20%.

## Rollback

Remove the hint or restore the previous join plan.
