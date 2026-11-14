# Skill: partition-sizing

## Detect

Use task duration, bytes per task, empty partitions and scheduler delay.

## Hypotheses

- adjust shuffle partitions;
- use AQE coalescing;
- avoid `coalesce(1)`;
- target stable file and task sizes.

## Reference

PySpark DataFrame and Spark SQL.

## Gates

- p95/p99 task duration improves;
- no new skew;
- output file count remains within policy;
- correctness remains unchanged.
