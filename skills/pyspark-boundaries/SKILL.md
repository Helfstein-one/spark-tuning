# Skill: pyspark-boundaries

## Detect

Use Python time, UDF time, serialization, Arrow and executor CPU metrics.

## Hypotheses

- replace scalar Python UDFs with built-ins;
- use pandas UDF only when measured;
- avoid `collect` and `toPandas`;
- reduce Python/JVM crossings.

## Reference

PySpark DataFrame, compared with pandas API on Spark when semantics match.

## Gates

- NULL semantics preserved;
- schema and checksum unchanged;
- Python time or duration improves;
- driver memory remains safe.
