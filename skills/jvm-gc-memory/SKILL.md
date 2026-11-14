# Skill: jvm-gc-memory

## Detect

Use heap, overhead, allocation, GC, spill and OOM metrics.

## Hypotheses

- reduce object creation;
- remove unnecessary caches;
- validate Kryo;
- resize partitions before increasing heap;
- separate heap pressure from off-heap pressure.

## Reference

Scala Spark for JVM behavior, with a PySpark equivalent when needed.

## Gates

- zero OOM;
- Full GC ratio below policy;
- spill does not regress;
- result remains correct.
