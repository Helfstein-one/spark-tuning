# Skill: parquet-layout

## Detect

Use bytes read, predicate pruning, file sizes, row groups and listing time.

## Hypotheses

- write only required columns;
- select compression based on CPU/I/O;
- compact small files;
- partition by selective, frequently filtered columns.

## Reference

Spark SQL reading and writing Parquet.

## Gates

- schema and checksum unchanged;
- bytes read decreases;
- file sizes stay within target range;
- maintenance cost is recorded.
