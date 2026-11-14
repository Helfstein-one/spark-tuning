# Skill: data-quality-gate

## Detect

Run before accepting any performance candidate.

## Checks

- schema;
- row count;
- nullability;
- key cardinality;
- domain constraints;
- checksum;
- freshness and completeness.

## Reference

Deequ or Great Expectations, with a deterministic local fallback.

## Rule

Missing quality evidence is `unknown` and blocks promotion. It is never treated as success.
