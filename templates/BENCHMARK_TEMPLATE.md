# Benchmark: `<benchmark-id>`

## Identidade

```yaml
id: "<benchmark-id>"
workload: "<name>"
reference_framework: "pandas-on-spark"
candidate_framework: "pyspark"
dataset_id: "<dataset>"
seed: 42
scale: "small"
repetitions: 5
```

## Objetivo

Medir `<latência | custo | throughput | estabilidade | qualidade>`.

## Controle de variáveis

- mesmo dataset;
- mesma seed;
- mesmo resultado esperado;
- mesma região;
- mesmo runtime quando comparável;
- mesmo perfil de máquina;
- cache frio e quente identificados;
- sem concorrência externa não registrada.

## Casos

- normal;
- skew;
- small files;
- alta cardinalidade;
- NULLs;
- escala crescente;
- falha/retry.

## Saída mínima

```json
{
  "benchmark_id": "<id>",
  "framework": "<framework>",
  "runtime": "<runtime>",
  "duration_seconds": 0,
  "cost_usd": 0,
  "shuffle_read_bytes": 0,
  "shuffle_write_bytes": 0,
  "spill_disk_bytes": 0,
  "gc_seconds": 0,
  "task_p50_seconds": 0,
  "task_p95_seconds": 0,
  "correctness": true,
  "checksum": "<sha256>"
}
```

## Decisão

- **pass:** melhora o objetivo sem quebrar guardrails;
- **fail:** regressão ou saída incorreta;
- **inconclusive:** ruído insuficiente para decidir.
