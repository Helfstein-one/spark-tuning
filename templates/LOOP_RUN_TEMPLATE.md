# Loop run: `<run-id>`

## Escopo

```yaml
run_id: "<run-id>"
skill: "<skill-name>"
branch: "tune/<skill>/<run-id>"
base_commit: "<sha>"
owner: "<owner>"
```

## Objetivo

- SLO:
- métrica primária:
- melhoria mínima:
- orçamento:

## Baseline

- framework:
- runtime:
- dataset:
- seed:
- plano:
- duração:
- custo:
- checksum:

## Limites

```yaml
max_iterations: 7
max_total_cost_usd: 200
max_runtime_minutes: 120
max_changes_per_iteration: 1
```

## Histórico

| Iteração | Hipótese | Mudança | Resultado | Decisão |
|---|---|---|---|---|
| 001 |  |  |  |  |

## Critérios para PR

- [ ] baseline imutável;
- [ ] candidate reproduzível;
- [ ] correção aprovada;
- [ ] benchmark repetido;
- [ ] custo dentro do limite;
- [ ] CI verde;
- [ ] rollback descrito;
- [ ] diff revisado;
- [ ] descrição da PR pronta.
