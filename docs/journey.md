# Jornada completa de uso

## 1. Preparar

1. escolher o workload;
2. identificar entrada, saída e SLO;
3. escolher o framework de referência;
4. registrar runtime, cloud, máquina e commit;
5. criar ou selecionar dataset versionado;
6. definir checks de correção;
7. definir orçamento e limites de risco.

## 2. Observar

Coletar sem alterar o workload:

- plano;
- configuração;
- duração;
- stages;
- tasks;
- input/output;
- shuffle;
- spill;
- GC;
- CPU/memória/rede/disco;
- retries;
- custo;
- qualidade da saída.

## 3. Diagnosticar

O diagnóstico deve produzir:

```yaml
diagnosis:
  primary_bottleneck: "shuffle"
  secondary_bottlenecks: ["skew", "spill"]
  evidence:
    - metric: "shuffle_write_bytes"
      value: 500000000000
    - metric: "p99_task_seconds"
      value: 920
  confidence: 0.91
  unknowns:
    - "cardinality of customer_id after filter"
```

## 4. Formular hipóteses

Gerar hipóteses ordenadas por:

1. impacto potencial;
2. confiança;
3. baixo risco;
4. custo do experimento;
5. reversibilidade;
6. proximidade da causa raiz.

## 5. Experimentar

Alterar uma variável por vez, salvo experimento fatorial explicitamente documentado.

Cada experimento deve ter:

- baseline;
- mudança;
- previsão;
- métricas;
- guardrails;
- tempo limite;
- rollback.

## 6. Verificar

Executar nesta ordem:

1. validade do plano;
2. correção;
3. estabilidade;
4. performance;
5. custo;
6. generalização em outra escala.

## 7. Decidir

- `accept`: melhora comprovada;
- `reject`: não melhorou;
- `rollback`: risco ou regressão;
- `inconclusive`: evidência insuficiente.

## 8. Aprender

Salvar:

- sintomas;
- métricas;
- hipótese;
- mudança;
- resultado;
- ambiente;
- versão;
- decisão;
- limites de validade.

Uma decisão aceita vira conhecimento reutilizável, não configuração global cega.
