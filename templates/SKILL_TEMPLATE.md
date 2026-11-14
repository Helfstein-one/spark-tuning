# Skill: `<skill-name>`

## 1. Identidade

- **ID:** `skill-name`
- **Categoria:** `engine | language | storage | cloud | observability | validation`
- **Status:** `draft | experimental | validated | deprecated`
- **Owner:** `<owner>`
- **Última revisão:** `YYYY-MM-DD`
- **Versões testadas:** `<Spark/runtime/framework versions>`

## 2. Objetivo

Descrever o problema de performance que esta skill resolve e o resultado mensurável esperado.

## 3. Escopo

### Inclui

- `<caso 1>`
- `<caso 2>`

### Não inclui

- `<caso fora do escopo>`

## 4. Pré-condições

- dataset identificado e versionado;
- baseline executado;
- plano físico armazenado;
- runtime e infraestrutura registrados;
- critério de correção definido;
- limite de custo definido.

## 5. Sintomas observáveis

| Sintoma | Métrica | Limite de investigação |
|---|---|---|
| `<sintoma>` | `<métrica>` | `<threshold>` |

## 6. Hipótese

> Se `<mudança controlada>`, então `<resultado esperado>`, porque `<mecanismo técnico>`.

## 7. Benchmark de referência

- **Framework primário:** `<pandas-on-Spark/Koalas, PySpark, Spark SQL, Scala, Glue, EMR, Databricks>`
- **Implementação baseline:** `<caminho>`
- **Implementação candidate:** `<caminho>`
- **Dataset:** `<id, escala, seed, distribuição>`
- **Ações materializadas:** `<count/write/query>`
- **Repetições:** `<n>`

### Regra de seleção do framework

Use o framework que melhor representa a API e o custo investigado:

| Domínio | Referência padrão |
|---|---|
| API pandas distribuída | pandas API on Spark, antiga Koalas |
| DataFrame Python nativo | PySpark DataFrame |
| Otimização de consulta | Spark SQL |
| Código JVM | Scala Spark |
| AWS gerenciado | Glue/EMR equivalente |
| Databricks | Databricks Runtime/SQL |
| Tabela e layout | Delta/Iceberg + Spark SQL |
| Execução Kubernetes | Spark Operator |

Não comparar APIs semanticamente diferentes sem registrar a diferença de comportamento.

## 8. Procedimento

1. executar baseline;
2. guardar plano, configuração e métricas;
3. aplicar uma mudança;
4. executar candidate;
5. validar correção;
6. comparar duração, custo, recursos e estabilidade;
7. repetir para reduzir ruído;
8. registrar decisão.

## 9. Métricas obrigatórias

- duração total;
- p50/p95/p99 de task;
- input/output bytes;
- shuffle read/write;
- spill memory/disk;
- GC;
- CPU e memória;
- retries e falhas;
- custo;
- qualidade da saída;
- plano antes/depois.

## 10. Critérios de aceitação

```text
correctness == true
failures == 0
duration_delta <= -10%
ou cost_delta <= -10%
p99_delta <= +20%
spill_delta <= +20%
```

## 11. Riscos e contraindicações

- `<risco>`
- `<quando não usar>`

## 12. Rollback

Descrever como remover a mudança e restaurar a configuração anterior.

## 13. Artefatos

- `baseline/`
- `candidate/`
- `plans/`
- `metrics/`
- `results/`
- `decision.yaml`

## 14. Fontes

- `<documentação oficial>`
- `<issue ou paper>`
- `<benchmark reproduzível>`
