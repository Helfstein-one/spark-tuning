# Arquitetura exagonal do Spark Tuning

## 1. Objetivo arquitetural

O núcleo de decisão não deve depender de Spark, AWS, Databricks, Terraform ou de uma ferramenta de observabilidade específica. Essas tecnologias são adaptadores externos.

O domínio recebe sinais normalizados, formula hipóteses, seleciona experimentos e decide com base em evidências.

```text
                         Adaptadores externos
 ┌─────────────────────────────────────────────────────────────┐
 │ Spark UI │ sparkMeasure │ Cloud metrics │ Terraform │ APIs   │
 └──────────┬────────────┬────────────────┬───────────┬────────┘
            │            │                │           │
            ▼            ▼                ▼           ▼
       Inbound adapters / collectors / parsers
                         │
                         ▼
              ┌────────────────────────┐
              │       PORTAS           │
              │ metrics, plans, cost,  │
              │ quality, lineage, run  │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │       DOMÍNIO           │
              │ diagnosis               │
              │ hypothesis              │
              │ experiment              │
              │ decision                │
              │ learning                │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ PORTAS DE SAÍDA         │
              │ benchmark runner        │
              │ config applier          │
              │ report writer           │
              │ regression gate         │
              └────────────┬───────────┘
                           │
                           ▼
                Spark / cloud / storage / CI
```

## 2. Camadas

### Domínio

Regras puras e independentes de infraestrutura:

- classificação de gargalo;
- severidade;
- confiança;
- formulação de hipótese;
- seleção de experimento;
- cálculo de delta;
- critérios de aceitação;
- decisão e rollback;
- aprendizado de evidências anteriores.

### Aplicação

Orquestra casos de uso:

- `CreateBaseline`;
- `DiagnoseWorkload`;
- `GenerateHypotheses`;
- `RunExperiment`;
- `CompareRuns`;
- `ValidateCorrectness`;
- `EvaluateDecision`;
- `PublishEvidence`.

### Portas

Contratos abstratos:

- `MetricsReader`;
- `PlanReader`;
- `CostReader`;
- `QualityChecker`;
- `LineageWriter`;
- `BenchmarkRunner`;
- `ConfigurationProvider`;
- `ArtifactStore`;
- `DecisionRepository`.

### Adaptadores

Implementações concretas:

- Spark event log;
- SparkMeasure;
- Spark SQL UI export;
- AWS CloudWatch/Glue/EMR;
- Google Cloud Monitoring/Dataproc;
- Azure Monitor/Databricks/Synapse;
- Prometheus;
- Terraform;
- Parquet/Delta/Iceberg;
- GitHub Actions.

## 3. Estrutura de pastas

```text
src/
├── domain/
│   ├── entities/
│   │   ├── workload.py
│   │   ├── run.py
│   │   ├── hypothesis.py
│   │   ├── diagnosis.py
│   │   └── decision.py
│   ├── value_objects/
│   │   ├── metric.py
│   │   ├── threshold.py
│   │   ├── confidence.py
│   │   └── fingerprint.py
│   ├── services/
│   │   ├── bottleneck_classifier.py
│   │   ├── hypothesis_generator.py
│   │   ├── experiment_selector.py
│   │   └── decision_evaluator.py
│   └── rules/
│       ├── correctness_rules.py
│       ├── performance_rules.py
│       └── safety_rules.py
├── application/
│   ├── use_cases/
│   ├── commands/
│   └── dto/
├── ports/
│   ├── inbound/
│   └── outbound/
└── adapters/
    ├── inbound/
    │   ├── cli/
    │   ├── ci/
    │   └── api/
    └── outbound/
        ├── spark/
        ├── cloud/
        ├── storage/
        ├── observability/
        └── terraform/
```

## 4. Invariantes

- o domínio não importa SDK de cloud;
- um adaptador não decide aceitação sozinho;
- toda decisão referencia baseline e candidate;
- toda recomendação possui hipótese e rollback;
- toda melhoria de performance passa por correção;
- ausência de métrica é `unknown`, nunca `zero`;
- falha de coleta é erro explícito, nunca sucesso vazio.
