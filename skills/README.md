# Catálogo de skills

Cada diretório de skill deve conter `SKILL.md`, benchmark, exemplos, testes de correção e referências.

## Contrato de cada diretório

```text
skills/<skill-name>/
├── SKILL.md
├── README.md
├── hypothesis.yaml
├── benchmark/
│   ├── baseline.yaml
│   ├── candidate.yaml
│   └── expected.json
├── examples/
│   ├── before.py
│   └── after.py
├── tests/
│   ├── test_correctness.py
│   └── test_metrics.py
└── references.md
```

`SKILL.md` segue `templates/SKILL_TEMPLATE.md`. Todo exemplo deve ter uma versão **antes**, uma versão **depois**, a hipótese, métricas e decisão. O arquivo `templates/OPTIMIZATION_BEFORE_AFTER.md` é o padrão para documentar essa comparação.

| Skill | Responsabilidade | Referência de teste |
|---|---|---|
| `baseline-design` | definir baseline, controle de variáveis e métricas | pandas API on Spark para pipeline tabular |
| `workload-classification` | classificar CPU, I/O, memória, rede e shuffle | PySpark DataFrame + Spark UI |
| `plan-inspection` | interpretar plano lógico, físico e adaptativo | Spark SQL `EXPLAIN FORMATTED` |
| `partition-sizing` | escolher paralelismo e tamanho de partição | PySpark DataFrame com escalas small/large |
| `join-strategy` | comparar broadcast, sort-merge e hash join | Spark SQL com dimensão controlada |
| `aqe-tuning` | testar coalescing, skew join e troca de estratégia | Spark SQL com AQE on/off |
| `pyspark-boundaries` | reduzir UDF, Python/JVM e materialização | PySpark built-ins versus pandas UDF |
| `scala-spark` | medir JVM, GC, serialização e closures | Scala Dataset/DataFrame |
| `spark-sql` | otimizar expressões, filtros, agregações e joins | Spark SQL |
| `pandas-on-spark` | validar API pandas distribuída e custo de índice | pandas API on Spark/Koalas |
| `parquet-layout` | otimizar arquivos, row groups e pruning | Spark SQL + Parquet |
| `delta-iceberg-layout` | testar snapshots, clustering e manutenção | Delta/Iceberg |
| `jvm-gc-memory` | evitar GC, OOM e pressão de heap/off-heap | Scala ou PySpark equivalente |
| `spill-control` | reduzir spill de memória/disco | workload com shuffle |
| `skew-detection` | detectar e corrigir chaves hot | join/groupBy skewed |
| `cloud-compute` | escolher CPU, RAM, rede e disco | runtime cloud equivalente |
| `autoscaling-policy` | validar backlog, p95, scale-out e scale-in | EMR/Dataproc/Databricks |
| `spot-and-fleet` | medir custo efetivo de interrupções | EMR/Dataproc/Azure Spot |
| `terraform-infrastructure` | reproduzir infraestrutura e controlar drift | `terraform plan` + smoke job |
| `glue-runtime` | otimizar workers, bookmarks e DPU | AWS Glue |
| `emr-runtime` | otimizar release, fleets e managed scaling | Amazon EMR |
| `emr-serverless` | otimizar cold start e capacidade | EMR Serverless |
| `databricks-runtime` | validar Photon, Delta, DBU e SQL UI | Databricks |
| `dataproc-runtime` | validar workers, GCS e autoscaling | Google Dataproc |
| `synapse-runtime` | validar pools, ADLS e autoscale | Azure Synapse |
| `metrics-collection` | coletar métricas reproduzíveis por execução | sparkMeasure |
| `data-quality` | validar resultado antes de aceitar ganho | Deequ/Great Expectations |
| `lineage-cost` | relacionar código, dados, run e custo | OpenLineage |
| `lineage-context` | preservar contexto de inputs, outputs e owners | OpenLineage/DataHub |
| `telemetry-correlation` | correlacionar Spark, JVM, cloud e CI | OpenTelemetry Collector |
| `data-quality-gate` | bloquear resultados incorretos | Deequ/Great Expectations |
| `resilience-chaos` | testar perda, retry e recuperação | Chaos Monkey/policies cloud |
| `dataset-context` | usar tamanho, SLA, cardinalidade e consumidores | DataHub/catalog |
| `static-quality` | validar scripts e configurações antes do benchmark | Ruff/type checks |
| `benchmark-generation` | gerar dados com seed e distribuições | gerador interno + TPC-DS |
| `stress-testing` | testar escala, skew, falhas e concorrência | workload parametrizado |
| `regression-gate` | bloquear regressões de performance e qualidade | CI |

## Regra de baseline

O benchmark padrão para APIs tabulares distribuídas é **pandas API on Spark**, sucessora de Koalas. Quando a skill não for sobre essa API, o baseline muda para o framework que representa o problema:

- engine SQL: Spark SQL;
- API PySpark: DataFrame API;
- JVM: Scala Spark;
- cloud: runtime gerenciado correspondente;
- storage: Spark SQL lendo/escrevendo Parquet, Delta ou Iceberg;
- operação: manifesto do Spark Operator ou Terraform.

O resultado deve sempre declarar qual baseline foi usado e por que ele é semanticamente comparável.
