# Skills complementares identificadas

## 1. Por que incorporar skills adjacentes

O tuning de Spark pode produzir um job mais rápido, porém ainda falhar por:

- dado incorreto;
- lineage ausente;
- custo não atribuído;
- regressão silenciosa;
- falta de contexto do dataset;
- falha de infraestrutura;
- ausência de teste de recuperação;
- código Python inconsistente;
- métricas não correlacionadas entre sistemas.

A solução deve tratar performance como uma propriedade de produto de dados, não como uma coleção de parâmetros.

## 2. Matriz de incorporação

| Skill externa | Padrão observado | Como melhorar o projeto | Prioridade |
|---|---|---|---|
| OpenLineage | evento padronizado de execução e lineage | adicionar `lineage_run_id`, inputs, outputs e facets ao run | alta |
| Marquez | coletar, agregar e visualizar metadados | backend local para consultar histórico de runs | média |
| DataHub | catálogo e contexto de ativos | registrar owner, schema, tags, uso e criticidade do dataset | média |
| OpenTelemetry Collector | pipeline de métricas, logs e traces | padronizar exportação multi-cloud | alta |
| sparkMeasure | task/stage metrics e Flight Recorder | adaptador de métricas e comparação no CI | alta |
| Deequ | unit tests de dados em escala | gate de correção antes do gate de performance | alta |
| Great Expectations | expectativas declarativas e profiling | templates de contrato por dataset | média |
| Chaos Monkey | falhas controladas e resiliência | stress tests de executor, Spot, retry e perda de nó | média |
| Ruff | lint e análise estática rápidos | qualidade de scripts PySpark e geradores | média |
| Spark Operator | execução declarativa Kubernetes | manifestos reprodutíveis e lifecycle controlado | média |

## 3. Novas skills do catálogo

### `lineage-context`

Relaciona código, dataset, runtime, plano, execução, custo e decisão.

**Aceitação:**

- `lineage_run_id` presente;
- inputs e outputs identificados;
- schema e versão registrados;
- sem dados sensíveis nos eventos;
- consulta reproduzível do histórico.

### `telemetry-correlation`

Correlaciona métricas Spark, JVM, cloud, storage e CI usando um `run_id` comum.

**Aceitação:**

- timestamps normalizados;
- métricas com unidade;
- fonte e qualidade do sinal;
- ausência explícita marcada como `unknown`;
- exportação local e remota.

### `data-quality-gate`

Impede que uma melhoria de performance com resultado incorreto seja aceita.

**Aceitação:**

- row count;
- checksum;
- schema;
- nullability;
- cardinalidade;
- expectativas críticas;
- falha explícita.

### `resilience-chaos`

Verifica comportamento sob falhas esperadas.

**Casos:**

- executor perdido;
- Spot interrompido;
- retry;
- storage lento;
- fonte indisponível;
- limite de memória;
- concorrência.

**Aceitação:**

- recuperação dentro do SLO;
- nenhum dado duplicado;
- nenhuma perda silenciosa;
- custo de recuperação medido.

### `dataset-context`

Mantém o contexto necessário para escolher o tuning correto:

- tamanho;
- cardinalidade;
- distribuição;
- SLA;
- owner;
- criticidade;
- padrão de leitura;
- retenção;
- custo;
- consumidores.

### `static-quality`

Valida scripts e configuração antes de executar benchmarks caros:

- lint;
- formatação;
- tipos;
- dependências;
- imports;
- regras de segurança;
- compatibilidade de runtime.

## 4. Padrão de eventos

```json
{
  "run_id": "run-2026-001",
  "parent_run_id": null,
  "job": "orders",
  "framework": "pyspark",
  "runtime": "emr-7.x",
  "inputs": ["s3://lake/orders"],
  "outputs": ["s3://lake/orders_curated"],
  "lineage": {
    "namespace": "spark-tuning",
    "name": "orders"
  },
  "metrics_source": "sparkmeasure",
  "quality": {
    "status": "passed",
    "checksum": "sha256:..."
  },
  "decision": "accept"
}
```

## 5. Ordem recomendada de implementação

1. `data-quality-gate`;
2. `telemetry-correlation`;
3. `lineage-context`;
4. `static-quality`;
5. `resilience-chaos`;
6. `dataset-context`;
7. integração visual com Marquez/DataHub.

Essa ordem reduz o risco de otimizar uma execução sem saber se o resultado está correto, observável ou reproduzível.
