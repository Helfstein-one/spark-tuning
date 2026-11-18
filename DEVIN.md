# Spark Tuning — Devin Skill & Memory Pack

> **Objetivo:** este arquivo é um pacote único, copiável e portátil para orientar Devin, Cursor, Copilot, Antigravity ou outro agente a diagnosticar, testar e propor otimizações de Apache Spark com evidência, segurança e rollback.

---

## 0. Como usar este arquivo

1. Copie este `DEVIN.md` para o workspace do projeto alvo.
2. Leia primeiro as seções **Regras invioláveis**, **Memória** e **Loop cognitivo**.
3. Execute os comandos de validação antes de alterar código.
4. Trabalhe sempre em branch isolada.
5. Use uma skill por hipótese e uma mudança por iteração.
6. Abra PR somente depois de todos os gates passarem.

Este arquivo é a versão operacional resumida. A documentação técnica detalhada permanece em:

- `docs/reference-playbook.md`
- `docs/engineering-loop.md`
- `docs/cognitive-loop.md`
- `docs/pr-safety.md`
- `docs/architecture.md`
- `skills/README.md`

---

## 1. Identidade do produto

**Nome:** Spark Tuning
**Tipo:** engine de performance engineering + catálogo de skills
**Domínio:** Apache Spark, PySpark, Spark SQL, Scala, pandas API on Spark/Koalas, Parquet, Delta, Iceberg, AWS, GCP, Azure, Databricks e Terraform.
**Princípio:** nenhuma configuração é universalmente melhor; ela é melhor apenas para um workload, versão, escala, distribuição, ambiente e orçamento conhecidos.

### Resultado esperado

O agente deve sair de:

```text
job lento/caro/instável
```

para:

```text
diagnóstico evidenciado
→ hipótese falsificável
→ experimento isolado
→ resultado correto e reproduzível
→ decisão explicável
→ PR segura
```

---

## 2. Regras invioláveis

### Segurança de código

- Nunca trabalhar diretamente em `main`.
- Nunca sobrescrever ou alterar o baseline original.
- Nunca fazer merge automático.
- Nunca abrir PR com correção, CI ou guardrail falhando.
- Nunca misturar tuning com refatoração não relacionada.
- Nunca aplicar múltiplas mudanças sem declarar experimento fatorial.
- Nunca converter métrica ausente em zero.
- Nunca considerar `unknown` como sucesso.
- Nunca expor secrets, tokens, PII, paths sensíveis ou dados de produção em artifacts.
- Nunca executar benchmark destrutivo em produção por padrão.

### Segurança operacional

- Definir limite de iterações, tempo, custo, retries e tamanho do dataset.
- Usar dados sintéticos ou amostras anonimizadas.
- Preferir modo fixture/local antes de cloud.
- Fazer rollback ao primeiro sinal de corrupção, regressão ou risco.
- Parar e pedir aprovação humana quando houver alteração de infraestrutura produtiva, dados sensíveis, permissões ou orçamento fora do limite.

### Qualidade

Performance só é válida quando a saída continua correta:

```text
schema == equivalente
row_count == esperado
checksum == esperado
cardinalidade == esperada
NULL semantics == preservada
falhas == 0
```

---

## 3. Memória operacional do Devin

Use memória em três níveis. Nunca armazene secrets, dados pessoais ou conteúdo bruto de produção.

### 3.1 Memória do projeto

Guardar fatos estáveis:

```yaml
project_memory:
  name: spark-tuning
  default_branch: main
  python: ">=3.10"
  architecture: hexagonal
  cognitive_loop: O-C-H-E-M-V-D-L
  default_reference:
    tabular_distributed: pandas-api-on-spark
  validation:
    command: "bash scripts/validate_loop.sh"
    tests: "PYTHONPATH=src python -m unittest discover -s tests -v"
  artifact_policy:
    directory: artifacts/
    secrets: forbidden
```

### 3.2 Memória de workload

Guardar somente metadados e fingerprints:

```yaml
workload_memory:
  fingerprint: "<hash estável>"
  framework: "pyspark | spark-sql | scala | pandas-on-spark"
  runtime: "<spark/cloud/runtime>"
  input_profile:
    rows: 0
    bytes: 0
    cardinalities: {}
    skew: "<none|low|medium|high|unknown>"
    file_profile: "<small-files|balanced|large-files|unknown>"
  slo:
    duration_seconds: 0
    cost_usd: 0
  known_bottlenecks: []
  accepted_hypotheses: []
  rejected_hypotheses: []
```

### 3.3 Memória de experimento

Todo experimento deve registrar:

```yaml
experiment_memory:
  run_id: "<id>"
  iteration: 0
  hypothesis_id: "<id>"
  baseline_run: "<id>"
  candidate_run: "<id>"
  change_summary: "<uma mudança>"
  evidence:
    plan_hash: "<hash>"
    metrics: {}
    quality: {}
  decision: "accept | reject | rollback | inconclusive | blocked"
  reason: "<explicação>"
  validity_limits:
    spark_versions: []
    runtimes: []
    dataset_ranges: []
  rollback: "<passo>"
```

### Regras de memória

- Não repetir hipótese rejeitada sem evidência nova.
- Não transportar uma recomendação para outro workload sem verificar fingerprint e validade.
- Rebaixar confiança quando houver drift de dados, runtime ou layout.
- Preferir uma memória curta e estruturada a longos logs.
- Persistir decisões rejeitadas: elas evitam ciclos inúteis.

---

## 4. Arquitetura mental

```text
Host do agente
  ↓
DEVIN.md + skill bundle + memória
  ↓
CLI/protocolo de execução
  ↓
casos de uso
  ↓
domínio cognitivo
  ├─ diagnóstico
  ├─ priorização
  ├─ avaliação
  ├─ qualidade
  └─ decisão
  ↓
adapters
  ├─ Spark/event logs
  ├─ cloud/custo
  ├─ storage
  ├─ lineage/telemetria
  └─ Git/PR
```

O domínio decide. Adapters apenas coletam, executam ou persistem.

---

## 5. Loop cognitivo O-C-H-E-M-V-D-L

```text
OBSERVE → CLASSIFY → HYPOTHESIZE → EXPERIMENT
   ↑                                      ↓
 LEARN ← DECIDE ← VERIFY ← MEASURE
```

### O — Observe

Coletar antes de sugerir:

- código/SQL e commit;
- plano lógico/físico/adaptativo;
- Spark/Python/Java/Scala/runtime;
- input/output bytes;
- stages/tasks;
- shuffle read/write;
- spill memory/disk;
- GC e memória;
- task p50/p95/p99;
- retries/falhas;
- layout e quantidade de arquivos;
- custo;
- qualidade da saída.

### C — Classify

Classificar a causa primária e secundárias:

```text
cpu
memory_gc
io
shuffle
spill
skew
driver
python_jvm
small_files
provisioning
quality
unknown
```

Se a evidência for insuficiente, retornar `unknown` e pedir coleta adicional.

### H — Hypothesize

Formato obrigatório:

> Se `<mudança>`, então `<métrica>` deve mudar de `<baseline>` para `<alvo>`, sem violar `<guardrails>`, porque `<mecanismo>`.

Uma hipótese precisa declarar:

- impacto esperado;
- confiança;
- risco;
- custo;
- reversibilidade;
- skill usada;
- framework de referência;
- experimento mínimo.

### E — Experiment

- Criar branch `tune/<skill>/<run-id>`.
- Aplicar uma única mudança.
- Guardar snapshot/rollback.
- Executar smoke test.
- Não tocar em produção.

### M — Measure

Comparar baseline e candidate com:

- duração;
- custo;
- throughput;
- shuffle;
- spill;
- GC;
- p95/p99;
- retries;
- utilização;
- tamanho/quantidade de arquivos;
- qualidade.

### V — Verify

Repetir quando necessário:

- cache frio;
- cache quente separado;
- múltiplas execuções;
- escala diferente;
- distribuição skewed;
- outro runtime quando a skill declarar compatibilidade;
- teste de falha quando houver Spot/retry/autoscaling.

### D — Decide

```text
accept       melhoria correta, estável e dentro do orçamento
reject       hipótese falsificada ou sem ganho
rollback     risco, corrupção, regressão ou falha
inconclusive variância/evidência insuficiente
blocked      limite, dependência ou permissão impede continuar
```

### L — Learn

Persistir:

- evidência;
- decisão;
- limites de validade;
- hipótese rejeitada;
- próxima ação;
- rollback.

---

## 6. Seleção de baseline

| Problema | Baseline recomendado |
|---|---|
| API tabular distribuída | pandas API on Spark/Koalas |
| DataFrame Python | PySpark DataFrame |
| consulta/plano | Spark SQL |
| código JVM | Scala Spark |
| layout | Spark + Parquet/Delta/Iceberg |
| AWS | Glue/EMR equivalente |
| GCP | Dataproc/BigQuery connector |
| Azure | Databricks/Synapse |
| Kubernetes | Spark Operator |
| infraestrutura | Terraform plan + smoke job |

O benchmark deve declarar equivalência de schema, NULLs, ordem, cardinalidade e determinismo. Não comparar somente duração entre implementações semanticamente diferentes.

---

## 7. Catálogo de skills

### Diagnóstico e execução

- `baseline-design`: criar baseline controlado.
- `workload-classification`: classificar recurso limitante.
- `plan-inspection`: interpretar plano físico e AQE.
- `metrics-collection`: coletar event logs, SparkMeasure e métricas.
- `benchmark-generation`: gerar dados com seed e distribuição.
- `stress-testing`: testar escala, skew, falhas e concorrência.

### Performance Spark

- `join-strategy`: broadcast, sort-merge, hash join e skew.
- `partition-sizing`: paralelismo, bytes/task e AQE.
- `aqe-tuning`: coalescing, skew join e troca de estratégia.
- `pyspark-boundaries`: UDF, Arrow e fronteiras Python/JVM.
- `scala-spark`: closures, serialização, codegen e JVM.
- `jvm-gc-memory`: heap, overhead, GC, cache e OOM.
- `spill-control`: spill de memória/disco e shuffle.
- `skew-detection`: chaves hot, salting e distribuição.
- `spark-sql`: filtros, agregações, joins e hints.
- `pandas-on-spark`: custo de índice e API distribuída.

### Storage

- `parquet-layout`: compressão, row groups, pruning e small files.
- `delta-iceberg-layout`: snapshots, clustering, schema e manutenção.
- `connector-pushdown`: JDBC, BigQuery, S3, GCS, ADLS e pushdown.

### Cloud e infraestrutura

- `cloud-compute`: CPU, RAM, rede e disco.
- `autoscaling-policy`: backlog, p95, scale-out/in e cold start.
- `spot-and-fleet`: interrupção, retry e custo efetivo.
- `glue-runtime`: workers, bookmarks e DPU.
- `emr-runtime`: release, fleets e managed scaling.
- `emr-serverless`: capacidade e inicialização.
- `databricks-runtime`: Photon, Delta, DBU e SQL UI.
- `dataproc-runtime`: workers, GCS e autoscaling.
- `synapse-runtime`: pools, ADLS e autoscale.
- `terraform-infrastructure`: infraestrutura reproduzível e drift.

### Segurança e confiança

- `data-quality-gate`: schema, row count, checksum e contratos.
- `lineage-context`: inputs, outputs, owners e versões.
- `telemetry-correlation`: Spark, JVM, cloud e CI.
- `resilience-chaos`: executor, Spot, retry e storage.
- `static-quality`: lint, tipos e segurança de código.
- `regression-gate`: promoção e bloqueio.

Cada skill deve possuir, quando implementada:

```text
SKILL.md
hypothesis.yaml
examples/before.py
examples/after.py
benchmark/baseline.yaml
benchmark/candidate.yaml
tests/test_correctness.py
tests/test_metrics.py
references.md
```

---

## 8. Padrões técnicos de alto valor

### Joins

- Filtrar/projetar antes do join.
- Validar tamanho da dimensão antes de broadcast.
- Detectar skew por distribuição de chave e p99/p50.
- Não usar `DISTINCT` para esconder duplicação.
- Verificar cardinalidade e checksum.

### Partições e shuffle

- Dimensionar por bytes/task e cores disponíveis.
- Evitar `coalesce(1)`.
- Usar AQE quando o plano e métricas justificarem.
- Corrigir skew antes de apenas aumentar executores.

### PySpark

- Preferir funções nativas.
- Evitar `collect()` e `toPandas()` sem limite.
- Medir pandas UDF e Arrow.
- Reduzir travessias Python/JVM.

### JVM e GC

- Reduzir criação de objetos.
- Evitar cache não reutilizado.
- Validar Kryo.
- Separar heap, off-heap e overhead.
- Não aumentar heap sem evidência.

### Storage

- Usar Parquet/Delta/Iceberg conforme o caso.
- Controlar small files e arquivos gigantes.
- Particionar por colunas seletivas e filtradas.
- Escolher Snappy/ZSTD por trade-off CPU/I/O.
- Medir bytes lidos versus bytes usados.

---

## 9. Contrato de benchmark

```yaml
benchmark:
  id: "<id>"
  reference_framework: "<framework>"
  candidate_framework: "<framework>"
  dataset_id: "<id>"
  seed: 42
  repetitions: 5
  cache: "cold | warm | both"
  correctness:
    schema: required
    row_count: required
    checksum: required
  guardrails:
    failures: 0
    max_cost_delta: 0.10
    max_p95_delta: 0.20
    max_spill_delta: 0.20
```

### Regra de aceitação

```text
correctness == true
failures == 0
evidence_complete == true
duration_delta <= -10%
ou cost_delta <= -10%
p95_delta <= +20%
spill_delta <= +20%
resultado reproduzível
```

Se não houver evidência suficiente, o resultado é `inconclusive`, nunca `accept`.

---

## 10. Comandos do projeto

### Validação completa

```bash
bash scripts/validate_loop.sh
```

### Testes

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

### Loop fixture local

```bash
PYTHONPATH=src python -m spark_tuning.cli run \
  examples/runs/demo.json \
  --artifacts artifacts/runs
```

### Runner Spark real

O adapter real usa `spark-submit`, event log e artifacts em Python.
A CLI atual está orientada ao fixture runner; use o exemplo fixture até o protocolo hospedeiro expor
a seleção explícita do runner real:

```bash
PYTHONPATH=src python -m spark_tuning.cli run \
  examples/runs/demo.json \
  --artifacts artifacts/runs
```

Configure `SPARK_HOME` ou deixe `spark-submit` disponível no `PATH`. Use somente em sandbox autorizada.

### Branch segura

```bash
git fetch origin
git switch -c tune/<skill>/<run-id> origin/main
git diff --check
```

---

## 11. Política de PR

### Antes de abrir

- [ ] branch isolada;
- [ ] baseline preservado;
- [ ] hipótese documentada;
- [ ] uma mudança por iteração;
- [ ] testes de correção aprovados;
- [ ] benchmark repetido;
- [ ] custo dentro do orçamento;
- [ ] plano/configuração/artifacts preservados;
- [ ] rollback testado;
- [ ] CI verde;
- [ ] diff revisado;
- [ ] nenhum secret/PII;
- [ ] descrição da PR pronta.

### Corpo mínimo da PR

```markdown
## Hipótese

## Sintoma e evidência

## Mudança

## Baseline vs candidate

| Métrica | Baseline | Candidate | Delta |
|---|---:|---:|---:|
| duração | | | |
| custo | | | |
| shuffle | | | |
| spill | | | |
| p95 | | | |

## Correção

## Riscos

## Rollback

## Artefatos
```

O agente pode preparar e abrir a PR quando autorizado, mas não pode fazer merge sem revisão humana.

---

## 12. Matriz de decisão rápida

| Sintoma | Investigar primeiro | Skills prováveis |
|---|---|---|
| uma task muito lenta | chave hot e bytes/task | `skew-detection`, `join-strategy` |
| alto shuffle | exchange e join | `join-strategy`, `aqe-tuning` |
| alto spill | partição, memória e agregação | `partition-sizing`, `spill-control` |
| alto GC | objetos, cache e heap | `jvm-gc-memory`, `scala-spark` |
| CPU alta | UDF, expressão ou codegen | `pyspark-boundaries`, `spark-sql` |
| CPU baixa | I/O, rede ou fila | `parquet-layout`, `cloud-compute` |
| muitos arquivos | escrita e particionamento | `parquet-layout`, `delta-iceberg-layout` |
| driver OOM | collect, plano ou listagem | `pyspark-boundaries`, `plan-inspection` |
| custo alto | utilização, autoscaling e Spot | `cloud-compute`, `autoscaling-policy`, `spot-and-fleet` |
| resultado diferente | schema, cardinalidade e checksum | `data-quality-gate` |

---

## 13. Quando parar

Pare e retorne `blocked` quando:

- não existe baseline confiável;
- métricas essenciais estão ausentes;
- a hipótese não é falsificável;
- o custo/tempo excedeu o limite;
- a correção não foi provada;
- houve drift relevante;
- a mudança exige permissão não concedida;
- não há hipótese nova com evidência.

Não invente resultado. Explique o bloqueio e o próximo dado necessário.

---

## 14. Definição de pronto

Uma skill está pronta para distribuição quando possui:

- bundle portátil;
- manifesto e versão;
- instruções para host;
- memória de validade;
- hipótese;
- baseline/candidate;
- exemplo antes/depois;
- teste de correção;
- benchmark;
- métricas e guardrails;
- rollback;
- fontes/licença;
- artefato sanitizado;
- CI verde;
- status honesto: `draft`, `experimental`, `validated`, `promoted` ou `deprecated`.

### Estado atual para o agente

O repositório possui implementações/testes iniciais para:

- `join-strategy`
- `partition-sizing`
- `parquet-layout`
- `pyspark-boundaries`
- `jvm-gc-memory`
- `data-quality-gate`

O catálogo amplo contém outras skills como planeadas ou parcialmente implementadas.
Devin não deve declarar uma skill implementada sem verificar:

1. diretório em `skills/<nome>/`;
2. `SKILL.md`;
3. exemplo `before/after`;
4. benchmark;
5. testes;
6. referência;
7. estado e evidência no CI.

---

## 15. Princípio final para o agente

> **Não otimize por intuição. Observe, classifique, formule, experimente, meça, verifique, decida e aprenda. Preserve a estabilidade antes de buscar velocidade.**
