# Spark Tuning

![Spark Tuning Hero Banner](docs/assets/spark-hero.jpg)

Playbook e engine seguro para descobrir, testar e promover otimizações de Apache Spark sem alterar diretamente o código estável.

## O que este projeto faz

1. observa um workload;
2. identifica o gargalo provável;
3. formula uma hipótese;
4. executa um benchmark isolado;
5. valida correção, performance, custo e estabilidade;
6. repete apenas dentro de limites definidos;
7. abre uma PR somente quando todos os gates passam.

O fluxo cognitivo é:

```text
OBSERVE -> CLASSIFY -> HYPOTHESIZE -> EXPERIMENT
    ^                                  |
    |                                  v
 LEARN <- DECIDE <- VERIFY <- MEASURE
```

O loop executável está em `src/spark_tuning`. Ele pode usar fixtures determinísticas no CI e, depois, adaptadores reais de Spark event logs, SparkMeasure e cloud metrics.

## Início rápido

### 1. Escolha uma skill

Consulte [`skills/README.md`](skills/README.md). Cada skill define sintomas, hipótese, benchmark, métricas e guardrails.

### 2. Crie uma execução

Use [`templates/LOOP_RUN_TEMPLATE.md`](templates/LOOP_RUN_TEMPLATE.md) para declarar:

- workload e dataset;
- baseline;
- objetivo;
- limite de iterações;
- critérios de correção;
- orçamento;
- política de PR.

### 3. Execute em branch isolada

```bash
git fetch origin
git switch -c tune/<skill>/<run-id> origin/main
```

Nunca faça tuning diretamente em `main`.

### 4. Rode os gates

```bash
./scripts/validate_loop.sh
```

O CI também executa documentação, schema, correção, benchmark, performance, infraestrutura e regression gate.

### 5. Abra a PR

A PR só pode ser aberta depois de:

- baseline preservado;
- candidate reproduzível;
- testes de correção aprovados;
- thresholds de performance aprovados;
- rollback documentado;
- nenhum arquivo estável alterado fora do escopo.

### Executar o loop local

```bash
PYTHONPATH=src python -m spark_tuning.cli run \
  examples/runs/demo.json \
  --artifacts artifacts/runs
```

O comando percorre hipóteses em ordem, persiste cada iteração, para na primeira candidate aceita e retorna código diferente de zero quando fica bloqueado ou sem evidência suficiente.

## Documentação por responsabilidade

| Documento | Conteúdo |
|---|---|
| [`docs/engineering-loop.md`](docs/engineering-loop.md) | loop iterativo completo |
| [`docs/pr-safety.md`](docs/pr-safety.md) | proteção de `main` e política de PR |
| [`docs/skill-lifecycle.md`](docs/skill-lifecycle.md) | ciclo de vida de uma skill |
| [`docs/architecture.md`](docs/architecture.md) | arquitetura exagonal |
| [`docs/cognitive-loop.md`](docs/cognitive-loop.md) | padrão cognitivo de diagnóstico |
| [`docs/journey.md`](docs/journey.md) | jornada operacional |
| [`docs/ci-validation-tree.md`](docs/ci-validation-tree.md) | árvore de validação |
| [`docs/diagrams.md`](docs/diagrams.md) | diagramas Mermaid |
| [`docs/reference-playbook.md`](docs/reference-playbook.md) | referência técnica extensa |
| [`docs/related-skills.md`](docs/related-skills.md) | skills complementares |

## Estrutura

```text
spark-tuning/
├── README.md
├── CHANGELOG.md
├── docs/
├── skills/
├── templates/
├── scripts/
├── benchmarks/
├── examples/
├── tests/
└── .github/workflows/
```

## Regra de segurança

O engine pode experimentar em branch temporária, mas não pode:

- fazer push em `main`;
- sobrescrever baseline;
- aceitar resultado sem correção;
- ocultar falha de métrica;
- abrir PR com gate vermelho;
- misturar mudanças de código, infraestrutura e tuning sem hipótese explícita.

## Baseline e Validação de Correção

Para workloads tabulares distribuídos, o baseline padrão é pandas API on Spark, sucessora de Koalas. Para outros domínios, use a referência definida em [`skills/README.md`](skills/README.md).

Toda validação de hipótese exige aferição estrita de correção via contrato de `row_count`, checksum e integridade de schema.

## Licença e contribuição

Consulte [`CHANGELOG.md`](CHANGELOG.md) e os templates antes de adicionar uma skill.
