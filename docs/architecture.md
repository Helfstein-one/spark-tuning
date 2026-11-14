"""Architecture specification for spark-tuning."""

# Arquitetura Granular do Spark Tuning

O sistema é organizado modularmente por domínio funcional, desacoplando o motor central das integrações específicas de cluster, storage e diagnósticos.

## Árvore de Diretórios

```text
src/spark_tuning/
├── __init__.py               # Fachada pública e retrocompatibilidade
├── cli.py                   # Interface de linha de comando
├── engine.py                # Orquestrador do loop O-C-H-E-M-V-D-L
├── core/                    # Modelos de domínio, entidades e contratos fundamentais
│   └── __init__.py          # Metrics, RunSpec, RunState, Decision, Iteration
├── diagnostics/             # Classificação automática de gargalos (Issue #2)
│   └── __init__.py          # Skew, spill, GC, shuffle, I/O, CPU
├── evaluation/              # Guardrails, deltas e verificação estatística
│   └── __init__.py          # Evaluation, evaluate, pareto
├── runners/                 # Executores de workload e telemetria (Issue #1)
│   └── __init__.py          # SparkSubmit, FixtureRunner, EventLogParser
├── reporting/               # Relatórios explicáveis e dashboards
│   └── __init__.py          # Markdown, JSON, antes/depois
├── drift/                   # Detecção de deriva de dados e workload
│   └── __init__.py          # Schema, cardinalidade, drift
└── safety/                  # Mudanças isoladas, rollback e guardrails de branch
    └── __init__.py          # Git adapter, allowlist, rollback

tests/
├── fixtures/                # Amostras determinísticas de event logs e datasets
├── unit/                    # Testes isolados por domínio funcional
│   ├── core/
│   ├── diagnostics/
│   ├── evaluation/
│   ├── runners/
│   ├── reporting/
│   ├── drift/
│   └── safety/
└── integration/             # Testes de loop ponta-a-ponta (E2E)
```

## Responsabilidades dos Módulos

1. **`core`**: Define a verdade única dos tipos de dados (`Metrics`, `RunSpec`, `RunState`, `Decision`).
2. **`diagnostics`**: Analisa planos e métricas para apontar causas primárias e secundárias com índice de confiança.
3. **`evaluation`**: Aplica guardrails de regressão (`duration`, `p95`, `spill`), multiobjetivo Pareto e gates de qualidade.
4. **`runners`**: Abstrai a execução física (`spark-submit`, cloud adapters) e parser de telemetria sem perda de fidelidade.
5. **`safety`**: Garante que nenhuma mudança quebre produção ou polua a branch `main`, permitindo rollback atômico.
6. **`drift`**: Garante que uma otimização continue válida frente a variações no volume ou perfil de distribuição.
7. **`reporting`**: Produz artefatos explicáveis com contexto de negócios e FinOps.
