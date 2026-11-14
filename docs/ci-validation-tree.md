# Árvore de validação do CI

```text
CI
├── 01-documentation
│   ├── README existe
│   ├── cada skill possui SKILL.md
│   ├── links e headings válidos
│   └── changelog atualizado
├── 02-schema
│   ├── templates YAML/JSON válidos
│   ├── IDs únicos
│   ├── campos obrigatórios presentes
│   └── versões de runtime declaradas
├── 03-examples
│   ├── exemplos compilam/importam
│   ├── SparkSession inicia
│   ├── seed é determinística
│   └── saída tem schema esperado
├── 04-correctness
│   ├── row count
│   ├── checksum
│   ├── schema
│   ├── nullability
│   └── expectativas de qualidade
├── 05-benchmark
│   ├── baseline
│   ├── candidate
│   ├── repetição
│   ├── p50/p95/p99
│   └── custo e recursos
├── 06-performance
│   ├── duração
│   ├── shuffle
│   ├── spill
│   ├── GC
│   ├── skew
│   └── retries
├── 07-infrastructure
│   ├── terraform fmt/validate
│   ├── terraform plan
│   ├── policy checks
│   └── runtime compatibility
└── 08-regression-gate
    ├── correctness == true
    ├── failures == 0
    ├── duration/cost threshold
    ├── p95 threshold
    ├── spill/GC threshold
    └── decisão accept/reject/inconclusive
```

## Política

Uma falha de documentação, schema ou correção bloqueia o CI imediatamente. Performance pode ser `inconclusive` quando houver variância insuficiente, mas nunca pode ser convertida silenciosamente em `pass`.
