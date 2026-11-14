# Changelog

Todas as mudanças relevantes deste playbook devem ser registradas aqui.

## [Unreleased]

### Added

- skills complementares de lineage, telemetria, qualidade, contexto, resiliência e análise estática;
- matriz de integração com OpenLineage, DataHub, Marquez, OpenTelemetry, Chaos Monkey e Ruff;
- contrato de evento correlacionando run, inputs, outputs, métricas e decisão;
- templates granulares de skill, benchmark e decisão;
- catálogo de skills com framework de referência por domínio;
- regra de baseline usando pandas API on Spark/Koalas para workloads tabulares distribuídos;
- validação de correção junto com performance;
- estrutura de CI em árvore para documentação, schemas, exemplos, benchmarks e regressões;
- integração conceitual com sparkMeasure, Deequ, Great Expectations, OpenLineage, Iceberg e Spark Operator.

### Changed

- decisões de tuning agora exigem hipótese, baseline, métricas e rollback;
- comparação de frameworks deve declarar equivalência semântica.

## [0.1.0] - 2026-11-14

### Added

- playbook inicial de Spark tuning;
- PySpark, Spark SQL, Scala, Glue, EMR, EMR Serverless, Databricks;
- GCP, Azure, AWS, Terraform, Spot, autoscaling e layout Parquet;
- benchmarks, stress tests, GC, skew, spill, joins e custo.
