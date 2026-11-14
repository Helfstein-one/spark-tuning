# Diagramas da plataforma

## 1. Visão geral da plataforma

```mermaid
flowchart LR
    U[Usuário / CI] --> API[CLI / API de tuning]
    API --> UC[Casos de uso]
    UC --> DOM[Domínio cognitivo]
    DOM --> H[Hipótese]
    DOM --> EXP[Experimento]
    DOM --> DEC[Decisão]

    AD1[Spark UI / Event Log] --> P1[Metrics Adapter]
    AD2[SparkMeasure] --> P1
    AD3[Cloud Metrics] --> P1
    AD4[Terraform / Runtime] --> P2[Context Adapter]
    AD5[Deequ / GX] --> P3[Quality Adapter]
    AD6[OpenLineage] --> P4[Lineage Adapter]

    P1 --> PORTS[Portas]
    P2 --> PORTS
    P3 --> PORTS
    P4 --> PORTS
    PORTS --> DOM

    EXP --> RUN[Benchmark Runner]
    RUN --> SPARK[Spark / PySpark / Scala / SQL]
    RUN --> CLOUD[AWS / GCP / Azure / Databricks]
    SPARK --> ART[Artifacts]
    CLOUD --> ART
    ART --> DEC
    DEC --> GATE[Regression Gate]
    GATE -->|accept| PROMOTE[Promover]
    GATE -->|reject| ROLLBACK[Rollback]
    GATE -->|inconclusive| MORE[Coletar mais evidência]
    MORE --> DOM
```

## 2. Arquitetura exagonal

```mermaid
graph TB
    subgraph External["Mundo externo"]
        Spark[Spark Runtime]
        Cloud[Cloud APIs]
        Storage[Parquet / Delta / Iceberg]
        Observability[Metrics / Logs / Traces]
        CI[GitHub Actions]
    end

    subgraph Adapters["Adaptadores"]
        InSpark[Spark Adapter]
        InCloud[Cloud Adapter]
        InStorage[Storage Adapter]
        InObs[Observability Adapter]
        OutCI[CI Adapter]
    end

    subgraph Ports["Portas"]
        MetricsPort[MetricsReader]
        PlanPort[PlanReader]
        QualityPort[QualityChecker]
        RunPort[BenchmarkRunner]
        ArtifactPort[ArtifactStore]
        DecisionPort[DecisionRepository]
    end

    subgraph Core["Núcleo"]
        Domain[Domínio de performance]
        App[Casos de uso]
    end

    Spark --> InSpark --> MetricsPort
    Cloud --> InCloud --> MetricsPort
    Storage --> InStorage --> QualityPort
    Observability --> InObs --> MetricsPort
    CI --> OutCI --> DecisionPort

    MetricsPort --> App
    PlanPort --> App
    QualityPort --> App
    RunPort --> App
    ArtifactPort --> App
    DecisionPort --> App
    App --> Domain
```

## 3. Padrão cognitivo O-C-H-E-M-V-D-L

```mermaid
stateDiagram-v2
    [*] --> Observe
    Observe --> Classify: sinais coletados
    Classify --> Hypothesize: gargalo provável
    Classify --> Observe: sinal insuficiente
    Hypothesize --> Experiment: hipótese testável
    Hypothesize --> Observe: hipótese fraca
    Experiment --> Measure: execução concluída
    Experiment --> Rollback: falha ou risco
    Measure --> Verify: métricas disponíveis
    Measure --> Observe: coleta incompleta
    Verify --> Decide: correção e repetição aprovadas
    Verify --> Experiment: resultado inconclusivo
    Decide --> Learn: aceitar, rejeitar ou rollback
    Learn --> Observe: novo ciclo
    Rollback --> Learn
```

## 4. Descoberta do gargalo

```mermaid
flowchart TD
    Start[Job lento ou caro] --> Plan[Inspecionar plano]
    Plan --> Stage[Encontrar stage dominante]
    Stage --> Resource{Qual recurso domina?}

    Resource -->|CPU| CPU[CPU alta / expressão cara / UDF]
    Resource -->|Memória| MEM[GC / OOM / cache / objetos]
    Resource -->|Rede| SHUF[Shuffle / join / exchange]
    Resource -->|I/O| IO[Formato / pruning / small files]
    Resource -->|Distribuição| SKEW[Skew / chave hot]
    Resource -->|Provisionamento| SCALE[Autoscaling / cold start]
    Resource -->|Driver| DRIVER[collect / plano / listagem]

    CPU --> H[Gerar hipótese]
    MEM --> H
    SHUF --> H
    IO --> H
    SKEW --> H
    SCALE --> H
    DRIVER --> H
    H --> Test[Teste mínimo reversível]
    Test --> Correct{Correção preservada?}
    Correct -->|não| Reject[Rejeitar]
    Correct -->|sim| Compare[Comparar custo e performance]
    Compare --> Gate{Guardrails aprovados?}
    Gate -->|sim| Accept[Aceitar]
    Gate -->|não| Reject
```

## 5. Ciclo antes/depois

```mermaid
sequenceDiagram
    participant C as CI/CLI
    participant D as Diagnóstico
    participant B as Baseline
    participant E as Experimento
    participant Q as Quality Gate
    participant G as Regression Gate
    participant A as Artifact Store

    C->>D: workload + objetivo
    D->>B: executar baseline
    B->>A: plano + métricas + saída
    D->>D: classificar gargalo
    D->>E: hipótese + mudança controlada
    E->>A: candidate metrics
    E->>Q: validar schema/checksum/expectations
    Q-->>G: pass/fail
    G->>A: comparar baseline/candidate
    G-->>C: accept/reject/inconclusive
```

## 6. Escolha do framework de referência

```mermaid
flowchart LR
    Problem{O que está sendo otimizado?}
    Problem -->|API pandas distribuída| Koalas[pandas API on Spark / Koalas]
    Problem -->|DataFrame Python| PySpark[PySpark DataFrame]
    Problem -->|Plano e SQL| SQL[Spark SQL]
    Problem -->|JVM| Scala[Scala Spark]
    Problem -->|Tabela/layout| Table[Parquet / Delta / Iceberg]
    Problem -->|AWS| AWS[Glue / EMR]
    Problem -->|GCP| GCP[Dataproc / BigQuery connector]
    Problem -->|Azure| Azure[Databricks / Synapse]
    Problem -->|Kubernetes| K8s[Spark Operator]
    Koalas --> Benchmark[Benchmark comparável]
    PySpark --> Benchmark
    SQL --> Benchmark
    Scala --> Benchmark
    Table --> Benchmark
    AWS --> Benchmark
    GCP --> Benchmark
    Azure --> Benchmark
    K8s --> Benchmark
```

## 7. Observabilidade e lineage

```mermaid
flowchart LR
    Job[Spark Job] --> Event[Run Event]
    Event --> Metrics[Metrics]
    Event --> Logs[Logs]
    Event --> Plan[Plan Hash]
    Event --> Quality[Quality Results]
    Event --> Cost[Cost]
    Event --> Lineage[Lineage]

    Metrics --> OTel[OpenTelemetry]
    Logs --> OTel
    Cost --> OTel
    Lineage --> OL[OpenLineage]
    OL --> Catalog[Marquez / DataHub]
    OTel --> Dash[Dashboard / Alert]
    Catalog --> Context[Contexto do dataset]
    Context --> Diagnosis[Próximo diagnóstico]
```

## 8. Promoção e rollback

```mermaid
flowchart TD
    Candidate[Candidate] --> Correct{Correção}
    Correct -->|fail| Rollback[Rollback]
    Correct -->|pass| Stable{Estabilidade]
    Stable -->|fail| Rollback
    Stable -->|pass| Perf{Performance}
    Perf -->|regressão| Reject[Reject]
    Perf -->|melhora| Cost{Custo}
    Cost -->|piorou além do limite| Reject
    Cost -->|aceitável| Promote[Promote]
    Promote --> Registry[Skill/Decision Registry]
    Rollback --> Registry
    Reject --> Registry
```

## 9. Diagrama de recursos do job

```mermaid
pie title Distribuição conceitual do tempo
    "CPU" : 35
    "Shuffle/Rede" : 25
    "I/O" : 20
    "GC/Memória" : 10
    "Scheduler/Overhead" : 10
```

O gráfico é ilustrativo. Os valores reais devem vir do event log, SparkMeasure ou métricas do runtime.
