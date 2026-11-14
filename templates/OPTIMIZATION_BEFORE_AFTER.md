# Exemplo de otimização antes/depois

Use este template para mostrar código, plano, métricas e decisão. O exemplo não está completo sem correção e rollback.

## Exemplo A — join com dimensão pequena

### Antes

```python
joined = fact.join(dim, on="customer_id", how="left")
```

### Evidência antes

```text
shuffle_write: 480 GB
p95_task: 740 s
spill_disk: 120 GB
duration: 1,820 s
```

### Hipótese

Se `dim` couber com margem em memória, broadcast deve remover o shuffle da dimensão e reduzir spill.

### Depois

```python
from pyspark.sql import functions as F

joined = fact.join(
    F.broadcast(dim.select("customer_id", "segment")),
    on="customer_id",
    how="left",
)
```

### Guardrails

- tamanho da dimensão validado antes do broadcast;
- executor OOM igual a zero;
- contagem e checksum preservados;
- fallback para sort-merge se a dimensão ultrapassar o limite.

## Exemplo B — UDF Python para expressão nativa

### Antes

```python
@udf("double")
def normalize(x):
    return x / 100.0 if x is not None else None

result = df.withColumn("normalized", normalize("amount"))
```

### Depois

```python
from pyspark.sql import functions as F

result = df.withColumn(
    "normalized",
    F.col("amount") / F.lit(100.0),
)
```

### Métricas

Comparar Python time, CPU, duração, schema, NULL semantics e checksum.

## Exemplo C — repartition que cria skew

### Antes

```python
df.repartition("country")
```

### Evidência antes

```text
task_p50: 8 s
task_p99: 510 s
skew_ratio: 63.7
```

### Depois

```python
from pyspark.sql import functions as F

salted = df.withColumn(
    "_salt",
    F.pmod(F.xxhash64("customer_id"), F.lit(32)),
)
balanced = salted.repartition("country", "_salt")
```

### Guardrails

- remover coluna técnica na saída;
- validar agregações que exigem recombinação;
- comparar número de arquivos;
- testar distribuição com países pequenos e grandes.

## Exemplo D — escrita Parquet fragmentada

### Antes

```python
df.write.partitionBy("date", "country").parquet(output)
```

### Depois

```python
(
    df.repartition("date", "country")
      .write
      .option("compression", "zstd")
      .partitionBy("date", "country")
      .parquet(output)
)
```

O candidate deve ser ajustado ao padrão de leitura. Medir tamanho dos arquivos, bytes lidos, pruning, tempo de listagem e custo de compactação.

## Resultado

| Métrica | Antes | Depois | Delta | Aceito? |
|---|---:|---:|---:|---|
| duração |  |  |  |  |
| custo |  |  |  |  |
| shuffle |  |  |  |  |
| spill |  |  |  |  |
| GC |  |  |  |  |
| p95 task |  |  |  |  |
| correção |  |  |  |  |

## Decisão

`accept | reject | rollback | inconclusive`

## Rollback

`<configuração ou commit anterior>`
