# Padrão cognitivo de diagnóstico

## 1. O loop O-C-H-E-M-V-D-L

O sistema usa um ciclo fixo:

```text
OBSERVE
  ↓
CLASSIFY
  ↓
HYPOTHESIZE
  ↓
EXPERIMENT
  ↓
MEASURE
  ↓
VERIFY
  ↓
DECIDE
  ↓
LEARN
  └─────────────── volta para OBSERVE
```

### O — Observe

Pergunta: **o que aconteceu?**

Não inferir causa antes de coletar:

- plano;
- métricas;
- configuração;
- versão;
- dados;
- custo;
- qualidade.

### C — Classify

Pergunta: **qual recurso está limitando?**

Classificar em:

- CPU;
- memória/GC;
- I/O;
- rede/shuffle;
- skew;
- paralelismo;
- driver;
- Python/JVM;
- storage/layout;
- provisionamento;
- qualidade/correção.

Um diagnóstico pode ter uma causa primária e causas secundárias.

### H — Hypothesize

Pergunta: **qual mudança explicaria as evidências?**

Formato obrigatório:

> Se `<mudança>`, então `<métrica>` deve mudar de `<baseline>` para `<alvo>`, sem violar `<guardrails>`, porque `<mecanismo>`.

### E — Experiment

Pergunta: **qual menor teste pode falsificar a hipótese?**

Preferir:

- mudança isolada;
- dataset pequeno representativo;
- execução barata;
- rollback simples;
- mesmo resultado funcional.

### M — Measure

Pergunta: **o que mudou de fato?**

Medir:

- objetivo primário;
- objetivos secundários;
- guardrails;
- variância;
- custo;
- qualidade.

### V — Verify

Pergunta: **o resultado é real e generalizável?**

Repetir:

- em outra execução;
- com cache frio;
- em escala diferente;
- com distribuição skewed;
- com falha quando relevante.

### D — Decide

Pergunta: **aceitar, rejeitar, reverter ou repetir?**

Nunca decidir somente por tempo de uma execução.

### L — Learn

Pergunta: **o que fica disponível para o próximo diagnóstico?**

Persistir evidências e limites de validade.

## 2. Matriz sintoma → investigação

| Sintoma | Primeira pergunta | Próximo teste |
|---|---|---|
| job lento | qual stage domina? | stage/task profile |
| uma task lenta | há chave hot? | distribuição de chaves |
| alto shuffle | qual exchange gerou? | plano físico |
| alto spill | partições estão grandes? | bytes/task e memória |
| alto GC | há objetos/caches excessivos? | heap/GC report |
| CPU baixa | está esperando I/O/rede? | timeline |
| driver OOM | houve coleta local? | rastrear collect/toPandas |
| custo alto | há baixa utilização? | recurso por fase |
| muitos arquivos | escrita está fragmentada? | file-size profile |
| resultado diferente | qual transformação divergiu? | checksum por etapa |

## 3. Confiança

```text
confidence =
  evidence_strength
  * mechanism_fit
  * reproducibility
  * measurement_quality
```

Faixas:

- `0.00–0.39`: especulação;
- `0.40–0.69`: hipótese plausível;
- `0.70–0.89`: forte;
- `0.90–1.00`: sustentada por múltiplas execuções.

Confiança não substitui experimento.

## 4. Regras de parada

Parar quando:

- o SLO foi atingido com margem;
- a próxima mudança tem risco maior que benefício;
- a hipótese foi falsificada;
- a variância impede conclusão;
- o custo do experimento excede o valor esperado;
- não há dados suficientes e isso foi registrado como `unknown`.
