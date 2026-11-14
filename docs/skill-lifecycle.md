# Ciclo de vida de uma skill

## Estados

```text
draft -> experimental -> validated -> promoted
                  \-> rejected
validated -> deprecated
```

## Draft

Obrigatório:

- objetivo;
- sintomas;
- hipótese;
- framework de referência;
- benchmark;
- métricas;
- guardrails;
- rollback;
- fontes.

## Experimental

Exige:

- exemplo antes/depois;
- dataset com seed;
- baseline executado;
- teste de correção;
- pelo menos uma execução candidate;
- resultado armazenado.

## Validated

Exige:

- múltiplas execuções;
- estabilidade;
- escala diferente;
- custo medido;
- runtime declarado;
- CI verde;
- revisão humana.

## Promoted

Uma skill promovida:

- tem versionamento;
- possui limites de validade;
- é referenciada no catálogo;
- tem exemplo reproduzível;
- possui estratégia de rollback;
- pode ser usada pelo loop automático.

## Rejected

Registrar a razão:

- hipótese falsificada;
- ganho não reproduzível;
- custo maior;
- risco operacional;
- resultado incorreto;
- incompatibilidade de runtime.

Não apagar a evidência rejeitada.
