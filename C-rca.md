# C) RCA – Latência e Contenda (Relatório x UPDATE)

**Cenário:** Em um sábado às 10h30, a latência do e‑commerce aumentou. Houve contenda entre:
- Um **relatório** (SELECT com JOIN em `Pedido` + `Expedicao`), janela de 7 dias e `ORDER BY ClienteId, DataPedido DESC`;
- Um **UPDATE** em `Pedido` que altera `Status` e `DataExpedicao` para múltiplos pedidos.

**Escreva em 8–12 linhas:**
- **Causa provável** (bloqueios S/X por índice ruim, scans + sort custoso, spills em tempdb, lock escalation).  
- **Mitigação imediata** (limitar/pausar relatório, hints/janelas, leitura em réplica ou `RCSI` se disponível).  
- **Prevenção** (índices compostos/cobertura, manutenção de estatísticas, Query Store, janelas off‑peak).  
- **Runbook** (passos do plantão: coletar evidências com DMVs/Query Store, comunicar, mitigar, validar e encerrar).
