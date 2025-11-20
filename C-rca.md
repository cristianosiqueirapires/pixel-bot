# C) RCA – Latência e Contenda (Relatório x UPDATE)

**Cenário:** Sábado 10h30, aumento súbito de latência no e-commerce causado por contenda entre:

- Relatório pesado (SELECT com JOIN Pedido + Expedicao, janela 7 dias, ORDER BY ClienteId, DataPedido DESC);
- UPDATE em lote alterando Status e DataExpedicao em Pedido.

**Causa provável:**  
O relatório realiza Clustered Index Scan e mantém S-locks prolongados por falta de índice cobrindo JOIN/ORDER BY. O UPDATE precisa de X-locks nas mesmas chaves, formando blocking chain. O sort do ORDER BY gera spill no tempdb, aumentando o tempo de retenção dos locks e elevando risco de lock escalation (páginas → tabela).

**Mitigação imediata:**
1. Identificar o head blocker via sp_whoisactive ou sys.dm_exec_requests.
2. Encerrar a sessão (KILL <spid>) ou aplicar temporariamente WITH (NOLOCK) no relatório.
3. Se o banco estivesse com RCSI, a contenda leitura × escrita não ocorreria.

**Prevenção:**
- Criar índice non-clustered cobrindo (ClienteId, DataPedido DESC) + colunas do JOIN (INCLUDE).
- Agendar relatórios off-peak ou mover para réplica de leitura.
- Ativar READ_COMMITTED_SNAPSHOT (RCSI) para eliminar bloqueios entre SELECT e UPDATE.
- Monitorar via Query Store e manter estatísticas sempre atualizadas.

**Runbook (plantão):**
1. Coletar evidências: sp_whoisactive, waits LCK_M_*, plano de execução.
2. Comunicar incidente no canal adequado.
3. Mitigar: derrubar a sessão bloqueadora e validar throughput.
4. Confirmar queda da latência e normalização dos waits.
5. Registrar pós-incidente com ações preventivas (índice, RCSI, nova janela de relatório).