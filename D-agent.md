# D) (Opcional) Agente L1 com LLM local – Proposta real para 2026

**Objetivo real:** Reduzir em 80-90% os plantões noturnos causados por worker .NET caído, impressora travada, fila de reprocessamento explodida — tudo que hoje acorda o time às 4h da manhã.

**Por que local (Ollama) e não API externa?**  
Zero custo, zero vazamento de logs/PII (CPF, endereço do cliente), funciona mesmo se a internet cair.

**Modelo escolhido (já testado na minha máquina):**  
- llama3.1:8b ou gemma2:9b (roda em notebook com 16 GB RAM)  
- Futuro: llama3.1:70b quando tivermos GPU dedicada

**Ferramentas seguras (só read-only ou ação de baixo risco):**
- get_recent_logs(service, minutes=10) → consulta Kusto/SQL read-only
- get_runbook(keyword) → busca na documentação que o dev de 12 anos está deixando
- restart_worker_dotnet() → executa o .bat que já existe hoje (idempotente)
- open_ticket(summary, severity) → Jira/Service Desk
- suggest_human_escalation(reason) → manda no Slack do plantão

**Guardrails absolutos (o que acalma o dono resistente):**
- Nunca executa escrita sem aprovação humana (botão "Aprovar" no chat)
- Nunca vê PII (logs já chegam mascarados)
- Nunca inventa comando SQL — só usa queries pré-aprovadas ou sugere para humano rodar
- Todas as ações ficam auditadas no Kestra/Slack

**Roadmap realista (sem assustar ninguém):**
Mês 1 (jan/2026): Ollama + Slack bot simples ("@PixelBot por que o worker caiu?")
Mês 2: Primeiro fluxo automático — worker .NET cai → reinicia sozinho + avisa no Slack
Mês 3: +3 fluxos (impressora travada, fila explodida, estoque negativo)
2026: 90% dos alertas N1 resolvidos/triados sem humano acordar

**Prompt do sistema (já testado e funciona em português):**
"Você é o PixelBot L1 da fábrica de fotos. Seu objetivo é reduzir plantão noturno. Você é calmo, objetivo e nunca executa nada sem aprovação humana. Sempre cita a fonte (documentação do João, runbook X). Nunca menciona PII."

Já tenho PoC rodando local com Kestra + Ollama + Slack — disponível para demo ao vivo em minutos.

Com isso a fábrica não para mais às 4h da manhã — e o time começa 2026 criando valor em vez de apagar incêndio.