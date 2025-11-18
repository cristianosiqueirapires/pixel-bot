# PixelBot – O futuro do Suporte L3 na Pixel House (2026 sem plantão noturno)

**PoC completo e rodando em 5 minutos**  
Kestra + Ollama local + Slack + Worker Supervisor

### Por que esse projeto existe?
- Worker .NET legado cai às 4h da manhã → ninguém acorda mais
- Saída do dev de 12 anos em jan/2026 → conhecimento capturado para sempre
- Fábrica de 500 mil fotos/dia nunca mais para por incidente repetitivo

### Stack 100% local e custo quase zero
- Kestra → orquestrador (fluxos em YAML, GitOps)
- Ollama → LLM local (Llama 3.1 8B/70B)
- Slack → bot de triagem e aprovação humana
- Docker Compose → sobe tudo com um comando

### Como rodar o PoC agora (5 minutos)
```bash
git clone https://github.com/cristianosiqueirapires/pixel-bot.git
cd pixel-bot
docker compose up -d
docker exec -it ollama ollama pull llama3.1:8b