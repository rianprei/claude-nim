# claude-nim

Proxy Anthropic Messages API para **[Claude Code](https://claude.com/claude-code)** que entrega modelos **NVIDIA NIM** com a mesma experiência dos modelos Claude nativos — e ao mesmo tempo faz **passthrough real** para `api.anthropic.com` na mesma sessão.

```
Claude Code
    │  POST /v1/messages  (stream SSE, tools, thinking)
    ▼
claude-nim (binário único ~2 MB, RSS ~15 MB)
    ├── claude-nvidia-* / nvidia-*   →  traduz ↔ OpenAI Chat Completions → integrate.api.nvidia.com
    └── claude-* / outro modelo      →  passthrough SSE → api.anthropic.com (seu token)
```

## Por que existe

O `litellm` + Python usava **983 MB de RAM** em idle. Este proxy faz o mesmo trabalho (tradução de protocolo) com **um binário Rust de 2 MB e ~15 MB sob carga**, sem runtime, sem venv, sem deps.

## Features

- **Streaming SSE bidirecional** — `message_start` → `content_block_delta` → `message_stop`, índice de blocos alinhado
- **Tool calling nativo** — `tools` / `tool_use` / `tool_result` / `tool_choice` mapeados 1:1; `tool_calls` intercalados viram blocos separados sem duplicar
- **Thinking nativo** — `reasoning_content` do NIM vira `content_block` tipo `thinking` + `thinking_delta` (renderiza colapsado, igual ao Claude real)
- **Fallback em cadeia com circuit breaker** — 2 passagens; modelos que respondem 429/5xx entram em cooldown de 60s e são pulados
- **Keepalive SSE** — `event: ping` a cada 5s de silêncio do upstream (modelos de reasoning pensam em silêncio)
- **Uso real de tokens** — `stream_options.include_usage`: `output_tokens` no `message_delta` vem do usage do NIM, não de estimativa
- **Erros no formato Anthropic** — 503 `overloaded_error`, 401, 400, 413, 404 — sempre `{"type":"error","error":{...}}`, nunca envelope OpenAI cru
- **Falha honesta em stream** — upstream truncado/reset emite `event: error`, não `message_stop` falso
- **HTTP robusto** — hyper 1.x (HTTP/1.1, chunked request bodies, keep-alive), rustls (sem OpenSSL), timeout de conexão, limite de body 32 MB
- **Anti-rebinding** — só aceita `Host: 127.0.0.1`/`localhost` no modo gateway
- **Auth flexível** — `LITELLM_MASTER_KEY` (comparação constant-time) ou `Bearer sk-ant-*` (token OAuth do Claude Code)
- **Segurança** — a master key do gateway nunca é repassada ao upstream; zero secrets em log; `Fail-closed` no start

## Modelos

| Alias Claude Code | Cadeia NIM (em ordem) |
|---|---|
| `claude-nvidia-ultra` | nemotron-3-ultra-550b → nemotron-3-super-120b → nemotron-3-nano-reasoning |
| `claude-nvidia-nemotron` | nemotron-3-super-120b → nemotron-3-nano-reasoning |
| `claude-nvidia-kimi` | kimi-k3 → nemotron-3-super-120b → nemotron-3-nano-reasoning |
| `claude-nvidia-nano` | nemotron-3-nano-reasoning |

`nvidia-*` direto (ex: `nvidia-nano`) também funciona. Qualquer outro nome vai pro passthrough Anthropic.

## Instalação

### Binário pronto (release)

```bash
# x86-64 Linux
curl -L https://github.com/rianprei/claude-nim/releases/latest/download/nim-proxy-linux-x86_64 -o nim-proxy
chmod +x nim-proxy
```

### Do fonte

```bash
cargo build --release
# binário em target/release/nim-proxy (~2 MB)
```

## Uso

```bash
export NVIDIA_API_KEY=nvapi-...
export LITELLM_MASTER_KEY=sk-...      # obrigatório em MODE=gateway
export MODE=gateway                   # gateway = NIM + passthrough Anthropic
nim-proxy -addr 127.0.0.1:4111
```

### systemd (user)

```ini
[Unit]
Description=claude-nim
After=network-online.target

[Service]
Environment=MODE=gateway
Environment=ANTHROPIC_BASE=https://api.anthropic.com
Environment=MALLOC_ARENA_MAX=2
EnvironmentFile=%h/.config/nvidia/proxy.env
ExecStart=%h/.local/bin/nim-proxy -addr 127.0.0.1:4111
Restart=always
RestartSec=2
MemoryMax=256M
NoNewPrivileges=true

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now claude-nim
```

### Claude Code

```json
// ~/.claude/settings.json
{ "env": { "ANTHROPIC_BASE_URL": "http://127.0.0.1:4111" } }
```

Faça `/login` normalmente. Modelos Claude reais e `claude-nvidia-*` funcionam na mesma sessão.

## Variáveis de ambiente

| Variável | Default | Descrição |
|---|---|---|
| `NVIDIA_API_KEY` | — | **obrigatório**; chave do build.nvidia.com |
| `LITELLM_MASTER_KEY` | — | **obrigatório em `MODE=gateway`** (fail-closed) |
| `MODE` | `nim` | `gateway` = NIM + passthrough Anthropic |
| `ANTHROPIC_BASE` | `https://api.anthropic.com` | base do passthrough |
| `NIM_API_BASE` | `https://integrate.api.nvidia.com/v1` | override para testes/mock |
| `NIM_READ_TIMEOUT_SECS` | `300` | idle-timeout de leitura do upstream |
| `EMAIL_REWRITE_FROM` / `EMAIL_REWRITE_TO` | — | reescrita opcional de string no corpo |

## Endpoints

| Rota | Descrição |
|---|---|
| `POST /v1/messages` | Messages API (sync + stream, tools, thinking) |
| `POST /v1/messages/count_tokens` | estimativa local (heurística PT/EN + CJK) |
| `GET /v1/models` | lista os aliases NIM (modo gateway) |
| `GET /health` | health check |
| `HEAD *` | probe de conectividade → 200 |

## Testes

```bash
cargo test
```

17 testes cobrindo: conversão Anthropic↔OpenAI (tools, hint, clamp, arrays vazios), parser de linha SSE UTF-8-safe (multibyte partido entre chunks), mapeamento de erro, circuit breaker, aliases de modelo, heurística de tokens.

## Performance

Medido em produção (i5-9600K, stream de 334–1573 tokens):

| Métrica | Valor |
|---|---|
| RSS em carga | ~15 MB |
| Binário | ~2.2 MB (`opt-level=z`, `lto=fat`, `strip`) |
| Chunks SSE | 1.93 µs/evento (dominated por I/O de rede) |
| CPU | 0.39% de 1 core — 99.6% I/O-bound |

Alvo validado também para hardware fraco (Celeron 2C/4T, 4 GB): `tokio` com 2 workers, pool keep-alive pequeno, `MemoryMax=256M` no cgroup.

## Limitações conhecidas (honestas)

- **Sem prompt caching** — a API NIM não suporta `cache_control`; contexto grande custa mais latência
- **Janela ~128k** — `max_tokens` é clampado em 16384 por segurança
- **Text-only** — blocos `image`/`document` viram aviso textual explícito
- **Qualidade de tools** — melhor que média open-weight (hint de schema injetado), abaixo do Claude
- **503 da NIM** — acontece; o fallback + cooldown + erro `overloaded_error` no formato certo minimizam

## Licença

MIT
