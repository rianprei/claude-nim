# Arquitetura

Proxy de protocolo em um único arquivo (`src/main.rs`): recebe Anthropic Messages
API, traduz para OpenAI Chat Completions, entrega modelos NVIDIA NIM — e no modo
gateway faz passthrough real para `api.anthropic.com` na mesma sessão.

```
Claude Code
   │  POST /v1/messages (HTTP/1.1, SSE, tools, thinking)
   ▼
hyper direto (sem axum/actix)          ── tokio current_thread, 1 thread
   │  route(): auth → body (≤32MB) → convert_to_oai()
   ▼
cadeia de modelos (2 passagens, circuit breaker)
   │  reqwest HTTP/2 prior knowledge → integrate.api.nvidia.com
   ▼
pump_stream(): SSE OpenAI → SSE Anthropic  ── keepalive ping 5s
```

## Fluxo de request

1. **Conexão** — `TcpListener` no loop (bind default `127.0.0.1:4000`, fail-closed).
   Cada conexão vira uma task no runtime `current_thread`; timeout de conexão de
   900s mata slowloris.
2. **`route()`** — auth (`LITELLM_MASTER_KEY` comparação constant-time, ou
   `Bearer sk-ant-*` no gateway), host anti-rebinding, HEAD probe, 401 cedo
   em endpoint de mensagens (antes de bufferizar body), limite de 32 MB.
3. **Gateway dispatch** — `gateway_map_model()` decide o destino:
   - `claude-nvidia-*`/`nvidia-*` → cadeia NIM (tradução);
   - qualquer outro modelo → `anthropic_relay()` (passthrough SSE, sem tocar no
     corpo; a master key do gateway nunca é repassada).
4. **`convert_to_oai()`** — mensagens/blocks Anthropic → messages OpenAI;
   `system` vira role `system` (+ hint de tools), `tool_result` vira role
   `tool`, `tool_use` vira `tool_calls`; `tools` → schema function;
   `max_tokens` clampado em [1, 16384]; `stream_options.include_usage`.
5. **Fallback** — por modelo da cadeia, 2 passagens com sleep de 2s entre elas.
   `should_retry` (5xx/429) e `looks_retryable` (context length, not found)
   marcam cooldown de 60s e pulam para o próximo; 200 sem choices/caixa não-JSON
   também derrubam o modelo (cooldown 30s). Esgotado tudo: `503 overloaded_error`.
6. **Resposta** — sync: envelope Anthropic montado de `choices[0]`; stream:
   `pump_stream()` traduz deltas OpenAI para blocos Anthropic.

## Mapeamento de effort (`output_config.effort` → `reasoning_effort`)

O Claude Code sinaliza raciocínio de duas formas. Precedência em
`convert_to_oai()`:

1. **`output_config.effort`** — o slider nativo. Só `low`/`medium`/`high` são
   aceitos (`filter`); qualquer outro valor (string inventada, número, `null`)
   é **rejeitado** e a cadeia cai para o degrau seguinte — nível nunca é
   inventado.
2. **`thinking.budget_tokens`** — mapeamento grosseiro:
   `< 4096` → `low`, `< 16384` → `medium`, senão `high`.
   As fronteiras exatas (4095/4096 e 16383/16384) estão pinadas em teste.
3. **Default `"high"`** — quando o cliente não sinaliza nada, reasoning MAXIMIZADO.

O resultado sai como `reasoning_effort` no payload OpenAI. Modelos NIM
compatíveis (kimi-k3 & cia.) obedecem; nemotron-3-* já raciocina nativamente e
ignora o campo.

## Cadeias de modelos

| Alias | Cadeia (ordem de tentativa) |
|---|---|
| `claude-nvidia-ultra` / `nvidia-ultra` | nemotron-3-ultra-550b → nemotron-3-super-120b → nemotron-3-nano |
| `claude-nvidia-nemotron` / `nvidia-nemotron` | nemotron-3-super-120b → nemotron-3-nano |
| `claude-nvidia-kimi` / `nvidia-kimi` | kimi-k3 → nemotron-3-super-120b → nemotron-3-nano |
| `claude-nvidia-nano` / `nvidia-nano` | nemotron-3-nano |

Cada modelo tem seu circuit breaker independente (cooldown em epoch-ms,
`Mutex<HashMap>`). Alias `nvidia-*` desconhecido → `400 invalid_request_error`
imediato (não tenta upstream).

## Máquina de estados do stream (`pump_stream`)

- `message_start` → `ping` → blocos: `thinking` (abre/reabre por
  `reasoning_content`), `text` (um bloco, índice fixo), `tool_use` (um bloco por
  índice OpenAI, `input_json_delta` incremental) → `content_block_stop` dos
  abertos → `message_delta` (usage real: `input_tokens` do chunk final do NIM,
  `output_tokens` do `include_usage` ou estimativa) → `message_stop`.
- Keepalive `event: ping` a cada 5s de silêncio do upstream.
- Morte anormal (sem `[DONE]` nem `finish_reason`) → `event: error` e **sem**
  `message_stop` — falha honesta, cliente não confunde truncamento com sucesso.
- Linha SSE extraída byte-a-byte (`take_line`) — multibyte UTF-8 partido entre
  chunks TCP nunca vira U+FFFD.

## Escolhas de performance (commit "overclock")

- `#[tokio::main(flavor = "current_thread")]` — proxy 99% I/O-bound; 1 thread
  elimina contenção de scheduler e wake-ups extras. RSS ~15 MB sob carga.
- HTTP/2 prior knowledge no client builder — a API NVIDIA fala h2; pula ALPN
  (1 RTT a menos por conexão nova).
- `no_gzip`/`no_deflate`/`no_brotli` — o corpo SSE já vem do upstream; CPU por
  chunk economizada.
- Server: hyper direto, HTTP/1.1 (o que o Claude Code fala para localhost).
- Release: `opt-level=3`, `lto=fat`, `codegen-units=1`, `strip`, `panic=abort`
  → ~3.2 MB (≈1,2 MB com UPX).

## Segurança

- Fail-closed: gateway sem `LITELLM_MASTER_KEY` não sobe; bind default loopback.
- Master key nunca vai ao upstream; comparação constant-time; zero secrets em log.
- Anti DNS-rebinding no gateway (Host check); timeout por conexão; body ≤ 32 MB.
- Deploy de referência com `MemoryMax=256M` + `MemorySwapMax=0` (ver `deploy/`).
