# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere a [SemVer](https://semver.org/).

## [Unreleased]

### Fixed

- **Effort slider respeitado** — `convert_to_oai` lê `output_config.effort` do Claude Code
  (valores `low`/`medium`/high`; lixo é rejeitado, nunca inventado) com precedência
  `output_config` > `thinking.budget_tokens` > default `"high"`.
- **Vazamento de memória por request** — alias `nvidia-*` desconhecido no modo gateway
  fazia `Box::leak` da string a cada request e ainda queimava 2 passagens upstream num
  404 garantido; agora retorna `400 invalid_request_error` imediatamente.
- **`input_tokens` real no stream** — o chunk final do NIM (`stream_options.include_usage`)
  entrega o usage real de entrada, que era lido e descartado (bloco `if` vazio);
  agora vai no `message_delta` junto com `output_tokens`.
- **`sigterm()` dupla falha** — (1) sem `#[cfg(unix)]`, quebrava compilação em não-unix;
  (2) se o registro do sinal falhasse, a função retornava imediatamente e o `select!`
  encerrava o servidor na hora. Agora pendura para sempre no fallback.
- **Bind default fail-closed** — sem `-addr` (ou `-addr :porta`), escuta em
  `127.0.0.1` em vez de `0.0.0.0`; expor para a rede exige endereço explícito.
- **401 antes do buffer** — request não-autenticado nos endpoints de mensagens era
  rejeitado só depois de bufferizar até 32 MB de body.
- `cargo fmt` reintroduzido pela correção.

### Changed

- **Overclock** — runtime `current_thread` (1 thread de I/O, zero contenção de
  scheduler), HTTP/2 prior knowledge para a API NVIDIA (1 RTT a menos por conexão),
  `opt-level=3` + `lto=fat` + `codegen-units=1`, decompressores desligados
  (`no_gzip`/`no_deflate`/`no_brotli` — o corpo SSE já vem compactado do upstream).

### Tests

- Boundary pins do mapeamento de esforço: `budget_tokens` 4095/4096 (low→medium) e
  16383/16384 (medium→high); lixo e tipos não-string em `output_config.effort` caem
  no degrau seguinte da precedência.
- Pin da invariância aliases→chains (todo alias canônico do gateway tem chain —
  base do 400 para alias desconhecido).
- 22 testes, clippy zero warnings.

### Added

- `CHANGELOG.md`, `docs/architecture.md` (fluxo de request, mapeamento de effort,
  cadeias de fallback), `deploy/` (systemd de referência com `MemorySwapMax=0`),
  `.github/workflows/ci.yml` (fmt + clippy + test + release build com budget de
  tamanho do binário).

## [2.0.0] - 2026-10-09

### Added

- Proxy Anthropic Messages API ↔ NVIDIA NIM para Claude Code, binário único.
- Streaming SSE bidirecional (`message_start` → `content_block_delta` → `message_stop`),
  índice de blocos alinhado, keepalive `event: ping` a cada 5s.
- Tool calling nativo: `tools`/`tool_use`/`tool_result`/`tool_choice` 1:1.
- Thinking nativo: `reasoning_content` → bloco `thinking` + `thinking_delta`.
- Fallback em cadeia (2 passagens) com circuit breaker por modelo (cooldown 60s).
- Modo gateway: `claude-nvidia-*` → NIM, qualquer outro modelo → passthrough
  SSE real para `api.anthropic.com` na mesma sessão.
- Erros no envelope Anthropic; falha honesta em stream (`event: error`, sem
  `message_stop` falso).
- Anti DNS-rebinding (só `Host: 127.0.0.1`/`localhost` no gateway), auth
  constant-time, fail-closed no start, limite de body 32 MB.
- Cadeias: `ultra` → super → nano; `kimi` → super → nano; binários da release
  com `SHA256SUMS.txt`.

[Unreleased]: https://github.com/rianprei/claude-nim/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/rianprei/claude-nim/releases/tag/v2.0.0
