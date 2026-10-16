# deploy/ — systemd de referência

Unidade endurecida para rodar o claude-nim como serviço de sistema.
Para o serviço de usuário (sem root), veja a variante no README.md.

```bash
# 1. binário
install -m 755 target/release/nim-proxy /usr/local/bin/nim-proxy

# 2. usuário dedicado sem shell
useradd --system --home /nonexistent --shell /usr/sbin/nologin claude-nim

# 3. segredos (chmod 600, dono claude-nim)
install -d -m 750 -o claude-nim -g claude-nim /etc/claude-nim
cat > /etc/claude-nim/proxy.env <<'EOF'
NVIDIA_API_KEY=nvapi-...
LITELLM_MASTER_KEY=sk-...
MODE=gateway
EOF
chown claude-nim:claude-nim /etc/claude-nim/proxy.env
chmod 600 /etc/claude-nim/proxy.env

# 4. instala a unidade
install -m 644 deploy/claude-nim.service /etc/systemd/system/claude-nim.service
systemctl daemon-reload
systemctl enable --now claude-nim
systemctl status claude-nim
curl -s http://127.0.0.1:4111/health
```

## Hardening aplicado na unidade

| Diretiva | Por quê |
|---|---|
| `MemoryMax=256M` + `MemorySwapMax=0` | teto rígido de RAM e **zero swap** — OOM-kill em vez de degenerar o host; o proxy tem RSS ~15 MB, 256M sobra folga |
| `NoNewPrivileges=true` | nada de setuid/sudo a partir do serviço |
| `ProtectSystem=strict` | filesystem inteiro read-only (o proxy não escreve nada) |
| `PrivateTmp=true` | /tmp isolado de outros serviços |
| `ProtectHome=read-only` | não lê segredos de /home; env vem de /etc/claude-nim |
| `CapabilityBoundingSet=` (vazio) | drop total de capabilities — não precisa de nenhuma |
| `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6` | só sockets IP |
| `SystemCallFilter=@system-service` | syscall whitelist padrão de serviço |
| `Restart=always` + `RestartSec=2` | volta sozinho após crash raro |
