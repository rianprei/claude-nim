// nim-proxy (Rust) — proxy ultra-lean Anthropic Messages API <-> NVIDIA NIM
// Otimizado para CPU fraco (Celeron Gemini Lake Refresh, sem AVX2) e 4GB RAM.
//
//   NVIDIA_API_KEY=nvapi-... LITELLM_MASTER_KEY=sk-... nim-proxy -addr 127.0.0.1:4000
//   NIM_API_BASE=http://127.0.0.1:9100/v1   (override p/ testes)

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;

use tokio::sync::mpsc;
use std::env;

use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Response, StatusCode};
use hyper::service::service_fn;
use hyper::server::conn::http1;

const DEFAULT_NIM_BASE: &str = "https://integrate.api.nvidia.com/v1";
const REQUEST_TIMEOUT_SECS: u64 = 600;
const DIAL_TIMEOUT_SECS: u64 = 15;

type Chain = Vec<&'static str>;

fn model_chains() -> HashMap<&'static str, Chain> {
    let nano = vec!["nvidia/nemotron-3-nano-omni-30b-a3b-reasoning"];
    let nemotron = vec![
        "nvidia/nemotron-3-super-120b-a12b",
        "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning",
    ];
    let ultra = vec![
        "nvidia/nemotron-3-ultra-550b-a55b",
        "nvidia/nemotron-3-super-120b-a12b",
        "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning",
    ];
    let kimi = vec![
        "moonshotai/kimi-k3",
        "nvidia/nemotron-3-super-120b-a12b",
        "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning",
    ];
    let mut m = HashMap::new();
    m.insert("nvidia-ultra", ultra.clone());
    m.insert("claude-nvidia-ultra", ultra.clone());
    m.insert("nvidia-kimi", kimi.clone());
    m.insert("claude-nvidia-kimi", kimi.clone());
    m.insert("nvidia-nemotron", nemotron.clone());
    m.insert("claude-nvidia-nemotron", nemotron.clone());
    m.insert("nvidia-nano", nano.clone());
    m.insert("claude-nvidia-nano", nano.clone());
    m
}

struct Cfg {
    nim_base: String,
    nim_key: String,
    master_key: String,
    client: Client,
    chains: HashMap<&'static str, Chain>,
    mode: Mode,
    anthropic_base: String,
    // Circuit breaker por modelo: epoch ms até quando o modelo está "friando" após 429/5xx
    cooldowns: std::sync::Mutex<HashMap<String, i64>>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    NimProxy,
    Gateway,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let addr = parse_addr();
    let nim_key = env::var("NVIDIA_API_KEY").unwrap_or_default();
    let master_key = env::var("LITELLM_MASTER_KEY").unwrap_or_default();
    if nim_key.is_empty() {
        eprintln!("[nim-proxy] NVIDIA_API_KEY não definida");
        std::process::exit(1);
    }
    let nim_base = env::var("NIM_API_BASE").unwrap_or_else(|_| DEFAULT_NIM_BASE.to_string());

    let mode = match env::var("MODE").as_deref() {
        Ok("gateway") | Ok("GATEWAY") => Mode::Gateway,
        _ => Mode::NimProxy,
    };
    let anthropic_base =
        env::var("ANTHROPIC_BASE").unwrap_or_else(|_| "https://api.anthropic.com".to_string());

    // Fail-closed: no gateway, sem master key válida não sobe
    if mode == Mode::Gateway && master_key.trim().is_empty() {
        eprintln!("[nim-proxy] FATAL: MODE=gateway exige LITELLM_MASTER_KEY definida");
        std::process::exit(1);
    }

    let read_timeout = Duration::from_secs(
        env::var("NIM_READ_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
    );

    let client = Client::builder()
        .pool_max_idle_per_host(4)
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(DIAL_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .read_timeout(read_timeout)
        .build()
        .expect("client");

    let cfg = Arc::new(Cfg {
        nim_base,
        nim_key,
        master_key,
        client,
        chains: model_chains(),
        mode,
        anthropic_base,
        cooldowns: std::sync::Mutex::new(HashMap::new()),
    });

    let listener = TcpListener::bind(&addr).await.expect("bind");
    eprintln!("[nim-proxy] Rust turbo ativo em {} | modelos: {} | modo: {:?}", addr, cfg.chains.len(), if mode == Mode::Gateway { "gateway" } else { "nim" });

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let cfg = cfg.clone();
                        tokio::spawn(async move {
                            hyperserve_conn(stream, cfg).await;
                        });
                    }
                    Err(e) => eprintln!("[nim-proxy] accept: {e}"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("[nim-proxy] shutdown limpo");
                break;
            }
            _ = sigterm() => {
                eprintln!("[nim-proxy] shutdown limpo (SIGTERM)");
                break;
            }
        }
    }
}

#[cfg(unix)]
async fn hyperserve_conn(stream: tokio::net::TcpStream, cfg: Arc<Cfg>) {
    let io = hyper_util::rt::TokioIo::new(stream);
    let svc = service_fn(move |req: hyper::Request<Incoming>| {
        let cfg = cfg.clone();
        async move {
            let resp = route(req, cfg).await;
            Ok::<_, Infallible>(resp)
        }
    });
    // Timeout por conexão: sem isso, slowloris segura task+buffer para sempre
    match tokio::time::timeout(
        Duration::from_secs(900),
        http1::Builder::new().serve_connection(io, svc),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("[nim-proxy] conn: {e}"),
        Err(_) => eprintln!("[nim-proxy] conn: idle >900s, fechando"),
    }
}

async fn sigterm() {
    if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        s.recv().await;
    }
}

#[cfg(not(unix))]
async fn sigterm() {
    std::future::pending::<()>().await;
}

fn parse_addr() -> String {
    let mut args = env::args();
    let _ = args.next();
    let mut addr = ":4000".to_string();
    while let Some(a) = args.next() {
        if a == "-addr" {
            if let Some(v) = args.next() {
                addr = v;
            }
        }
    }
    if addr.starts_with(':') {
        format!("0.0.0.0{}", addr)
    } else {
        addr
    }
}

// ─── HTTP mínimo (sem hyper, parsing manual enxuto) ─────────────────────────


// Router: recebe hyper::Request e devolve hyper::Response (body streaming compatível)
async fn route(
    req: hyper::Request<Incoming>,
    cfg: Arc<Cfg>,
) -> Response<BoxBody<Bytes, Infallible>> {
    let method = req.method().to_string();
    let path_full = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/").to_string();
    let path = req.uri().path().to_string();

    let mut headers: HashMap<String, String> = HashMap::new();
    for (k, v) in req.headers() {
        headers.insert(k.as_str().to_lowercase(), v.to_str().unwrap_or("").to_string());
    }

    let authed = if cfg.mode == Mode::Gateway {
        headers
            .get("authorization")
            .map(|a| a.starts_with("Bearer sk-ant-"))
            .unwrap_or(false)
            || check_auth(&headers, &cfg.master_key)
    } else {
        check_auth(&headers, &cfg.master_key)
    };

    // Só responde a requests destinados a este proxy (anti DNS-rebinding)
    if cfg.mode == Mode::Gateway {
        let host_ok = headers
            .get("host")
            .map(|h| {
                let h = h.split(':').next().unwrap_or("");
                h == "127.0.0.1" || h == "localhost" || h.is_empty()
            })
            .unwrap_or(true);
        if !host_ok {
            return full_response(403, "application/json", json!({"type":"error","error":{"type":"forbidden","message":"host não permitido"}}).to_string().into_bytes());
        }
    }

    // HEAD: probe de conectividade, sem corpo e sem custo de upstream
    if method == "HEAD" {
        let mut r = Response::new(box_body(Full::new(Bytes::new())));
        *r.status_mut() = StatusCode::OK;
        return r;
    }

    // Body com limite: sem isso, cliente não-autenticado força alocação
    let mut body_vec: Vec<u8> = Vec::new();
    let mut incoming = req.into_body();
    loop {
        match incoming.frame().await {
            Some(Ok(f)) => {
                if let Some(chunk) = f.data_ref() {
                    body_vec.extend_from_slice(chunk);
                    if body_vec.len() > 32 << 20 {
                        return full_response(413, "application/json", br#"{"type":"error","error":{"type":"request_too_large","message":"body > 32MB"}}"#.to_vec());
                    }
                }
            }
            Some(Err(_)) => return full_response(400, "application/json", br#"{"type":"error","error":{"type":"invalid_request_error","message":"falha ao ler body"}}"#.to_vec()),
            None => break,
        }
    }
    let mut body = body_vec;

    // EMAIL_REWRITE_FROM/TO (opcional): reescreve correspondências exatas no
    // corpo — útil em cadeias de proxy. Sem valores hardcoded.
    if cfg.mode == Mode::Gateway {
        if let (Ok(from), Ok(to)) = (env::var("EMAIL_REWRITE_FROM"), env::var("EMAIL_REWRITE_TO")) {
            if !from.is_empty() && from != to {
                if let Ok(as_str) = std::str::from_utf8(&body) {
                    let owned = as_str.replace(&from, &to);
                    body = owned.into_bytes();
                }
            }
        }
    }

    match (method.as_str(), path.as_str()) {
        ("GET", "/health") | ("GET", "/") => {
            return full_response(200, "application/json",
                br#"{"status":"ok","proxy":"nim-proxy-rust","version":"2.0.0-gateway"}"#.to_vec());
        }
        ("GET", "/v1/models") | ("GET", "/models") => {
            if cfg.mode == Mode::Gateway && !authed {
                return full_response(401, "application/json", json!({"type":"error","error":{"type":"authentication_error","message":"não autorizado"}}).to_string().into_bytes());
            }
            if cfg.mode == Mode::Gateway {
                let fused = json!({"data": [
                    {"id": "claude-nvidia-nemotron", "display_name": "NVIDIA Nemotron 3 Super 120B (nvidia-nemotron)", "description": "NVIDIA Nemotron 3 Super 120B"},
                    {"id": "claude-nvidia-kimi", "display_name": "Moonshot Kimi K3 (nvidia-kimi)", "description": "Moonshot Kimi K3"},
                    {"id": "claude-nvidia-ultra", "display_name": "NVIDIA Nemotron 3 Ultra 550B (nvidia-ultra)", "description": "NVIDIA Nemotron 3 Ultra 550B"},
                    {"id": "claude-nvidia-nano", "display_name": "NVIDIA Nemotron Nano 30B (nvidia-nano)", "description": "NVIDIA Nemotron Nano 30B"},
                ]}).to_string();
                return full_response(200, "application/json", fused.into_bytes());
            }
            let data: Vec<Value> = cfg.chains.keys().map(|k| json!({"id": k, "object": "model", "created": 0, "owned_by": "nvidia"})).collect();
            let body = json!({"object": "list", "data": data}).to_string();
            return full_response(200, "application/json", body.into_bytes());
        }
        ("POST", "/v1/messages/count_tokens") | ("POST", "/messages/count_tokens") => {
            if !authed {
                return full_response(401, "application/json", json!({"type":"error","error":{"type":"authentication_error","message":"não autorizado"}}).to_string().into_bytes());
            }
            let parsed: Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(_) => return full_response(400, "application/json", json!({"type":"error","error":{"type":"invalid_request_error","message":"JSON inválido"}}).to_string().into_bytes()),
            };
            if cfg.mode == Mode::Gateway {
                let model = parsed.get("model").and_then(|m| m.as_str()).unwrap_or("");
                if gateway_map_model(model).is_some() {
                    let n = estimate_tokens(&body);
                    return full_response(200, "application/json", json!({"input_tokens": n}).to_string().into_bytes());
                }
                return anthropic_relay(&cfg, &headers, &path_full, &body, &method).await;
            }
            let n = estimate_tokens(&body);
            return full_response(200, "application/json", json!({"input_tokens": n}).to_string().into_bytes());
        }
        ("POST", "/v1/messages") | ("POST", "/messages") => {
            if !authed {
                return full_response(401, "application/json", json!({"type":"error","error":{"type":"authentication_error","message":"não autorizado"}}).to_string().into_bytes());
            }
            let parsed: Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(_) => return full_response(400, "application/json", json!({"type":"error","error":{"type":"invalid_request_error","message":"JSON inválido"}}).to_string().into_bytes()),
            };
            let is_stream = parsed.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
            let orig_model = parsed.get("model").and_then(|v| v.as_str()).unwrap_or("nvidia-nano").to_string();

            let chain: Chain = if cfg.mode == Mode::Gateway {
                let alias = match gateway_map_model(&orig_model) {
                    None => return anthropic_relay(&cfg, &headers, &path_full, &body, &method).await,
                    Some(a) => a,
                };
                cfg.chains.get(alias.as_str()).cloned().unwrap_or_else(|| vec![Box::leak(alias.into_boxed_str()) as &'static str])
            } else {
                match cfg.chains.get(orig_model.as_str()) {
                    None => return full_response(400, "application/json", json!({"type":"error","error":{"type":"invalid_request_error","message":"modelo desconhecido"}}).to_string().into_bytes()),
                    Some(c) => c.clone(),
                }
            };
            let mut oai_parsed = parsed.clone();
            if cfg.mode == Mode::Gateway {
                match gateway_map_model(&orig_model) {
                    Some(alias) => oai_parsed["model"] = json!(alias),
                    None => {}
                }
            }
            let oai = convert_to_oai(&oai_parsed);

            if is_stream {
                match prepare_stream(&cfg, &oai, &chain).await {
                    None => return full_response(503, "application/json",
                        br#"{"type":"error","error":{"type":"overloaded_error","message":"todos os modelos falharam no streaming"}}"#.to_vec()),
                    Some((resp, used_model)) => {
                        if resp.status().as_u16() != 200 {
                            let status = resp.status().as_u16();
                            let raw = resp.text().await.unwrap_or_default();
                            return full_response(status, "application/json", raw.into_bytes());
                        }
                        let (tx, rx) = mpsc::channel::<Bytes>(8);
                        let orig = orig_model.clone();
                        tokio::spawn(async move { pump_stream(tx, resp, used_model, &orig).await });
                        return stream_sse(rx);
                    }
                }
            } else {
                let (status, ctype, out) = run_sync(&cfg, oai, chain, &orig_model).await;
                return full_response(status, &ctype, out.into_bytes());
            }
        }
        _ => {
            if cfg.mode == Mode::Gateway && authed {
                return anthropic_relay(&cfg, &headers, &path_full, &body, &method).await;
            }
            return full_response(404, "application/json", json!({"type":"error","error":{"type":"proxy_error","message":"endpoint não encontrado"}}).to_string().into_bytes());
        }
    }
}


type BoxBody<B, E> = http_body_util::combinators::UnsyncBoxBody<B, E>;

fn box_body<B, E>(b: impl http_body::Body<Data = B, Error = E> + Send + 'static) -> BoxBody<B, E>
where
    B: bytes::Buf + Sync,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    b.boxed_unsync()
}

fn full_response(status: u16, content_type: &str, body: Vec<u8>) -> Response<BoxBody<Bytes, Infallible>> {
    let mut r = Response::new(box_body(Full::new(Bytes::from(body)).map_err(|e| match e {})));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    if let Ok(hv) = content_type.parse() {
        r.headers_mut().insert(hyper::header::CONTENT_TYPE, hv);
    }
    r
}

fn stream_sse(rx: mpsc::Receiver<Bytes>) -> Response<BoxBody<Bytes, Infallible>> {
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
        .map(|b| Ok::<_, Infallible>(Frame::data(b)));
    let body = StreamBody::new(Box::pin(stream)).boxed_unsync();
    let mut r = Response::new(body);
    *r.status_mut() = StatusCode::OK;
    if let Ok(hv) = "text/event-stream".parse() {
        r.headers_mut().insert(hyper::header::CONTENT_TYPE, hv);
    }
    r.headers_mut().insert("cache-control", "no-cache".parse().unwrap());
    r.headers_mut().insert("connection", "keep-alive".parse().unwrap());
    r.headers_mut().insert("x-accel-buffering", "no".parse().unwrap());
    r
}


// Circuit breaker: marca modelo em cooldown (429/5xx) e diz se pode tentar
fn model_cooling(cfg: &Cfg, model: &str) -> bool {
    let now = chrono_lite();
    if let Ok(map) = cfg.cooldowns.lock() {
        if let Some(until) = map.get(model) {
            return *until > now;
        }
    }
    false
}

fn model_cooldown(cfg: &Cfg, model: &str, secs: i64) {
    let until = chrono_lite() + secs * 1000;
    if let Ok(mut map) = cfg.cooldowns.lock() {
        map.insert(model.to_string(), until);
    }
}

// Converte erro do NIM (envelope OpenAI) para o envelope Anthropic
fn nim_error_to_anthropic(status: u16, raw: &str) -> (u16, String, String) {
    let msg = serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.get("message")).and_then(|m| m.as_str()).map(String::from))
        .unwrap_or_else(|| {
            if raw.is_empty() {
                format!("upstream status {status}")
            } else {
                raw.chars().take(300).collect()
            }
        });
    (status, "application/json".into(), json!({"type":"error","error":{"type":"api_error","message":msg}}).to_string())
}

async fn run_sync(cfg: &Cfg, oai: Value, chain: Chain, orig_model: &str) -> (u16, String, String) {
    let mut last_status = 0u16;
    let mut last_cause = String::from("nenhuma tentativa");

    for pass in 0..2 {
        if pass == 1 { tokio::time::sleep(Duration::from_secs(2)).await; }
        for model in &chain {
            if model_cooling(cfg, model) {
                eprintln!("[nim-proxy] sync {model} em cooldown — pulando");
                continue;
            }
            match nim_post(cfg, &oai, model).await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let raw = resp.text().await.unwrap_or_default();
                    last_status = status;
                    if should_retry(status) || status == 400 && looks_retryable(&raw) || status == 404 {
                        eprintln!("[nim-proxy] sync {model} HTTP {status} — fallback");
                        model_cooldown(cfg, model, 60);
                        last_cause = raw.clone();
                        continue;
                    }
                    if status != 200 {
                        return nim_error_to_anthropic(status, &raw);
                    }
                    // 200 com corpo vazio/inválido = falha, não sucesso
                    let v: Value = match serde_json::from_str(&raw) {
                        Ok(v) => v,
                        Err(_) => {
                            eprintln!("[nim-proxy] sync {model} corpo não-JSON em 200");
                            model_cooldown(cfg, model, 30);
                            last_cause = "corpo não-JSON".into();
                            continue;
                        }
                    };
                    if v.get("choices").and_then(|c| c.as_array()).map(|a| a.is_empty()).unwrap_or(true) {
                        eprintln!("[nim-proxy] sync {model} sem choices em 200");
                        model_cooldown(cfg, model, 30);
                        last_cause = "resposta sem choices".into();
                        continue;
                    }
                    let choice = v.get("choices").and_then(|c| c.get(0)).unwrap_or(&Value::Null);
                    let finish = choice.get("finish_reason").and_then(|f| f.as_str()).unwrap_or("");
                    let mut stop_reason_owned = if finish == "length" { "max_tokens".to_string() } else { "end_turn".to_string() };
                    if finish == "tool_calls" { stop_reason_owned = "tool_use".into(); }
                    let msg = choice.get("message").unwrap_or(&Value::Null);
                    let content = msg.get("content").and_then(|c| c.as_str()).unwrap_or("");
                    let reasoning = msg.get("reasoning_content").and_then(|r| r.as_str()).unwrap_or("");
                    let mut blocks: Vec<Value> = Vec::new();
                    if !reasoning.is_empty() {
                        blocks.push(json!({"type": "thinking", "thinking": reasoning}));
                    }
                    if !content.is_empty() {
                        blocks.push(json!({"type": "text", "text": content}));
                    }
                    if let Some(tcs) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                        if !tcs.is_empty() && stop_reason_owned == "end_turn" { stop_reason_owned = "tool_use".into(); }
                        for tc in tcs {
                            let id = tc.get("id").and_then(|i| i.as_str()).unwrap_or("");
                            let name = tc.get("function").and_then(|f| f.get("name")).and_then(|n| n.as_str()).unwrap_or("");
                            let args_str = tc.get("function").and_then(|f| f.get("arguments")).and_then(|a| a.as_str()).unwrap_or("{}");
                            let input: Value = serde_json::from_str(args_str).unwrap_or(json!({}));
                            blocks.push(json!({"type": "tool_use", "id": id, "name": name, "input": input}));
                        }
                    }
                    if blocks.is_empty() { blocks.push(json!({"type": "text", "text": ""})); }
                    let usage = v.get("usage").unwrap_or(&Value::Null);
                    let anth = json!({
                        "id": format!("msg_{}", v.get("id").and_then(|i| i.as_str()).unwrap_or("x")),
                        "type": "message", "role": "assistant",
                        "content": blocks,
                        "model": orig_model, "stop_reason": stop_reason_owned, "stop_sequence": null,
                        "usage": {
                            "input_tokens": usage.get("prompt_tokens").and_then(|t| t.as_i64()).unwrap_or(0),
                            "output_tokens": usage.get("completion_tokens").and_then(|t| t.as_i64()).unwrap_or(0),
                        }
                    });
                    eprintln!("[nim-proxy] sync OK via {model}");
                    return (200, "application/json".into(), anth.to_string());
                }
                Err(e) => eprintln!("[nim-proxy] sync {model} erro: {e} — fallback"),
            }
        }
    }
    let msg = format!("todos os modelos falharam (último status: {last_status}, causa: {last_cause})");
    (503, "application/json".into(), json!({"type":"error","error":{"type":"overloaded_error","message":msg}}).to_string())
}

async fn prepare_stream(cfg: &Cfg, oai: &Value, chain: &Chain) -> Option<(reqwest::Response, &'static str)> {
    let mut last_cause = String::new();
    for pass in 0..2 {
        if pass == 1 { tokio::time::sleep(Duration::from_secs(2)).await; }
        for model in chain {
            if model_cooling(cfg, model) {
                eprintln!("[nim-proxy] stream {model} em cooldown — pulando");
                continue;
            }
            match nim_post(cfg, oai, *model).await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if should_retry(status) {
                        eprintln!("[nim-proxy] stream {model} HTTP {status} — fallback");
                        model_cooldown(cfg, *model, 60);
                        continue;
                    }
                    return Some((resp, *model));
                }
                Err(e) => {
                    eprintln!("[nim-proxy] stream {model} erro: {e} — fallback");
                    last_cause = e.to_string();
                }
            }
        }
    }
    if !last_cause.is_empty() {
        eprintln!("[nim-proxy] stream falhou: {last_cause}");
    }
    None
}

async fn pump_stream(
    tx: mpsc::Sender<Bytes>,
    resp: reqwest::Response,
    used_model: &'static str,
    orig_model: &str,
) {
    let msg_id = format!("msg_{}", chrono_lite());
    if !sse(&tx, "message_start", json!({
        "type":"message_start","message":{
            "id":msg_id,"type":"message","role":"assistant",
            "content":[],"model":orig_model,
            "stop_reason":null,"stop_sequence":null,
            "usage":{"input_tokens":1,"output_tokens":0}
        }
    })).await { return; }
    if !sse(&tx, "ping", json!({"type":"ping"})).await { return; }

    let mut bytes_stream = Box::pin(resp.bytes_stream());
    let mut buf: Vec<u8> = Vec::new();
    let mut output_tokens = 0i64;
    let mut stop_reason = "end_turn".to_string();
    let mut thinking_open = false;      // bloco thinking atualmente aberto
    let mut thinking_anth_idx: Option<usize> = None;

    // Controle de blocos de conteúdo Anthropic (index sequencial)
    let mut next_block_idx = 0usize;
    let mut text_block_idx: Option<usize> = None;
    // tool_calls: oai_idx -> (anth_idx, visto id/name)
    let mut tool_blocks: HashMap<i64, usize> = HashMap::new();
    let mut open_tool_anth: Vec<usize> = Vec::new();

    let mut saw_done = false;
    let mut saw_finish = false;
    let mut upstream_err: Option<String> = None;

    let mut keepalive = tokio::time::interval(Duration::from_secs(5));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // fecha o bloco thinking se estiver aberto (antes de abrir texto/tool)
    macro_rules! close_thinking {
        () => {
            if thinking_open {
                if let Some(idx) = thinking_anth_idx.take() {
                    let _ = sse(&tx, "content_block_stop", json!({"type":"content_block_stop","index":idx})).await;
                }
                thinking_open = false;
            }
        };
    }

    loop {
        let chunk = tokio::select! {
            c = bytes_stream.next() => c,
            _ = keepalive.tick() => {
                if !sse(&tx, "ping", json!({"type":"ping"})).await { return; }
                continue;
            }
        };
        let Some(chunk) = chunk else { break };
        let Ok(b) = chunk else {
            upstream_err = Some("stream do upstream caiu".into());
            break;
        };
        buf.extend_from_slice(&b);
        keepalive.reset();

        while let Some(raw_line) = take_line(&mut buf) {
            let line = String::from_utf8_lossy(&raw_line).to_string();
            let Some(rest) = line.strip_prefix("data:") else { continue };
            let data = rest.trim_start();
            if data == "[DONE]" {
                saw_done = true;
                continue;
            }
            let Ok(v) = serde_json::from_str::<Value>(data) else { continue };

            if let Some(u) = v.get("usage").and_then(|u| u.get("completion_tokens")).and_then(|t| t.as_i64()) {
                output_tokens = u;
            }
            if let Some(u) = v.get("usage").and_then(|u| u.get("prompt_tokens")).and_then(|t| t.as_i64()) {
                if u > 0 {
                    //usage real de entrada chega no chunk final quando stream_options.include_usage
                }
            }
            let Some(choice) = v.get("choices").and_then(|c| c.get(0)) else { continue };
            match choice.get("finish_reason").and_then(|f| f.as_str()) {
                Some("length") => stop_reason = "max_tokens".into(),
                Some("tool_calls") => stop_reason = "tool_use".into(),
                Some(_) => saw_finish = true,
                None => {}
            }
            let delta = choice.get("delta").unwrap_or(&Value::Null);

            // reasoning_content: abre (ou REABRE) bloco thinking
            if let Some(rc) = delta.get("reasoning_content").and_then(|r| r.as_str()) {
                if !rc.is_empty() {
                    if !thinking_open {
                        let idx = next_block_idx; next_block_idx += 1;
                        if !sse(&tx, "content_block_start", json!({"type":"content_block_start","index":idx,"content_block":{"type":"thinking","thinking":""}})).await { return; }
                        thinking_open = true;
                        thinking_anth_idx = Some(idx);
                    }
                    output_tokens += 1;
                    let ti = thinking_anth_idx.unwrap_or(0);
                    if !sse(&tx, "content_block_delta", json!({"type":"content_block_delta","index":ti,"delta":{"type":"thinking_delta","thinking":rc}})).await { return; }
                }
            }

            // content: fecha thinking pendente, abre bloco texto
            if let Some(c) = delta.get("content").and_then(|c| c.as_str()) {
                if !c.is_empty() {
                    close_thinking!();
                    if text_block_idx.is_none() {
                        let idx = next_block_idx; next_block_idx += 1;
                        if !sse(&tx, "content_block_start", json!({"type":"content_block_start","index":idx,"content_block":{"type":"text","text":""}})).await { return; }
                        text_block_idx = Some(idx);
                    }
                    let ti = text_block_idx.unwrap_or(0);
                    output_tokens += 1;
                    if !sse(&tx, "content_block_delta", json!({"type":"content_block_delta","index":ti,"delta":{"type":"text_delta","text":c}})).await { return; }
                }
            }

            // tool_calls: mapa por índice OpenAI — reabertura não duplica bloco
            if let Some(tcs) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                for tc in tcs {
                    let idx = tc.get("index").and_then(|i| i.as_i64()).unwrap_or(0);
                    let args = tc.get("function").and_then(|f| f.get("arguments")).and_then(|a| a.as_str()).unwrap_or("");
                    let name = tc.get("function").and_then(|f| f.get("name")).and_then(|n| n.as_str()).unwrap_or("");
                    let id = tc.get("id").and_then(|i| i.as_str()).unwrap_or("");
                    let anth_idx = if let Some(&a) = tool_blocks.get(&idx) {
                        a
                    } else {
                        close_thinking!();
                        if let Some(open_idx) = text_block_idx.take() {
                            if !sse(&tx, "content_block_stop", json!({"type":"content_block_stop","index":open_idx})).await { return; }
                        }
                        let a = next_block_idx; next_block_idx += 1;
                        if !sse(&tx, "content_block_start", json!({
                            "type":"content_block_start","index":a,
                            "content_block":{"type":"tool_use","id":id,"name":name,"input":{}}
                        })).await { return; }
                        tool_blocks.insert(idx, a);
                        open_tool_anth.push(a);
                        a
                    };
                    if !args.is_empty() {
                        if !sse(&tx, "content_block_delta", json!({
                            "type":"content_block_delta","index":anth_idx,
                            "delta":{"type":"input_json_delta","partial_json":args}
                        })).await { return; }
                    }
                }
            }
        }
    }

    // fecha thinking se sobrou aberto
    if thinking_open {
        if let Some(idx) = thinking_anth_idx.take() {
            let _ = sse(&tx, "content_block_stop", json!({"type":"content_block_stop","index":idx})).await;
        }
    }

    // stream morreu de forma anormal (sem [DONE] e sem finish_reason): erro explícito
    if upstream_err.is_some() || (!saw_done && !saw_finish) {
        let reason = upstream_err.unwrap_or_else(|| "upstream encerrou sem [DONE]".into());
        eprintln!("[nim-proxy] stream {used_model} ANORMAL: {reason}");
        let _ = sse(&tx, "error", json!({
            "type":"error",
            "error":{"type":"api_error","message":format!("stream interrompido: {reason}")}
        })).await;
        return; // sem message_stop — cliente trata como falha
    }

    // fecha blocos abertos
    if let Some(open_idx) = text_block_idx {
        let _ = sse(&tx, "content_block_stop", json!({"type":"content_block_stop","index":open_idx})).await;
    } else if tool_blocks.is_empty() && next_block_idx == 0 {
        let _ = sse(&tx, "content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})).await;
        let _ = sse(&tx, "content_block_stop", json!({"type":"content_block_stop","index":0})).await;
    }
    for a in open_tool_anth {
        let _ = sse(&tx, "content_block_stop", json!({"type":"content_block_stop","index":a})).await;
    }

    if stop_reason == "end_turn" && !tool_blocks.is_empty() {
        stop_reason = "tool_use".into();
    }

    let _ = sse(&tx, "message_delta", json!({"type":"message_delta","delta":{"stop_reason":stop_reason,"stop_sequence":null},"usage":{"output_tokens":output_tokens}})).await;
    let _ = sse(&tx, "message_stop", json!({"type":"message_stop"})).await;
    eprintln!("[nim-proxy] stream OK via {used_model} (~{output_tokens} tokens)");
}

fn gateway_map_model(model: &str) -> Option<String> {
    match model {
        "claude-nvidia-nemotron" => Some("nvidia-nemotron".into()),
        "claude-nvidia-kimi" => Some("nvidia-kimi".into()),
        "claude-nvidia-ultra" => Some("nvidia-ultra".into()),
        "claude-nvidia-nano" => Some("nvidia-nano".into()),
        m if m.starts_with("nvidia-") => Some(m.into()),
        _ => None,
    }
}

async fn anthropic_relay(
    cfg: &Cfg,
    headers: &HashMap<String, String>,
    path_full: &str,
    body: &[u8],
    method: &str,
) -> Response<BoxBody<Bytes, Infallible>> {
    let url = format!("{}{}", cfg.anthropic_base, path_full);
    let http_method = match method {
        "GET" => reqwest::Method::GET,
        _ => reqwest::Method::POST,
    };
    let mut req = cfg.client.request(http_method, &url);
    if method != "GET" {
        req = req.body(body.to_vec());
    }
    if let Some(ct) = headers.get("content-type") { req = req.header("content-type", ct); }
    // Não repassa a master key do gateway para terceiros
    let is_master = |h: &str| -> bool {
        h.trim_start_matches("Bearer ") == cfg.master_key.trim() || h == cfg.master_key.trim()
    };
    if let Some(auth) = headers.get("authorization") {
        if !is_master(auth) { req = req.header("authorization", auth); }
    }
    if let Some(ak) = headers.get("x-api-key") {
        if !is_master(ak) { req = req.header("x-api-key", ak); }
    }
    if let Some(beta) = headers.get("anthropic-beta") { req = req.header("anthropic-beta", beta); }
    if let Some(av) = headers.get("anthropic-version") { req = req.header("anthropic-version", av); }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return full_response(503, "application/json", format!("{{\"type\":\"error\",\"error\":{{\"type\":\"overloaded_error\",\"message\":\"anthropic relay: {}\"}}}}", e).into_bytes()),
    };

    let status = resp.status();
    let mut extra_headers: Vec<(String, String)> = Vec::new();
    for (k, v) in resp.headers() {
        let name = k.as_str().to_lowercase();
        if name == "content-length" || name == "transfer-encoding" || name == "connection" { continue; }
        if let Ok(rv) = v.to_str() {
            extra_headers.push((k.to_string(), rv.to_string()));
        }
    }
    let (tx, rx) = mpsc::channel::<Bytes>(8);
    tokio::spawn(async move {
        let mut s2 = resp.bytes_stream();
        while let Some(c) = s2.next().await {
            if let Ok(c) = c {
                if tx.send(c).await.is_err() { break; }
            }
        }
    });

    let body = StreamBody::new(Box::pin(
        tokio_stream::wrappers::ReceiverStream::new(rx)
            .map(|b| Ok::<_, Infallible>(Frame::data(b))),
    ))
    .boxed_unsync();

    let mut r = Response::builder()
        .status(status)
        .body(body)
        .unwrap_or_else(|_| Response::new(box_body(Full::new(Bytes::new()))));
    for (k, v) in extra_headers {
        if let Ok(hv) = hyper::header::HeaderValue::from_str(&v) {
            if let Ok(h) = hyper::header::HeaderName::from_bytes(k.as_bytes()) {
                r.headers_mut().insert(h, hv);
            }
        }
    }
    r
}

fn check_auth(headers: &HashMap<String, String>, master: &str) -> bool {
    if master.is_empty() {
        return true;
    }
    let key = headers
        .get("x-api-key")
        .cloned()
        .or_else(|| {
            headers
                .get("authorization")
                .map(|a| a.trim_start_matches("Bearer ").trim_start_matches("bearer ").to_string())
        })
        .unwrap_or_default();
    constant_time_eq(key.trim().as_bytes(), master.trim().as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut r = 0u8;
    for i in 0..a.len() {
        r |= a[i] ^ b[i];
    }
    r == 0
}

fn estimate_tokens(body: &[u8]) -> usize {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let mut text_all = String::new();
    if let Some(sys) = v.get("system") {
        text_all.push_str(&extract_text(sys));
        text_all.push(' ');
    }
    if let Some(msgs) = v.get("messages").and_then(|m| m.as_array()) {
        for m in msgs {
            text_all.push_str(&extract_text(m.get("content").unwrap_or(&Value::Null)));
            text_all.push(' ');
        }
    }
    estimate_tokens_str(&text_all)
}

// Heurística: ~4 chars/token PT/EN + contagem de chars CJK/latin denso
fn estimate_tokens_str(text: &str) -> usize {
    let mut total = 0usize;
    for ch in text.chars() {
        let c = ch as u32;
        if (0x4E00..=0x9FFF).contains(&c) || (0x3040..=0x30FF).contains(&c) {
            total += 1; // CJK ≈ 1 token/char
        }
    }
    let non_cjk = text.chars().filter(|c| {
        let c = *c as u32;
        !((0x4E00..=0x9FFF).contains(&c) || (0x3040..=0x30FF).contains(&c))
    }).count();
    // ~3.7 chars por token em PT/EN técnico
    total += (non_cjk * 10) / 37;
    total.max(1) + 3 // + overhead de estrutura por mensagem
}

fn extract_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(arr) => {
            let mut parts: Vec<String> = Vec::new();
            for item in arr {
                let typ = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match typ {
                    "text" => {
                        if let Some(t) = item.get("text").and_then(|t| t.as_str()) {
                            if !t.is_empty() {
                                parts.push(t.to_string());
                            }
                        }
                    }
                    "tool_result" => {
                        if let Some(c) = item.get("content") {
                            match c {
                                Value::String(s) => parts.push(format!("[tool_result: {s}]")),
                                other => parts.push(format!("[tool_result: {}]", extract_text(other))),
                            }
                        }
                    }
                    "tool_use" => {
                        let name = item.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                        let input = item.get("input").map(|i| i.to_string()).unwrap_or_default();
                        parts.push(format!("[tool_use {name}: {input}]"));
                    }
                    "image" => {
                        parts.push("[imagem omitida — o modelo NVIDIA configurado é text-only]".into());
                    }
                    "document" => {
                        parts.push("[documento omitido — o modelo NVIDIA configurado é text-only]".into());
                    }
                    _ => {}
                }
            }
            parts.join("\n")
        }
        _ => String::new(),
    }
}

fn convert_to_oai(req: &Value) -> Value {
    let mut msgs: Vec<Value> = Vec::new();
    let sys = extract_text(req.get("system").unwrap_or(&Value::Null));
    if !sys.is_empty() {
        msgs.push(json!({"role": "system", "content": sys}));
    }

    if let Some(arr) = req.get("messages").and_then(|m| m.as_array()) {
        for m in arr {
            let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            let content = m.get("content").unwrap_or(&Value::Null);

            let mut text_parts: Vec<String> = Vec::new();
            let mut tool_results: Vec<Value> = Vec::new();
            let mut tool_calls: Vec<Value> = Vec::new();

            match content {
                Value::String(s) => text_parts.push(s.clone()),
                Value::Array(blocks) => {
                    for b in blocks {
                        let typ = b.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        match typ {
                            "text" => {
                                if let Some(t) = b.get("text").and_then(|t| t.as_str()) {
                                    if !t.is_empty() {
                                        text_parts.push(t.to_string());
                                    }
                                }
                            }
                            "tool_result" => {
                                let id = b.get("tool_use_id").and_then(|i| i.as_str()).unwrap_or("");
                                let c = extract_text(b.get("content").unwrap_or(&Value::Null));
                                tool_results.push(json!({"role": "tool", "tool_call_id": id, "content": c}));
                            }
                            "tool_use" => {
                                let id = b.get("id").and_then(|i| i.as_str()).unwrap_or("");
                                let name = b.get("name").and_then(|n| n.as_str()).unwrap_or("");
                                let args = b
                                    .get("input")
                                    .map(|i| i.to_string())
                                    .unwrap_or_else(|| "{}".into());
                                tool_calls.push(json!({"id": id, "type": "function", "function": {"name": name, "arguments": args}}));
                            }
                            "image" => {
                                text_parts.push("[imagem omitida — o modelo NVIDIA configurado é text-only]".into());
                            }
                            "document" => {
                                text_parts.push("[documento omitido — o modelo NVIDIA configurado é text-only]".into());
                            }
                            other => {
                                eprintln!("[nim-proxy] bloco Anthropic desconhecido descartado: {other}");
                            }
                        }
                    }
                }
                _ => {}
            }

            match role {
                "assistant" => {
                    let mut msg = json!({"role": "assistant", "content": text_parts.join("\n")});
                    if !tool_calls.is_empty() {
                        msg["tool_calls"] = json!(tool_calls);
                    }
                    msgs.push(msg);
                }
                _ => {
                    for tr in tool_results {
                        msgs.push(tr);
                    }
                    let t = text_parts.join("\n");
                    if !t.is_empty() {
                        msgs.push(json!({"role": "user", "content": t}));
                    }
                }
            }
        }
    }

    // Hint de formato de tools (ANTES de serializar em `out`, senão é código morto)
    let tools_arr = req.get("tools").and_then(|t| t.as_array());
    if tools_arr.map(|a| !a.is_empty()).unwrap_or(false) {
        let arr = tools_arr.unwrap();
        let tool_names: Vec<String> = arr
            .iter()
            .map(|t| t.get("name").and_then(|n| n.as_str()).unwrap_or("?").to_string())
            .collect();
        let hint = format!(
            "Quando precisar de dados externos, chame uma ferramenta no formato JSON do schema. Ferramentas disponíveis: {}.",
            tool_names.join(", ")
        );
        if let Some(first) = msgs.get_mut(0) {
            if first.get("role").and_then(|r| r.as_str()) == Some("system") {
                let base = first.get("content").and_then(|c| c.as_str()).unwrap_or("").to_string();
                first["content"] = json!(format!("{base}\n\n{hint}"));
            } else {
                msgs.insert(0, json!({"role": "system", "content": hint}));
            }
        }
    }

    let is_stream = req.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let max_tokens = req.get("max_tokens").and_then(|v| v.as_i64()).unwrap_or(4096);
    let max_tokens = max_tokens.min(16384).max(1); // teto por modelo NIM (nano/super truncam >16k)
    let mut out = json!({
        "max_tokens": max_tokens,
        "messages": msgs,
        "stream": is_stream,
    });
    if is_stream {
        // usage real no chunk final (sem isso, output_tokens é só estimativa)
        out["stream_options"] = json!({"include_usage": true});
    }
    if let Some(t) = req.get("temperature") {
        out["temperature"] = t.clone();
    }
    if let Some(t) = req.get("top_p") {
        out["top_p"] = t.clone();
    }

    // Anthropic tools[] -> OpenAI tools[]
    if let Some(tools) = req.get("tools").and_then(|t| t.as_array()).filter(|a| !a.is_empty()) {
        let mapped: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.get("name").cloned().unwrap_or(Value::Null),
                        "description": t.get("description").cloned().unwrap_or(Value::Null),
                        "parameters": t.get("input_schema").cloned().unwrap_or_else(|| json!({"type":"object","properties":{}})),
                    }
                })
            })
            .collect();
        out["tools"] = json!(mapped);
    }

    // tool_choice Anthropic -> OpenAI
    if let Some(tc) = req.get("tool_choice") {
        let t = tc.get("type").and_then(|x| x.as_str()).unwrap_or("");
        out["tool_choice"] = match t {
            "auto" => json!("auto"),
            "any" => json!("required"),
            "tool" => {
                let name = tc.get("name").and_then(|n| n.as_str()).unwrap_or("");
                json!({"type": "function", "function": {"name": name}})
            }
            "none" => json!("none"),
            _ => json!("auto"),
        };
    }

    // stop_sequences Anthropic -> stop OpenAI
    if let Some(ss) = req.get("stop_sequences").and_then(|v| v.as_array()).filter(|a| !a.is_empty()) {
        out["stop"] = json!(ss);
    }

    // Reasoning: precedência é o que o Claude Code realmente manda —
    // 1) output_config.effort ("low"|"medium"|"high", slider nativo;
    //    qualquer outro valor é rejeitado, nunca inventamos nível);
    // 2) senão thinking.budget_tokens, mapeado grosso (<4096 "low",
    //    <16384 "medium", senão "high");
    // 3) senão "high" (Reasoning MAXIMIZADO quando o cliente não sinaliza).
    // Modelos NIM compatíveis (kimi-k3, qwen-thinking) obedecem;
    // nemotron-3-* reasoning já vem com thinking ligado e ignora o campo.
    let effort = req
        .get("output_config")
        .and_then(|oc| oc.get("effort"))
        .and_then(|e| e.as_str())
        .filter(|e| matches!(*e, "low" | "medium" | "high"))
        .or_else(|| {
            req.get("thinking")
                .and_then(|t| t.get("budget_tokens"))
                .and_then(|b| b.as_i64())
                .map(|b| {
                    if b < 4096 {
                        "low"
                    } else if b < 16384 {
                        "medium"
                    } else {
                        "high"
                    }
                })
        })
        .unwrap_or("high");
    out["reasoning_effort"] = json!(effort);

    out
}

fn should_retry(status: u16) -> bool {
    status >= 500 || status == 429
}

// 400/404 que merecem fallback: limite de contexto, parâmetro não suportado, modelo sumiu
fn looks_retryable(raw: &str) -> bool {
    let l = raw.to_lowercase();
    l.contains("context length")
        || l.contains("context_length")
        || l.contains("too many tokens")
        || l.contains("maximum context")
        || l.contains("unsupported")
        || l.contains("not found")
}

async fn nim_post(cfg: &Cfg, body: &Value, model: &str) -> reqwest::Result<reqwest::Response> {
    let mut b = body.clone();
    b["model"] = json!(model);
    cfg.client
        .post(format!("{}/chat/completions", cfg.nim_base))
        .bearer_auth(&cfg.nim_key)
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .json(&b)
        .send()
        .await
}

async fn sse(tx: &mpsc::Sender<Bytes>, event: &str, data: Value) -> bool {
    let buf = format!("event: {event}\ndata: {}\n\n", data);
    tx.send(Bytes::from(buf)).await.is_ok()
}

// Extrai uma linha (até \n) do buffer de Bytes. UTF-8-safe: só converte
// depois que a linha está completa — multibyte partido entre chunks TCP
/// não pode virar U+FFFD.
fn take_line(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    let pos = buf.iter().position(|b| *b == b'\n')?;
    let mut line = buf.split_off(pos + 1);
    std::mem::swap(buf, &mut line);
    line.pop(); // remove o \n
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Some(line)
}

fn chrono_lite() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn req(v: Value) -> Value {
        v
    }

    #[test]
    fn convert_tools_hint_vai_no_payload() {
        let r = req(json!({
            "model": "nvidia-nano",
            "max_tokens": 100,
            "tools": [{"name": "buscar", "input_schema": {"type":"object"}}],
            "messages": [{"role": "user", "content": "oi"}]
        }));
        let out = convert_to_oai(&r);
        let msgs = out.get("messages").unwrap().as_array().unwrap();
        assert_eq!(msgs[0]["role"], "system", "hint de tools deve virar system");
        assert!(msgs[0]["content"].as_str().unwrap().contains("buscar"));
        assert_eq!(out["tools"].as_array().unwrap().len(), 1);
        assert!(out.get("stream_options").is_none(), "sync não leva stream_options");
    }

    #[test]
    fn convert_stream_options_include_usage() {
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,"stream":true,"messages":[]}));
        let out = convert_to_oai(&r);
        assert_eq!(out["stream"], true);
        assert_eq!(out["stream_options"]["include_usage"], true);
    }

    #[test]
    fn convert_empty_tools_omitidos() {
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,"tools":[],"stop_sequences":[],"messages":[]}));
        let out = convert_to_oai(&r);
        assert!(out.get("tools").is_none(), "tools vazio não deve ir ao upstream");
        assert!(out.get("stop").is_none());
    }

    #[test]
    fn convert_max_tokens_clamp() {
        let r = req(json!({"model":"nvidia-nano","max_tokens":99999,"messages":[]}));
        let out = convert_to_oai(&r);
        assert_eq!(out["max_tokens"], 16384);
        let r2 = req(json!({"model":"nvidia-nano","messages":[]}));
        assert_eq!(convert_to_oai(&r2)["max_tokens"], 4096);
    }

    #[test]
    fn convert_reasoning_effort_high() {
        // Sem output_config.effort nem thinking: default MAXIMIZADO é "high".
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,"messages":[]}));
        let out = convert_to_oai(&r);
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn convert_output_config_effort_passa_direto() {
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,
            "output_config":{"effort":"low"},"messages":[]}));
        assert_eq!(convert_to_oai(&r)["reasoning_effort"], "low");
        let r2 = req(json!({"model":"nvidia-nano","max_tokens":10,
            "output_config":{"effort":"medium"},"messages":[]}));
        assert_eq!(convert_to_oai(&r2)["reasoning_effort"], "medium");
    }

    #[test]
    fn convert_output_config_effort_lixo_nao_e_encaminhado() {
        // Valor inventado pelo cliente nunca chega ao upstream.
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,
            "output_config":{"effort":"ultracode"},"messages":[]}));
        assert_eq!(convert_to_oai(&r)["reasoning_effort"], "high");
    }

    #[test]
    fn convert_budget_tokens_mapeia_grosso() {
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,
            "thinking":{"type":"enabled","budget_tokens":2000},"messages":[]}));
        assert_eq!(convert_to_oai(&r)["reasoning_effort"], "low");
        let r2 = req(json!({"model":"nvidia-nano","max_tokens":10,
            "thinking":{"type":"enabled","budget_tokens":8000},"messages":[]}));
        assert_eq!(convert_to_oai(&r2)["reasoning_effort"], "medium");
        let r3 = req(json!({"model":"nvidia-nano","max_tokens":10,
            "thinking":{"type":"enabled","budget_tokens":32000},"messages":[]}));
        assert_eq!(convert_to_oai(&r3)["reasoning_effort"], "high");
    }

    #[test]
    fn convert_effort_vence_budget_tokens() {
        let r = req(json!({"model":"nvidia-nano","max_tokens":10,
            "output_config":{"effort":"low"},
            "thinking":{"type":"enabled","budget_tokens":32000},
            "messages":[]}));
        assert_eq!(convert_to_oai(&r)["reasoning_effort"], "low");
    }

    #[test]
    fn convert_tool_result_vira_role_tool() {
        let r = req(json!({
            "model":"nvidia-nano","max_tokens":10,
            "messages":[
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"resultado"}]}
            ]
        }));
        let out = convert_to_oai(&r);
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "tool");
        assert_eq!(msgs[0]["tool_call_id"], "call_1");
    }

    #[test]
    fn convert_imagem_placeholder() {
        let r = req(json!({
            "model":"nvidia-nano","max_tokens":10,
            "messages":[{"role":"user","content":[{"type":"image","source":{}},{"type":"text","text":"veja"}]}]
        }));
        let out = convert_to_oai(&r);
        let content = out["messages"][0]["content"].as_str().unwrap();
        assert!(content.contains("[imagem omitida"), "imagem deve virar aviso textual");
        assert!(content.contains("veja"));
    }

    #[test]
    fn take_line_utf8_split_between_chunks() {
        // "ç" (2 bytes UTF-8) e "✓" (3 bytes) partidos entre 2 chunks
        let full = "data: coração ✓ fim\n".as_bytes();
        let split_at = full.len() - 6; // cai no meio de "ação"
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&full[..split_at]);
        assert!(take_line(&mut buf).is_none(), "linha incompleta não pode sair");
        buf.extend_from_slice(&full[split_at..]);
        let line = take_line(&mut buf).expect("linha completa");
        assert_eq!(String::from_utf8(line).unwrap(), "data: coração ✓ fim");
        assert!(buf.is_empty());
    }

    #[test]
    fn take_line_crlf_e_multiplas() {
        let mut buf = b"a\r\nb\nc".to_vec();
        assert_eq!(take_line(&mut buf).unwrap(), b"a");
        assert_eq!(take_line(&mut buf).unwrap(), b"b");
        assert!(take_line(&mut buf).is_none());
        assert_eq!(buf, b"c");
    }

    #[test]
    fn constant_time_eq_basico() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn should_retry_cobre_5xx_e_429() {
        assert!(should_retry(500));
        assert!(should_retry(503));
        assert!(should_retry(429));
        assert!(!should_retry(400));
        assert!(!should_retry(200));
        assert!(!should_retry(401));
    }

    #[test]
    fn looks_retryable_contexto() {
        assert!(looks_retryable("maximum context length exceeded"));
        assert!(!looks_retryable("model loaded"));
    }

    #[test]
    fn gateway_map_model_aliases() {
        assert_eq!(gateway_map_model("claude-nvidia-ultra").as_deref(), Some("nvidia-ultra"));
        assert_eq!(gateway_map_model("nvidia-nano").as_deref(), Some("nvidia-nano"));
        assert_eq!(gateway_map_model("claude-opus-4-5"), None);
    }

    #[test]
    fn nim_error_vira_envelope_anthropic() {
        let (status, ctype, body) = nim_error_to_anthropic(404, r#"{"error":{"message":"model not found"}}"#);
        assert_eq!(status, 404);
        assert_eq!(ctype, "application/json");
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["error"]["message"], "model not found");
    }

    #[test]
    fn estimate_tokens_str_cjk_e_latin() {
        assert!(estimate_tokens_str("你好世界") >= 4);
        assert!(estimate_tokens_str("hello world") >= 1);
        assert_eq!(estimate_tokens_str(""), 4); // piso 1 + overhead 3
    }

    #[test]
    fn extract_text_document_placeholder() {
        let v = json!([{"type":"document","source":{}},{"type":"text","text":"x"}]);
        let t = extract_text(&v);
        assert!(t.contains("[documento omitido"));
    }

    #[test]
    fn model_chains_ultra_tem_tres() {
        let chains = model_chains();
        assert_eq!(chains["nvidia-ultra"].len(), 3);
        assert_eq!(chains["nvidia-nano"].len(), 1);
    }
}
