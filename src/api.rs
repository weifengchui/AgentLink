//! Local HTTP API consumed by agents on the same machine, plus the peer-facing
//! WebSocket endpoint.
//!
//! A single listener serves both; `/v1/*` requires the local API token while
//! `/peer/ws` authenticates via the cluster token during the handshake.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequest, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{async_trait, Json, Router as AxRouter};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};

use crate::config::Config;
use crate::conn;
use crate::file::FileManager;
use crate::peer::{DiscoveredPeer, PeerTable};
use crate::proto::{constant_time_eq, Envelope};
use crate::router::Router;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Live-adjustable identity fields (name / tags), synced without restart.
#[derive(Debug, Clone)]
pub struct Runtime {
    pub name: String,
    pub tags: Vec<String>,
}

/// Shared state for the running node.
pub struct AppState {
    pub config: Config,
    /// Path of the on-disk config file (for `PUT /v1/config` persistence).
    pub config_file: PathBuf,
    pub started_at: Instant,
    pub peers: PeerTable,
    /// Discovery -> connection manager channel.
    pub peer_tx: mpsc::Sender<DiscoveredPeer>,
    /// Message router (messaging + reliability core).
    pub router: Arc<Router>,
    /// File transfer counter.
    pub files: Arc<FileManager>,
    /// Live identity (node name / tags), editable from the web UI.
    pub runtime: std::sync::RwLock<Runtime>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        config_file: PathBuf,
        peers: PeerTable,
        peer_tx: mpsc::Sender<DiscoveredPeer>,
        router: Arc<Router>,
        files: Arc<FileManager>,
    ) -> Self {
        let runtime = Runtime {
            name: config.name.clone(),
            tags: config.tags.clone(),
        };
        AppState {
            config,
            config_file,
            started_at: Instant::now(),
            peers,
            peer_tx,
            router,
            files,
            runtime: std::sync::RwLock::new(runtime),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Current live node name.
    pub fn display_name(&self) -> String {
        self.runtime.read().unwrap().name.clone()
    }

    /// Current live tags (machine capability labels).
    pub fn tags(&self) -> Vec<String> {
        self.runtime.read().unwrap().tags.clone()
    }

    /// Update live identity and persist `name`/`tags` into the config file.
    pub fn set_identity(
        &self,
        name: Option<String>,
        tags: Option<Vec<String>>,
    ) -> anyhow::Result<()> {
        {
            let mut rt = self.runtime.write().unwrap();
            if let Some(n) = name {
                rt.name = n;
            }
            if let Some(t) = tags {
                rt.tags = t;
            }
        }
        // Persist into the on-disk config so it survives restarts.
        let mut cfg = self.config.clone();
        cfg.name = self.display_name();
        cfg.tags = self.tags();
        cfg.save(&self.config_file)?;
        Ok(())
    }
}

pub type SharedState = Arc<AppState>;

/// Build the router: local API and the peer WebSocket endpoint.
pub fn router(state: SharedState) -> AxRouter {
    AxRouter::new()
        .route("/v1/health", get(health))
        .route("/v1/status", get(status))
        .route("/v1/peers", get(peers))
        .route("/v1/send", post(send))
        .route("/v1/send-file", post(send_file))
        .route("/v1/send-file-upload", post(send_file_upload))
        .route("/v1/ack-inbox", post(ack_inbox))
        .route(
            "/v1/agents",
            get(agents_get).post(agents_post).delete(agents_delete),
        )
        .route("/v1/fleet/agents", get(fleet_agents))
        .route("/v1/history", get(history_get))
        .route("/v1/config", get(config_get).put(config_put))
        .route("/web", get(web_page))
        .route("/v1/web", get(web_page))
        .route("/web/setup", get(web_setup))
        .route("/v1/ws", get(subscribe_ws))
        .route("/peer/ws", get(peer_ws))
        .route("/peer/file/ws", get(file_ws))
        .with_state(state)
}

/// Liveness probe. Intentionally unauthenticated.
async fn health(State(state): State<SharedState>) -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "version": VERSION,
        "node_id": state.config.node_id,
        "name": state.display_name(),
        "tags": state.tags(),
        "uptime_seconds": state.uptime_seconds(),
    }))
}

/// Node status. Requires a valid local API token.
async fn status(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }

    let peers = state.peers.snapshot();
    let online = state.router.peers_online();
    let offline_queue = state.router.offline_queue_len().await;

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "version": VERSION,
            "node_id": state.config.node_id,
            "name": state.display_name(),
            "listen_port": state.config.listen_port,
            "uptime_seconds": state.uptime_seconds(),
            "peers_known": peers.len(),
            "peers_online": online,
            "pending_acks": state.router.pending_acks(),
            "offline_queue": offline_queue,
            "local_pending": state.router.local_pending_count(),
            "active_transfers": state.files.active(),
        })),
    )
}

/// List known peers and their online state.
async fn peers(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "peers": state.peers.snapshot() })),
    )
}

// ---- messaging --------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SendRequest {
    to: String,
    #[serde(rename = "type")]
    kind: String,
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    reply_to: Option<String>,
    /// Sender agent id (multiple agents may share this node).
    #[serde(default)]
    from_agent: Option<String>,
    /// Target agent id on the destination node.
    #[serde(default)]
    to_agent: Option<String>,
}

/// Send a message to a peer (`node_id` or name) or broadcast to `"*"`.
async fn send(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<SendRequest>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let to = match resolve_target(&state, &req.to) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    };
    let payload = if req.payload.is_null() {
        json!({})
    } else {
        req.payload
    };
    let env = Envelope::business(
        &state.config.node_id,
        &to,
        &req.kind,
        &req.action,
        payload,
        req.ttl_seconds,
        req.reply_to,
        req.from_agent,
        req.to_agent,
    );
    match state.router.send(env).await {
        Ok(msg_id) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "msg_id": msg_id })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

// ---- file sending -----------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SendFileRequest {
    to: String,
    file_path: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

/// Send a local file to a peer via a dedicated transfer connection.
async fn send_file(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<SendFileRequest>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let to = match resolve_target(&state, &req.to) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    };
    if to == "*" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "cannot send a file to broadcast" })),
        );
    }
    match crate::file::send_file(&state, &to, Path::new(&req.file_path), req.ttl_seconds).await {
        Ok(file_id) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "file_id": file_id, "to": to })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

/// Browser-style file upload: raw bytes in the body, file name in `X-File-Name`.
#[derive(Debug, Deserialize)]
struct UploadParams {
    to: String,
    #[serde(default)]
    to_agent: Option<String>,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

async fn send_file_upload(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(params): Query<UploadParams>,
    body: Bytes,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    if body.len() > state.config.files.max_file_size as usize {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({ "ok": false, "error": format!(
                "file too large: {} bytes (limit {})", body.len(), state.config.files.max_file_size
            ) })),
        );
    }
    let to = match resolve_target(&state, &params.to) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    };
    if to == "*" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "cannot send a file to broadcast" })),
        );
    }
    let _ = params.to_agent; // files are node-level delivery for now
    let raw_name = headers
        .get("x-file-name")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("upload.bin")
        .to_string();
    let Some(name) = crate::file::sanitize_name(&raw_name) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "unsafe file name" })),
        );
    };

    let dir = state
        .config
        .data_dir
        .join("uploads")
        .join(&crate::proto::new_id()[..8]);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }
    // Keep the original name as the file's basename so the receiver stores it
    // under the correct name (uniqueness comes from the directory).
    let path = dir.join(&name);
    if let Err(e) = tokio::fs::write(&path, body.as_ref()).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }

    let result = crate::file::send_transfer(&state, &to, &path, params.ttl_seconds).await;
    let _ = tokio::fs::remove_file(&path).await;
    let _ = tokio::fs::remove_dir(&dir).await;
    match result {
        Ok(file_id) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "file_id": file_id, "to": to, "file_name": name })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

/// Resolve `to` into a `node_id` or `"*"`.
fn resolve_target(state: &AppState, to: &str) -> Result<String, String> {
    if to == "*" {
        return Ok("*".to_string());
    }
    if to.len() == 12 && to.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(to.to_string());
    }
    for p in state.peers.snapshot() {
        if p.name == to {
            return Ok(p.node_id);
        }
    }
    Err(format!(
        "unknown target '{to}' (not a node_id or known peer name)"
    ))
}

#[derive(Debug, Deserialize)]
struct AckInboxRequest {
    msg_id: String,
}

/// Acknowledge a locally delivered message (`/v1/ack-inbox`).
async fn ack_inbox(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<AckInboxRequest>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let acked = state.router.ack_local(&req.msg_id);
    (StatusCode::OK, Json(json!({ "ok": true, "acked": acked })))
}

// ---- agents & conversation history (M7) -------------------------------------

#[derive(Debug, Deserialize)]
struct RegisterAgentRequest {
    agent_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
}

/// Register (or refresh) a local AI agent. Capabilities are broadcast to peers.
async fn agents_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<RegisterAgentRequest>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let id = req.agent_id.trim();
    let valid = !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if !valid {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "agent_id must be 1-64 chars of [A-Za-z0-9_.-]" })),
        );
    }
    let rec =
        state
            .router
            .register_agent(id, req.name.map(|s| s.trim().to_string()), req.capabilities);
    (StatusCode::OK, Json(json!({ "ok": true, "agent": rec })))
}

/// Remove a local agent (its registration vanishes from the fleet view too).
async fn agents_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<RegisterAgentRequest>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let removed = state.router.unregister_agent(req.agent_id.trim());
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "removed": removed })),
    )
}

/// List agents registered on this node.
async fn agents_get(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "node": { "node_id": state.config.node_id, "name": state.config.name },
            "agents": state.router.local_agents(),
        })),
    )
}

/// List agents across the whole LAN fleet (local + synced from peers).
async fn fleet_agents(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let mut names: HashMap<String, String> = HashMap::new();
    for p in state.peers.snapshot() {
        names.insert(p.node_id.clone(), p.name.clone());
    }
    names.insert(state.config.node_id.clone(), state.config.name.clone());
    let agents: Vec<serde_json::Value> = state
        .router
        .fleet_agents()
        .iter()
        .map(|a| {
            let mut v = serde_json::to_value(a).unwrap_or(serde_json::Value::Null);
            if let serde_json::Value::Object(m) = &mut v {
                m.insert(
                    "node_name".to_string(),
                    serde_json::json!(names
                        .get(&a.node_id)
                        .cloned()
                        .unwrap_or_else(|| a.node_id.clone())),
                );
            }
            v
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "agents": agents })),
    )
}

/// Recent bus traffic (ring buffer) for the human view.
async fn history_get(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "history": state.router.history() })),
    )
}

// ---- config view / edit (web 配置页) ---------------------------------------

#[derive(Debug, Deserialize)]
struct ConfigUpdateRequest {
    name: Option<String>,
    tags: Option<Vec<String>>,
    log_level: Option<String>,
    history_len: Option<usize>,
    max_file_size: Option<u64>,
    static_peers: Option<Vec<String>>,
    cluster_token: Option<String>,
    auto_open_web: Option<bool>,
}

/// Mask secrets so the config view never leaks full tokens.
fn mask_secret(s: &str) -> String {
    if s.len() <= 8 {
        "****".to_string()
    } else {
        format!("{}…{}", &s[..4], &s[s.len() - 2..])
    }
}

/// Read the effective configuration (secrets masked).
async fn config_get(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let c = &state.config;
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "config": {
                "name": state.display_name(),
                "tags": state.tags(),
                "node_id": c.node_id,
                "listen_port": c.listen_port,
                "data_dir": c.data_dir,
                "discovery": {
                    "enabled": c.discovery.enabled,
                    "methods": c.discovery.methods,
                    "broadcast_port": c.discovery.broadcast_port,
                    "announce_interval_seconds": c.discovery.announce_interval_seconds,
                    "heartbeat_interval_seconds": c.discovery.heartbeat_interval_seconds,
                    "offline_timeout_seconds": c.discovery.offline_timeout_seconds,
                    "static_peers": c.discovery.static_peers,
                },
                "security": {
                    "mode": c.security.mode,
                    "cluster_token": mask_secret(&c.security.cluster_token),
                },
                "local_api": {
                    "host": c.local_api.host,
                    "token": mask_secret(&c.local_api.token),
                },
                "files": {
                    "max_file_size": c.files.max_file_size,
                    "chunk_size": c.files.chunk_size,
                    "recv_dir": c.files.recv_dir,
                    "max_concurrent": c.files.max_concurrent,
                },
                "log": { "level": c.log.level, "format": c.log.format, "file": c.log.file },
                "history": { "enabled": c.history.enabled, "len": c.history.len },
                "web": { "auto_open": c.web.auto_open },
            }
        })),
    )
}

/// Update configuration. `name`/`tags` apply live; most other fields persist to
/// the config file and take effect after a node restart.
async fn config_put(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<ConfigUpdateRequest>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let name = req
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from);
    if let Some(n) = &name {
        if n.len() > 64 {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "name too long (max 64)" })),
            );
        }
    }
    if let Err(e) = state.set_identity(name.clone(), req.tags.clone()) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }

    // Reload the file (already contains updated name/tags) and merge the rest.
    let mut cfg = match Config::load(&state.config_file) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e.to_string() })),
            );
        }
    };
    if let Some(l) = &req.log_level {
        if !matches!(l.as_str(), "debug" | "info" | "warn" | "error") {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "log_level must be debug|info|warn|error" })),
            );
        }
        cfg.log.level = l.clone();
    }
    if let Some(h) = req.history_len {
        cfg.history.len = h.max(1);
    }
    if let Some(m) = req.max_file_size {
        cfg.files.max_file_size = m.max(1);
    }
    if let Some(sp) = req.static_peers {
        cfg.discovery.static_peers = sp;
    }
    if let Some(t) = req.cluster_token {
        let t = t.trim().to_string();
        if !t.is_empty() && t != cfg.security.cluster_token {
            cfg.security.cluster_token = t;
        }
    }
    if let Some(a) = req.auto_open_web {
        cfg.web.auto_open = a;
    }
    if let Err(e) = cfg.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }
    if let Err(e) = cfg.save(&state.config_file) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "config": "saved",
            "restart_hint": "端口/广播端口/安全模式/日志格式/历史条数/最大文件/static_peers 保存后重启生效；名称与标签已即时生效",
        })),
    )
}

/// Auto-provision the local API token for the browser, but ONLY for requests
/// coming from the loopback interface (the same machine). Remote clients get
/// 403, so a LAN visitor cannot read it.
async fn web_setup(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
) -> Response {
    if !addr.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "ok": false, "error": "remote address; token is not exposed" })),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "token": state.config.local_api.token })),
    )
        .into_response()
}

/// Human-friendly dashboard page (no assets; token is entered in-session and
/// sent via `X-Auth-Token` header, never in the URL).
async fn web_page(State(state): State<SharedState>) -> Response {
    (StatusCode::OK, Html(web_html(state.config.history.len))).into_response()
}

fn web_html(history_len: usize) -> String {
    const HTML: &str = r#"<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AgentLink · Console</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;background:#0b0e14;color:#dbe2ea;font-size:13px}
a{color:#4f8cff;text-decoration:none}
.hidden{display:none!important}
#app{display:grid;grid-template-columns:220px 1fr;min-height:100vh}
#sidebar{background:#0e1219;border-right:1px solid #1c2330;padding:16px 12px;position:sticky;top:0;height:100vh;overflow:auto}
#logo{display:flex;align-items:center;gap:8px;padding:4px 8px 14px;font-weight:700;font-size:15px;letter-spacing:.2px}
#logo .dot{width:10px;height:10px;border-radius:50%;background:#4f8cff;box-shadow:0 0 10px #4f8cff}
#sidebar .nav{margin:2px 0;padding:8px 10px;border-radius:8px;cursor:pointer;color:#9aa7b8;display:flex;align-items:center;gap:8px}
#sidebar .nav:hover{background:#141b26;color:#e6edf5}
#sidebar .nav.active{background:#16233b;color:#7fb0ff}
#main{display:flex;flex-direction:column;min-width:0}
#topbar{display:flex;align-items:center;gap:14px;padding:12px 20px;border-bottom:1px solid #1c2330;position:sticky;top:0;background:rgba(11,14,20,.92);backdrop-filter:blur(6px);z-index:5}
#topbar .t{font-weight:600}
#topbar .sub{color:#7d8aa0;font-size:12px}
.spacer{flex:1}
.badge{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:20px;font-size:11px;line-height:1.4}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.on{background:rgba(63,185,80,.12);color:#3fb950}.on .dot{background:#3fb950}
.off{background:rgba(248,81,73,.12);color:#f85149}.off .dot{background:#f85149}
.warn{background:rgba(210,153,34,.12);color:#d29922}
.content{padding:20px;max-width:1200px;width:100%}
.card{background:#10151d;border:1px solid #1c2330;border-radius:12px;padding:16px;margin-bottom:16px}
.card h2{margin:0 0 12px;font-size:13px;color:#9aa7b8;font-weight:600;letter-spacing:.4px;text-transform:uppercase}
table{width:100%;border-collapse:collapse}
th{font-size:11px;color:#7d8aa0;text-align:left;padding:6px 8px;border-bottom:1px solid #1c2330;font-weight:500}
td{padding:8px;border-bottom:1px solid #171e29;vertical-align:middle}
tr:hover td{background:#131a24}
.chip{display:inline-block;padding:1px 8px;border-radius:12px;font-size:11px;background:#182238;color:#9db4dd;margin:1px 2px}
.metric{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}
.metric .m{background:#10151d;border:1px solid #1c2330;border-radius:12px;padding:14px}
.metric .v{font-size:24px;font-weight:700;margin-top:4px}
.metric .k{font-size:11px;color:#7d8aa0}
.msg{border-left:3px solid #263041;padding:8px 12px;margin:8px 0;background:#0d1219;border-radius:0 8px 8px 0}
.msg .meta{color:#7d8aa0;font-size:11px;margin-bottom:2px}
.msg .pay{color:#9cc5ff;word-break:break-all;white-space:pre-wrap}
.msg.out{border-left-color:#4f8cff}
.msg.in{border-left-color:#3fb950}
.row{display:flex;gap:8px;align-items:center;margin:8px 0;flex-wrap:wrap}
input,textarea,select{background:#0b1017;color:#dbe2ea;border:1px solid #263041;border-radius:8px;padding:8px 10px;font-size:13px;font-family:inherit;outline:none}
input:focus,textarea:focus,select:focus{border-color:#4f8cff}
button{background:#1a2a45;color:#e6edf5;border:1px solid #2b4a7c;border-radius:8px;padding:8px 14px;font-size:13px;cursor:pointer;font-family:inherit}
button.primary{background:#2f6fe4;border-color:#2f6fe4}
button:hover{filter:brightness(1.12)}
button:active{filter:brightness(.95)}
textarea{width:100%;resize:vertical}
.hint{color:#5f6b7f;font-size:12px;margin:2px 0 6px}
.cfg-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.cfg-grid label{display:block;font-size:12px;color:#9aa7b8;margin:6px 0 4px}
input,textarea,select{width:100%}
row input{width:auto}
#modal{position:fixed;inset:0;background:rgba(4,6,10,.8);display:flex;align-items:center;justify-content:center;z-index:50;backdrop-filter:blur(4px)}
#modal .box{background:#121823;border:1px solid #263041;border-radius:14px;padding:24px;width:360px;box-shadow:0 20px 60px #0008}
#modal h3{margin:0 0 6px}
.switch{position:relative;display:inline-block;width:38px;height:20px;margin-left:8px;vertical-align:middle}
.switch input{display:none}
.slider{position:absolute;inset:0;background:#263041;border-radius:20px;transition:.2s}
.slider:before{content:"";position:absolute;width:14px;height:14px;left:3px;top:3px;background:#dbe2ea;border-radius:50%;transition:.2s}
.switch input:checked+.slider{background:#2f6fe4}
.switch input:checked+.slider:before{transform:translateX(18px)}
</style></head>
<body>
<div id="modal"><div class="box">
<h3>AgentLink</h3>
<p class="hint">输入本机 <code>agentlink.json</code> 里 <code>local_api.token</code> 的值。Token 只保存在当前浏览器会话，不放进地址栏。</p>
<input id="token-input" style="width:100%" placeholder="local_api.token">
<div class="row"><button class="primary" onclick="saveToken()">进入控制台</button></div>
</div></div>

<div id="app" class="hidden">
<aside id="sidebar">
  <div id="logo"><span class="dot"></span>AgentLink</div>
  <div class="nav active" data-p="overview" onclick="show('overview')">总览</div>
  <div class="nav" data-p="nodes" onclick="show('nodes')">节点</div>
  <div class="nav" data-p="agents" onclick="show('agents')">Agent</div>
  <div class="nav" data-p="chat" onclick="show('chat')">对话 / 发送</div>
  <div class="nav" data-p="config" onclick="show('config')">配置</div>
</aside>
<div id="main">
  <div id="topbar">
    <div><div class="t" id="tb-self">—</div><div class="sub" id="tb-meta">—</div></div>
    <div class="spacer"></div>
    <span class="sub">自动刷新 <label class="switch"><input type="checkbox" id="auto-refresh" checked><span class="slider"></span></label></span>
    <button onclick="refresh()">刷新</button>
  </div>
  <div class="content">
    <section id="p-overview">
      <div class="metric">
        <div class="m"><div class="k">在线节点</div><div class="v" id="m-nodes">0</div></div>
        <div class="m"><div class="k">历史消息</div><div class="v" id="m-history">0</div></div>
        <div class="m"><div class="k">传输中</div><div class="v" id="m-transfer">0</div></div>
        <div class="m"><div class="k">版本</div><div class="v" id="m-version">—</div></div>
      </div>
      <div class="card"><h2>快捷发送</h2>
        <div class="row"><input id="to" list="peers-list" placeholder="目标 node_id / 节点名 / * (广播)" style="flex:2"><input id="to_agent" placeholder="Agent (可选)" style="flex:1"></div>
        <div class="row"><textarea id="payload" rows="2" placeholder='{"content":"你好"}'></textarea></div>
        <div class="row"><button class="primary" onclick="doSend()">发送消息</button><input type="file" id="file"><button onclick="doFile()">发送文件</button></div>
        <div id="sendres" class="hint"></div>
      </div>
    </section>
    <section id="p-nodes" class="hidden">
      <div class="card"><h2>节点 Peers</h2><table><thead><tr><th>名称</th><th>node_id</th><th>地址</th><th>标签</th><th>状态</th></tr></thead><tbody id="peers-body"></tbody></table></div>
    </section>
    <section id="p-agents" class="hidden">
      <div class="card"><h2>Agent（全网）</h2><table><thead><tr><th>节点</th><th>agent_id</th><th>名字</th><th>能力</th><th>状态</th></tr></thead><tbody id="agents-body"></tbody></table></div>
    </section>
    <section id="p-chat" class="hidden">
      <div class="card"><h2>发送消息 / 文件</h2>
        <datalist id="peers-list"></datalist>
        <div class="row"><input id="to2" list="peers-list" placeholder="目标 node_id / 节点名 / * (广播)" style="flex:2"><input id="to_agent2" placeholder="Agent (可选)" style="flex:1"></div>
        <div class="row"><textarea id="payload2" rows="2" placeholder='{"content":"你好"}'></textarea></div>
        <div class="row"><button class="primary" onclick="doSend()">发送消息</button><input type="file" id="file2"><button onclick="doFile()">发送文件</button></div>
        <div id="sendres2" class="hint"></div>
      </div>
      <div class="card"><h2>对话记录（最近 __LEN__ 条）</h2><div id="history"></div></div>
    </section>
    <section id="p-config" class="hidden">
      <div class="card"><h2>节点配置</h2>
        <div class="cfg-grid">
          <div><label>节点名称（改后即时生效）</label><input id="cf-name"></div>
          <div><label>标签 tags（逗号分隔）</label><input id="cf-tags"></div>
          <div><label>日志级别</label><select id="cf-log"><option>debug</option><option>info</option><option>warn</option><option>error</option></select></div>
          <div><label>历史条数（重启生效）</label><input id="cf-hist" type="number" min="10"></div>
          <div><label>最大文件 MB（重启生效）</label><input id="cf-fmax" type="number" min="1"></div>
        </div>
        <p class="hint" style="margin-top:12px">静态节点 static_peers（每行一个 IP:端口，重启生效）：</p>
        <textarea id="cf-peers" rows="3"></textarea>
        <div class="row"><span class="hint">启动自动打开 Web</span><label class="switch"><input type="checkbox" id="cf-open"><span class="slider"></span></label></div>
        <div class="row" style="margin-top:6px"><button class="primary" onclick="saveConfig()">保存配置</button><span id="cf-res" class="hint"></span></div>
        <p class="hint" id="cf-restart"></p>
      </div>
      <div class="card"><h2>只读信息</h2><div id="cf-ro" class="hint"></div></div>
    </section>
  </div>
</div>
</div>

<script>
const $=id=>document.getElementById(id);
const LS_KEY='al_token';
let TOKEN=sessionStorage.getItem(LS_KEY)||'';
const HDR=()=>({'X-Auth-Token':TOKEN});
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmt(ts){try{return new Date(ts).toLocaleString();}catch{return ts;}}
function saveToken(){TOKEN=$('token-input').value.trim();sessionStorage.setItem(LS_KEY,TOKEN);enter();}
function enter(){$('modal').classList.add('hidden');$('app').classList.remove('hidden');refresh();loadConfig();}
async function boot(){
 // 自动取本机 token（仅 127.0.0.1 可成功）；失败则退回手动输入
 try{
  const r=await fetch('/web/setup');
  const d=await r.json();
  if(d&&d.ok){TOKEN=d.token;sessionStorage.setItem(LS_KEY,TOKEN);enter();return;}
 }catch(e){}
 if(TOKEN){enter();return;}
 $('modal').classList.remove('hidden');
}
boot();
async function j(p,opt){const o=opt||{};const r=await fetch(p,{...o,headers:{...HDR(),...(o.headers||{})}});const d=await r.json().catch(()=>({}));if(r.status===401){alert('token 无效，请重新输入');sessionStorage.removeItem(LS_KEY);location.reload();throw new Error('401');}return d;}
function show(panel){document.querySelectorAll('.nav').forEach(n=>n.classList.toggle('active',n.dataset.p===panel));document.querySelectorAll('.content>section').forEach(s=>s.classList.add('hidden'));$('p-'+panel).classList.remove('hidden');}
function input(id){return ($(id)&&$(id).value||'').trim();}
async function doSend(){
  const to=input('to')||input('to2'); if(!to){alert('请填目标 to');return;}
  const to_agent=input('to_agent')||input('to_agent2')||undefined;
  let raw=input('payload')||input('payload2'),payload;
  try{payload=raw?JSON.parse(raw):{};}catch{payload={content:raw};}
  try{const d=await j('/v1/send',{method:'POST',body:JSON.stringify({to,to_agent,type:'text',action:'send',payload})});foo(d);}catch(e){}
}
async function doFile(){
  const f=($('file').files[0])||($('file2').files[0]); if(!f){alert('请选择文件');return;}
  const to=input('to')||input('to2'); if(!to){alert('请填目标 to');return;}
  const q=new URLSearchParams({to}); if(input('to_agent')||input('to_agent2'))q.set('to_agent',input('to_agent')||input('to_agent2'));
  try{const d=await j('/v1/send-file-upload?'+q,{method:'POST',headers:{'X-File-Name':f.name},body:f});foo(d);}catch(e){}
}
function foo(d){$('sendres').textContent=JSON.stringify(d);$('sendres2').textContent=JSON.stringify(d);refresh();}
async function refresh(){
 try{
  const [st,peers,agents,hist]=await Promise.all([j('/v1/status'),j('/v1/peers'),j('/v1/fleet/agents'),j('/v1/history')]);
  $('tb-self').textContent=st.name;
  $('tb-meta').textContent=`${st.node_id} · v${st.version} · 在线 ${st.peers_online}/${st.peers_known} · ${new Date().toLocaleTimeString()}`;
  $('m-nodes').textContent=`${st.peers_online}/${st.peers_known}`;
  $('m-history').textContent=hist.history.length;
  $('m-transfer').textContent=st.active_transfers;
  $('m-version').textContent=st.version;
  let pe=''; if(peers&&peers.ok)for(const p of peers.peers)pe+=`<tr><td>${esc(p.name)}</td><td>${esc(p.node_id)}</td><td>${esc(p.addr)}</td><td>${(p.tags||[]).map(t=>`<span class="chip">${esc(t)}</span>`).join('')}</td><td><span class="badge ${p.online?'on':'off'}"><span class="dot"></span>${p.online?'在线':'离线'}</span></td></tr>`;
  $('peers-body').innerHTML=pe;
  let ag=''; if(agents&&agents.ok)for(const a of agents.agents)ag+=`<tr><td>${esc(a.node_name||a.node_id)}</td><td>${esc(a.agent_id)}</td><td>${esc(a.name)}</td><td>${(a.capabilities||[]).map(c=>`<span class="chip">${esc(c)}</span>`).join('')}</td><td><span class="badge ${a.online?'on':'off'}"><span class="dot"></span>${a.online?'在线':'离线'}</span></td></tr>`;
  $('agents-body').innerHTML=ag;
  let dl=''; if(peers&&peers.ok)for(const p of peers.peers)dl+=`<option value="${esc(p.node_id)}">${esc(p.name)}</option>`;
  if(agents&&agents.ok)for(const a of agents.agents)dl+=`<option value="${esc(a.node_id)}">${esc(a.node_name||a.node_id)} / ${esc(a.agent_id)}</option>`;
  $('peers-list').innerHTML=dl;
  let hh='';
  if(hist&&hist.ok)for(const m of hist.history.slice().reverse())hh+=`<div class="msg ${m.direction==='out'?'out':m.direction==='in'?'in':''}"><div class="meta">${fmt(m.ts)} · ${esc(m.from_agent||'节点')} ${esc(m.from)} → ${esc(m.to_agent||'')} ${esc(m.to)} · ${esc(m.kind)}/${esc(m.action)}</div><div class="pay">${esc(JSON.stringify(m.payload))}</div></div>`;
  $('history').innerHTML=hh||'<p class="hint">暂无业务消息</p>';
 }catch(e){}
}
async function loadConfig(){
 try{
  const d=await j('/v1/config');
  const c=d.config;
  $('cf-name').value=c.name; $('cf-tags').value=(c.tags||[]).join(',');
  $('cf-log').value=c.log.level; $('cf-hist').value=c.history.len; $('cf-fmax').value=Math.round(c.files.max_file_size/1048576);
  $('cf-peers').value=(c.discovery.static_peers||[]).join('\n');
  $('cf-open').checked=c.web.auto_open;
  $('cf-ro').textContent=`node_id ${c.node_id} · 端口 ${c.listen_port} · 广播 ${c.discovery.broadcast_port} · 模式 ${c.security.mode} · data_dir ${c.data_dir}`;
  $('cf-restart').textContent='名称与标签立即生效；端口 / 广播端口 / 日志格式 / 历史条数 / 最大文件 / static_peers 保存后重启生效。';
 }catch(e){}
}
async function saveConfig(){
 const peers=$('cf-peers').value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);
 const body={name:$('cf-name').value.trim(),tags:$('cf-tags').value.split(/[,，]/).map(s=>s.trim()).filter(Boolean),
  log_level:$('cf-log').value,history_len:parseInt($('cf-hist').value)||500,
  max_file_size:(parseInt($('cf-fmax').value)||100)*1048576,static_peers:peers,auto_open_web:$('cf-open').checked};
 try{const d=await j('/v1/config',{method:'PUT',body:JSON.stringify(body)});$('cf-res').textContent=JSON.stringify(d);refresh();}catch(e){}
}
setInterval(()=>{if($('auto-refresh').checked)refresh();},2000);
</script></body></html>"#;
    HTML.replace("__LEN__", &history_len.to_string())
}
// ---- local subscription -----------------------------------------------------

#[derive(Debug, Deserialize)]
struct SubParams {
    token: String,
    #[serde(default)]
    client: Option<String>,
}

/// Subscribe to inbound messages/events (ws://127.0.0.1:8765/v1/ws).
async fn subscribe_ws(
    ws: WebSocketUpgrade,
    Query(params): Query<SubParams>,
    State(state): State<SharedState>,
) -> Response {
    let expected = state.config.local_api.token.as_bytes();
    if !constant_time_eq(params.token.as_bytes(), expected) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "bad token" })),
        )
            .into_response();
    }
    let client = params.client.unwrap_or_default();
    ws.on_upgrade(move |socket| handle_subscriber(socket, state, client))
}

/// Forward events/messages to a local WS client (an agent).
/// The `client` id is the agent id: messages carrying `to_agent` only reach the
/// matching client.
async fn handle_subscriber(socket: WebSocket, state: SharedState, client_id: String) {
    if !client_id.is_empty() {
        state.router.client_online(&client_id);
    }
    let mut rx: broadcast::Receiver<Envelope> = state.router.subscribe();
    let (mut sender, mut receiver) = socket.split();
    loop {
        tokio::select! {
            ev = rx.recv() => {
                match ev {
                    Ok(env) => {
                        // Agent-level routing: only deliver when addressed to
                        // this client, or to the node in general.
                        let pass = env
                        .to_agent
                        .as_deref()
                        .is_none_or(|a| a == client_id.as_str());
                        if !pass {
                            continue;
                        }
                        let text = serde_json::to_string(&env).unwrap_or_default();
                        if sender.send(WsMessage::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(lag = n, "subscriber lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    None => break,
                    Some(Ok(WsMessage::Close(_))) => break,
                    _ => {} // ignore client frames
                }
            }
        }
    }
    if !client_id.is_empty() {
        state.router.client_offline(&client_id);
    }
}

// ---- peer endpoint ----------------------------------------------------------

/// Accept a peer WebSocket upgrade.
async fn peer_ws(
    ws: WebSocketUpgrade,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| conn::handle_inbound(socket, state, remote))
}

/// Accept a dedicated file-transfer WebSocket upgrade.
async fn file_ws(
    ws: WebSocketUpgrade,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| crate::file::handle_inbound(socket, state, remote))
}

// ---- helpers ----------------------------------------------------------------

/// Body extractor that parses JSON regardless of `Content-Type` (agents may
/// forget the header; we do not want that to break local integration).
pub struct JsonAny<T>(pub T);

#[async_trait]
impl<S, T> FromRequest<S> for JsonAny<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, _state).await.map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "bad body" })),
            )
        })?;
        let value = serde_json::from_slice(&bytes).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid JSON: {e}") })),
            )
        })?;
        Ok(JsonAny(value))
    }
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let expected = state.config.local_api.token.as_bytes();
    match headers.get("x-auth-token").and_then(|v| v.to_str().ok()) {
        Some(got) => constant_time_eq(got.as_bytes(), expected),
        None => false,
    }
}

fn unauthorized() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "invalid or missing X-Auth-Token" })),
    )
}
