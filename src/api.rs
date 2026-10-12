//! Local HTTP API consumed by agents on the same machine, plus the peer-facing
//! WebSocket endpoint.
//!
//! A single listener serves both; `/v1/*` requires the local API token while
//! `/peer/ws` authenticates via the cluster token during the handshake.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::Path as UrlPath;
use axum::extract::{ConnectInfo, FromRequest, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
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
        .route("/v1/files/:file_name", get(file_download))
        .route("/v1/open-folder", post(open_folder))
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

/// Download a file that this node received (served from `files.recv_dir`).
async fn file_download(
    UrlPath(file_name): UrlPath<String>,
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers) {
        return unauthorized().into_response();
    }
    let Some(name) = crate::file::sanitize_name(&file_name) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "invalid file name" })),
        )
            .into_response();
    };
    let path = state.config.files.recv_dir.join(&name);
    if !path.is_file() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "file not found" })),
        )
            .into_response();
    }
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let mut resp = Response::new(Body::from(bytes));
            resp.headers_mut().insert(
                "Content-Type",
                HeaderValue::from_static("application/octet-stream"),
            );
            resp.headers_mut().insert(
                "Content-Disposition",
                HeaderValue::from_static("attachment"),
            );
            resp.headers_mut().insert(
                "X-Content-Type-Options",
                HeaderValue::from_static("nosniff"),
            );
            resp
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": "read failed" })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct OpenFolderRequest {
    file: String,
}

/// Open the folder (highlighting the file) in the OS file manager.
/// Local-only (loopback) + token required, since it triggers an OS action.
async fn open_folder(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<OpenFolderRequest>,
) -> Response {
    if !addr.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "ok": false, "error": "remote address; not allowed" })),
        )
            .into_response();
    }
    if !authorized(&state, &headers) {
        return unauthorized().into_response();
    }
    let Some(name) = crate::file::sanitize_name(&req.file) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "invalid file name" })),
        )
            .into_response();
    };
    let path = state.config.files.recv_dir.join(&name);
    let dir = path.parent().map(|p| p.to_path_buf());
    let Some(dir) = dir else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "no folder" })),
        )
            .into_response();
    };
    if !dir.is_dir() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "folder not found" })),
        )
            .into_response();
    }
    let opened = if path.is_file() {
        open_in_folder(&path)
    } else {
        open_dir(&dir)
    };
    (StatusCode::OK, Json(json!({ "ok": opened }))).into_response()
}

/// Windows: `explorer /select,<file>`; Linux: open parent dir; macOS: `open -R`.
fn open_in_folder(path: &Path) -> bool {
    #[cfg(target_os = "windows")]
    {
        Command::new("explorer")
            .arg("/select,")
            .arg(path)
            .spawn()
            .is_ok()
    }
    #[cfg(target_os = "linux")]
    {
        path.parent()
            .and_then(|d| Command::new("xdg-open").arg(d).spawn().ok())
            .is_some()
    }
    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg("-R").arg(path).spawn().is_ok()
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

fn open_dir(dir: &Path) -> bool {
    #[cfg(target_os = "windows")]
    {
        Command::new("explorer").arg(dir).spawn().is_ok()
    }
    #[cfg(target_os = "linux")]
    {
        Command::new("xdg-open").arg(dir).spawn().is_ok()
    }
    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg(dir).spawn().is_ok()
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        false
    }
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
    let mut headers = HeaderMap::new();
    headers.insert("Cache-Control", "no-store".parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    (
        StatusCode::OK,
        headers,
        Html(web_html(state.config.history.len)),
    )
        .into_response()
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
select#to,select#to2{width:230px;max-width:230px}
select#to_agent,select#to_agent2{width:170px;max-width:170px}
.row select{width:auto}
.msg .meta{font-size:12px}
.lbl{color:#8b949e;font-size:11px;margin-right:2px}
.fl{position:relative;color:#7d8aa0;font-size:11px}
.tool{display:none;gap:4px;margin-left:6px}
.fl:hover .tool{display:inline-flex}
.tbtn{display:inline-block;padding:1px 8px;border-radius:8px;font-size:11px;text-decoration:none;color:#9cc5ff;background:#182238;border:1px solid #263041;cursor:pointer;font-family:inherit;margin-left:2px}
.chip-btn{display:inline-block;padding:2px 10px;border-radius:14px;font-size:11px;background:#16233b;color:#9db4dd;border:1px solid #263041;cursor:pointer;margin:2px}
.chip-btn:hover{border-color:#4f8cff;color:#e6edf5}
.msg.file .pay{color:#7ee787}
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
<div id="agent-modal" hidden style="position:fixed;inset:0;background:rgba(4,6,10,.8);align-items:center;justify-content:center;z-index:60;backdrop-filter:blur(4px)">
  <div class="box">
    <h3>编辑 Agent</h3>
    <p class="hint">提示：也可在列表中「双击名称」就地改名（回车保存）。</p>
    <label>agent_id（身份，不可改）</label><input id="ag-id" readonly>
    <label>名称 name（改后全网同步）</label><input id="ag-name">
    <label>能力 capabilities（逗号分隔）</label><input id="ag-caps">
    <div class="row"><button class="primary" onclick="saveAgentEdit()">保存</button><button onclick="hideAgentEdit()">取消</button></div>
    <div id="ag-res" class="hint"></div>
  </div>
</div>
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
        <div class="row"><span class="lbl">目标</span><select id="to"></select><span class="lbl">Agent</span><select id="to_agent"></select></div>
        <div class="row"><textarea id="payload" rows="2" placeholder='{"content":"你好"}'></textarea></div>
        <div class="row"><button class="primary" onclick="doSend()">发送消息</button><input type="file" id="file"><button onclick="doFile()">发送文件</button></div>
        <div class="row" id="quick-targets"></div>
        <div id="sendres" class="hint"></div>
      </div>
      <div class="card"><h2>最近对话（总览 · 分页与查询）</h2>
        <div class="row"><input id="ov-kw" placeholder="搜索：内容 / 节点 / agent…" style="flex:2" oninput="ovGo(1)">
        <select id="ov-size" onchange="ovGo(1)"><option value="10">10</option><option value="20">20</option><option value="30" selected>30</option><option value="50">50</option></select></div>
        <div id="ov-hist"></div>
        <div class="row"><button onclick="ovGo(OV.page-1)">上一页</button><span class="hint" id="ov-info"></span><button onclick="ovGo(OV.page+1)">下一页</button></div>
      </div>
    </section>
    <section id="p-nodes" class="hidden">
      <div class="card"><h2>节点 Peers</h2><table><thead><tr><th>名称</th><th>node_id</th><th>地址</th><th>标签</th><th>状态</th></tr></thead><tbody id="peers-body"></tbody></table></div>
    </section>
    <section id="p-agents" class="hidden">
      <div class="card"><h2>Agent（全网）</h2><table><thead><tr><th>节点</th><th>agent_id</th><th>名字</th><th>能力</th><th>状态</th><th>操作</th></tr></thead><tbody id="agents-body"></tbody></table></div>
    </section>
    <section id="p-chat" class="hidden">
      <div class="card"><h2>发送消息 / 文件</h2>
                <div class="row"><span class="lbl">目标</span><select id="to2"></select><span class="lbl">Agent</span><select id="to_agent2"></select></div>
        <div class="row"><textarea id="payload2" rows="2" placeholder='{"content":"你好"}'></textarea></div>
        <div class="row"><button class="primary" onclick="doSend('2')">发送消息</button><input type="file" id="file2"><button onclick="doFile('2')">发送文件</button></div>
        <div id="sendres2" class="hint"></div>
      </div>
      <div class="card"><h2>对话记录（最近 __LEN__ 条）</h2>
        <div class="row"><input id="ch-kw" placeholder="搜索：内容 / 节点 / agent…" style="flex:2" oninput="chGo(1)">
        <select id="ch-size" onchange="chGo(1)"><option value="20">20</option><option value="50" selected>50</option><option value="100">100</option></select></div>
        <div id="history"></div>
        <div class="row"><button onclick="chGo(CH.page-1)">上一页</button><span class="hint" id="ch-info"></span><button onclick="chGo(CH.page+1)">下一页</button></div>
      </div>
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
let HIST=[],OV={page:1},CH={page:1},NAMES={},AN={},AGENTS=[];
function who(n){return NAMES[n]||n;}
function whoa(n,a){return a?AN[n+'|'+a]||a:''}
function dirLab(m){if(m.kind==='file')return '📄 文件';if(m.direction==='out')return '⬆ 发出';if(m.direction==='in')return '⬇ 收到';return '• 本机';}
function prettyPay(m){
  const p=m.payload||{};
  if(p&&typeof p==='object'&&typeof p.content==='string'){const t=p.content;return t.length>260?t.slice(0,260)+'…':t;}
  return JSON.stringify(p);
}
function histRow(m){
  const dir=(m.direction==='out'?'out':m.direction==='in'?'in':'');
  const fromW=who(m.from)+(m.from_agent?` [${whoa(m.from,m.from_agent)}]`:'');
  const toW=who(m.to)+(m.to_agent?` [${whoa(m.to,m.to_agent)}]`:'');
  if(m.kind==='file'){
    const nm=esc((m.payload&&m.payload.file_name)||'file');
    const sp=(m.payload&&m.payload.saved_path)||'';
    const sz=humanSize(m.payload&&m.payload.size||0);
    const local=(m.direction==='in'||m.direction==='local')&&sp;
    let head;
    if(local){
      const dl=encodeURIComponent(nm);
      head=`<span class="fl">📄 <a href="/v1/files/${dl}" target="_blank" rel="noopener">${nm}</a> <span class="tool"><a class="tbtn" href="/v1/files/${dl}" target="_blank" rel="noopener">打开文件</a><button class="tbtn" onclick="openFl('${dl}')">打开文件夹</button></span></span>`;
    }else{head=`📤 ${nm}`;}
    return `<div class="msg file"><div class="meta">${fmt(m.ts)} · ${dirLab(m)} · ${esc(fromW)} → ${esc(toW)}</div><div class="pay">${head} · ${sz}</div></div>`;
  }
  return `<div class="msg ${dir}"><div class="meta">${fmt(m.ts)} · ${dirLab(m)} · ${esc(fromW)} → ${esc(toW)} · <span class="k">${esc(m.kind)}/${esc(m.action)}</span></div><div class="pay">${esc(prettyPay(m))}</div></div>`;
}
function humanSize(n){if(!n)return '';if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(1)+' MB';}
function filterHist(kw,arr){if(!kw)return arr.slice();return arr.filter(m=>JSON.stringify({f:m.from,a:m.from_agent,t:m.to,ta:m.to_agent,k:m.kind,p:m.payload}).toLowerCase().includes(kw.toLowerCase()));}
function renderList(elx,info,arr,page,size){
  const total=arr.length,pages=Math.max(1,Math.ceil(total/size));
  if(page<1)page=1; if(page>pages)page=pages;
  const part=arr.slice().reverse().slice((page-1)*size,page*size);
  elx.innerHTML=part.map(histRow).join('')||'<p class="hint">无记录</p>';
  info.textContent=`第 ${page}/${pages} 页 · 共 ${total} 条`;
  return page;
}
function ovGo(p){OV.page=renderList(el('ov-hist'),el('ov-info'),filterHist(el('ov-kw').value,HIST),p,parseInt(el('ov-size').value)||20);}
function chGo(p){CH.page=renderList(el('history'),el('ch-info'),filterHist(el('ch-kw').value,HIST),p,parseInt(el('ch-size').value)||50);}
async function openFl(nm){try{await j('/v1/open-folder',{method:'POST',body:JSON.stringify({file:decodeURIComponent(nm)})});}catch(e){}}
function editAgent(i){const a=AGENTS[i]||{};el('ag-id').value=a.agent_id||'';el('ag-name').value=a.name||'';el('ag-caps').value=(a.capabilities||[]).join(', ');el('ag-res').textContent='';const m=el('agent-modal');m.hidden=false;m.style.display='flex';}
function hideAgentEdit(){const m=el('agent-modal');m.hidden=true;m.style.display='none';}
async function saveAgentEdit(){
  const body={agent_id:el('ag-id').value.trim(),name:el('ag-name').value.trim(),capabilities:el('ag-caps').value.split(/[,，]/).map(x=>x.trim()).filter(Boolean)};
  try{const d=await j('/v1/agents',{method:'POST',body:JSON.stringify(body)});el('ag-res').textContent=JSON.stringify(d);hideAgentEdit();refresh();}catch(e){el('ag-res').textContent='保存失败: '+(e.message||e);}
}
function inlineName(td,i){
  const a=AGENTS[i]||{}; if(!a) return;
  const old=el('ed-input');
  if(old) return; // 已有输入框在编辑中
  const inp=document.createElement('input');
  inp.id='ed-input'; inp.value=a.name||''; td.innerHTML=''; td.appendChild(inp);
  inp.focus(); inp.select();
  let done=false;
  function close(){
    if(done) return; done=true; el('ed-input')&&el('ed-input').parentNode&&(el('ed-input').remove());
  }
  inp.addEventListener('keydown',ev=>{
    if(ev.key==='Enter'){ ev.preventDefault(); const v=inp.value.trim(); close(); if(v&&v!==(a.name||'')){ saveName(i,v); } else { refresh(); } }
    else if(ev.key==='Escape'){ close(); refresh(); }
  });
  inp.addEventListener('blur',()=>{ const v=inp.value.trim(); close(); if(v&&v!==(a.name||'')){ saveName(i,v); } else { refresh(); } });
}
async function saveName(i,newName){
  const a=AGENTS[i]||{};
  try{
    const d=await j('/v1/agents',{method:'POST',body:JSON.stringify({agent_id:a.agent_id,name:newName,capabilities:a.capabilities||[]})});
    refresh();
  }catch(e){ alert('改名失败: '+(e.message||e)); refresh(); }
}
async function delAgent(i){const a=AGENTS[i]||{};if(!confirm('删除 Agent '+a.agent_id+'？'))return;try{const d=await j('/v1/agents',{method:'DELETE',body:JSON.stringify({agent_id:a.agent_id})});alert(JSON.stringify(d));refresh();}catch(e){alert('删除失败: '+(e.message||e));}}
function pick(id,ag){
  const t=el('to'),t2=el('to2'),a=el('to_agent'),a2=el('to_agent2');
  if(t){t.value=id;} if(t2){t2.value=id;}
  if(a){a.value=ag||'';} if(a2){a2.value=ag||'';}
}
[['to_agent','to'],['to_agent2','to2']].forEach(([s,t])=>{
  const se=el(s),te=el(t);
  if(se&&te)se.addEventListener('change',()=>{
    const o=se.options[se.selectedIndex]; if(o&&o.dataset.node)te.value=o.dataset.node;
  });
});
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
function el(id){return document.getElementById(id);}
function val(id){const e=el(id);return e?(e.value||'').trim():'';}
async function doSend(sfx){
  sfx=sfx||'';
  const res=el('sendres'+sfx);
  const to=val('to'+sfx); if(!to){res.textContent='请填目标 to';return;}
  const to_agent=val('to_agent'+sfx)||undefined;
  let raw=val('payload'+sfx),payload;
  try{payload=raw?JSON.parse(raw):{};}catch{payload={content:raw};}
  try{
    const d=await j('/v1/send',{method:'POST',body:JSON.stringify({to,to_agent,type:'text',action:'send',payload})});
    res.textContent=JSON.stringify(d);refresh();
  }catch(e){res.textContent='发送失败: '+(e.message||e);}
}
async function doFile(sfx){
  sfx=sfx||'';
  const res=el('sendres'+sfx);
  const fe=el('file'+sfx);
  const f=fe&&fe.files&&fe.files[0];
  if(!f){res.textContent='请先选择文件';return;}
  const to=val('to'+sfx); if(!to){res.textContent='请填目标 to';return;}
  const q=new URLSearchParams({to});
  if(val('to_agent'+sfx))q.set('to_agent',val('to_agent'+sfx));
  try{
    res.textContent='上传发送中…';
    const d=await j('/v1/send-file-upload?'+q,{method:'POST',headers:{'X-File-Name':f.name},body:f});
    res.textContent=JSON.stringify(d);refresh();
    fe.value='';
  }catch(e){res.textContent='发送失败: '+(e.message||e);}
}
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
  AGENTS=agents&&agents.ok?agents.agents:[];
  let ag=''; AGENTS.forEach((a,i)=>ag+=`<tr><td>${esc(a.node_name||a.node_id)}</td><td>${esc(a.agent_id)}</td><td class="ed-name" title="双击改名，回车保存" ondblclick="inlineName(this,${i})">${esc(a.name)}</td><td>${(a.capabilities||[]).map(c=>`<span class="chip">${esc(c)}</span>`).join('')}</td><td><span class="badge ${a.online?'on':'off'}"><span class="dot"></span>${a.online?'在线':'离线'}</span></td><td><button class="tbtn" onclick="editAgent(${i})">改名</button> <button class="tbtn" onclick="delAgent(${i})">删除</button></td></tr>`);
  $('agents-body').innerHTML=ag;
  // 节点名 / agent 名映射（给人看）
  NAMES={}; AN={};
  if(st&&st.node_id){NAMES[st.node_id]=st.name||st.node_id;}
  if(peers&&peers.ok)for(const p of peers.peers)NAMES[p.node_id]=p.name;
  if(agents&&agents.ok)for(const a of agents.agents){AN[a.node_id+'|'+a.agent_id]=(a.name||a.agent_id); NAMES[a.node_id]=a.node_name||NAMES[a.node_id]||a.node_id;}
  // 目标下拉（纯选择）
  const curTo=el('to').value, curTo2=el('to2').value, curAg=el('to_agent').value, curAg2=el('to_agent2').value;
  let opn='<option value="*">* 广播（全部节点）</option>';
  if(peers&&peers.ok)for(const p of peers.peers)opn+=`<option value="${esc(p.node_id)}">${esc(p.name)}${p.online?'':'（离线）'}</option>`;
  [el('to'),el('to2')].forEach(x=>{const c=x===el('to')?curTo:curTo2;x.innerHTML=opn;if(c&&[...x.options].some(o=>o.value===c))x.value=c;});
  let opa='<option value="">（无 · 发给整机）</option>';
  if(agents&&agents.ok)for(const a of agents.agents)opa+=`<option value="${esc(a.agent_id)}" data-node="${esc(a.node_id)}">${esc(a.node_name||a.node_id)} · ${esc(a.agent_id)}</option>`;
  [el('to_agent'),el('to_agent2')].forEach(x=>{const c=x===el('to_agent')?curAg:curAg2;x.innerHTML=opa;if(c&&[...x.options].some(o=>o.value===c))x.value=c;});
  HIST=hist&&hist.ok?hist.history.slice():[];
  ovGo(OV.page); chGo(CH.page);
  let qt='';
  if(peers&&peers.ok)for(const p of peers.peers){if(p.online)qt+=`<button class="chip-btn" onclick="pick('${esc(p.node_id)}','')" title="${esc(p.name)}">${esc(p.name)}</button>`;}
  if(agents&&agents.ok)for(const a of agents.agents){if(a.online)qt+=`<button class="chip-btn" onclick="pick('${esc(a.node_id)}','${esc(a.agent_id)}')" title="${esc(a.node_name||a.node_id)} / ${esc(a.agent_id)}">${esc(a.node_name||a.node_id)} · ${esc(a.agent_id)}</button>`;}
  const qtEl=el('quick-targets'); if(qtEl)qtEl.innerHTML=qt?('快捷选择：'+qt):'';
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
