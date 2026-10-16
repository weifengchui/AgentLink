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
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequest, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{async_trait, Json, Router as AxRouter};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncReadExt;
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
    // Browser uploads stream raw bytes into `Bytes`. Axum's default body limit is
    // 2 MiB, which would reject larger files with a plain-text 413 *before* the
    // handler runs, silently overriding `files.max_file_size`. Raise the limit on
    // that route to the configured ceiling (plus one byte, so a file that is one
    // byte too big still reaches the handler's own check and gets the clean JSON
    // "file too large" message instead of axum's terse text). Taken at startup:
    // changing `max_file_size` via `/v1/config` needs a node restart to move it.
    let upload_route = AxRouter::new()
        .route("/v1/send-file-upload", post(send_file_upload))
        .layer(DefaultBodyLimit::max(
            state.config.files.max_file_size as usize + 1,
        ));
    AxRouter::new()
        .route("/v1/health", get(health))
        .route("/v1/status", get(status))
        .route("/v1/peers", get(peers))
        .route(
            "/v1/peers/:node_id/forget",
            axum::routing::delete(peers_forget),
        )
        .route(
            "/v1/peers/:node_id/unforget",
            axum::routing::post(peers_unforget),
        )
        .route("/v1/send", post(send))
        .route("/v1/send-file", post(send_file))
        .route("/v1/ack-inbox", post(ack_inbox))
        .route(
            "/v1/agents",
            get(agents_get).post(agents_post).delete(agents_delete),
        )
        .route("/v1/fleet/agents", get(fleet_agents))
        .route("/v1/history", get(history_get))
        .route("/v1/events", get(events_get))
        .route("/v1/history/search", get(history_search_get))
        .route("/v1/colors", get(colors_get).put(colors_put))
        .route("/v1/history/stats", get(history_stats_get))
        .route("/v1/history/export", get(history_export_get))
        .route("/v1/files/:file_name", get(file_download))
        .route("/v1/open-folder", post(open_folder))
        .route("/v1/config", get(config_get).put(config_put))
        .route("/web", get(web_page))
        .route("/v1/web", get(web_page))
        .route("/v1/starmap", get(starmap_page))
        .route("/web/setup", get(web_setup))
        .route("/v1/ws", get(subscribe_ws))
        .route("/peer/ws", get(peer_ws))
        .route("/peer/file/ws", get(file_ws))
        .merge(upload_route)
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
        // v0.5 §5: capability probe for orchestrators — endpoints listed
        // here exist on this node; absence means "degrade to legacy polling".
        "features": ["events", "redact"],
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
            "inbox": state.router.inbox_len(),
            "durable": state.router.durable(),
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

/// Persist the current suppress list under the node's data dir.
fn persist_forgotten(state: &AppState) -> Vec<String> {
    let ids = state.peers.suppressed();
    let path = state.config.data_dir.join("forgotten_peers.json");
    let _ = std::fs::write(
        &path,
        serde_json::to_string_pretty(&ids).unwrap_or_default(),
    );
    ids
}

/// Forget a (ghost) peer: removes it from the table and suppresses
/// re-discovery until that node handshakes live again.
async fn peers_forget(
    State(state): State<SharedState>,
    headers: HeaderMap,
    UrlPath(node_id): UrlPath<String>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let removed = state.peers.forget(&node_id);
    let ids = persist_forgotten(&state);
    state.router.close_peer(&node_id);
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "removed": removed, "suppressed": ids })),
    )
}

async fn peers_unforget(
    State(state): State<SharedState>,
    headers: HeaderMap,
    UrlPath(node_id): UrlPath<String>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    state.peers.unforget(&node_id);
    let ids = persist_forgotten(&state);
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "suppressed": ids })),
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
    names.insert(state.config.node_id.clone(), state.display_name());
    // Ghost guard: an agent can only be online if its NODE is online. Remote
    // agent caches used to keep `online:true` forever after the owner node
    // went away (new identity/reboot), confusing the fleet view.
    let node_online = |nid: &str| nid == state.config.node_id || state.peers.is_online(nid);
    let agents: Vec<serde_json::Value> = state
        .router
        .fleet_agents()
        .iter()
        .map(|a| {
            let mut v = serde_json::to_value(a).unwrap_or(serde_json::Value::Null);
            if let serde_json::Value::Object(m) = &mut v {
                m.insert(
                    "online".to_string(),
                    serde_json::json!(a.online && node_online(&a.node_id)),
                );
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

/// `/v1/events` query params (v0.5 §1).
#[derive(Debug, Deserialize)]
struct EventsQuery {
    /// Return events with `seq >= since`; pass the `next_seq` from the
    /// previous response to continue without loss on reconnect. Omitted or 0
    /// returns whatever the ring still holds.
    #[serde(default)]
    since: Option<u64>,
    /// Only events involving this agent id (as `from_agent` or `to_agent`).
    #[serde(default)]
    agent: Option<String>,
    /// Comma-separated frame types, matched against the envelope `type`
    /// (e.g. `text,file`). ctrl/heartbeat frames are never in this view.
    #[serde(default)]
    types: Option<String>,
    /// Max events per response (default 500, clamped to 5000).
    #[serde(default)]
    limit: Option<usize>,
}

/// Control-plane event stream for orchestrators (v0.5 §1): incremental,
/// append-only pull over business frames with per-node `seq`. Payloads are
/// sanitized through the redaction pipeline (§2) before output. When `since`
/// falls below the ring after a rotation the response is 409 with the oldest
/// available seq, and the caller rebuilds from full `/v1/history`.
async fn events_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let since = q.since.unwrap_or(0);
    let limit = q.limit.unwrap_or(500).clamp(1, 5000);
    let agent = q
        .agent
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty());
    let types: Option<Vec<String>> = q.types.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    });
    let view = state
        .router
        .events_view(since, agent.as_deref(), types.as_deref(), limit);
    if view.gap {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "ok": false,
                "error": "since_before_history",
                "oldest_seq": view.oldest_seq,
                "next_seq": view.next_seq,
                "hint": "events below the ring were rotated out; rebuild from /v1/history",
            })),
        );
    }
    let events: Vec<serde_json::Value> = view
        .events
        .iter()
        .map(|e| json!({ "seq": e.seq, "ts": e.ts, "frame": e }))
        .collect();
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "events": events,
            "next_seq": view.next_seq,
            "oldest_seq": view.oldest_seq,
        })),
    )
}

/// Conversation-history footprint for the web console. Metadata only, and it
/// never mutates records — history is append/roll-over, never deleted.
async fn history_stats_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let h = &state.config.history;
    if !h.enabled || h.file.is_empty() || h.file == "-" {
        return (
            StatusCode::OK,
            Json(json!({ "ok": false, "error": "history disabled" })),
        );
    }
    let mut v = crate::store::history_stats(
        &state.config.data_dir,
        &h.file,
        h.max_file_mb,
        h.archive_days,
        h.archive_keep,
    );
    v["ok"] = json!(true);
    (StatusCode::OK, Json(v))
}

#[derive(Debug, Deserialize)]
pub struct HistorySearchQuery {
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    who: String,
    #[serde(default)]
    direction: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    q: String,
    #[serde(default)]
    since: String,
    #[serde(default)]
    until: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    max_mb: Option<u64>,
    #[serde(default)]
    scope: String,
}

/// Disk-backed conversation-history search with filters (sender / receiver /
/// time window / keyword / max results). Streams the JSONL backwards, so hits
/// near the tail of a huge log return fast. Read-only.
async fn history_search_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(qs): Query<HistorySearchQuery>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized().into_response();
    }
    let h = &state.config.history;
    if !h.enabled || h.file.is_empty() || h.file == "-" {
        return (
            StatusCode::OK,
            Json(json!({ "ok": false, "error": "history disabled" })),
        )
            .into_response();
    }
    let f = crate::store::HistSearch {
        from: qs.from,
        to: qs.to,
        who: qs.who,
        direction: qs.direction,
        kind: qs.kind,
        q: qs.q,
        since: qs.since,
        until: qs.until,
        limit: qs.limit.unwrap_or(200).clamp(1, 2000),
        max_bytes: qs.max_mb.unwrap_or(128).clamp(1, 2048) * 1_048_576,
        scope_all: qs.scope != "active",
    };
    let dir = state.config.data_dir.clone();
    let file = h.file.clone();
    let v = tokio::task::spawn_blocking(move || crate::store::history_search(&dir, &file, &f))
        .await
        .unwrap_or_else(|_| json!({ "ok": false, "error": "search panicked" }));
    (StatusCode::OK, Json(v)).into_response()
}

/// Identity colors for the console (see `store::COLORS_FILE`).
async fn colors_get(State(state): State<SharedState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let dir = state.config.data_dir.clone();
    let m = tokio::task::spawn_blocking(move || crate::store::load_colors(&dir))
        .await
        .unwrap_or_default();
    (StatusCode::OK, Json(json!({ "ok": true, "colors": m })))
}

/// Replace the whole color map. Keys `node:<id>` / `agent:<node>|<agent>`,
/// values `#rrggbb`. Colors live on the node, so every viewer sees the same
/// palette without touching any agent's own config.
async fn colors_put(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(body): JsonAny<serde_json::Value>,
) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    let Some(map) = body.get("colors").and_then(|c| c.as_object()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "expected {\"colors\": {...}}" })),
        );
    };
    if map.len() > 2000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "too many entries (max 2000)" })),
        );
    }
    for (k, v) in map {
        let hex = v.as_str().unwrap_or("");
        let good_hex = hex.len() == 7
            && hex.starts_with('#')
            && hex.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit);
        let good_key = (k.starts_with("node:") || k.starts_with("agent:")) && k.len() <= 200;
        if !good_hex || !good_key {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": format!("bad entry {k}={hex}") })),
            );
        }
    }
    let dir = state.config.data_dir.clone();
    let m = map.clone();
    match tokio::task::spawn_blocking(move || crate::store::save_colors(&dir, &m)).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": "join error" })),
        ),
    }
}

#[derive(Debug, Deserialize)]
struct HistoryExportQuery {
    /// `json` (raw JSONL, default) or `text` (human readable).
    #[serde(default)]
    format: String,
    /// `all` (archives + active, default), `active`, or `archive:<name>`.
    #[serde(default = "default_export_scope")]
    scope: String,
}

fn default_export_scope() -> String {
    "all".to_string()
}

/// Stream the conversation history out as a downloadable attachment.
///
/// Export is strictly read-only: the node copies records out and deletes
/// nothing, so an operator can archive to taste (grep-able text, or JSONL for
/// re-ingest) whenever the console warns about size.
async fn history_export_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(q): Query<HistoryExportQuery>,
) -> Response {
    if !authorized(&state, &headers) {
        return unauthorized().into_response();
    }
    let h = &state.config.history;
    if !h.enabled || h.file.is_empty() || h.file == "-" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "history disabled" })),
        )
            .into_response();
    }
    let files = crate::store::history_export_files(&state.config.data_dir, &h.file, &q.scope);
    if files.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "no history files to export" })),
        )
            .into_response();
    }
    let as_text = matches!(q.format.as_str(), "text" | "txt" | "md");
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let scope_tag = q
        .scope
        .replace("archive:", "")
        .replace('/', "_")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '.')
        .collect::<String>();
    let scope_tag = if scope_tag.is_empty() {
        "all".to_string()
    } else {
        scope_tag
    };
    let ext = if as_text { "txt" } else { "jsonl" };
    let name = format!("agentlink-history-{scope_tag}-{secs}.{ext}");

    struct Step {
        files: Vec<PathBuf>,
        idx: usize,
        file: Option<tokio::fs::File>,
        carry: Vec<u8>,
        text: bool,
    }
    let step = Step {
        files,
        idx: 0,
        file: None,
        carry: Vec::new(),
        text: as_text,
    };
    let stream = futures_util::stream::unfold(step, |mut s| async move {
        loop {
            if s.file.is_none() {
                if s.idx >= s.files.len() {
                    if s.carry.is_empty() {
                        return None;
                    }
                    let out = std::mem::take(&mut s.carry);
                    return Some((Ok::<_, std::io::Error>(Bytes::from(out)), s));
                }
                s.file = tokio::fs::File::open(&s.files[s.idx]).await.ok();
                s.idx += 1;
                if s.file.is_none() {
                    continue;
                }
            }
            let mut buf = vec![0u8; 256 * 1024];
            let read = match s.file.as_mut() {
                Some(f) => f.read(&mut buf).await,
                None => return None,
            };
            match read {
                Ok(0) => {
                    s.file = None;
                    continue;
                }
                Ok(n) => {
                    buf.truncate(n);
                    if !s.text {
                        let mut out = std::mem::take(&mut s.carry);
                        out.extend_from_slice(&buf);
                        return Some((Ok::<_, std::io::Error>(Bytes::from(out)), s));
                    }
                    s.carry.extend_from_slice(&buf);
                    let mut chunk: Vec<u8> = Vec::new();
                    let mut consumed = 0usize;
                    while let Some(rel) = s.carry[consumed..].iter().position(|b| *b == b'\n') {
                        let line =
                            String::from_utf8_lossy(&s.carry[consumed..consumed + rel]).to_string();
                        consumed += rel + 1;
                        chunk.extend_from_slice(
                            crate::store::history_line_to_text(&line).as_bytes(),
                        );
                        chunk.push(b'\n');
                    }
                    s.carry.drain(..consumed);
                    if chunk.is_empty() {
                        continue;
                    }
                    return Some((Ok::<_, std::io::Error>(Bytes::from(chunk)), s));
                }
                Err(e) => return Some((Err(e), s)),
            }
        }
    });

    let mut resp = Response::new(Body::from_stream(stream));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(if as_text {
            "text/plain; charset=utf-8"
        } else {
            "application/x-ndjson; charset=utf-8"
        }),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")) {
        resp.headers_mut()
            .insert(axum::http::header::CONTENT_DISPOSITION, v);
    }
    resp
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
    let path = state.config.files.recv_dir_resolved().join(&name);
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
    let path = state.config.files.recv_dir_resolved().join(&name);
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
                "history": { "enabled": c.history.enabled, "len": c.history.len, "file": c.history.file,
                    "max_file_mb": c.history.max_file_mb, "archive_days": c.history.archive_days, "archive_keep": c.history.archive_keep },
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

/// 消息星图（v0.4.16）：总览页 iframe 嵌入；数据源 /v1/history/search + 拓扑接口。
async fn starmap_page() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert("Cache-Control", "no-store".parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    (
        StatusCode::OK,
        headers,
        Html(include_str!("../web/starmap.html")),
    )
        .into_response()
}

fn web_html(history_len: usize) -> String {
    const HTML: &str = r#"<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AgentLink · Console v__VER__</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;background:#0b0e14;color:#dbe2ea;font-size:13px}
a{color:#4f8cff;text-decoration:none}
.hidden{display:none!important}
#app{display:grid;grid-template-columns:var(--sw,220px) 8px 1fr;min-height:100vh}
#sidebar{background:#0e1219;border-right:1px solid #1c2330;padding:16px 12px;position:sticky;top:0;height:100vh;overflow:auto}
#logo{display:flex;align-items:center;gap:8px;padding:4px 8px 14px;font-weight:700;font-size:15px;letter-spacing:.2px}
#logo .dot{width:10px;height:10px;border-radius:50%;background:#4f8cff;box-shadow:0 0 10px #4f8cff}
#sidebar .nav{margin:2px 0;padding:8px 10px;border-radius:8px;cursor:pointer;color:#9aa7b8;display:flex;align-items:center;gap:8px}
#sidebar .nav:hover{background:#141b26;color:#e6edf5}
#sidebar .nav.active{background:#16233b;color:#7fb0ff}
/* R5（10-16 第四轮）：侧栏拖拽把手（拖动调宽 · 单击折叠/展开）与折叠态 */
#side-resize{cursor:col-resize;display:flex;align-items:center;justify-content:center;position:relative;z-index:6}
#side-resize::before{content:"";position:absolute;top:0;bottom:0;left:3px;width:1px;background:transparent;transition:background .18s,box-shadow .18s}
#side-resize:hover::before{background:#4f8cff;box-shadow:0 0 8px rgba(79,140,255,.9)}
.rz-ic{width:5px;height:30px;border-radius:3px;background:#2a3547;transition:height .22s,background .22s,box-shadow .22s}
#side-resize:hover .rz-ic{height:52px;background:#4f8cff;box-shadow:0 0 12px rgba(79,140,255,.85);animation:rzp 1.1s ease-in-out infinite alternate}
@keyframes rzp{from{transform:scaleY(1)}to{transform:scaleY(1.16)}}
#sidebar .ful{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
#sidebar .sho{display:none}
#sidebar.coll .ful,#sidebar.coll #logo .lgn,#sidebar.coll .ver-badge{display:none}
#sidebar.coll .sho{display:inline}
#sidebar.coll .nav{justify-content:center;font-size:14px;padding:8px 0;gap:0}
#sidebar.coll #logo{justify-content:center;padding-left:0;padding-right:0;gap:0}
#main{display:flex;flex-direction:column;min-width:0}
#topbar{display:flex;align-items:center;gap:14px;padding:12px 20px;border-bottom:1px solid #1c2330;position:sticky;top:0;background:rgba(11,14,20,.92);backdrop-filter:blur(6px);z-index:5}
#topbar .t{font-weight:600}
#topbar .sub{color:#7d8aa0;font-size:12px}
/* R6（10-16 第四轮）：顶栏合并为一条全局状态栏，统计胶囊紧凑化 */
#topbar .stat{display:inline-flex;align-items:baseline;gap:7px;background:#10151d;border:1px solid #1c2330;border-radius:10px;padding:3px 12px;font-size:11px;color:#7d8aa0;white-space:nowrap}
#topbar .stat b{font-size:14px;color:#dbe2ea;font-weight:700;font-family:ui-monospace,Consolas,monospace}
.spacer{flex:1}
.badge{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:20px;font-size:11px;line-height:1.4}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.on{background:rgba(63,185,80,.12);color:#3fb950}.on .dot{background:#3fb950}
.off{background:rgba(248,81,73,.12);color:#f85149}.off .dot{background:#f85149}
.warn{background:rgba(210,153,34,.12);color:#d29922}
.content{padding:20px 24px;width:100%;max-width:none}
.card{background:#10151d;border:1px solid #1c2330;border-radius:12px;padding:16px;margin-bottom:16px}
.card h2{margin:0 0 12px;font-size:13px;color:#9aa7b8;font-weight:600;letter-spacing:.4px;text-transform:uppercase}
table{width:100%;border-collapse:collapse}
th{font-size:11px;color:#7d8aa0;text-align:left;padding:6px 8px;border-bottom:1px solid #1c2330;font-weight:500}
td{padding:8px;border-bottom:1px solid #171e29;vertical-align:middle}
tr:hover td{background:#131a24}
.chip{display:inline-block;padding:1px 8px;border-radius:12px;font-size:11px;background:#182238;color:#9db4dd;margin:1px 2px}
#agents-body tr.local td{background:#12203a;box-shadow:inset 0 0 0 0 transparent}
#agents-body tr.local td:first-child{box-shadow:inset 3px 0 0 #4f8cff}
#agents-body tr.local:hover td{background:#182a4c}
.self-chip{display:inline-block;padding:1px 7px;border-radius:10px;font-size:10px;background:#234a86;color:#a8cbff;margin-right:6px}
.msg{border-left:3px solid #263041;padding:8px 12px;margin:8px 0;background:#0d1219;border-radius:0 8px 8px 0}
.msg .meta{color:#7d8aa0;font-size:11px;margin-bottom:2px}
.msg .pay{color:#9cc5ff;word-break:break-all;white-space:pre-wrap}
.msg.out{border-left-color:#4f8cff}
.msg.in{border-left-color:#3fb950}
select#to,select#to2{width:340px;max-width:340px}
.ver-badge{font-size:11px;color:#a8cbff;background:#16233b;border:1px solid #26436f;border-radius:12px;padding:1px 9px;letter-spacing:.3px}
.hide-in{position:absolute;left:-9999px;width:1px;height:1px;opacity:0}
.dz{margin-top:8px;padding:10px 12px;border:1px dashed #2b3648;border-radius:10px;color:#7d8aa0;font-size:12px;text-align:center;transition:.15s}
.dz.drag{border-color:#4f8cff;background:#12203a;color:#a8cbff}
.dz a{color:#7fb0ff}
.att-chip{display:inline-flex;align-items:center;gap:7px;padding:2px 10px;border-radius:14px;background:#182238;color:#9db4dd;font-size:12px}
.idc{display:inline-flex;align-items:center;justify-content:flex-end;background:var(--nc,#22344f);border-radius:13px;padding:2px 2px 2px 14px;margin-right:4px;vertical-align:middle;max-width:100%}
.idc .idci{display:inline-block;border-radius:999px;padding:0 9px;font-weight:600;font-size:12px;line-height:18px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:320px;vertical-align:middle}
.idn{opacity:.62;font-size:11px;margin-right:2px}
#cf-colors{display:flex;flex-direction:column;gap:7px;margin-top:6px}
#cf-colors .cfrow{display:flex;align-items:center;gap:12px}
#cf-colors .cfrow .idc{min-width:0}
#cf-colors .cfrow .idci{max-width:100%}
#cf-colors input[type=color]{width:34px;height:26px;padding:1px;background:#10151d;border:1px solid #2a3550;border-radius:6px;cursor:pointer}
.cfauto{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#9aa7b8;white-space:nowrap;cursor:pointer}
.lk{background:none;border:none;color:#7fb0ff;cursor:pointer;padding:0;font-size:12px;text-decoration:underline;font-family:inherit}
.lk.red{color:#f85149;text-decoration:none;font-size:13px}
optgroup{color:#7d8aa0;font-style:normal;font-weight:600}
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
th.srt{cursor:pointer;user-select:none}
th.srt:hover{color:#dbe2ea}
.msg{user-select:text}
.msg .pay{user-select:text}
.msg .det{color:#67748a;font-size:11px;margin-top:6px;line-height:1.6;word-break:break-all;background:#0b1017;border:1px solid #1c2330;border-radius:8px;padding:6px 8px}
#toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:#1c2a44;color:#cfe3ff;border:1px solid #2f6fe4;padding:8px 18px;border-radius:20px;font-size:13px;opacity:0;transition:opacity .25s;pointer-events:none;z-index:90}
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
    <label>所属节点</label><div id="ag-node" class="hint" style="margin-bottom:8px"></div>
    <label>agent_id（身份，不可改）</label><input id="ag-id" readonly>
    <label>名称 name（改后全网同步）</label><input id="ag-name">
    <label>能力 capabilities（逗号分隔）</label><input id="ag-caps">
    <div class="row"><button class="primary" onclick="saveAgentEdit()">保存</button><button onclick="hideAgentEdit()">取消</button></div>
    <div id="ag-res" class="hint"></div>
  </div>
</div>
<div id="app" class="hidden">
<aside id="sidebar">
  <div id="logo"><span class="dot"></span><span class="lgn">AgentLink</span> <span class="ver-badge" title="当前节点版本">v__VER__</span></div>
  <div class="nav active" data-p="overview" onclick="show('overview')"><span class="sho">总</span><span class="ful">总览</span></div>
  <div class="nav" data-p="nodes" onclick="show('nodes')"><span class="sho">节</span><span class="ful">节点</span></div>
  <div class="nav" data-p="agents" onclick="show('agents')"><span class="sho">智</span><span class="ful">Agent</span></div>
  <div class="nav" data-p="chat" onclick="show('chat')"><span class="sho">话</span><span class="ful">对话 / 发送</span></div>
  <div class="nav" data-p="config" onclick="show('config')"><span class="sho">配</span><span class="ful">配置</span></div>
</aside>
<div id="side-resize" title="拖动调宽 · 单击折叠/展开"><span class="rz-ic"></span></div>
<div id="main">
  <div id="topbar">
    <div style="display:flex;align-items:baseline;gap:10px"><div class="t" id="tb-self">—</div><div class="sub" id="tb-meta">—</div></div>
    <span class="stat" title="在线节点 / 已知节点"><span>在线节点</span><b id="m-nodes">0</b></span>
    <span class="stat" title="内存中最近对话条数"><span>历史消息</span><b id="m-history">0</b></span>
    <span class="stat" title="进行中的文件传输"><span>传输中</span><b id="m-transfer">0</b></span>
    <div class="spacer"></div>
    <span class="sub">自动刷新 <label class="switch"><input type="checkbox" id="auto-refresh" checked><span class="slider"></span></label></span>
    <button onclick="refresh()">刷新</button>
  </div>
  <div class="content">
    <section id="p-overview">
      <div class="card"><iframe id="sm-fr" src="/v1/starmap" title="消息星图" style="width:100%;height:72vh;min-height:540px;border:1px solid #1c2b40;border-radius:12px;background:#04070d" allow="fullscreen"></iframe></div>
      <div class="card"><h2>最近对话（总览 · 分页与查询）</h2>
        <div class="row"><input id="ov-kw" placeholder="搜索：内容 / 节点 / agent…" style="flex:2" oninput="ovGo(1)">
        <select id="ov-size" onchange="ovGo(1)"><option value="10">10</option><option value="20">20</option><option value="30" selected>30</option><option value="50">50</option></select></div>
        <div class="row"><span class="hint" id="ov-stats" style="flex:1">对话记录：—</span><select id="ov-scope"><option value="all">全部（含归档）</option><option value="active">仅当前文件</option></select><button onclick="exportHist('ov','json')">导出 JSON</button><button onclick="exportHist('ov','text')">导出 文本</button></div>
        <div id="ov-hist"></div>
        <div class="row"><button onclick="ovGo(OV.page-1)">上一页</button><span class="hint" id="ov-info"></span><button onclick="ovGo(OV.page+1)">下一页</button></div>
      </div>
    </section>
    <section id="p-nodes" class="hidden">
      <div class="card"><h2>节点 Peers</h2><table><thead><tr><th class="srt" onclick="pSortBy('name')">名称<span id="ps-name"></span></th><th class="srt" onclick="pSortBy('node_id')">node_id<span id="ps-node_id"></span></th><th>地址</th><th>标签</th><th class="srt" onclick="pSortBy('online')">状态<span id="ps-online"></span></th><th>操作</th></tr></thead><tbody id="peers-body"></tbody></table></div>
    </section>
    <section id="p-agents" class="hidden">
      <div class="card"><h2>Agent（全网）</h2><table><thead><tr><th class="srt" onclick="aSortBy('node')">节点<span id="as-node"></span></th><th class="srt" onclick="aSortBy('id')">agent_id<span id="as-id"></span></th><th class="srt" onclick="aSortBy('name')">名字<span id="as-name"></span></th><th>能力</th><th class="srt" onclick="aSortBy('online')">状态<span id="as-online"></span></th><th>操作</th></tr></thead><tbody id="agents-body"></tbody></table></div>
    </section>
    <section id="p-chat" class="hidden">
      <div class="card"><h2>发送消息 / 文件</h2>
                <div class="row"><span class="lbl">目标</span><select id="to2"></select></div>
        <div class="row"><textarea id="payload2" rows="2" placeholder='{"content":"你好"}　可只发附件；在框内 Ctrl+V 可直接贴图'></textarea></div>
        <div class="dz" id="dz2">📎 拖拽文件或文件夹到此　·　<button type="button" class="lk" onclick="el('file2').click()">选文件</button>　·　<button type="button" class="lk" onclick="el('dir2').click()">选目录（自动打包 zip）</button><input type="file" id="file2" class="hide-in" multiple><input type="file" id="dir2" class="hide-in" webkitdirectory multiple></div>
        <div class="row"><button class="primary" onclick="doSendAll('2')">发送</button><span id="attinfo2"></span></div>
        <div id="sendres2" class="hint"></div>
      </div>
      <div class="card"><h2>对话记录（内存显示最近 __LEN__ 条 · 下方可按条件查磁盘全量）</h2>
        <div class="row"><input id="ch-kw" placeholder="搜索：内容 / 节点 / agent…" style="flex:2" oninput="chGo(1)">
        <select id="ch-size" onchange="chGo(1)"><option value="20">20</option><option value="50" selected>50</option><option value="100">100</option></select></div>
        <div class="row">
          <input id="ch-from" list="ch-agents" placeholder="发送方（agent/节点/IP）" style="flex:1">
          <input id="ch-to" list="ch-agents" placeholder="接收方（agent/节点/IP）" style="flex:1">
        </div>
        <div class="row">
          <span style="display:inline-flex;align-items:center;gap:6px;white-space:nowrap"><input type="datetime-local" id="ch-since" title="起始时间（本机时区，服务端按 UTC 过滤）"><span class="hint">→</span><input type="datetime-local" id="ch-until" title="结束时间"></span>
          <select id="ch-limit" title="最多返回条数"><option value="50">最多 50</option><option value="200" selected>最多 200</option><option value="1000">最多 1000</option><option value="2000">最多 2000</option></select>
          <label class="hint" style="font-size:12px"><input type="checkbox" id="ch-all">含归档</label>
          <button class="primary" onclick="histSearch()">查磁盘</button>
          <button onclick="histReset()">重置</button>
        </div>
        <datalist id="ch-agents"></datalist>
        <div class="hint" id="ch-qmeta"></div>
        <div class="row"><span class="hint" id="ch-stats" style="flex:1">对话记录：—</span><select id="ch-scope"><option value="all">全部（含归档）</option><option value="active">仅当前文件</option></select><button onclick="exportHist('ch','json')">导出 JSON</button><button onclick="exportHist('ch','text')">导出 文本</button></div>
        <div id="history"></div>
        <div class="row"><button onclick="chGo(CH.page-1)">上一页</button><span class="hint" id="ch-info"></span><button onclick="chGo(CH.page+1)">下一页</button></div>
      </div>
    </section>
    <section id="p-config" class="hidden">
      <div class="card"><h2>对话记录颜色标识</h2>
        <div class="hint" style="margin-bottom:8px">配色体系：外层深色胶囊＝节点（按 node_id 哈希固定，改名不漂移），内嵌亮色小胶囊＝agent（色相以所在节点为基准偏移，一眼看出归属）。手动选色保存到本节点 colors.json，全网查看者共享；勾回「自动色」即恢复算法色。</div>
        <div id="cf-colors"><span class="hint">加载中…</span></div>
      </div>
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
<div id="toast"></div>

<script>
const $=id=>document.getElementById(id);
const LS_KEY='al_token';
let TOKEN=sessionStorage.getItem(LS_KEY)||'';
const HDR=()=>({'X-Auth-Token':TOKEN});
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmt(ts){try{return new Date(ts).toLocaleString();}catch{return ts;}}
let HIST=[],CHQ=null,COLORS={},OV={page:1},CH={page:1},NAMES={},AN={},AGENTS=[],SELF='';
let PEERS=[],ST={},ASORT={k:'node',d:1},PSORT={k:'name',d:1},EXP=new Set(),LAST={ag:'',pe:'',op:'',hi:'',qt:'',cf:'',dl:''};
// 用户正在选择文本时跳过所有 DOM 重写：选择/复制不再被 2 秒自动刷新打断
function selecting(){const s=window.getSelection();return s&&s.rangeCount>0&&!s.isCollapsed;}
// 用户正在操作控件（下拉展开、输入中、颜色选择器打开）：冻结本轮所有 DOM 重写，防止刷新冲掉控件状态
function ctlBusy(){const a=document.activeElement;if(!a)return false;const t=a.tagName;if(t!=='SELECT'&&t!=='INPUT'&&t!=='TEXTAREA')return false;if(a.id==='auto-refresh'||a.closest('#topbar'))return false;return true;}
function srtIcon(pfx,cur,k){const e=el(pfx+k);if(e)e.textContent=cur.k===k?(cur.d>0?' ↑':' ↓'):'';}
function aSortBy(k){if(ASORT.k===k)ASORT.d=-ASORT.d;else ASORT={k:k,d:1};renderAgents();}
function pSortBy(k){if(PSORT.k===k)PSORT.d=-PSORT.d;else PSORT={k:k,d:1};renderPeers();}
function who(n){return NAMES[n]||n;}
function whoa(n,a){return a?AN[n+'|'+a]||a:''}
function dirLab(m){if(m.kind==='file')return '📄 文件';if(m.direction==='out')return '⬆ 发出';if(m.direction==='in')return '⬇ 收到';return '• 本机';}
function fullPay(m){const p=m.payload;if(p&&typeof p==='object'&&typeof p.content==='string')return p.content;try{return JSON.stringify(p,null,2);}catch(e){return String(p);}}
function tgExp(id){if(EXP.has(id))EXP.delete(id);else EXP.add(id);ovGo(OV.page);chGo(CH.page);}
function toast(s){const t=el('toast');if(!t)return;t.textContent=s;t.style.opacity='1';clearTimeout(t._h);t._h=setTimeout(()=>{t.style.opacity='0';},1400);}
function copyMsg(id){const m=HIST.find(x=>x.msg_id===id);if(!m)return;const p=m.payload||{};let t;if(m.kind==='file'){t=[p.file_name,(p.size!==undefined?humanSize(p.size):''),p.saved_path].filter(Boolean).join(' | ');}else{t=(p&&typeof p==='object'&&typeof p.content==='string')?p.content:JSON.stringify(p,null,2);}copyText(t);}
function copyText(t){if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(t).then(()=>toast('已复制到剪贴板'),()=>fbCopy(t));}else{fbCopy(t);}}
function fbCopy(t){const ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.left='-9999px';document.body.appendChild(ta);ta.focus();ta.select();try{document.execCommand('copy');toast('已复制到剪贴板');}catch(e){toast('复制失败');}ta.remove();}
function fnv(s){let h=2166136261;for(let i=0;i<s.length;i++){h^=s.charCodeAt(i);h=Math.imul(h,16777619)>>>0;}return h;}
function fm32(x){x^=x>>>16;x=Math.imul(x,0x85ebca6b);x^=x>>>13;x=Math.imul(x,0xc2b2ae35);x^=x>>>16;return x>>>0;}
// 双层胶囊配色（用户 10-16 规格）：外层=节点深色容器（左侧留粗弧），内嵌小胶囊=agent 亮色。
// 层级铁律：节点比 agent 深一档。节点色按 node_id 哈希固定（改名/刷新不漂移）。
// agent 配色（10-16 第三轮 R1）：色相/饱和/亮度全部只由 agent_id 哈希派生——同 id 全网同色；
// S/L 扰动位让不同 id 撞同色相时仍可肉眼区分。与 web/starmap.html 必须逐字一致。
function nodeHue(n){return fnv('N|'+String(n))%360;}
function nodeCol(n){return COLORS['node:'+n]||('hsl('+nodeHue(n)+',46%,30%)');}
function nodeHi(n){return COLORS['node:'+n]||('hsl('+nodeHue(n)+',58%,66%)');}
function agHue(a){return fm32(fnv('|uo'+String(a)))%360;}
function agSat(a){return 46+(fm32(fnv('S|'+String(a)))%28);}
function agLit(a){return 56+(fm32(fnv('L|'+String(a)))%24);}
function agCol(n,a){return COLORS['agent:'+n+'|'+a]||('hsl('+agHue(a)+','+agSat(a)+'%,'+agLit(a)+'%)');}
// 手选色可能是深 hex——按亮度给胶囊内文字配可读前景
function onCol(c){if(typeof c==='string'&&c[0]==='#'&&c.length===7){const r=parseInt(c.substr(1,2),16),g=parseInt(c.substr(3,2),16),b=parseInt(c.substr(5,2),16);return (0.299*r+0.587*g+0.114*b)>150?'#0b1420':'#eef4fb';}return '#0b1420';}
function colFor(n,a){return a?agCol(n,a):nodeHi(n);}
function chip(n,a){
  if(a)return `<span class="idc" style="--nc:${nodeCol(n)}" title="${esc(String(n))} / ${esc(String(a))}"><span class="idci" style="background:${agCol(n,a)};color:${onCol(agCol(n,a))}">${esc(whoa(n,a)||a)}</span></span><span class="idn" style="color:${nodeHi(n)};opacity:.85" title="${esc(String(n))}">${esc(who(n))}</span>`;
  return `<span style="color:${nodeHi(n)}" title="${esc(String(n))}">●</span><span class="idn">${esc(who(n))}</span>`;}
function histRow(m){
  const dir=(m.direction==='out'?'out':m.direction==='in'?'in':'');
  const open=EXP.has(m.msg_id);
  const cpy=`<button class="tbtn" title="复制消息内容" onclick="copyMsg('${esc(m.msg_id)}')">复制</button>`;
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
    const ftg=`<button class="tbtn" onclick="tgExp('${esc(m.msg_id)}')">${open?'收起':'详情'}</button>`;
    const fdet=open?`<div class="det">msg_id: ${esc(m.msg_id)} · ${esc(m.direction)} · from ${esc(m.from)}${m.from_agent?' / '+esc(m.from_agent):''} → to ${esc(m.to)}${m.to_agent?' / '+esc(m.to_agent):''} · size: ${esc(String((m.payload&&m.payload.size)||0))} B${sp?` · saved_path: ${esc(sp)}`:''} · ts: ${esc(m.ts)}</div>`:'';
    return `<div class="msg file"><div class="meta">${fmt(m.ts)} · ${dirLab(m)} · ${chip(m.from,m.from_agent)} → ${chip(m.to,m.to_agent)} · ${cpy}${ftg}</div><div class="pay">${head} · ${sz}</div>${fdet}</div>`;
  }
  const full=fullPay(m);
  const long=full.length>260;
  const showTxt=open?full:(long?full.slice(0,260)+'…':full);
  const tg=long?`<button class="tbtn" onclick="tgExp('${esc(m.msg_id)}')">${open?'收起':'展开'}</button>`:'';
  const det=open?`<div class="det">msg_id: ${esc(m.msg_id)} · ${esc(m.direction)} · from ${esc(m.from)}${m.from_agent?' / '+esc(m.from_agent):''} → to ${esc(m.to)}${m.to_agent?' / '+esc(m.to_agent):''} · 类型: ${esc(m.kind)}/${esc(m.action)} · ts: ${esc(m.ts)}</div>`:'';
  return `<div class="msg ${dir}"><div class="meta">${fmt(m.ts)} · ${dirLab(m)} · ${chip(m.from,m.from_agent)} → ${chip(m.to,m.to_agent)} · <span class="k">${esc(m.kind)}/${esc(m.action)}</span> · ${cpy}${tg}</div><div class="pay">${esc(showTxt)}</div>${det}</div>`;
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
function chGo(p){CH.page=renderList(el('history'),el('ch-info'),filterHist(el('ch-kw').value,CHQ||HIST),p,parseInt(el('ch-size').value)||50);}
async function openFl(nm){try{await j('/v1/open-folder',{method:'POST',body:JSON.stringify({file:decodeURIComponent(nm)})});}catch(e){}}
function findAg(nodeId,agentId){return AGENTS.find(x=>x.node_id===nodeId&&x.agent_id===agentId)||{};}
function editAgent(nodeId,agentId){const a=findAg(nodeId,agentId);el('ag-id').value=a.agent_id||'';el('ag-node').textContent=a.node_name||nodeId;el('ag-name').value=a.name||'';el('ag-caps').value=(a.capabilities||[]).join(', ');el('ag-res').textContent='';const m=el('agent-modal');m.hidden=false;m.style.display='flex';}
function hideAgentEdit(){const m=el('agent-modal');m.hidden=true;m.style.display='none';}
async function saveAgentEdit(){
  const body={agent_id:el('ag-id').value.trim(),name:el('ag-name').value.trim(),capabilities:el('ag-caps').value.split(/[,，]/).map(x=>x.trim()).filter(Boolean)};
  try{const d=await j('/v1/agents',{method:'POST',body:JSON.stringify(body)});el('ag-res').textContent=JSON.stringify(d);hideAgentEdit();refresh();}catch(e){el('ag-res').textContent='保存失败: '+(e.message||e);}
}
function inlineName(td,nodeId,agentId){
  const a=findAg(nodeId,agentId); if(!a.agent_id) return;
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
    if(ev.key==='Enter'){ ev.preventDefault(); const v=inp.value.trim(); close(); if(v&&v!==(a.name||'')){ saveName(nodeId,agentId,v); } else { refresh(); } }
    else if(ev.key==='Escape'){ close(); refresh(); }
  });
  inp.addEventListener('blur',()=>{ const v=inp.value.trim(); close(); if(v&&v!==(a.name||'')){ saveName(nodeId,agentId,v); } else { refresh(); } });
}
async function saveName(nodeId,agentId,newName){
  const a=findAg(nodeId,agentId);
  try{
    const d=await j('/v1/agents',{method:'POST',body:JSON.stringify({agent_id:agentId,name:newName,capabilities:a.capabilities||[]})});
    refresh();
  }catch(e){ alert('改名失败: '+(e.message||e)); refresh(); }
}
async function delAgent(nodeId,agentId){if(nodeId!==SELF){alert('远端 Agent 只能在其所属节点删除');return;}if(!confirm('删除 Agent '+agentId+'？'))return;try{const d=await j('/v1/agents',{method:'DELETE',body:JSON.stringify({agent_id:agentId})});alert(JSON.stringify(d));refresh();}catch(e){alert('删除失败: '+(e.message||e));}}
async function forgetPeer(id){if(!confirm('遗忘节点 '+id+' ？\n将从列表移除并忽略自动发现；它下次主动握手连上时会自动回来。'))return;try{await j('/v1/peers/'+id+'/forget',{method:'DELETE'});refresh();}catch(e){alert('失败: '+(e.message||e));}}
function renameNode(btn){if(el('rn-input'))return;const td=btn.closest('tr').children[0];const inp=document.createElement('input');inp.id='rn-input';inp.value=(ST&&ST.name)||'';inp.style.width='150px';td.textContent='';td.appendChild(inp);inp.focus();inp.select();
 inp.onkeydown=e=>{if(e.key==='Enter'){inp.onblur=null;const v=inp.value;inp.remove();saveNodeName(v);}else if(e.key==='Escape'){inp.onblur=null;refresh();}};
 inp.onblur=()=>{const v=inp.value;inp.remove();saveNodeName(v);};}
async function saveNodeName(v){const n=String(v||'').trim();if(!n){refresh();return;}try{await j('/v1/config',{method:'PUT',body:JSON.stringify({name:n})});refresh();}catch(e){alert('改名失败: '+(e.message||e));}}
function pick(id,ag){
  const v=ag?('agent:'+id+':'+ag):id;
  const t=el('to'),t2=el('to2');
  if(t){t.value=v;} if(t2){t2.value=v;}
}
// 合并下拉的值 -> {to, to_agent}；agent:<node_id>:<agent_id> 自动带出机器
function targetOf(sel){
  sel=(sel||'').trim();
  if(sel.startsWith('agent:')){const rest=sel.slice(6);const i=rest.indexOf(':');if(i>0)return {to:rest.slice(0,i),to_agent:rest.slice(i+1)||undefined};}
  return {to:sel,to_agent:undefined};
}
function saveToken(){TOKEN=$('token-input').value.trim();sessionStorage.setItem(LS_KEY,TOKEN);enter();}
/* R7（10-16 第四轮）：401 不再整页 reload——就地弹回 token 层并停轮询 */
function needToken(){TOKEN='';$('modal').classList.remove('hidden');$('app').classList.add('hidden');
 const ti=$('token-input');if(ti){ti.value='';ti.focus();}toast('token 无效，请重新输入');}
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
async function j(p,opt){const o=opt||{};const h={...HDR(),...(o.headers||{})};if(o.body&&typeof o.body==='string'&&!h['Content-Type']&&!h['content-type'])h['Content-Type']='application/json';const r=await fetch(p,{...o,headers:h});const txt=await r.text();let d={};try{d=txt?JSON.parse(txt):{};}catch{d={error:txt.slice(0,300)};}if(r.status===401){sessionStorage.removeItem(LS_KEY);needToken();throw new Error('401');}if(!r.ok)throw new Error('HTTP '+r.status+' '+((d&&(d.error||d.err))||txt.slice(0,200)||r.statusText));return d;}
function show(panel){document.querySelectorAll('.nav').forEach(n=>n.classList.toggle('active',n.dataset.p===panel));document.querySelectorAll('.content>section').forEach(s=>s.classList.add('hidden'));$('p-'+panel).classList.remove('hidden');if(panel==='config')renderColorCfg();}
function input(id){return ($(id)&&$(id).value||'').trim();}
function el(id){return document.getElementById(id);}
function val(id){const e=el(id);return e?(e.value||'').trim():'';}
const ATT={'':[],'2':[]};
function fmtSize(n){n=Number(n)||0;if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(2)+' MB';}
function setAtt(sfx,f){ATT[sfx]=f?[f]:[];renderAtt(sfx);}
function addAtt(sfx,fs){const arr=Array.isArray(fs)?fs:Array.from(fs||[]);if(!arr.length)return;for(const x of arr)ATT[sfx].push(x);renderAtt(sfx);}
function delAtt(sfx,i){ATT[sfx].splice(i,1);const a=el('file'+sfx);if(a)a.value='';renderAtt(sfx);}
function clearAtt(sfx){sfx=sfx||'';ATT[sfx]=[];const a=el('file'+sfx);if(a)a.value='';const b=el('dir'+sfx);if(b)b.value='';renderAtt(sfx);}
function renderAtt(sfx){const box=el('attinfo'+sfx);if(!box)return;const L=ATT[sfx]||[];
  box.innerHTML=L.length?L.map((f,i)=>`<span class="att-chip">📎 ${esc(f.name)} · ${fmtSize(f.size)}${f.__zip?'（目录打包）':''} <button type="button" class="lk red" title="移除该附件" onclick="delAtt('${sfx}',${i})">✕</button></span>`).join('')+` <span class="hint">共 ${L.length} 个 · ${fmtSize(L.reduce((s,f)=>s+Number(f.size||0),0))}</span>`:'<span class="hint">无附件（可多选、可叠加拖拽/粘贴）</span>';}
const CRC_T=(()=>{const t=new Uint32Array(256);for(let i=0;i<256;i++){let c=i;for(let k=0;k<8;k++)c=(c&1)?(0xEDB88320^(c>>>1)):(c>>>1);t[i]=c>>>0;}return t;})();
function crc32(u){let c=0xFFFFFFFF;for(let i=0;i<u.length;i++)c=CRC_T[(c^u[i])&255]^(c>>>8);return (c^0xFFFFFFFF)>>>0;}
function dosT(d){return ((d.getHours()<<11)|(d.getMinutes()<<5)|(d.getSeconds()>>1))&65535;}
function dosD(d){return (((d.getFullYear()-1980)<<9)|((d.getMonth()+1)<<5)|d.getDate())&65535;}
// 极简 zip（store 不压缩，UTF-8 文件名），浏览器端把目录/多文件合成一个附件
async function zipStore(entries){const enc=new TextEncoder(),parts=[],cen=[];let off=0;const now=new Date(),dt=dosT(now),dd=dosD(now);
  for(const e of entries){const nm=enc.encode(e.name),u=e.u8,crc=crc32(u);
    const lh=new DataView(new ArrayBuffer(30));
    lh.setUint32(0,0x04034b50,true);lh.setUint16(4,20,true);lh.setUint16(6,0x0800,true);lh.setUint16(10,dt,true);lh.setUint16(12,dd,true);
    lh.setUint32(14,crc,true);lh.setUint32(18,u.length,true);lh.setUint32(22,u.length,true);lh.setUint16(26,nm.length,true);
    parts.push(lh.buffer,nm,u);
    const ch=new DataView(new ArrayBuffer(46));
    ch.setUint32(0,0x02014b50,true);ch.setUint16(4,20,true);ch.setUint16(6,20,true);ch.setUint16(8,0x0800,true);
    ch.setUint16(12,dt,true);ch.setUint16(14,dd,true);ch.setUint32(16,crc,true);ch.setUint32(20,u.length,true);ch.setUint32(24,u.length,true);
    ch.setUint16(28,nm.length,true);ch.setUint32(42,off,true);
    cen.push(ch.buffer,nm);off+=30+nm.length+u.length;}
  let csz=0;for(const c of cen)csz+=c.byteLength;
  const end=new DataView(new ArrayBuffer(22));
  end.setUint32(0,0x06054b50,true);end.setUint16(8,entries.length,true);end.setUint16(10,entries.length,true);
  end.setUint32(12,csz,true);end.setUint32(16,off,true);
  return new Blob([...parts,...cen,end.buffer],{type:'application/zip'});}
function readFileU8(f){return new Promise((res,rej)=>{const r=new FileReader();r.onload=()=>res(new Uint8Array(r.result));r.onerror=()=>rej(r.error);r.readAsArrayBuffer(f);});}
async function walkEntry(entry,base,out){out=out||[];base=base||'';
  if(entry.isFile){const f=await new Promise(r=>entry.file(r));out.push({name:base+entry.name,u8:await readFileU8(f)});}
  else if(entry.isDirectory){const rd=entry.createReader();let batch;do{batch=await new Promise((res,rej)=>rd.readEntries(res,rej));for(const b of batch)await walkEntry(b,base+entry.name+'/',out);}while(batch&&batch.length);}
  return out;}
async function doSendAll(sfx){
  sfx=sfx||'';
  const res=el('sendres'+sfx);
  const tgt=targetOf(val('to'+sfx)); if(!tgt.to){res.textContent='请选择目标';return;}
  const raw=val('payload'+sfx), atts=ATT[sfx]||[];
  if(!raw&&!atts.length){res.textContent='消息内容和附件都是空的';return;}
  const done=[];
  if(raw){let payload;try{payload=JSON.parse(raw);}catch{payload={content:raw};}
    if(atts.length&&!payload.file)payload.note='附件：'+atts.map(a=>a.name).join('、');
    try{await j('/v1/send',{method:'POST',body:JSON.stringify({to:tgt.to,to_agent:tgt.to_agent,type:'text',action:'send',payload})});done.push('消息 ok');res.textContent='消息 ok，处理附件中…';}
    catch(e){res.textContent='发送失败: '+(e.message||e);return;}}
  let okn=0;
  for(let i=0;i<atts.length;i++){const att=atts[i];const q=new URLSearchParams({to:tgt.to});if(tgt.to_agent)q.set('to_agent',tgt.to_agent);
    res.textContent=`上传附件 ${i+1}/${atts.length}：${att.name}（${fmtSize(att.size)}）`;
    try{const d=await j('/v1/send-file-upload?'+q,{method:'POST',headers:{'X-File-Name':att.name},body:att});
      okn++;done.push('📎'+att.name+' ok'+(d.file_id?' #'+String(d.file_id).slice(0,8):''));}
    catch(e){done.push('📎'+att.name+' 失败: '+(e.message||e));}}
  if(atts.length)clearAtt(sfx);
  res.textContent=done.join(' · ')||'完成';refresh();}
function onPaste(e,sfx){const cd=e.clipboardData;if(!cd)return;let f=null;
  for(const it of (cd.items||[])){if(it.kind==='file'&&it.type&&it.type.indexOf('image/')===0){f=it.getAsFile();break;}}
  if(!f){for(const x of (cd.files||[])){if(x.type&&x.type.indexOf('image/')===0){f=x;break;}}}
  if(!f)return;e.preventDefault();
  const ts=new Date().toISOString().slice(0,19).replace(/[-:T]/g,'');
  const named=(f.name&&f.name!=='image.png')?f:new File([f],'image-'+ts+'.png',{type:f.type||'image/png'});
  addAtt(sfx,[named]);el('sendres'+sfx).textContent='已附上剪贴板图片 '+named.name;}
async function packFromInput(inp,sfx,label){const fs=Array.from(inp.files||[]);if(!fs.length)return;
  const res=el('sendres'+sfx);const top=(fs[0].webkitRelativePath||'').split('/')[0]||label||'folder';
  res.textContent='打包目录中… '+fs.length+' 个文件';
  try{const es=[];for(const x of fs)es.push({name:x.webkitRelativePath||x.name,u8:new Uint8Array(await x.arrayBuffer())});
    const blob=await zipStore(es);const zf=new File([blob],top+'.zip',{type:'application/zip'});zf.__zip=true;
    addAtt(sfx,[zf]);res.textContent=`已打包 ${es.length} 个文件 → ${top}.zip（${fmtSize(zf.size)}）`;}
  catch(e){res.textContent='打包失败: '+(e.message||e);}}
async function onDrop(e,sfx){e.preventDefault();const dz=el('dz'+sfx);if(dz)dz.classList.remove('drag');const res=el('sendres'+sfx);
  const its=(e.dataTransfer&&e.dataTransfer.items)?Array.from(e.dataTransfer.items):[];
  const ents=its.map(i=>i.webkitGetAsEntry?i.webkitGetAsEntry():null).filter(Boolean);
  const dirs=ents.filter(x=>x.isDirectory);
  if(dirs.length){const made=[];
    for(const d of dirs){res.textContent='打包目录中… '+d.name;
      try{const es=await walkEntry(d,'',[]);if(!es.length){res.textContent='目录是空的';continue;}
        const blob=await zipStore(es);const zf=new File([blob],d.name+'.zip',{type:'application/zip'});zf.__zip=true;
        addAtt(sfx,[zf]);made.push(d.name+'.zip');}
      catch(err){res.textContent='目录打包失败: '+(err.message||err);}}
    if(made.length)res.textContent='已打包 '+made.length+' 个目录 → '+made.join('、');return;}
  const fl=(e.dataTransfer&&e.dataTransfer.files)?Array.from(e.dataTransfer.files):[];
  if(fl.length){addAtt(sfx,fl);res.textContent='';return;}
  if(ents.some(x=>x.isFile)){const fe=ents.find(x=>x.isFile);const f=await new Promise(r=>fe.file(r));addAtt(sfx,[f]);res.textContent='';return;}
  res.textContent='没识别到文件';}
function bindPanel(sfx){
  const fi=el('file'+sfx);if(fi)fi.onchange=()=>{addAtt(sfx,Array.from(fi.files||[]));fi.value='';};
  const di=el('dir'+sfx);if(di)di.onchange=()=>packFromInput(di,sfx);
  const dz=el('dz'+sfx);
  if(dz){['dragenter','dragover'].forEach(ev=>dz.addEventListener(ev,e=>{e.preventDefault();dz.classList.add('drag');}));
    ['dragleave','dragend'].forEach(ev=>dz.addEventListener(ev,()=>dz.classList.remove('drag')));
    dz.addEventListener('drop',e=>onDrop(e,sfx));}
  const ta=el('payload'+sfx);if(ta)ta.addEventListener('paste',e=>onPaste(e,sfx));
  renderAtt(sfx);}
bindPanel('');bindPanel('2');
function renderAgents(){
  ['node','id','name','online'].forEach(k=>srtIcon('as-',ASORT,k));
  const rank=a=>(a.node_id===SELF?0:1);
  const list=AGENTS.slice().sort((a,b)=>{
    const r=rank(a)-rank(b); if(r) return r;
    let c=0;
    if(ASORT.k==='node')c=(a.node_name||a.node_id).localeCompare(b.node_name||b.node_id,'zh')||a.agent_id.localeCompare(b.agent_id);
    else if(ASORT.k==='id')c=a.agent_id.localeCompare(b.agent_id);
    else if(ASORT.k==='name')c=(a.name||'').localeCompare(b.name||'','zh');
    else if(ASORT.k==='online')c=(b.online?1:0)-(a.online?1:0)||a.agent_id.localeCompare(b.agent_id);
    return c*ASORT.d;});
  let ag=''; list.forEach(a=>{const k=`'${esc(a.node_id)}','${esc(a.agent_id)}'`;const loc=a.node_id===SELF;
  ag+=`<tr${loc?' class="local"':''}><td>${loc?'<span class="self-chip">本机</span>':''}${esc(a.node_name||a.node_id)}</td><td>${esc(a.agent_id)}</td><td${loc?` class="ed-name" title="双击改名，回车保存" ondblclick="inlineName(this,${k})"`:''}>${esc(a.name)}</td><td>${(a.capabilities||[]).map(c=>`<span class="chip">${esc(c)}</span>`).join('')}</td><td><span class="badge ${a.online?'on':'off'}"><span class="dot"></span>${a.online?'在线':'离线'}</span></td><td>${loc?`<button class="tbtn" onclick="editAgent(${k})">改名</button> <button class="tbtn" onclick="delAgent(${k})">删除</button>`:`<span class="hint" title="远端 Agent：请在其所属节点上改名/删除（本机 API 只管本机注册）">远端</span>`}</td></tr>`;});
  if(ag!==LAST.ag){$('agents-body').innerHTML=ag;LAST.ag=ag;}
}
function renderPeers(){
  ['name','node_id','online'].forEach(k=>srtIcon('ps-',PSORT,k));
  const rank=p=>(ST.node_id&&p.node_id===ST.node_id?0:1);
  const merged=[{node_id:ST.node_id,name:ST.name||ST.node_id,addr:'本机',tags:[],online:true,last_seen_seconds_ago:0,direction:'self'},...PEERS.filter(p=>p.node_id!==ST.node_id)];
  const list=merged.slice().sort((a,b)=>{
    const r=rank(a)-rank(b); if(r) return r;
    let c=0;
    if(PSORT.k==='name')c=(a.name||'').localeCompare(b.name||'','zh');
    else if(PSORT.k==='node_id')c=a.node_id.localeCompare(b.node_id);
    else if(PSORT.k==='online')c=(b.online?1:0)-(a.online?1:0)||(a.name||'').localeCompare(b.name||'','zh');
    return c*PSORT.d;});
  let pe=''; for(const p of list){const loc=ST&&p.node_id===ST.node_id;pe+=`<tr${loc?' class="local"':''}><td>${loc?'<span class="self-chip">本机</span>':''}${esc(p.name)}</td><td>${esc(p.node_id)}</td><td>${esc(p.addr)}</td><td>${(p.tags||[]).map(t=>`<span class="chip">${esc(t)}</span>`).join('')}</td><td><span class="badge ${p.online?'on':'off'}"><span class="dot"></span>${p.online?'在线':'离线'}</span></td><td>${loc?`<button class="tbtn" title="改本机节点显示名（node_id 不变，改名后自动广播给全网）" onclick="renameNode(this)">✎ 改名</button>`:(!p.online?`<button class="tbtn" title="从列表移除且忽略自动发现，直到该节点重新握手连上" onclick="forgetPeer('${esc(p.node_id)}')">遗忘</button>`:`<span class="hint" title="远端节点的显示名请到该节点自身的 /web 页面修改">远端</span>`)}</td></tr>`;}
  if(pe!==LAST.pe){$('peers-body').innerHTML=pe;LAST.pe=pe;}
}
function histSig(){return HIST.length+'|'+(HIST.length?HIST[HIST.length-1].msg_id:'')+'|'+EXP.size;}
let LAST_HSTAT=0;
async function histStats(){if(Date.now()-LAST_HSTAT<30000)return;LAST_HSTAT=Date.now();try{const d=await j('/v1/history/stats');if(!d||d.ok!==true)return;const mb=Number(d.rotate_mb)||1024;const lim=mb*1048576;const tot=Number(d.total_bytes)||0;const act=Number(d.active_bytes)||0;const warn=act>=lim*0.8;let txt='当前 '+fmtSize(d.active_bytes)+' · 归档 '+fmtSize(d.archive_bytes)+'（'+(d.archive_count||0)+' 个）· 合计 '+fmtSize(tot)+' · 满 '+mb+' MB 或 '+(d.archive_days||0)+' 天自动归档，记录不删除';if(d.archive_keep)txt+=' · 仅保留最近 '+d.archive_keep+' 份归档';if(warn)txt+=' · 接近阈值，建议导出备份';for(const id of ['ov-stats','ch-stats']){const e=el(id);if(!e)continue;e.textContent=txt;e.style.color=warn?'#f85149':'';}}catch(err){}}
function exportHist(p,fmt){const s=el(p+'-scope');const sc=s?s.value:'all';window.location.href='/v1/history/export?format='+fmt+'&scope='+sc;}
async function histSearch(){
 const p=new URLSearchParams();
 const put=(k,id)=>{const v=val(id);if(v)p.set(k,v);};
 put('from','ch-from');put('to','ch-to');put('q','ch-kw');
 const sv=val('ch-since');if(sv)p.set('since',new Date(sv).toISOString());
 const uv=val('ch-until');if(uv)p.set('until',new Date(uv).toISOString());
 p.set('limit',el('ch-limit').value);
 p.set('scope',el('ch-all').checked?'all':'active');
 el('ch-qmeta').textContent='磁盘查询中…';
 try{
  const d=await j('/v1/history/search?'+p.toString());
  if(!d||d.ok!==true){el('ch-qmeta').textContent='查询失败：'+((d&&d.error)||'');return;}
  CHQ=d.matches||[];
  const kb=Math.round((d.scanned_bytes||0)/1024);
  let note='服务器命中 '+d.count+' 条 · 扫描 '+kb+' KB';
  if(d.stopped==='budget')note+=' · 达到扫描上限（可收窄时间范围或调低条数）';
  else if(d.stopped==='limit')note+=' · 已达上限截断（更早记录未扫，可提高上限）';
  else if(d.stopped==='older')note+=' · 早于起始时间自动停止';
  el('ch-qmeta').textContent=note+' · 结果在下方分页，可继续用上方关键词二次筛选';
  chGo(1);
 }catch(e){el('ch-qmeta').textContent='查询失败：'+(e.message||e);}
}
function histReset(){CHQ=null;for(const i of ['ch-from','ch-to','ch-since','ch-until','ch-kw'])if(el(i))el(i).value='';if(el('ch-all'))el('ch-all').checked=false;el('ch-qmeta').textContent='';chGo(1);}
function fillAgDl(){const d=el('ch-agents');if(!d)return;const s=new Set();
 for(const a of AGENTS){if(a.agent_id)s.add(a.agent_id);if(a.name)s.add(a.name);if(a.node_id)s.add(a.node_id);if(a.node_name)s.add(a.node_name);}
 const html=[...s].map(x=>`<option value="${esc(x)}"></option>`).join('');
 if(html!==LAST.dl){d.innerHTML=html;LAST.dl=html;}}/* R7：签名相同不重写 */
function renderColorCfg(){const box=el('cf-colors');if(!box||box.contains(document.activeElement))return;
 const ids=new Map();
 for(const a of AGENTS){ids.set('agent:'+a.node_id+'|'+a.agent_id,(a.node_name||who(a.node_id))+' · '+(a.name||a.agent_id));}
 for(const n of Object.keys(NAMES)){ids.set('node:'+n,'节点：'+NAMES[n]);}
 const rows=[...ids.entries()].sort((x,y)=>x[1].localeCompare(y[1]));
 const empty='<span class="hint">暂无节点/agent，等 fleet 同步后自动出现。</span>';
 if(!rows.length){if(LAST.cf!==empty){box.innerHTML=empty;LAST.cf=empty;}return;}
 const html=rows.map(([k,lbl])=>cfgRow(k,lbl)).join('');
 /* R7（10-16 第四轮）：内容不变不重写——配色区不再每 2s 整块闪重建，颜色选择器/勾选状态不再被刷掉 */
 if(html!==LAST.cf){box.innerHTML=html;LAST.cf=html;}}
function cfgPrev(k,n,a,lbl){const outer=nodeCol(n);const inner=a?agCol(n,a):nodeHi(n);
 return `<span class="idc" style="--nc:${esc(outer)}"><span class="idci" style="background:${esc(inner)};color:${onCol(inner)}">${esc(lbl)}</span></span>`;}
function cfgRow(k,lbl){const sep=k.indexOf('|');
 const n=sep>0?k.slice(6,sep):k.slice(5);const a=sep>0?k.slice(sep+1):'';const cur=COLORS[k]||'';
 const hexv=/^#[0-9a-fA-F]{6}$/.test(cur)?cur:'#5b8def';
 return `<div class="cfrow" data-k="${esc(k)}" data-n="${esc(n)}" data-a="${esc(a)}" data-lbl="${esc(lbl)}">${cfgPrev(k,n,a,lbl)}<input type="color" ${cur?'value="'+hexv+'"':''} ${cur?'':'disabled'} onchange="setColor('${k}',this.value)"><label class="cfauto" title="勾选＝跟随算法自动色"><input type="checkbox" ${cur?'':'checked'} onchange="tgAutoCol('${k}',this.checked)">自动色</label></div>`;}
function paintColorRow(k){const enc=CSS.escape?CSS.escape(k):k;
 const row=document.querySelector('#cf-colors .cfrow[data-k="'+enc+'"]');if(!row)return;const cur=COLORS[k]||'';
 const pv=row.querySelector('.idc');if(pv)pv.outerHTML=cfgPrev(k,row.dataset.n,row.dataset.a,row.dataset.lbl);
 const inp=row.querySelector('input[type="color"]');
 if(inp){inp.disabled=!cur;if(cur)inp.value=/^#[0-9a-fA-F]{6}$/.test(cur)?cur:'#5b8def';}
 const cb=row.querySelector('.cfauto input');if(cb)cb.checked=!cur;}
async function setColor(k,v){COLORS[k]=v;paintColorRow(k);await putColors();}
async function tgAutoCol(k,on){if(on)delete COLORS[k];else if(!COLORS[k])COLORS[k]='#5b8def';paintColorRow(k);await putColors();}
async function putColors(){try{await j('/v1/colors',{method:'PUT',body:JSON.stringify({colors:COLORS})});}catch(e){alert('保存颜色失败：'+(e.message||e));}}
async function refresh(){
 const md=$('modal');if(md&&!md.classList.contains('hidden'))return; // token 层打开时停轮询
 histStats();
 try{
  if(el('ed-input')||el('rn-input'))return; // 行内改名编辑中：跳过本轮刷新，防止重绘冲掉编辑框
  const [st,peers,agents,hist,cols]=await Promise.all([j('/v1/status'),j('/v1/peers'),j('/v1/fleet/agents'),j('/v1/history'),j('/v1/colors')]);
  // 用户正在选择/复制文本或操作控件：只更新内存数据，不重写 DOM、不覆盖未保存完的配色（下一轮自动恢复渲染）
  const sel=selecting()||ctlBusy();
  if(!sel)COLORS=(cols&&cols.colors)||{};
  ST=st||{}; PEERS=(peers&&peers.ok)?peers.peers:[]; AGENTS=(agents&&agents.ok)?agents.agents:[]; SELF=ST.node_id||'';
  $('tb-self').textContent=st.name;
  $('tb-meta').textContent=`${st.node_id} · ${new Date().toLocaleTimeString()}`;
  $('m-nodes').textContent=`${st.peers_online}/${st.peers_known}`;
  $('m-history').textContent=hist.history.length;
  $('m-transfer').textContent=st.active_transfers;
  // 节点名 / agent 名映射（给人看）
  NAMES={}; AN={};
  if(st&&st.node_id){NAMES[st.node_id]=st.name||st.node_id;}
  for(const p of PEERS)NAMES[p.node_id]=p.name;
  for(const a of AGENTS){AN[a.node_id+'|'+a.agent_id]=(a.name||a.agent_id); NAMES[a.node_id]=a.node_name||NAMES[a.node_id]||a.node_id;}
  if(!sel){renderPeers();renderAgents();fillAgDl();renderColorCfg();}
  // 目标下拉（纯选择）——签名相同不重写，保住控件状态
  let opn='<optgroup label="节点（发给整机）"><option value="*">* 广播（全部节点）</option>';
  for(const p of PEERS)opn+=`<option value="${esc(p.node_id)}">${esc(p.name)}${p.online?'':'（离线）'}</option>`;
  opn+='</optgroup><optgroup label="Agent（发给指定 Agent）">';
  for(const a of AGENTS)opn+=`<option value="agent:${esc(a.node_id)}:${esc(a.agent_id)}">${esc(a.node_name||a.node_id)} · ${esc(a.agent_id)}${a.online?'':'（离线）'}</option>`;
  opn+='</optgroup>';
  if(!sel&&opn!==LAST.op){LAST.op=opn;
    [el('to'),el('to2')].forEach(x=>{if(!x)return;const c=x.value;x.innerHTML=opn;if(c&&[...x.options].some(o=>o.value===c))x.value=c;});}
  HIST=hist&&hist.ok?hist.history.slice():[];
  if(!sel&&histSig()!==LAST.hi){LAST.hi=histSig();ovGo(OV.page);chGo(CH.page);}
  let qt='';
  for(const p of PEERS){if(p.online)qt+=`<button class="chip-btn" onclick="pick('${esc(p.node_id)}','')" title="${esc(p.name)}">${esc(p.name)}</button>`;}
  for(const a of AGENTS){if(a.online)qt+=`<button class="chip-btn" onclick="pick('${esc(a.node_id)}','${esc(a.agent_id)}')" title="${esc(a.node_name||a.node_id)} / ${esc(a.agent_id)}">${esc(a.node_name||a.node_id)} · ${esc(a.agent_id)}</button>`;}
  if(!sel&&qt!==LAST.qt){const qtEl=el('quick-targets'); if(qtEl){qtEl.innerHTML=qt?('快捷选择：'+qt):'';LAST.qt=qt;}}
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
/* ---------- R5（10-16 第四轮）：侧栏折叠/展开 + 拖拽调宽（localStorage 持久化） ---------- */
function sideSet(px){document.documentElement.style.setProperty('--sw',px+'px');}
function sideCur(){const s=$('sidebar');return s?Math.round(s.getBoundingClientRect().width):220;}
function applySide(){const w=Math.max(150,Math.min(460,parseInt(localStorage.getItem('al_sbw')||'220',10)||220));
 const coll=localStorage.getItem('al_sbc')==='1';
 sideSet(coll?56:w);$('sidebar').classList.toggle('coll',coll);}
function initSideResize(){const rz=el('side-resize');if(!rz)return;let dr=null;
 rz.addEventListener('pointerdown',e=>{e.preventDefault();dr={x:e.clientX,base:sideCur(),moved:false};
  try{rz.setPointerCapture(e.pointerId)}catch(_){}});
 rz.addEventListener('pointermove',e=>{if(!dr)return;const dx=e.clientX-dr.x;
  if(Math.abs(dx)>3)dr.moved=true;
  if(dr.moved){const w=Math.max(150,Math.min(460,dr.base+dx));sideSet(w);
   if($('sidebar').classList.contains('coll')){$('sidebar').classList.remove('coll');localStorage.setItem('al_sbc','0');}}});
 rz.addEventListener('pointerup',()=>{if(!dr)return;const moved=dr.moved;dr=null;
  if(moved)localStorage.setItem('al_sbw',String(sideCur()));
  else{localStorage.setItem('al_sbc',($('sidebar').classList.contains('coll')?'0':'1'));applySide();}});
 rz.addEventListener('pointercancel',()=>{dr=null;});}
applySide();initSideResize();
setInterval(()=>{if($('auto-refresh').checked)refresh();},2000);
</script></body></html>"#;
    HTML.replace("__LEN__", &history_len.to_string())
        .replace("__VER__", VERSION)
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
