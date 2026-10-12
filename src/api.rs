//! Local HTTP API consumed by agents on the same machine, plus the peer-facing
//! WebSocket endpoint.
//!
//! A single listener serves both; `/v1/*` requires the local API token while
//! `/peer/ws` authenticates via the cluster token during the handshake.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequest, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
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

/// Shared state for the running node.
pub struct AppState {
    pub config: Config,
    pub started_at: Instant,
    pub peers: PeerTable,
    /// Discovery -> connection manager channel.
    pub peer_tx: mpsc::Sender<DiscoveredPeer>,
    /// Message router (messaging + reliability core).
    pub router: Arc<Router>,
    /// File transfer counter.
    pub files: Arc<FileManager>,
}

impl AppState {
    pub fn new(
        config: Config,
        peers: PeerTable,
        peer_tx: mpsc::Sender<DiscoveredPeer>,
        router: Arc<Router>,
        files: Arc<FileManager>,
    ) -> Self {
        AppState {
            config,
            started_at: Instant::now(),
            peers,
            peer_tx,
            router,
            files,
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
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
        .route("/v1/ack-inbox", post(ack_inbox))
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
        "name": state.config.name,
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
            "name": state.config.name,
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
    let _ = params.client;
    ws.on_upgrade(move |socket| handle_subscriber(socket, state))
}

/// Forward router events (messages + join/leave) to a local WS client.
async fn handle_subscriber(socket: WebSocket, state: SharedState) {
    let mut rx: broadcast::Receiver<Envelope> = state.router.subscribe();
    let (mut sender, mut receiver) = socket.split();
    loop {
        tokio::select! {
            ev = rx.recv() => {
                match ev {
                    Ok(env) => {
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
