//! Peer WebSocket connections: outbound dialing, inbound handshake, heartbeat.
//!
//! Both directions are normalised into a concrete [`Frame`] stream via small
//! socket bridge tasks, so the session logic is written once. Sessions hand
//! business frames to the [`crate::router::Router`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message as AxMessage, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, RwLock};
use tokio_tungstenite::tungstenite::Message as TtMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::api::SharedState;
use crate::config::Config;
use crate::peer::DiscoveredPeer;
use crate::proto::{Envelope, Hello, PROTOCOL_VERSION};

const CHANNEL_CAPACITY: usize = 128;

/// Normalised frame exchanged between the socket bridge and the session logic.
#[derive(Debug)]
pub enum Frame {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

// ---- inbound ----------------------------------------------------------------

/// Handle an inbound peer connection upgraded by the HTTP server.
pub async fn handle_inbound(socket: WebSocket, state: SharedState, remote: SocketAddr) {
    let (out_tx, in_rx) = spawn_axum_bridge(socket);
    session(state, in_rx, out_tx, None, "in", remote.to_string()).await;
}

// ---- outbound ---------------------------------------------------------------

/// Connection manager: decides who dials whom and keeps dialers alive.
pub async fn manager(
    cfg: Config,
    state: SharedState,
    mut rx: mpsc::Receiver<DiscoveredPeer>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut dialers: HashMap<String, Arc<RwLock<String>>> = HashMap::new();

    // Static peers are dialled unconditionally (their node_id is unknown yet).
    for addr in cfg.discovery.static_peers.clone() {
        let key = format!("static:{addr}");
        let cell = Arc::new(RwLock::new(addr.clone()));
        dialers.insert(key.clone(), cell.clone());
        spawn_dialer(state.clone(), key, None, cell, shutdown.clone());
        tracing::info!(addr, "conn: static peer configured");
    }

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            ev = rx.recv() => {
                let Some(ev) = ev else { break };
                if ev.node_id == cfg.node_id {
                    continue;
                }
                // Dedup rule: only the lexicographically smaller node_id dials,
                // so each pair shares exactly one connection.
                if cfg.node_id.as_str() >= ev.node_id.as_str() {
                    continue;
                }
                match dialers.get(&ev.node_id) {
                    Some(cell) => *cell.write().await = ev.addr,
                    None => {
                        tracing::info!(
                            peer = %ev.name,
                            node_id = %ev.node_id,
                            addr = %ev.addr,
                            "conn: will dial peer"
                        );
                        let cell = Arc::new(RwLock::new(ev.addr.clone()));
                        dialers.insert(ev.node_id.clone(), cell.clone());
                        spawn_dialer(
                            state.clone(),
                            ev.node_id.clone(),
                            Some(ev.node_id.clone()),
                            cell,
                            shutdown.clone(),
                        );
                    }
                }
            }
        }
    }
    tracing::info!("conn: manager stopped");
}

/// Spawn a long-lived dialer that reconnects with exponential backoff.
fn spawn_dialer(
    state: SharedState,
    key: String,
    expected: Option<String>,
    addr: Arc<RwLock<String>>,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);
        loop {
            if *shutdown.borrow() {
                break;
            }

            // If we already have a live connection (e.g. inbound), idle.
            if let Some(pid) = &expected {
                if state.peers.is_online(pid) {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                        _ = shutdown.changed() => break,
                    }
                    continue;
                }
            }

            let target = addr.read().await.clone();
            if let Err(e) = dial_once(state.clone(), &target, expected.clone()).await {
                tracing::debug!(%key, target = %target, error = %e, "conn: dial failed");
            }

            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.changed() => break,
            }
            backoff = (backoff * 2).min(max_backoff);
        }
    });
}

/// One connection attempt. Returns after the session ends.
async fn dial_once(state: SharedState, addr: &str, expected: Option<String>) -> anyhow::Result<()> {
    let (out_tx, in_rx) = open_peer_ws(addr).await?;
    session(state, in_rx, out_tx, expected, "out", addr.to_string()).await;
    Ok(())
}

/// Bridge an axum (server-side) WebSocket into frame channels.
/// Returns `(sink, stream)`: send outbound frames to `sink`, read inbound from `stream`.
pub fn spawn_axum_bridge(socket: WebSocket) -> (mpsc::Sender<Frame>, mpsc::Receiver<Frame>) {
    let (in_tx, in_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
    bridge_axum(socket, in_tx, out_rx);
    (out_tx, in_rx)
}

/// Dial a peer WebSocket endpoint and return frame channels `(sink, stream)`.
async fn open_ws(
    addr: &str,
    path: &str,
) -> anyhow::Result<(mpsc::Sender<Frame>, mpsc::Receiver<Frame>)> {
    let url = format!("ws://{addr}{path}");
    let (socket, _resp) = tokio_tungstenite::connect_async(url.as_str()).await?;
    let (in_tx, in_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
    bridge_ws(socket, in_tx, out_rx);
    Ok((out_tx, in_rx))
}

/// Open a message channel to a peer (`/peer/ws`).
pub async fn open_peer_ws(
    addr: &str,
) -> anyhow::Result<(mpsc::Sender<Frame>, mpsc::Receiver<Frame>)> {
    open_ws(addr, "/peer/ws").await
}

/// Open a dedicated file-transfer channel to a peer (`/peer/file/ws`).
pub async fn open_file_ws(
    addr: &str,
) -> anyhow::Result<(mpsc::Sender<Frame>, mpsc::Receiver<Frame>)> {
    open_ws(addr, "/peer/file/ws").await
}

// ---- session ----------------------------------------------------------------

/// Handshake, then pump frames until the connection closes or times out.
async fn session(
    state: SharedState,
    mut recv: mpsc::Receiver<Frame>,
    send: mpsc::Sender<Frame>,
    expected_peer: Option<String>,
    direction: &'static str,
    remote_addr: String,
) {
    let cfg = &state.config;
    let strict = cfg.security.mode == "strict";

    // Always speak first: send our hello.
    let hello = Hello::new(
        &cfg.node_id,
        &state.display_name(),
        &cfg.security.cluster_token,
    );
    if send_text(&send, &hello).await.is_err() {
        return;
    }
    let mut hello_name = state.display_name();

    let mut peer_id: Option<String> = expected_peer;
    let mut last_recv = Instant::now();
    let heartbeat_secs = cfg.discovery.heartbeat_interval_seconds.max(1);
    let timeout = Duration::from_secs(cfg.discovery.offline_timeout_seconds.max(heartbeat_secs));
    let mut ticker = tokio::time::interval(Duration::from_secs(heartbeat_secs));

    loop {
        tokio::select! {
            frame = recv.recv() => {
                let Some(frame) = frame else { break };
                match frame {
                    Frame::Text(text) => {
                        last_recv = Instant::now();
                        let keep = handle_text(
                            &state, &text, &mut peer_id, direction, &remote_addr, strict, &send,
                        ).await;
                        if !keep {
                            break;
                        }
                    }
                    Frame::Ping(p) => {
                        last_recv = Instant::now();
                        let _ = send.send(Frame::Pong(p)).await;
                    }
                    Frame::Pong(_) => last_recv = Instant::now(),
                    Frame::Binary(_) => last_recv = Instant::now(),
                    Frame::Close => break,
                }
            }
            _ = ticker.tick() => {
                if last_recv.elapsed() > timeout {
                    tracing::warn!(?peer_id, "conn: heartbeat timeout");
                    break;
                }
                let beat = Envelope::heartbeat(&cfg.node_id);
                let _ = send_text(&send, &beat).await;
                let _ = send.send(Frame::Ping(Vec::new())).await;
                // 显示名变化（如 /web 控制台改名）：重播 hello，让对端刷新节点台账里的名字。
                let cur = state.display_name();
                if cur != hello_name {
                    hello_name = cur;
                    let nh = Hello::new(
                        &cfg.node_id,
                        &hello_name,
                        &cfg.security.cluster_token,
                    );
                    if send_text(&send, &nh).await.is_err() {
                        break;
                    }
                    tracing::info!(name = %hello_name, "conn: re-announced hello after rename");
                }
            }
        }
    }

    if let Some(pid) = &peer_id {
        state.peers.mark_offline(pid);
        state.router.unregister_peer(pid);
        tracing::info!(node_id = %pid, direction, "conn: peer offline");
    }
    let _ = send.send(Frame::Close).await;
}

/// Handle a decoded text frame. Returns `false` if the connection must close.
async fn handle_text(
    state: &SharedState,
    text: &str,
    peer_id: &mut Option<String>,
    direction: &'static str,
    remote_addr: &str,
    strict: bool,
    send: &mpsc::Sender<Frame>,
) -> bool {
    let cfg = &state.config;
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return true, // not JSON: ignore
    };
    let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");

    match action {
        // v0.5 收官：`__node__` hello 探针是业务帧（type=text，无 auth 字段），
        // 必须落进底部业务分支交给内核拦截；这里只认握手帧（Hello 必带 auth）。
        "hello" if value.get("auth").is_some() => {
            let hello: Hello = match serde_json::from_value(value) {
                Ok(h) => h,
                Err(_) => return true, // 畸形握手帧：忽略，不断链
            };
            if hello.v > PROTOCOL_VERSION {
                tracing::warn!(peer_v = hello.v, "conn: unsupported peer protocol version");
                return false;
            }
            if strict && !hello.verify(&cfg.security.cluster_token) {
                tracing::warn!(node_id = %hello.node_id, "conn: handshake rejected (bad cluster token)");
                return false;
            }
            let changed = peer_id.as_deref() != Some(hello.node_id.as_str());
            *peer_id = Some(hello.node_id.clone());
            state
                .peers
                .mark_online(&hello.node_id, &hello.name, remote_addr, direction);
            if changed {
                tracing::info!(
                    peer = %hello.name,
                    node_id = %hello.node_id,
                    addr = remote_addr,
                    direction,
                    "conn: peer online"
                );
            }
            // Prefer the advertised address (from discovery) over the ephemeral
            // TCP source port, so the peer can be re-dialled for file transfers.
            let advertised = state
                .peers
                .snapshot()
                .into_iter()
                .find(|p| p.node_id == hello.node_id)
                .map(|p| p.addr.clone())
                .unwrap_or_else(|| remote_addr.to_string());
            state.router.register_peer(crate::router::PeerSink {
                node_id: hello.node_id.clone(),
                name: hello.name,
                addr: advertised,
                tx: send.clone(),
            });
            // Tell the peer which agents live on this node.
            if let Some(env) = state.router.agents_update_envelope(&hello.node_id) {
                let _ = send_text(send, &env).await;
            }
            true
        }

        "heartbeat" => {
            if let Some(pid) = peer_id {
                state.peers.touch(pid);
            }
            true
        }

        "ack" => {
            let ref_id = value
                .get("payload")
                .and_then(|p| p.get("ref"))
                .and_then(|r| r.as_str())
                .unwrap_or("");
            if !ref_id.is_empty() {
                state.router.on_ack(ref_id);
            }
            true
        }

        "agents_update" => {
            let from = value.get("from").and_then(|v| v.as_str()).unwrap_or("");
            let agents: Option<Vec<crate::router::AgentRecord>> = value
                .get("payload")
                .and_then(|p| p.get("agents"))
                .and_then(|a| serde_json::from_value(a.clone()).ok());
            if let (Some(agents), false) = (agents, from.is_empty()) {
                state.router.on_agents_update(from, agents);
            }
            true
        }

        _ => {
            // Business frames (or unknown control): let the router decide.
            let Ok(mut env) = serde_json::from_value::<Envelope>(value) else {
                return true;
            };
            if env.kind == "ctrl" {
                return true; // unknown control frame, ignore
            }
            let addressed = env.to == cfg.node_id || env.to == "*" || env.to.is_empty();
            if !addressed {
                tracing::debug!(
                    msg_id = %env.msg_id,
                    to = %env.to,
                    "conn: frame not addressed to us, dropped (mesh does not relay)"
                );
                return true;
            }
            let first = state.router.handle_incoming(&mut env).await;
            if !first {
                tracing::trace!(msg_id = %env.msg_id, "conn: duplicate message");
            }
            // Point-to-point messages are acked (broadcasts are at-most-once).
            if env.to != "*" && !env.to.is_empty() {
                let ack = Envelope::ack(&cfg.node_id, &env.msg_id);
                let _ = send_text(send, &ack).await;
            }
            true
        }
    }
}

async fn send_text(
    send: &mpsc::Sender<Frame>,
    value: &impl serde::Serialize,
) -> anyhow::Result<()> {
    let text = serde_json::to_string(value)?;
    send.send(Frame::Text(text))
        .await
        .map_err(|_| anyhow::anyhow!("connection closed"))
}

// ---- socket bridges ---------------------------------------------------------

/// Bridge an axum (server-side) WebSocket into a [`Frame`] stream.
fn bridge_axum(socket: WebSocket, in_tx: mpsc::Sender<Frame>, mut out_rx: mpsc::Receiver<Frame>) {
    let (mut sink, mut stream) = socket.split();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            let frame = match msg {
                AxMessage::Text(t) => Frame::Text(t.to_string()),
                AxMessage::Binary(b) => Frame::Binary(b),
                AxMessage::Ping(v) => Frame::Ping(v),
                AxMessage::Pong(v) => Frame::Pong(v),
                AxMessage::Close(_) => Frame::Close,
            };
            if in_tx.send(frame).await.is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            let msg = match frame {
                Frame::Text(t) => AxMessage::Text(t),
                Frame::Binary(b) => AxMessage::Binary(b),
                Frame::Ping(v) => AxMessage::Ping(v),
                Frame::Pong(v) => AxMessage::Pong(v),
                Frame::Close => {
                    let _ = sink.send(AxMessage::Close(None)).await;
                    break;
                }
            };
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Bridge a tokio-tungstenite (client-side) WebSocket into a [`Frame`] stream.
fn bridge_ws(socket: WsStream, in_tx: mpsc::Sender<Frame>, mut out_rx: mpsc::Receiver<Frame>) {
    let (mut sink, mut stream) = socket.split();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            let frame = match msg {
                TtMessage::Text(t) => Frame::Text(t.to_string()),
                TtMessage::Binary(b) => Frame::Binary(b.to_vec()),
                TtMessage::Ping(v) => Frame::Ping(v.to_vec()),
                TtMessage::Pong(v) => Frame::Pong(v.to_vec()),
                TtMessage::Close(_) => Frame::Close,
                TtMessage::Frame(_) => continue,
            };
            if in_tx.send(frame).await.is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            let msg = match frame {
                Frame::Text(t) => TtMessage::Text(t),
                Frame::Binary(b) => TtMessage::Binary(b),
                Frame::Ping(v) => TtMessage::Ping(v),
                Frame::Pong(v) => TtMessage::Pong(v),
                Frame::Close => {
                    let _ = sink.send(TtMessage::Close(None)).await;
                    break;
                }
            };
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });
}
