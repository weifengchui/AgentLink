// SPDX-License-Identifier: Apache-2.0
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
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

use crate::api::{CaAdopt, SharedState};
use crate::config::{Config, MtlsConfig};
use crate::peer::DiscoveredPeer;
use crate::proto::{CaSync, Envelope, Hello, PROTOCOL_VERSION};

const CHANNEL_CAPACITY: usize = 128;

/// Dial target for a peer: the plain `host:port` plus the optional mTLS
/// `host:port` advertised by the peer (v0.6 P1). TLS is used only when this
/// node has mTLS enabled *and* the peer advertised an mTLS endpoint — an
/// opportunistic upgrade that never breaks mixed-version fleets.
#[derive(Clone, Default)]
struct DialTarget {
    addr: String,
    mtls_addr: Option<String>,
}

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
    let mut dialers: HashMap<String, Arc<RwLock<DialTarget>>> = HashMap::new();

    // Static peers are dialled unconditionally (their node_id is unknown yet).
    // A `wss://host:port` (or `mtls://host:port`) entry marks the endpoint as
    // a node-to-node TLS one (v0.6 P1), so static-peer fleets also get mTLS.
    for raw in cfg.discovery.static_peers.clone() {
        let (addr, mtls_addr) = parse_static_peer(&raw);
        let key = format!("static:{raw}");
        let cell = Arc::new(RwLock::new(DialTarget { addr, mtls_addr }));
        dialers.insert(key.clone(), cell.clone());
        spawn_dialer(state.clone(), key, None, cell, shutdown.clone());
        tracing::info!(peer = %raw, "conn: static peer configured");
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
                    Some(cell) => *cell.write().await = DialTarget {
                        addr: ev.addr.clone(),
                        mtls_addr: ev.mtls_addr.clone(),
                    },
                    None => {
                        tracing::info!(
                            peer = %ev.name,
                            node_id = %ev.node_id,
                            addr = %ev.addr,
                            "conn: will dial peer"
                        );
                        let cell = Arc::new(RwLock::new(DialTarget {
                            addr: ev.addr.clone(),
                            mtls_addr: ev.mtls_addr.clone(),
                        }));
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

/// Parse a `discovery.static_peers` entry into `(plain_addr, mtls_addr)`.
/// `wss://host:port` / `mtls://host:port` mark the endpoint as TLS-only.
fn parse_static_peer(raw: &str) -> (String, Option<String>) {
    for prefix in ["wss://", "mtls://"] {
        if let Some(rest) = raw.strip_prefix(prefix) {
            let addr = rest.trim_end_matches('/').to_string();
            return (addr.clone(), Some(addr));
        }
    }
    (raw.to_string(), None)
}

/// v0.6.9 地址漂移自愈：本节点对外声明的可拨监听地址（本机网卡 IP + 监听
/// 端口，与 /v1/status 的 listen_addr 同源）。拿不到本机 IP 时返回 None，
/// 对端回落 TCP 观测地址（与旧版行为一致）。
fn advertised_listen(cfg: &crate::config::Config) -> Option<String> {
    let ip = local_ip_address::local_ip().ok()?;
    Some(format!("{ip}:{}", cfg.listen_port))
}

/// v0.6.9 地址漂移自愈：为对端挑选可拨地址。对端在 hello 声明了合法
/// `ip:port`（非未指定地址）时优先采用并标记 announced——入站连接的临时源
/// 端口不可回拨，声明地址才能进目录并被其他节点拨通。
fn pick_peer_addr<'a>(announced: Option<&'a str>, observed: &'a str) -> (&'a str, bool) {
    if let Some(a) = announced {
        let ok = a
            .parse::<std::net::SocketAddr>()
            .map(|s| !s.ip().is_unspecified())
            .unwrap_or(false);
        if ok {
            return (a, true);
        }
    }
    (observed, false)
}

/// v0.6.9 地址漂移自愈：从收到的 `node_dir` 目录条目里，挑出「本节点已知、
/// 当前离线、且目录给出的地址与本地记录不同」的节点，生成重拨事件。
/// 这样只要集群里任一链路还活着（目录能到达），换了 IP 的节点就能被找回，
/// 不再依赖过期的静态地址或同网段广播。
///
/// 规则与 conn manager 的去重一致：只有 node_id 字典序较小的一方发起拨号，
/// 所以仅对 `self_id < peer_id` 的条目产事件；在线节点不动（其连接已权威）；
/// 被遗忘的节点不在快照里，天然跳过。地址变化后旧的 mtls 端点多半同 IP
/// 一起过期，故不带旧值——下次握手由对端 hello 重新广播（require 模式下
/// 宁可拒拨明文也不误连）。
fn dir_heal_events(
    nodes: &[serde_json::Value],
    self_id: &str,
    known: &[crate::peer::PeerView],
) -> Vec<DiscoveredPeer> {
    let mut out = Vec::new();
    for n in nodes {
        let (Some(id), Some(addr)) = (
            n.get("node_id").and_then(|v| v.as_str()),
            n.get("addr").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        if id == self_id || self_id >= id || addr.is_empty() {
            continue;
        }
        let Some(p) = known.iter().find(|x| x.node_id == id) else {
            continue; // 未知节点交给常规发现，不凭目录盲拨
        };
        if p.online {
            continue;
        }
        let mtls = n
            .get("mtls_addr")
            .and_then(|v| v.as_str())
            .map(String::from);
        if p.addr == addr && p.mtls_addr == mtls {
            continue; // 地址没变，旧拨号目标继续用
        }
        out.push(DiscoveredPeer {
            node_id: id.to_string(),
            name: n
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or(&p.name)
                .to_string(),
            addr: addr.to_string(),
            mtls_addr: mtls,
        });
    }
    out
}

/// Spawn a long-lived dialer that reconnects with exponential backoff.
fn spawn_dialer(
    state: SharedState,
    key: String,
    expected: Option<String>,
    target: Arc<RwLock<DialTarget>>,
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

            let target = target.read().await.clone();
            if let Err(e) = dial_once(state.clone(), target.clone(), expected.clone()).await {
                tracing::debug!(%key, target = %target.addr, error = %e, "conn: dial failed");
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
async fn dial_once(
    state: SharedState,
    target: DialTarget,
    expected: Option<String>,
) -> anyhow::Result<()> {
    let (out_tx, in_rx) = open_peer_ws(&state, &target).await?;
    session(state, in_rx, out_tx, expected, "out", target.addr.clone()).await;
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
///
/// Uses `wss://` (mutual TLS) when this node has mTLS enabled and the peer
/// advertised an mTLS endpoint; otherwise plain `ws://`. The TLS choice is
/// per-connection and opportunistic, so a mixed fleet keeps working.
async fn open_ws(
    state: &SharedState,
    target: &DialTarget,
    path: &str,
) -> anyhow::Result<(mpsc::Sender<Frame>, mpsc::Receiver<Frame>)> {
    let client = state.bridge().and_then(|b| b.tls_client_config());
    let use_tls = state.config.mtls.enabled && target.mtls_addr.is_some() && client.is_some();
    // v0.6 P1 `mtls.require`：强制模式 —— 明文目标拒拨（本地判定、零网络开销）。
    dial_allowed(&state.config.mtls, use_tls, target)
        .map_err(|reason| anyhow::anyhow!("conn: dial refused: {reason}"))?;

    let (socket, _resp) = if use_tls {
        let addr = target.mtls_addr.as_deref().unwrap_or_default();
        let url = format!("wss://{addr}{path}");
        let connector = Connector::Rustls(client.expect("checked above"));
        tokio_tungstenite::connect_async_tls_with_config(url.as_str(), None, false, Some(connector))
            .await?
    } else {
        let url = format!("ws://{}{path}", target.addr);
        tokio_tungstenite::connect_async(url.as_str()).await?
    };
    let (in_tx, in_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
    bridge_ws(socket, in_tx, out_rx);
    Ok((out_tx, in_rx))
}

/// Open a message channel to a peer (`/peer/ws`).
async fn open_peer_ws(
    state: &SharedState,
    target: &DialTarget,
) -> anyhow::Result<(mpsc::Sender<Frame>, mpsc::Receiver<Frame>)> {
    open_ws(state, target, "/peer/ws").await
}

/// Open a dedicated file-transfer channel to a peer (`/peer/file/ws`).
/// `addr` is the plain endpoint; `mtls_addr` (when present and mTLS is enabled)
/// upgrades the transfer to `wss://` so file bytes are never sent in the clear.
pub async fn open_file_ws(
    state: &SharedState,
    addr: &str,
    mtls_addr: Option<String>,
) -> anyhow::Result<(mpsc::Sender<Frame>, mpsc::Receiver<Frame>)> {
    let target = DialTarget {
        addr: addr.to_string(),
        mtls_addr,
    };
    open_ws(state, &target, "/peer/file/ws").await
}

/// 强制模式判定（纯函数，供 `open_ws` 与单测复用）：
///
/// - `require_enforced()` 为 false（默认）→ 机会式，任何目标都可拨；
/// - 强制模式下只允许 `wss://` 目标：要求对端广播了 mtls 端点**且**本机 TLS 客户端
///   配置已就绪（`use_tls=true`）。明文条目（无 mtls_addr）在拨号前被拒绝，
///   拒绝走 `debug!` 级别（对端广播 mtls_port 后即恢复可拨，不刷屏）。
fn dial_allowed(mtls: &MtlsConfig, use_tls: bool, target: &DialTarget) -> Result<(), String> {
    if !mtls.require_enforced() {
        return Ok(());
    }
    if use_tls {
        return Ok(());
    }
    if target.mtls_addr.is_none() {
        return Err(format!(
            "peer endpoint advertised no mTLS (mtls.require): {}",
            target.addr
        ));
    }
    Err("local TLS client not ready (mtls.require): no CA/client config yet".to_string())
}

// ---- session ----------------------------------------------------------------

/// v0.6.10 会话计数状态机（纯函数，便于单测）：握手成功后决定引用计数迁移。
/// 返回 `(begin 目标, 需要 end 的旧 node_id)`。
/// - 未计数过（`counted=false`）：首握手**无条件** begin——包括 expected_peer
///   预置 peer_id 的出站拨号场景（v0.6.9 漏计数的根因）。
/// - 已计数且同 node_id 重发 hello（改名重播）：不动计数。
/// - 已计数且切换 node_id：旧计数 end、新计数 begin（迁移）。
fn session_counting<'a>(
    counted: bool,
    prev: Option<&'a str>,
    hello_id: &'a str,
) -> (Option<&'a str>, Option<&'a str>) {
    if !counted {
        return (Some(hello_id), None);
    }
    match prev {
        Some(old) if old != hello_id => (Some(hello_id), Some(old)),
        _ => (None, None),
    }
}

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

    // Always speak first: send our hello (含 mTLS 状态：CA 指纹 + 是否参与；
    // 含准入要素：硬件指纹 + 准入令牌 HMAC，v0.6 P1）。
    let mut hello_name = state.display_name();
    let my_listen = advertised_listen(cfg);
    let mut hello = Hello::new(&cfg.node_id, &hello_name, &cfg.security.cluster_token)
        .with_mtls(state.bridge().and_then(|b| b.tls_ca_fp()), cfg.mtls.enabled)
        .with_listen(my_listen.clone());
    if let Some(b) = state.bridge() {
        b.seal_hello(&mut hello);
    }
    if send_text(&send, &hello).await.is_err() {
        return;
    }

    let mut peer_id: Option<String> = expected_peer;
    // v0.6.10 会话计数修复：本会话是否已计入 PeerTable 引用计数。此前出站
    // 拨号（expected 预置 peer_id）首握手会跳过 begin_session，导致
    // teardown 的 end_session 恒判"最后一条会话"而误杀同 node_id 的活会话。
    let mut counted = false;
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
                            &state, &text, &mut peer_id, &mut counted, direction, &remote_addr,
                            strict, &send,
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
                    let mut nh = Hello::new(&cfg.node_id, &hello_name, &cfg.security.cluster_token)
                        .with_mtls(state.bridge().and_then(|b| b.tls_ca_fp()), cfg.mtls.enabled)
                        .with_listen(my_listen.clone());
                    if let Some(b) = state.bridge() {
                        b.seal_hello(&mut nh);
                    }
                    if send_text(&send, &nh).await.is_err() {
                        break;
                    }
                    tracing::info!(name = %hello_name, "conn: re-announced hello after rename");
                }
            }
        }
    }

    if let Some(pid) = &peer_id {
        // v0.6.10：仅当本会话握手成功并计过数时才做 end_session/离线清理。
        // 未计数的连接（拨号后握手从未完成，如超时断链、被拒）不得误杀同
        // node_id 的活会话；活会话的离线判定由其自身 teardown 负责。
        if !counted {
            tracing::debug!(
                node_id = %pid,
                direction,
                "conn: session closed without completed handshake (not counted)"
            );
        } else if state.peers.end_session(pid) {
            // v0.6.9 ABA 保护：仅当该节点最后一条会话结束才判离线/注销。
            state.peers.mark_offline(pid);
            state.router.unregister_peer(pid);
            tracing::info!(node_id = %pid, direction, "conn: peer offline");
        } else {
            tracing::info!(
                node_id = %pid,
                direction,
                "conn: peer session closed (other live sessions remain)"
            );
        }
    }
    let _ = send.send(Frame::Close).await;
}

/// Handle a decoded text frame. Returns `false` if the connection must close.
#[allow(clippy::too_many_arguments)]
async fn handle_text(
    state: &SharedState,
    text: &str,
    peer_id: &mut Option<String>,
    counted: &mut bool,
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
                state.router.notify(
                    "audit_alert",
                    serde_json::json!({
                        "kind": "handshake_rejected",
                        "reason": "protocol_version",
                        "node_id": hello.node_id,
                        "addr": remote_addr,
                    }),
                );
                return false;
            }
            if strict && !hello.verify(&cfg.security.cluster_token) {
                tracing::warn!(node_id = %hello.node_id, "conn: handshake rejected (bad cluster token)");
                state.router.notify(
                    "audit_alert",
                    serde_json::json!({
                        "kind": "handshake_rejected",
                        "reason": "cluster_token",
                        "node_id": hello.node_id,
                        "addr": remote_addr,
                    }),
                );
                return false;
            }
            // v0.6 P1 新节点准入：bootstrap token + 硬件指纹双验证。必须在
            // 节点登记/CA 分发之前执行——未通过闸门的机器拿不到集群 CA。
            let gate = match state.bridge() {
                Some(b) => b.admission_gate(&hello),
                None => Ok(()),
            };
            if let Err(reason) = gate {
                tracing::warn!(
                    node_id = %hello.node_id,
                    addr = remote_addr,
                    reason = reason,
                    "conn: handshake rejected (admission)"
                );
                state.router.notify(
                    "audit_alert",
                    serde_json::json!({
                        "kind": "handshake_rejected",
                        "reason": "admission",
                        "node_id": hello.node_id,
                        "addr": remote_addr,
                    }),
                );
                return false;
            }
            let changed = peer_id.as_deref() != Some(hello.node_id.as_str());
            // v0.6.10 会话计数修复：首握手无条件计数——此前 expected_peer 预置
            // peer_id 会让出站拨号会话漏计数（match Some(_) => {} 跳过
            // begin_session），teardown 的 end_session 恒返回 true 而误杀同
            // node_id 的其它活会话。同连接切换 node_id 时迁移计数。
            let (begin_id, end_old) =
                session_counting(*counted, peer_id.as_deref(), &hello.node_id);
            if let Some(old) = end_old {
                state.peers.end_session(old);
            }
            if let Some(id) = begin_id {
                state.peers.begin_session(id);
                *counted = true;
            }
            *peer_id = Some(hello.node_id.clone());
            // v0.6.9 地址漂移自愈：优先采用对端在 hello 中主动声明的可拨监听
            // 地址；入站连接的临时源端口不可回拨，老版本无声明时维持原逻辑。
            let (use_addr, announced) = pick_peer_addr(hello.listen.as_deref(), remote_addr);
            state
                .peers
                .mark_online(&hello.node_id, &hello.name, use_addr, direction, announced);
            if changed {
                tracing::info!(
                    peer = %hello.name,
                    node_id = %hello.node_id,
                    addr = use_addr,
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
                name: hello.name.clone(),
                addr: advertised,
                tx: send.clone(),
            });
            // Tell the peer which agents live on this node.
            if let Some(env) = state.router.agents_update_envelope(&hello.node_id) {
                let _ = send_text(send, &env).await;
            }
            // v0.7 第二波：新 peer 注册后立刻推一份节点目录（spine 的目录
            // 含其它叶子；叶子收到即可在 /v1/peers 看到非直连节点）。
            // v0.6.9 地址漂移自愈：对端上线/换址时改为向**所有**直连 peer
            // 广播——否则离线节点只能等 relay 周期推送（relay 关闭时永远
            // 等不到），换 IP 的节点无法经邻居目录被找回。新 peer 也在
            // registry 里，广播天然覆盖它自己。
            if changed {
                state.router.sync_push_dir();
            } else {
                state.router.push_dir_to(&hello.node_id);
            }
            // v0.6 P1 Lease：新对端注册后补推一次当前负载快照——lease 任务的
            // 每 ticker 首拍常发生在对端注册之前（阈值内静默期不再重发），
            // 若不补推，新入网节点要等下一次阈值跨越才可见负载。
            if cfg.lease.enabled {
                if let Some(cpu) = state.bridge().and_then(|b| b.advertised_cpu()) {
                    let env = Envelope::lease(&cfg.node_id, Some(cpu));
                    let _ = send_text(send, &env).await;
                }
            }
            // v0.6 P1 CA 自动分发：经已认证（strict）链路，把集群 CA 推给
            // 「已开 mTLS 但指纹不符/缺失」的对端（同一信任域内传播/引导）。
            if strict && cfg.mtls.ca_sync {
                if let Some(b) = state.bridge() {
                    if let (Some(fp), Some((cert, key))) = (b.tls_ca_fp(), b.tls_ca_pems()) {
                        if hello.mtls_enabled && hello.ca_fp.as_deref() != Some(fp.as_str()) {
                            let frame = CaSync::new(&cfg.node_id, &cert, &key, &fp);
                            if send_text(send, &frame).await.is_ok() {
                                tracing::info!(
                                    peer = %hello.name,
                                    ca_fp = %fp,
                                    "conn: pushed cluster CA to peer"
                                );
                            }
                        }
                    }
                }
            }
            true
        }

        "ca_sync" => {
            // 仅接受已认证（strict）对端推送的集群 CA（CA 自动分发）。
            if !strict || !cfg.mtls.ca_sync {
                tracing::debug!("conn: ignoring ca_sync (disabled or non-strict mode)");
                return true;
            }
            let frame: CaSync = match serde_json::from_value(value) {
                Ok(f) => f,
                Err(_) => return true,
            };
            let Some(b) = state.bridge() else { return true };
            if !b.tls_enabled() {
                return true;
            }
            // 用 PEM 实算指纹，不信任帧内声明（经能力桥计算）。
            match b.ca_fingerprint(&frame.ca_cert) {
                Some(fp) if fp == frame.fingerprint => {}
                Some(actual) => {
                    tracing::warn!(
                        declared = %frame.fingerprint,
                        actual = %actual,
                        "conn: ca_sync fingerprint mismatch; ignored"
                    );
                    return true;
                }
                None => {
                    tracing::warn!("conn: ca_sync cert unparseable; ignored");
                    return true;
                }
            }
            match b.adopt_ca(&frame.ca_cert, &frame.ca_key) {
                CaAdopt::Adopted => {
                    tracing::info!(
                        peer = %frame.node_id,
                        ca_fp = %frame.fingerprint,
                        "conn: adopted cluster CA (mTLS now ready)"
                    );
                    // 重新广播 hello（携带新指纹），避免对端重复推送。
                    let nh = Hello::new(
                        &cfg.node_id,
                        &state.display_name(),
                        &cfg.security.cluster_token,
                    )
                    .with_mtls(state.bridge().and_then(|b| b.tls_ca_fp()), cfg.mtls.enabled)
                    .with_listen(advertised_listen(cfg));
                    let _ = send_text(send, &nh).await;
                }
                CaAdopt::Same => {}
                CaAdopt::Conflict { local, remote } => {
                    tracing::warn!(
                        local = %local,
                        remote = %remote,
                        "conn: CA conflict; kept local CA (refused to adopt)"
                    );
                }
                CaAdopt::Unavailable => {}
                CaAdopt::Failed(e) => {
                    tracing::warn!(error = %e, "conn: failed to adopt cluster CA")
                }
            }
            true
        }

        // v0.8 HA 复制帧（ctrl/ha）：只接受已握手对端（peer_id 已知）、
        // strict 模式验签通过的控制面帧。旧节点不认识该 action -> 忽略。
        // 验签/防冒名留在传输层，Raft 状态机交互全部
        // 经 MeshBridge 钩子反向调用闭源实现。
        "ha" => {
            let Some(pid) = peer_id.clone() else {
                return true;
            };
            let Some(b) = state.bridge() else {
                tracing::debug!(from = %pid, "conn: ha frame ignored (mesh off)");
                return true;
            };
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) {
                tracing::warn!(from = %env.from, "conn: ha frame rejected (control signature)");
                return true;
            }
            if env.from != pid {
                // 帧里的 from 必须与握手对端一致（防冒名）
                tracing::warn!(from = %env.from, peer = %pid, "conn: ha frame from mismatch");
                return true;
            }
            for (to, m) in b.ha_on_frame(&env) {
                if to == env.from {
                    let resp = state.router.ha_envelope(&to, serde_json::json!({"msg": m}));
                    let _ = send_text(send, &resp).await;
                } else {
                    // 非请求方：交给路由注册表投递（当前协议里极少发生）
                    if let Some(sink) = state.router.peer_sink(&to) {
                        let env = state.router.ha_envelope(&to, serde_json::json!({"msg": m}));
                        if let Ok(text) = serde_json::to_string(&env) {
                            let _ = sink.tx.send(Frame::Text(text)).await;
                        }
                    }
                }
            }
            true
        }

        // v0.8 M4：follower → leader 的复制 KV 请求（OIDC state/session）。
        // 执行放到 blocking 池：多数派 ack 也走本连接，同步等提交会让
        // 2 节点/最后一票场景必然超时。KV 执行经能力桥。
        "ha_kv" => {
            let Some(pid) = peer_id.clone() else {
                return true;
            };
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) || env.from != pid {
                return true;
            }
            let req = env
                .payload
                .get("req")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if req.is_empty() {
                return true;
            }
            let from = env.from.clone();
            let op = env.payload.get("op").cloned();
            let router = state.router.clone();
            let bridge = state.bridge().cloned();
            let tx = send.clone();
            tokio::task::spawn_blocking(move || {
                let resp = match bridge.as_ref() {
                    Some(b) => b.ha_kv_execute(&req, op),
                    None => serde_json::json!({
                        "req": req,
                        "error": "ha disabled",
                    }),
                };
                let env = router.signed_ctrl_envelope(&from, "ha_kv_resp", resp);
                if let Ok(text) = serde_json::to_string(&env) {
                    let _ = tx.blocking_send(Frame::Text(text));
                }
            });
            true
        }

        // v0.8 M4：复制 KV 应答（回填等待表；超时的请求已被移除，丢弃即可）。
        // 应答 JSON 原样交给能力桥，解析/回填在闭源发行版侧。
        "ha_kv_resp" => {
            let Some(pid) = peer_id.clone() else {
                return true;
            };
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) || env.from != pid {
                return true;
            }
            if let Some(b) = state.bridge() {
                b.ha_kv_deliver(&env.payload);
            }
            true
        }

        // v0.8 M4：follower 转发的审计事件（leader 串行追加到复制链）。
        "ha_audit" => {
            let Some(pid) = peer_id.clone() else {
                return true;
            };
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) || env.from != pid {
                return true;
            }
            if !state.bridge().is_some_and(|b| b.ha_is_leader()) {
                return true;
            }
            let Some(event) = env.payload.get("event").and_then(|v| v.as_str()) else {
                return true;
            };
            let actor = env
                .payload
                .get("actor")
                .and_then(|v| v.as_str())
                .unwrap_or("peer")
                .to_string();
            let detail = env
                .payload
                .get("detail")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let event = event.to_string();
            let router = state.router.clone();
            tokio::task::spawn_blocking(move || {
                router.audit_event(&event, &actor, detail);
            });
            true
        }

        // v0.8 M4：follower 转发的任务事件（leader 注入事件总线，由引擎收敛）。
        "ha_event" => {
            let Some(pid) = peer_id.clone() else {
                return true;
            };
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) || env.from != pid {
                return true;
            }
            if !state.bridge().is_some_and(|b| b.ha_is_leader()) {
                return true;
            }
            let Some(inner) = env.payload.get("env") else {
                return true;
            };
            let Ok(inner) = serde_json::from_value::<Envelope>(inner.clone()) else {
                return true;
            };
            state.router.inject_event(inner);
            true
        }

        "heartbeat" => {
            if let Some(pid) = peer_id {
                state.peers.touch(pid);
            }
            true
        }

        "lease" => {
            // v0.6 P1 Lease 差异上报：full（带 cpu）/裸心跳（只刷活性）。
            // 必须先通过 hello 握手（peer_id 已知），裸帧不创建台账条目。
            let Some(pid) = peer_id else { return true };
            let cpu = value
                .get("payload")
                .and_then(|p| p.get("cpu"))
                .and_then(|c| c.as_u64());
            if cpu.unwrap_or(0) > 100 {
                tracing::debug!(from = %pid, "conn: lease cpu out of range; ignored");
                return true;
            }
            let ts = value.get("ts").and_then(|v| v.as_str()).unwrap_or("");
            state.peers.set_load(pid, cpu.map(|c| c as u32), ts);
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
            // v0.7 第二波：控制面帧验签（带 sig 必验；control_auth=true 时
            // 无 sig 直接拒）。拒绝只影响 agent 表同步，不影响业务消息。
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) {
                tracing::warn!(from = %env.from, "conn: agents_update rejected (control signature)");
                state.router.notify(
                    "audit_alert",
                    serde_json::json!({
                        "kind": "control_frame_rejected",
                        "action": "agents_update",
                        "from": env.from,
                        "msg_id": env.msg_id,
                    }),
                );
                return true;
            }
            let from = env.from.as_str();
            let agents: Option<Vec<crate::router::AgentRecord>> = env
                .payload
                .get("agents")
                .and_then(|a| serde_json::from_value(a.clone()).ok());
            let tags: Vec<String> = env
                .payload
                .get("tags")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            if let (Some(agents), false) = (agents, from.is_empty()) {
                state.router.on_agents_update_with_tags(from, agents, tags);
            }
            // v0.7 spine/relay：spine 把收到的 agent 表扩散给其它直连 peer
            // （保留原 from）——叶子据此看到非直连节点的 agent，引擎才能跨
            // 叶子调度。hops 上限防环。
            if cfg.relay.enabled {
                state.router.relay_control(&env, peer_id.as_deref()).await;
            }
            true
        }

        // v0.7 §6.2：对端优雅下线（SIGTERM 前广播）。立即标记不可调度，
        // 在途任务跳过 reclaim 宽限期换机；多级拓扑经 spine 继续扩散。
        "node_drain" => {
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            if !state.router.verify_control(&env) {
                tracing::warn!(from = %env.from, "conn: node_drain rejected (control signature)");
                state.router.notify(
                    "audit_alert",
                    serde_json::json!({
                        "kind": "control_frame_rejected",
                        "action": "node_drain",
                        "from": env.from,
                        "msg_id": env.msg_id,
                    }),
                );
                return true;
            }
            let target = env
                .payload
                .get("node_id")
                .and_then(|v| v.as_str())
                .unwrap_or(env.from.as_str())
                .to_string();
            // v0.6.9：若该节点还有其他活跃会话（旧进程下线通告晚于新进程
            // 重连），忽略这次 drain，避免把新实例误标离线。
            if state.peers.session_count(&target) <= 1 {
                state.router.on_node_drain(&target);
            } else {
                tracing::debug!(node_id = %target, "conn: drain ignored (other live sessions remain)");
            }
            if cfg.relay.enabled {
                state.router.relay_control(&env, peer_id.as_deref()).await;
            }
            true
        }

        // v0.7 第二波：节点目录中继（spine 周期推送 / 新 peer 注册即推）。
        "node_dir" => {
            let Ok(env) = serde_json::from_value::<Envelope>(value.clone()) else {
                return true;
            };
            tracing::debug!(from = %env.from, "conn: node_dir frame in");
            if state.router.on_node_dir(&env, peer_id.as_deref()) {
                // v0.6.9 地址漂移自愈：目录里"已知但离线、且地址已变"的节点，
                // 立刻把新地址喂给拨号器——集群里任一链路活着即可找回换 IP 的节点。
                if let Some(nodes) = env.payload.get("nodes").and_then(|v| v.as_array()) {
                    let snap = state.peers.snapshot();
                    let evs = dir_heal_events(nodes, &cfg.node_id, &snap);
                    if !evs.is_empty() {
                        tracing::info!(
                            count = evs.len(),
                            peers = %evs
                                .iter()
                                .map(|e| format!("{}@{}", e.node_id, e.addr))
                                .collect::<Vec<_>>()
                                .join(","),
                            "conn: dir-heal re-dial events"
                        );
                    }
                    for ev in evs {
                        let _ = state.peer_tx.try_send(ev);
                    }
                }
                if cfg.relay.enabled {
                    state.router.relay_control(&env, peer_id.as_deref()).await;
                }
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
            // v0.7 spine/relay 三态判定：定向到第三方 → 中继转发；
            // 广播且本节点是 spine → 中继扩散 + 本地投递；否则本地处理。
            let is_broadcast = env.to == "*" || env.to.is_empty();
            let addressed_to_us = env.to == cfg.node_id || is_broadcast;
            if !addressed_to_us {
                if !cfg.relay.enabled {
                    tracing::debug!(
                        msg_id = %env.msg_id,
                        to = %env.to,
                        "conn: frame not addressed to us, dropped (relay disabled)"
                    );
                    return true;
                }
                // 中继：逐跳 ACK（叶片等的是 spine 的收件确认），本跳负责
                // 到下一跳的重试（handle_transit 内部 register_pending）。
                let _ = state
                    .router
                    .handle_transit(&mut env, peer_id.as_deref())
                    .await;
                let ack = Envelope::ack(&cfg.node_id, &env.msg_id);
                let _ = send_text(send, &ack).await;
                return true;
            }
            if is_broadcast && cfg.relay.enabled {
                // 广播中继：扩散给其它直连 peer（hops 上限内）+ 本地投递。
                let _ = state
                    .router
                    .handle_transit(&mut env, peer_id.as_deref())
                    .await;
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

#[cfg(test)]
mod tests {
    use super::{
        dial_allowed, dir_heal_events, parse_static_peer, pick_peer_addr, session_counting,
        DialTarget,
    };
    use crate::config::MtlsConfig;
    use crate::peer::PeerTable;
    use crate::peer::PeerView;

    /// v0.6.10 事故回归：出站拨号（expected 预置 peer_id）首握手必须计数，
    /// 否则 teardown 的 end_session 恒判"最后一条"而误杀活会话。
    #[test]
    fn session_counting_counts_preset_expected_dialer() {
        // 预置 expected 的首握手：无条件 begin（v0.6.9 漏计数的根因）。
        assert_eq!(session_counting(false, Some("a"), "a"), (Some("a"), None));
        // 入站首握手（无预置）同样 begin。
        assert_eq!(session_counting(false, None, "a"), (Some("a"), None));
        // 已计数后改名重播 hello：不动计数。
        assert_eq!(session_counting(true, Some("a"), "a"), (None, None));
        // 已计数后同连接切换身份：旧 end、新 begin（迁移）。
        assert_eq!(
            session_counting(true, Some("a"), "b"),
            (Some("b"), Some("a"))
        );
    }

    /// v0.6.10 回归：拨号会话计入后，第二条会话结束不再误杀第一条。
    #[test]
    fn counted_sessions_survive_sibling_teardown() {
        let t = PeerTable::new();
        // 模拟握手成功：两条会话（out 拨号 + in 入站）都 begin。
        t.mark_online("a", "A", "10.0.0.1:8765", "out", true);
        t.begin_session("a");
        t.begin_session("a");
        // 其一 teardown：end_session 返回 false → 不应 mark_offline/unregister。
        assert!(!t.end_session("a"));
        assert!(t.is_online("a"), "活会话仍在，不应被误标离线");
        assert!(t.end_session("a"), "最后一条会话结束应判离线");
        t.mark_offline("a");
        assert!(!t.is_online("a"));
    }

    /// v0.6.9：合法声明地址优先；未指定地址/非法串回落到观测地址。
    #[test]
    fn pick_peer_addr_prefers_valid_announcement() {
        assert_eq!(
            pick_peer_addr(Some("10.0.0.9:8765"), "127.0.0.1:44000"),
            ("10.0.0.9:8765", true)
        );
        assert_eq!(
            pick_peer_addr(None, "127.0.0.1:44000"),
            ("127.0.0.1:44000", false)
        );
        assert_eq!(
            pick_peer_addr(Some("0.0.0.0:8765"), "1.2.3.4:5"),
            ("1.2.3.4:5", false)
        );
        assert_eq!(
            pick_peer_addr(Some("not-an-addr"), "1.2.3.4:5"),
            ("1.2.3.4:5", false)
        );
    }

    /// v0.6.9 ABA 保护：同节点多条会话，仅最后一条结束时判离线。
    #[test]
    fn session_refcount_only_last_one_offlines() {
        let t = PeerTable::new();
        t.mark_online("b", "B", "10.0.0.2:8765", "in", true);
        t.begin_session("b");
        t.begin_session("b");
        assert!(!t.end_session("b"), "旧会话结束但新会话仍在，不应判离线");
        assert!(t.is_online("b"));
        assert!(t.end_session("b"), "最后一条会话结束应判离线");
        assert_eq!(t.session_count("b"), 0);
    }

    fn view(id: &str, addr: &str, online: bool) -> PeerView {
        PeerView {
            node_id: id.to_string(),
            name: id.to_string(),
            addr: addr.to_string(),
            mtls_addr: None,
            tags: vec![],
            online,
            last_seen_seconds_ago: 0,
            direction: String::new(),
            load: None,
            relayed: None,
            via: None,
        }
    }

    /// v0.6.9 地址漂移自愈：仅"已知+离线+地址已变"触发重拨；在线/未变/未知/
    /// 非本方拨号（self_id 较大）一律跳过。
    #[test]
    fn dir_heal_only_for_offline_changed_addrs() {
        let nodes = serde_json::json!([
            { "node_id": "bbb", "name": "B", "addr": "10.0.0.9:8765" }, // 离线且换址 → 自愈
            { "node_id": "ccc", "name": "C", "addr": "10.0.0.3:8765" }, // 地址未变 → 跳过
            { "node_id": "ddd", "name": "D", "addr": "10.0.0.4:8765" }, // 在线 → 跳过
            { "node_id": "eee", "name": "E", "addr": "10.0.0.5:8765" }, // 未知 → 跳过
            { "node_id": "aaa", "name": "A", "addr": "10.0.0.1:8765" }, // 自身 → 跳过
            { "node_id": "fff", "name": "F", "addr": "" },              // 空址 → 跳过
        ]);
        let known = vec![
            view("bbb", "10.0.0.8:8765", false),
            view("ccc", "10.0.0.3:8765", false),
            view("ddd", "10.0.0.4:8765", true),
            view("aaa", "10.0.0.1:8765", false),
        ];
        let evs = dir_heal_events(nodes.as_array().unwrap(), "aaa", &known);
        assert_eq!(evs.len(), 1, "只应产出 bbb：{evs:?}");
        assert_eq!(evs[0].node_id, "bbb");
        assert_eq!(evs[0].addr, "10.0.0.9:8765");
        assert_eq!(evs[0].name, "B");
        assert_eq!(evs[0].mtls_addr, None);
    }

    /// 目录条目若带 mtls 端点（未来扩展），自愈时原样透传。
    #[test]
    fn dir_heal_passes_mtls_when_advertised() {
        let nodes = serde_json::json!([
            { "node_id": "bbb", "addr": "10.0.0.9:8765", "mtls_addr": "10.0.0.9:8766" },
        ]);
        let evs = dir_heal_events(nodes.as_array().unwrap(), "aaa", &[]);
        assert!(evs.is_empty(), "未知节点不凭目录盲拨");
        let evs = dir_heal_events(
            nodes.as_array().unwrap(),
            "aaa",
            &[view("bbb", "10.0.0.8:8765", false)],
        );
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].mtls_addr.as_deref(), Some("10.0.0.9:8766"));
    }

    #[test]
    fn static_peer_scheme_marks_mtls() {
        // 明文：addr 原样，无 mTLS 端点。
        assert_eq!(
            parse_static_peer("10.0.0.2:8765"),
            ("10.0.0.2:8765".to_string(), None)
        );
        // wss:// / mtls://：标记为节点间 TLS 端点。
        assert_eq!(
            parse_static_peer("wss://10.0.0.2:8766"),
            (
                "10.0.0.2:8766".to_string(),
                Some("10.0.0.2:8766".to_string())
            )
        );
        assert_eq!(
            parse_static_peer("mtls://10.0.0.2:8766/"),
            (
                "10.0.0.2:8766".to_string(),
                Some("10.0.0.2:8766".to_string())
            )
        );
    }

    /// `mtls.require` 强制模式：明文目标拒拨（机会式默认全放行）。
    #[test]
    fn require_mode_dial_policy() {
        let plain = DialTarget {
            addr: "10.0.0.2:8765".into(),
            mtls_addr: None,
        };
        let tls = DialTarget {
            addr: "10.0.0.2:8765".into(),
            mtls_addr: Some("10.0.0.2:8766".into()),
        };

        // 默认（机会式）：全部放行。
        assert!(dial_allowed(&MtlsConfig::default(), false, &plain).is_ok());
        assert!(dial_allowed(&MtlsConfig::default(), false, &tls).is_ok());

        // 强制模式 + 对端没广播 mtls 端点 → 拒（fail-closed，无需碰网络）。
        let enforced = MtlsConfig {
            enabled: true,
            require: true,
            ..MtlsConfig::default()
        };
        let err = dial_allowed(&enforced, false, &plain).unwrap_err();
        assert!(err.contains("advertised no mTLS"), "{err}");

        // 强制模式 + 广播了 mtls 端点但本机 TLS 客户端未就绪（如成员未领 CA）
        // → 拒，且提示原因不同。
        let err = dial_allowed(&enforced, false, &tls).unwrap_err();
        assert!(err.contains("not ready"), "{err}");

        // 强制模式 + 双端就绪 → 放行（真正走 wss://）。
        assert!(dial_allowed(&enforced, true, &tls).is_ok());

        // enabled 但未开 require：保持机会式，不拦明文。
        let opportunistic = MtlsConfig {
            enabled: true,
            ..MtlsConfig::default()
        };
        assert!(dial_allowed(&opportunistic, false, &plain).is_ok());
    }
}
