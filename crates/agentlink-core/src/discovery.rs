// SPDX-License-Identifier: Apache-2.0
//! UDP broadcast based node discovery (primary method, per design doc §6).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::watch;

use crate::api::SharedState;
use crate::config::Config;
use crate::peer::DiscoveredPeer;
use crate::proto::DiscoveryMessage;

const BROADCAST_ADDR: &str = "255.255.255.255";

/// Run the discovery loop: periodically announce ourselves and listen for peers.
pub async fn run(cfg: Config, state: SharedState, mut shutdown: watch::Receiver<bool>) {
    let port = cfg.discovery.broadcast_port;
    let socket = match make_socket(port) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::error!(error = %e, port, "discovery: failed to open UDP socket; continuing without discovery");
            return;
        }
    };

    let interval_secs = cfg.discovery.announce_interval_seconds.max(1);
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    let mut buf = vec![0u8; 4096];

    send(&socket, &state, "announce").await;
    tracing::info!(port, "discovery: listening via UDP broadcast");

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                send(&socket, &state, "bye").await;
                tracing::info!("discovery: announced bye");
                break;
            }
            _ = ticker.tick() => {
                send(&socket, &state, "announce").await;
            }
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((n, src)) => handle_datagram(&buf[..n], src, &cfg, &state),
                    Err(e) => tracing::warn!(error = %e, "discovery: recv error"),
                }
            }
        }
    }
}

/// Create a UDP socket with `SO_REUSEADDR` (so several local nodes can share the
/// port) and `SO_BROADCAST` enabled.
fn make_socket(port: u16) -> anyhow::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    let std_socket: std::net::UdpSocket = socket.into();
    Ok(UdpSocket::from_std(std_socket)?)
}

async fn send(socket: &UdpSocket, state: &SharedState, action: &str) {
    let cfg = &state.config;
    // 仅当本节点开启 mTLS **且已就绪**（持有 CA/TLS 配置）时才广播 TLS 端口；
    // 成员节点在领取 CA 之前不广播，对端按明文 ws:// 连接（机会式降级）。
    let mtls_port = (cfg.mtls.enabled && state.bridge().is_some_and(|b| b.tls_ready()))
        .then(|| cfg.mtls_listen_port());
    let msg = DiscoveryMessage::new(
        action,
        &cfg.node_id,
        &state.display_name(),
        cfg.listen_port,
        mtls_port,
        &state.tags(),
    );
    let data = match serde_json::to_vec(&msg) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "discovery: failed to encode message");
            return;
        }
    };
    let dest = format!("{BROADCAST_ADDR}:{}", cfg.discovery.broadcast_port);
    if let Err(e) = socket.send_to(&data, &dest).await {
        tracing::debug!(error = %e, dest, "discovery: broadcast send failed");
    }
}

/// v0.6.10 陈旧 bye 判定（纯函数，便于单测）：只要该节点还有活会话
/// （引用计数 >0 或 router 仍持有 sink），bye 视为陈旧/误发，忽略。
fn bye_is_stale(session_count: u32, has_live_sink: bool) -> bool {
    session_count > 0 || has_live_sink
}

fn handle_datagram(data: &[u8], src: SocketAddr, cfg: &Config, state: &SharedState) {
    let msg: DiscoveryMessage = match serde_json::from_slice(data) {
        Ok(m) => m,
        Err(_) => return,
    };
    if msg.node_id == cfg.node_id {
        return; // our own broadcast
    }

    // The datagram source address is authoritative; the advertised IP is not
    // transmitted at all on purpose (multi-NIC safe).
    let addr = format!("{}:{}", src.ip(), msg.port);
    // mTLS 端点（若对端广播）：同源 IP + TLS 端口。
    let mtls_addr = msg.mtls_port.map(|p| format!("{}:{}", src.ip(), p));

    if msg.action == "bye" {
        // v0.6.10 陈旧 bye 守卫：bye 只在节点 shutdown 时广播一次，可能迟到
        // 或误发（对端重启后旧进程的关机广播、同网段克隆身份）。只要本地仍有
        // 该节点的活会话（引用计数 >0 或 router 仍持有 sink），就不标离线——
        // 否则一次陈旧 bye 会把活节点永久钉成"离线"：mark_offline 之后没有
        // 任何路径把 online 拨回 true（心跳只 touch last_seen），直到下次握手。
        // 真离线由会话自身的 heartbeat timeout teardown 负责。
        if bye_is_stale(
            state.peers.session_count(&msg.node_id),
            state.router.has_peer(&msg.node_id),
        ) {
            tracing::info!(
                peer = %msg.name,
                node_id = %msg.node_id,
                "discovery: stale bye ignored (live session present)"
            );
            return;
        }
        state.peers.mark_offline(&msg.node_id);
        tracing::info!(peer = %msg.name, node_id = %msg.node_id, "discovery: peer announced bye");
        // v0.6 P2 wake-kit 收敛：bye 不走 unregister_peer（registry 仍在
        // 或从未入册），here 单独补 OnNodeOffline（reason=bye）。
        state.router.notify(
            "node_offline",
            serde_json::json!({ "peer_id": msg.node_id, "reason": "bye" }),
        );
        return;
    }

    let is_new =
        state
            .peers
            .upsert_discovered(&msg.node_id, &msg.name, &addr, mtls_addr.clone(), msg.tags);
    if is_new {
        tracing::info!(peer = %msg.name, node_id = %msg.node_id, addr, "discovery: new peer");
    }

    // Notify the connection manager (best effort, non-blocking).
    let ev = DiscoveredPeer {
        node_id: msg.node_id,
        name: msg.name,
        addr,
        mtls_addr,
    };
    if let Err(e) = state.peer_tx.try_send(ev) {
        tracing::trace!(error = %e, "discovery: peer event dropped (channel full)");
    }
}

#[cfg(test)]
mod tests {
    use super::bye_is_stale;

    /// v0.6.10 事故回归：活会话存在时 bye 必须判陈旧（否则把活节点钉成离线）。
    #[test]
    fn stale_bye_ignored_while_session_alive() {
        assert!(bye_is_stale(1, false), "引用计数 >0 应忽略 bye");
        assert!(bye_is_stale(0, true), "registry 仍有 sink 应忽略 bye");
        assert!(bye_is_stale(2, true));
    }

    /// 无活会话时 bye 正常生效（真关机走这里标离线）。
    #[test]
    fn real_bye_applies_when_no_live_session() {
        assert!(!bye_is_stale(0, false));
    }
}
