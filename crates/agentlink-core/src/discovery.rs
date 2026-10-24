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
