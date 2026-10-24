// SPDX-License-Identifier: Apache-2.0
//! In-memory peer registry.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

use serde::Serialize;

/// A peer's last known state.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: String,
    pub name: String,
    /// `host:port` of the peer's WebSocket endpoint.
    pub addr: String,
    /// `host:port` of the peer's mTLS (node-to-node TLS) WebSocket endpoint,
    /// advertised only when the peer enabled mTLS (v0.6 P1).
    pub mtls_addr: Option<String>,
    /// Machine-level capability labels advertised by the peer.
    pub tags: Vec<String>,
    pub online: bool,
    pub last_seen: Instant,
    /// `in` or `out`: who dialled.
    pub direction: String,
    /// 节点负载（v0.6 P1 Lease 差异上报）。`None` = 对端未启用 lease 或尚未
    /// 发过 Full Status —— 该字段是纯加性观察位，缺省不存在于任何帧里。
    pub load: Option<PeerLoad>,
}

/// 节点负载快照（v0.6 P1 Lease）。`cpu` 为 0..=100 的整数利用率；
/// 采样不可用时对端只发裸心跳（本字段保持 `None`）。
#[derive(Debug, Clone, Serialize)]
pub struct PeerLoad {
    pub cpu: u32,
    /// 负载采样时刻（RFC3339，对端侧时钟）。
    pub ts: String,
}

/// Serializable view returned by `/v1/peers`.
#[derive(Debug, Clone, Serialize)]
pub struct PeerView {
    pub node_id: String,
    pub name: String,
    pub addr: String,
    pub mtls_addr: Option<String>,
    pub tags: Vec<String>,
    pub online: bool,
    pub last_seen_seconds_ago: u64,
    pub direction: String,
    /// 纯加性：缺省 `None` 且**跳过序列化**——默认态 `/v1/peers` JSON 与
    /// 0.5.9 逐字节一致（零行为变化门）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load: Option<PeerLoad>,
    /// v0.7 第二波：该条目是**经中继学到的节点目录**（非直连）。缺省
    /// `None` 且跳过序列化——直连条目字节不变。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relayed: Option<bool>,
    /// 该中继条目的来源（spine node_id，多级时是最近一跳的中继）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<String>,
}

/// A peer learned from discovery, forwarded to the connection manager.
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    pub node_id: String,
    pub name: String,
    /// `host:port` of the peer's WebSocket endpoint.
    pub addr: String,
    /// `host:port` of the peer's mTLS endpoint when advertised (else `None`).
    pub mtls_addr: Option<String>,
}

/// Thread-safe peer table. Critical sections are short and never await.
#[derive(Default)]
pub struct PeerTable {
    inner: RwLock<HashMap<String, PeerInfo>>,
    /// Node ids the user explicitly "forgot". Re-discovery/re-dial of these
    /// is suppressed until the node contacts us with a successful handshake
    /// (a live handshake means it really is back) or the user un-forgets.
    forgotten: RwLock<std::collections::HashSet<String>>,
}

impl PeerTable {
    pub fn new() -> Self {
        PeerTable::default()
    }

    /// Load the persisted suppress list (called once at startup).
    pub fn set_suppressed(&self, ids: impl IntoIterator<Item = String>) {
        let mut s = self.forgotten.write().unwrap();
        s.extend(ids);
    }

    pub fn suppressed(&self) -> Vec<String> {
        self.forgotten.read().unwrap().iter().cloned().collect()
    }

    /// Remove a peer from the table and remember to ignore future
    /// discoveries of it (user pressed "forget" in the console).
    pub fn forget(&self, node_id: &str) -> bool {
        let existed = self.inner.write().unwrap().remove(node_id).is_some();
        self.forgotten.write().unwrap().insert(node_id.to_string());
        existed
    }

    pub fn unforget(&self, node_id: &str) {
        self.forgotten.write().unwrap().remove(node_id);
    }

    fn is_forgotten(&self, node_id: &str) -> bool {
        self.forgotten.read().unwrap().contains(node_id)
    }

    /// Record a peer learned from discovery. Returns `true` if it is new.
    pub fn upsert_discovered(
        &self,
        node_id: &str,
        name: &str,
        addr: &str,
        mtls_addr: Option<String>,
        tags: Vec<String>,
    ) -> bool {
        if self.is_forgotten(node_id) {
            return false;
        }
        let mut map = self.inner.write().unwrap();
        match map.get_mut(node_id) {
            Some(p) => {
                p.name = name.to_string();
                p.addr = addr.to_string();
                // 仅在本次携带时更新，避免 mDNS 等无 mTLS 信息的来源清空已知值。
                if mtls_addr.is_some() {
                    p.mtls_addr = mtls_addr;
                }
                if !tags.is_empty() {
                    p.tags = tags;
                }
                false
            }
            None => {
                map.insert(
                    node_id.to_string(),
                    PeerInfo {
                        node_id: node_id.to_string(),
                        name: name.to_string(),
                        addr: addr.to_string(),
                        mtls_addr,
                        tags,
                        online: false,
                        last_seen: Instant::now(),
                        direction: String::new(),
                        load: None,
                    },
                );
                true
            }
        }
    }

    /// The peer's advertised mTLS endpoint (`host:port`), if known.
    pub fn mtls_addr(&self, node_id: &str) -> Option<String> {
        self.inner
            .read()
            .unwrap()
            .get(node_id)
            .and_then(|p| p.mtls_addr.clone())
    }

    /// Mark a peer online / refresh identity after a successful handshake.
    pub fn mark_online(&self, node_id: &str, name: &str, addr: &str, direction: &str) {
        // A live handshake overrides any earlier user "forget": the node
        // demonstrably exists again (e.g. config restored after re-init).
        self.forgotten.write().unwrap().remove(node_id);
        let mut map = self.inner.write().unwrap();
        let entry = map.entry(node_id.to_string()).or_insert_with(|| PeerInfo {
            node_id: node_id.to_string(),
            name: name.to_string(),
            addr: addr.to_string(),
            mtls_addr: None,
            tags: Vec::new(),
            online: false,
            last_seen: Instant::now(),
            direction: direction.to_string(),
            load: None,
        });
        entry.name = name.to_string();
        entry.direction = direction.to_string();
        // Prefer the discovered (advertised) address; for outbound dials the
        // connected address is exactly the advertised one anyway.
        if direction == "out" || entry.addr.is_empty() {
            entry.addr = addr.to_string();
        }
        entry.online = true;
        entry.last_seen = Instant::now();
    }

    /// Refresh the last-seen timestamp for a peer.
    pub fn touch(&self, node_id: &str) {
        let mut map = self.inner.write().unwrap();
        if let Some(p) = map.get_mut(node_id) {
            p.last_seen = Instant::now();
        }
    }

    /// Record the peer's latest load snapshot (v0.6 P1 lease Full Status).
    /// Receiving any lease frame (bare or full) also refreshes `last_seen`.
    pub fn set_load(&self, node_id: &str, cpu: Option<u32>, ts: &str) {
        let mut map = self.inner.write().unwrap();
        if let Some(p) = map.get_mut(node_id) {
            p.last_seen = Instant::now();
            if let Some(cpu) = cpu {
                p.load = Some(PeerLoad {
                    cpu,
                    ts: ts.to_string(),
                });
            }
        }
    }

    pub fn mark_offline(&self, node_id: &str) {
        let mut map = self.inner.write().unwrap();
        if let Some(p) = map.get_mut(node_id) {
            p.online = false;
            p.direction.clear();
        }
    }

    pub fn is_online(&self, node_id: &str) -> bool {
        self.inner
            .read()
            .unwrap()
            .get(node_id)
            .map(|p| p.online)
            .unwrap_or(false)
    }

    pub fn snapshot(&self) -> Vec<PeerView> {
        let map = self.inner.read().unwrap();
        let mut views: Vec<PeerView> = map
            .values()
            .map(|p| PeerView {
                node_id: p.node_id.clone(),
                name: p.name.clone(),
                addr: p.addr.clone(),
                mtls_addr: p.mtls_addr.clone(),
                tags: p.tags.clone(),
                online: p.online,
                last_seen_seconds_ago: p.last_seen.elapsed().as_secs(),
                direction: p.direction.clone(),
                load: p.load.clone(),
                relayed: None,
                via: None,
            })
            .collect();
        views.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        views
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// v0.6 P1 Lease：`load` 为纯加性字段，`None` 时 /v1/peers JSON 里
    /// **不存在** key —— 默认态（lease 关闭、对端未发 Full）逐字节与旧版一致。
    #[test]
    fn peer_view_load_is_additive() {
        let table = PeerTable::new();
        table.mark_online("node-b", "peer-b", "127.0.0.1:9000", "out");
        let view = table.snapshot().remove(0);
        let v = serde_json::to_value(&view).unwrap();
        assert!(v.get("load").is_none(), "default view must omit load: {v}");

        // Full Status 登记 → 视图带 cpu；裸心跳（cpu=None）保持既有值不覆盖。
        table.set_load("node-b", Some(37), "2026-10-19T12:00:00+08:00");
        let view = table.snapshot().remove(0);
        let v = serde_json::to_value(&view).unwrap();
        assert_eq!(v["load"]["cpu"], 37);
        assert_eq!(v["load"]["ts"], "2026-10-19T12:00:00+08:00");

        table.set_load("node-b", None, "");
        let v = serde_json::to_value(table.snapshot().remove(0)).unwrap();
        assert_eq!(v["load"]["cpu"], 37, "bare lease must not clear load");
    }
}
