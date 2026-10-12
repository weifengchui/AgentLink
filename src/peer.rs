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
    /// Machine-level capability labels advertised by the peer.
    pub tags: Vec<String>,
    pub online: bool,
    pub last_seen: Instant,
    /// `in` or `out`: who dialled.
    pub direction: String,
}

/// Serializable view returned by `/v1/peers`.
#[derive(Debug, Clone, Serialize)]
pub struct PeerView {
    pub node_id: String,
    pub name: String,
    pub addr: String,
    pub tags: Vec<String>,
    pub online: bool,
    pub last_seen_seconds_ago: u64,
    pub direction: String,
}

/// A peer learned from discovery, forwarded to the connection manager.
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    pub node_id: String,
    pub name: String,
    /// `host:port` of the peer's WebSocket endpoint.
    pub addr: String,
}

/// Thread-safe peer table. Critical sections are short and never await.
#[derive(Default)]
pub struct PeerTable {
    inner: RwLock<HashMap<String, PeerInfo>>,
}

impl PeerTable {
    pub fn new() -> Self {
        PeerTable::default()
    }

    /// Record a peer learned from discovery. Returns `true` if it is new.
    pub fn upsert_discovered(
        &self,
        node_id: &str,
        name: &str,
        addr: &str,
        tags: Vec<String>,
    ) -> bool {
        let mut map = self.inner.write().unwrap();
        match map.get_mut(node_id) {
            Some(p) => {
                p.name = name.to_string();
                p.addr = addr.to_string();
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
                        tags,
                        online: false,
                        last_seen: Instant::now(),
                        direction: String::new(),
                    },
                );
                true
            }
        }
    }

    /// Mark a peer online / refresh identity after a successful handshake.
    pub fn mark_online(&self, node_id: &str, name: &str, addr: &str, direction: &str) {
        let mut map = self.inner.write().unwrap();
        let entry = map.entry(node_id.to_string()).or_insert_with(|| PeerInfo {
            node_id: node_id.to_string(),
            name: name.to_string(),
            addr: addr.to_string(),
            tags: Vec::new(),
            online: false,
            last_seen: Instant::now(),
            direction: direction.to_string(),
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
                tags: p.tags.clone(),
                online: p.online,
                last_seen_seconds_ago: p.last_seen.elapsed().as_secs(),
                direction: p.direction.clone(),
            })
            .collect();
        views.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        views
    }
}
