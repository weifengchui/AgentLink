//! Message router: addressing, dedup, at-least-once delivery with ACK/retry,
//! and short-term offline storage. See `docs/protocol.md` §4.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc};

use crate::config::Config;
use crate::conn::Frame;
use crate::proto::{self, envelope_sign, ts_within_skew, Envelope, PROTOCOL_VERSION};

const EVENT_CHANNEL_CAPACITY: usize = 1024;
const INBOX_MAX: usize = 1024;
const OFFLINE_MAX_PER_PEER: usize = 1024;
const TS_SKEW_SECONDS: i64 = 60;
const LOCAL_PENDING_TTL: Duration = Duration::from_secs(3600);

/// A live outbound handle to a peer connection.
#[derive(Clone)]
pub struct PeerSink {
    pub node_id: String,
    pub name: String,
    pub addr: String,
    pub tx: mpsc::Sender<Frame>,
}

/// Registry of live peer connections keyed by node_id.
#[derive(Default)]
pub struct PeerRegistry {
    map: RwLock<HashMap<String, PeerSink>>,
}

impl PeerRegistry {
    pub fn get(&self, node_id: &str) -> Option<PeerSink> {
        self.map.read().unwrap().get(node_id).cloned()
    }

    pub fn all(&self) -> Vec<PeerSink> {
        self.map.read().unwrap().values().cloned().collect()
    }

    pub fn insert(&self, sink: PeerSink) {
        self.map.write().unwrap().insert(sink.node_id.clone(), sink);
    }

    pub fn remove(&self, node_id: &str) -> Option<PeerSink> {
        self.map.write().unwrap().remove(node_id)
    }

    pub fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }
}

/// LRU dedup of incoming message ids.
struct Dedup {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl Dedup {
    /// Returns `true` if `msg_id` is new and was recorded.
    fn record(&mut self, msg_id: &str) -> bool {
        if self.set.contains(msg_id) {
            return false;
        }
        self.set.insert(msg_id.to_string());
        self.order.push_back(msg_id.to_string());
        if self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

#[derive(Clone)]
struct Pending {
    peer_id: String,
    env: Envelope,
    sent_at: Instant,
    tries: u32,
}

#[derive(Clone)]
struct OfflineEntry {
    env: Envelope,
    expires_at: Instant,
}

pub struct Router {
    cfg: Config,
    registry: PeerRegistry,
    events: broadcast::Sender<Envelope>,
    /// Buffer for messages received while no local subscriber is connected.
    inbox: tokio::sync::Mutex<VecDeque<Envelope>>,
    dedup: Mutex<Dedup>,
    pending: Mutex<HashMap<String, Pending>>,
    offline: tokio::sync::Mutex<HashMap<String, VecDeque<OfflineEntry>>>,
    /// Messages delivered locally but not yet acked by the local agent.
    local_pending: Mutex<HashMap<String, Instant>>,
}

impl Router {
    pub fn new(cfg: &Config) -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Router {
            cfg: cfg.clone(),
            registry: PeerRegistry::default(),
            events,
            inbox: tokio::sync::Mutex::new(VecDeque::new()),
            dedup: Mutex::new(Dedup {
                set: HashSet::new(),
                order: VecDeque::new(),
                cap: cfg.delivery.dedup_cache_size.max(1),
            }),
            pending: Mutex::new(HashMap::new()),
            offline: tokio::sync::Mutex::new(HashMap::new()),
            local_pending: Mutex::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Envelope> {
        self.events.subscribe()
    }

    // ---- peer registration --------------------------------------------------

    pub fn register_peer(&self, sink: PeerSink) {
        self.registry.insert(sink.clone());
        tracing::info!(peer = %sink.name, node_id = %sink.node_id, addr = %sink.addr, "router: peer registered");
        let ev = Envelope::event(&self.cfg.node_id, "join", &sink.node_id, &sink.name);
        let _ = self.events.send(ev);
    }

    pub fn unregister_peer(&self, node_id: &str) {
        if let Some(sink) = self.registry.remove(node_id) {
            tracing::info!(peer = %sink.name, node_id, "router: peer unregistered");
            let ev = Envelope::event(&self.cfg.node_id, "leave", node_id, &sink.name);
            let _ = self.events.send(ev);
        }
    }

    // ---- outbound (local agent -> network) ---------------------------------

    /// Send an envelope addressed to a specific peer or `"*"`.
    pub async fn send(&self, mut env: Envelope) -> anyhow::Result<String> {
        env.v = PROTOCOL_VERSION;
        env.from = self.cfg.node_id.clone();
        if env.msg_id.is_empty() {
            return Err(anyhow::anyhow!("envelope needs a msg_id"));
        }
        if self.cfg.security.mode == "strict" {
            let sig = envelope_sign(
                &self.cfg.security.cluster_token,
                &env.from,
                &env.msg_id,
                &env.ts,
            );
            env.sig = Some(sig);
        }

        let is_broadcast = env.to == "*" || env.to.is_empty();

        if is_broadcast {
            let sinks: Vec<PeerSink> = self.registry.all();
            for sink in sinks {
                self.send_raw(&sink, &env).await;
            }
            // Deliver to our own local subscribers too.
            self.deliver_local(env.clone()).await;
            return Ok(env.msg_id);
        }

        match self.registry.get(&env.to) {
            Some(sink) => {
                if self.send_raw(&sink, &env).await {
                    self.register_pending(&env);
                } else {
                    self.enqueue_offline(&env).await;
                }
            }
            None => self.enqueue_offline(&env).await,
        }
        Ok(env.msg_id)
    }

    async fn send_raw(&self, sink: &PeerSink, env: &Envelope) -> bool {
        let text = match serde_json::to_string(env) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "router: failed to encode envelope");
                return false;
            }
        };
        match sink.tx.send(Frame::Text(text)).await {
            Ok(_) => true,
            Err(_) => {
                // Connection dropped: remove the sink so future sends go offline.
                self.unregister_peer(&sink.node_id);
                false
            }
        }
    }

    fn register_pending(&self, env: &Envelope) {
        let mut pending = self.pending.lock().unwrap();
        pending.insert(
            env.msg_id.clone(),
            Pending {
                peer_id: env.to.clone(),
                env: env.clone(),
                sent_at: Instant::now(),
                tries: 0,
            },
        );
    }

    // ---- inbound (network -> local) ----------------------------------------

    /// Record an incoming envelope. Returns `true` when first seen.
    pub async fn handle_incoming(&self, env: &Envelope) -> bool {
        if !self.verify_incoming(env) {
            tracing::warn!(msg_id = %env.msg_id, from = %env.from, "router: incoming rejected (signature or timestamp)");
            return false;
        }
        let is_new = {
            let mut dedup = self.dedup.lock().unwrap();
            dedup.record(&env.msg_id)
        };
        if is_new {
            self.deliver_local(env.clone()).await;
            if env.to != "*" {
                self.local_pending
                    .lock()
                    .unwrap()
                    .insert(env.msg_id.clone(), Instant::now());
            }
        }
        is_new
    }

    fn verify_incoming(&self, env: &Envelope) -> bool {
        if self.cfg.security.mode != "strict" {
            return true;
        }
        let Some(sig) = &env.sig else {
            return false;
        };
        let expected = envelope_sign(
            &self.cfg.security.cluster_token,
            &env.from,
            &env.msg_id,
            &env.ts,
        );
        if !proto::constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
            return false;
        }
        ts_within_skew(&env.ts, TS_SKEW_SECONDS)
    }

    async fn deliver_local(&self, env: Envelope) {
        if self.events.receiver_count() == 0 {
            let mut inbox = self.inbox.lock().await;
            if inbox.len() >= INBOX_MAX {
                inbox.pop_front();
            }
            inbox.push_back(env);
        } else {
            let _ = self.events.send(env);
        }
    }

    // ---- acks ---------------------------------------------------------------

    /// Remote ACKed one of our point-to-point messages.
    pub fn on_ack(&self, msg_id: &str) {
        if self.pending.lock().unwrap().remove(msg_id).is_some() {
            tracing::debug!(msg_id, "router: delivery acked");
        }
    }

    /// Local agent ACKed a delivered message (`/v1/ack-inbox`).
    pub fn ack_local(&self, msg_id: &str) -> bool {
        self.local_pending.lock().unwrap().remove(msg_id).is_some()
    }

    // ---- maintenance --------------------------------------------------------

    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                this.retry_pending().await;
                this.flush_offline().await;
                this.evict_expired_offline().await;
                this.drain_inbox().await;
                this.evict_local_pending();
            }
        })
    }

    async fn retry_pending(&self) {
        let ack_timeout = Duration::from_secs(self.cfg.delivery.ack_timeout_seconds.max(1));
        let max_retries = self.cfg.delivery.max_retries;

        let overdue: Vec<(String, Pending)> = {
            let mut pending = self.pending.lock().unwrap();
            let mut items = Vec::new();
            pending.retain(|k, v| {
                if v.sent_at.elapsed() >= ack_timeout {
                    items.push((k.clone(), v.clone()));
                    false
                } else {
                    true
                }
            });
            items
        };

        for (msg_id, pend) in overdue {
            if pend.tries >= max_retries {
                tracing::warn!(msg_id, peer = %pend.peer_id, "router: delivery failed after retries");
                continue;
            }
            match self.registry.get(&pend.peer_id) {
                Some(sink) => {
                    if self.send_raw(&sink, &pend.env).await {
                        let mut pending = self.pending.lock().unwrap();
                        pending.insert(
                            msg_id,
                            Pending {
                                tries: pend.tries + 1,
                                sent_at: Instant::now(),
                                ..pend
                            },
                        );
                    } else {
                        self.enqueue_offline(&pend.env).await;
                    }
                }
                None => self.enqueue_offline(&pend.env).await,
            }
        }
    }

    async fn enqueue_offline(&self, env: &Envelope) {
        let ttl = env
            .ttl_seconds
            .unwrap_or(self.cfg.delivery.offline_ttl_seconds)
            .max(1);
        let mut map = self.offline.lock().await;
        let queue = map.entry(env.to.clone()).or_default();
        if queue.len() >= OFFLINE_MAX_PER_PEER {
            queue.pop_front();
        }
        queue.push_back(OfflineEntry {
            env: env.clone(),
            expires_at: Instant::now() + Duration::from_secs(ttl),
        });
    }

    async fn flush_offline(&self) {
        let mut to_send: Vec<(String, Envelope)> = Vec::new();
        {
            let mut map = self.offline.lock().await;
            for (peer_id, queue) in map.iter_mut() {
                if self.registry.get(peer_id).is_some() {
                    while let Some(entry) = queue.pop_front() {
                        to_send.push((peer_id.clone(), entry.env));
                    }
                }
            }
            map.retain(|_, q| !q.is_empty());
        }
        for (peer_id, env) in to_send {
            if let Some(sink) = self.registry.get(&peer_id) {
                if self.send_raw(&sink, &env).await {
                    self.register_pending(&env);
                } else {
                    self.enqueue_offline(&env).await;
                }
            } else {
                self.enqueue_offline(&env).await;
            }
        }
    }

    async fn evict_expired_offline(&self) {
        let now = Instant::now();
        let mut map = self.offline.lock().await;
        for queue in map.values_mut() {
            queue.retain(|e| e.expires_at > now);
        }
        map.retain(|_, q| !q.is_empty());
    }

    async fn drain_inbox(&self) {
        if self.events.receiver_count() == 0 {
            return;
        }
        let mut inbox = self.inbox.lock().await;
        while let Some(env) = inbox.pop_front() {
            if let Err(send_err) = self.events.send(env) {
                inbox.push_front(send_err.0);
                break;
            }
        }
    }

    fn evict_local_pending(&self) {
        let now = Instant::now();
        let mut pending = self.local_pending.lock().unwrap();
        pending.retain(|_, t| now.duration_since(*t) < LOCAL_PENDING_TTL);
    }

    // ---- stats ---------------------------------------------------------------

    pub fn peers_online(&self) -> usize {
        self.registry.len()
    }

    /// Advertised address of a live peer, used to open file connections.
    pub fn peer_addr(&self, node_id: &str) -> Option<String> {
        self.registry.get(node_id).map(|s| s.addr)
    }

    pub fn pending_acks(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    pub async fn offline_queue_len(&self) -> usize {
        self.offline.lock().await.values().map(|q| q.len()).sum()
    }

    pub fn local_pending_count(&self) -> usize {
        self.local_pending.lock().unwrap().len()
    }
}
