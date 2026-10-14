//! Message router: addressing, dedup, at-least-once delivery with ACK/retry,
//! and short-term offline storage. See `docs/protocol.md` §4.
//!
//! Reliability: every queue mutation (inbox / offline / pending / local ack /
//! dedup ids) is appended to `data_dir/journal.jsonl` with fsync, and the
//! local agent registry is snapshotted to `data_dir/agents.json`. On boot we
//! replay the journal, so a restart never loses in-flight messages. Expired
//! offline entries and stale agents (no WS client, not re-registered within
//! `delivery.agent_stale_ttl_seconds`) are cleaned automatically.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};

use crate::config::Config;
use crate::conn::Frame;
use crate::proto::{self, envelope_sign, ts_within_skew, Envelope, PROTOCOL_VERSION};
use crate::store;

const EVENT_CHANNEL_CAPACITY: usize = 1024;
const INBOX_MAX: usize = 1024;
const OFFLINE_MAX_PER_PEER: usize = 1024;
const TS_SKEW_SECONDS: i64 = 60;
const LOCAL_PENDING_TTL: Duration = Duration::from_secs(3600);

/// Durable queue state restored from the journal at startup:
/// `(inbox, pending_acks, offline_queues, local_pending, seen_ids)`.
type Replayed = (
    VecDeque<Envelope>,
    HashMap<String, Pending>,
    HashMap<String, VecDeque<OfflineEntry>>,
    HashMap<String, (Instant, i64)>,
    Vec<String>,
);
/// Journal records since last compaction that triggers a rewrite.
const COMPACT_AFTER_OPS: usize = 4096;
/// Re-sign outbound messages older than this many seconds before sending
/// (peer-side replay window is `TS_SKEW_SECONDS`; queueing must not eat it).
const RESTAMP_AFTER_SECONDS: i64 = 45;

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
    /// Wall time of the original send attempt (persisted across restarts).
    sent_wall: i64,
}

#[derive(Clone)]
struct OfflineEntry {
    env: Envelope,
    expires_at: Instant,
    /// Absolute wall-clock expiry in unix seconds (persisted).
    exp_wall: i64,
}

/// An AI agent registered on some node (M7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// True when at least one WS subscriber with `client == agent_id` is connected.
    pub online: bool,
    /// Owning node_id (filled in the fleet view).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node_id: String,
    /// Wall clock of the last registration / reconnect, unix seconds.
    /// Stale agents are auto-cleaned (`delivery.agent_stale_ttl_seconds`).
    #[serde(default)]
    pub last_seen: i64,
}

/// One history entry for the human view (`/web`, `/v1/history`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub ts: String,
    pub msg_id: String,
    /// Address string of the sending/receiving node.
    pub node: String,
    /// `in` | `out` | `local`.
    pub direction: String,
    pub from: String,
    #[serde(default)]
    pub from_agent: Option<String>,
    pub to: String,
    #[serde(default)]
    pub to_agent: Option<String>,
    pub kind: String,
    pub action: String,
    #[serde(default)]
    pub payload: serde_json::Value,
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
    local_pending: Mutex<HashMap<String, (Instant, i64)>>,
    /// Local AI agents registered on this node (agent_id -> record).
    agents: Mutex<HashMap<String, AgentRecord>>,
    /// Remote agents, keyed by their owning node_id (synced via ctrl frames).
    fleet_agents: Mutex<HashMap<String, Vec<AgentRecord>>>,
    /// Active local WS subscribers count per client id.
    clients: Mutex<HashMap<String, usize>>,
    /// Ring buffer of recent traffic for the human view.
    history: Mutex<VecDeque<HistoryEntry>>,
    /// Append-only journal backing inbox/offline/pending/local/seen.
    journal: store::Journal,
    journal_ops: AtomicUsize,
    compact_due: AtomicBool,
    /// Seconds a non-online local agent may stay un-refreshed before it is
    /// auto-removed. 0 disables the cleanup.
    agent_ttl: i64,
}

impl Router {
    pub fn new(cfg: &Config) -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let persist = cfg.delivery.persist;
        let journal = store::Journal::open(&cfg.data_dir, persist);
        let agent_ttl = cfg.delivery.agent_stale_ttl_seconds as i64;
        let now_wall = store::now_wall();

        // ---- replay durable message state -----------------------------------
        let ack_timeout = Duration::from_secs(cfg.delivery.ack_timeout_seconds.max(1));
        let (inbox, pending, offline, local_pending, seen) =
            Self::replay_journal(cfg, &journal, ack_timeout, now_wall);

        // ---- restore agent registry ------------------------------------------
        let mut agents: HashMap<String, AgentRecord> = HashMap::new();
        let mut expired_agents = 0usize;
        let agents_path = cfg.data_dir.join("agents.json");
        let restored: Vec<AgentRecord> = store::read_json_opt(&agents_path).unwrap_or_default();
        for mut rec in restored {
            rec.online = false; // connections died with the old process
            if rec.last_seen <= 0 {
                rec.last_seen = now_wall; // pre-0.4.12 snapshot: grace period
            }
            if agent_ttl > 0 && now_wall - rec.last_seen > agent_ttl {
                expired_agents += 1;
                continue;
            }
            agents.insert(rec.agent_id.clone(), rec);
        }
        if !agents.is_empty() || expired_agents > 0 {
            tracing::info!(
                restored = agents.len(),
                expired = expired_agents,
                "router: agent registry restored from disk"
            );
        }
        if expired_agents > 0 {
            Self::write_agents_snapshot(&cfg.data_dir, &agents);
        }

        // ---- restore recent history ------------------------------------------
        let mut history: VecDeque<HistoryEntry> = VecDeque::new();
        if cfg.history.enabled && !cfg.history.file.is_empty() && cfg.history.file != "-" {
            let lines = store::tail_history(&cfg.data_dir, &cfg.history.file, cfg.history.len);
            let total = lines.len();
            for line in lines {
                if let Some(entry) = Self::parse_history_line(&line, &cfg.node_id) {
                    if history.len() >= cfg.history.len {
                        history.pop_front();
                    }
                    history.push_back(entry);
                }
            }
            if total > 0 {
                tracing::info!(
                    loaded = history.len(),
                    file = %cfg.history.file,
                    "router: conversation history restored from disk"
                );
            }
        }

        let mut dedup = Dedup {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap: cfg.delivery.dedup_cache_size.max(1),
        };
        for id in &seen {
            dedup.record(id);
        }

        Router {
            cfg: cfg.clone(),
            registry: PeerRegistry::default(),
            events,
            inbox: tokio::sync::Mutex::new(inbox),
            dedup: Mutex::new(dedup),
            pending: Mutex::new(pending),
            offline: tokio::sync::Mutex::new(offline),
            local_pending: Mutex::new(local_pending),
            agents: Mutex::new(agents),
            fleet_agents: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
            history: Mutex::new(history),
            journal,
            journal_ops: AtomicUsize::new(0),
            // Rewrite the journal from live state on the first maintenance
            // tick so replayed duplicates/expired entries do not accumulate.
            compact_due: AtomicBool::new(persist),
            agent_ttl,
        }
    }

    /// Replay the journal into live state. Last line wins per msg_id.
    fn replay_journal(
        cfg: &Config,
        journal: &store::Journal,
        ack_timeout: Duration,
        now_wall: i64,
    ) -> Replayed {
        let mut inbox: VecDeque<Envelope> = VecDeque::new();
        let mut pending: HashMap<String, Pending> = HashMap::new();
        let mut offline: HashMap<String, VecDeque<OfflineEntry>> = HashMap::new();
        let mut local_pending: HashMap<String, (Instant, i64)> = HashMap::new();
        let mut seen: Vec<String> = Vec::new();
        let mut dropped_expired = 0usize;
        for r in journal.replay() {
            let is_add = r.op == "add";
            match (r.q.as_str(), is_add) {
                ("inbox", true) => {
                    if let Some(env) = r.env {
                        if !inbox.iter().any(|e| e.msg_id == env.msg_id) {
                            inbox.push_back(env);
                        }
                    }
                }
                ("inbox", false) => inbox.retain(|e| e.msg_id != r.msg_id),
                ("offline", true) => {
                    if let Some(env) = r.env {
                        let exp = r
                            .exp
                            .unwrap_or(now_wall + cfg.delivery.offline_ttl_seconds as i64);
                        if exp > now_wall {
                            let rem = Duration::from_secs((exp - now_wall).max(1) as u64);
                            offline
                                .entry(env.to.clone())
                                .or_default()
                                .push_back(OfflineEntry {
                                    env,
                                    expires_at: Instant::now() + rem,
                                    exp_wall: exp,
                                });
                        } else {
                            dropped_expired += 1;
                        }
                    }
                }
                ("offline", false) => {
                    // del carries only msg_id; find it by scanning queues.
                    for q in offline.values_mut() {
                        q.retain(|e| e.env.msg_id != r.msg_id);
                    }
                    offline.retain(|_, q| !q.is_empty());
                }
                ("pending", true) => {
                    if let Some(env) = r.env {
                        // Restored messages are due for retry right away.
                        pending.insert(
                            env.msg_id.clone(),
                            Pending {
                                peer_id: env.to.clone(),
                                sent_at: Instant::now() - ack_timeout,
                                tries: r.tries.unwrap_or(0),
                                env,
                                sent_wall: r.wall.unwrap_or(now_wall),
                            },
                        );
                    }
                }
                ("pending", false) => {
                    pending.remove(&r.msg_id);
                }
                ("local", true) => {
                    let wall = r.wall.unwrap_or(now_wall);
                    let age = (now_wall - wall).max(0) as u64;
                    if Duration::from_secs(age) < LOCAL_PENDING_TTL {
                        local_pending.insert(
                            r.msg_id.clone(),
                            (Instant::now() - Duration::from_secs(age), wall),
                        );
                    }
                }
                ("local", false) => {
                    local_pending.remove(&r.msg_id);
                }
                ("seen", true) => {
                    if !seen.contains(&r.msg_id) {
                        seen.push(r.msg_id.clone());
                    }
                }
                ("seen", false) => {
                    seen.retain(|s| *s != r.msg_id);
                }
                _ => {}
            }
        }
        if dropped_expired > 0 {
            tracing::info!(
                count = dropped_expired,
                "router: dropped expired offline-queue messages after replay"
            );
        }
        (inbox, pending, offline, local_pending, seen)
    }

    /// Legacy history lines stored raw envelopes; new lines are HistoryEntry
    /// JSON. Accept both (tolerant: unparseable lines are skipped).
    fn parse_history_line(line: &str, self_node: &str) -> Option<HistoryEntry> {
        if let Ok(e) = serde_json::from_str::<HistoryEntry>(line) {
            return Some(e);
        }
        let env: Envelope = serde_json::from_str(line).ok()?;
        if env.kind == "ctrl" || env.kind == "event" {
            return None;
        }
        let (direction, node) = if env.from == self_node {
            ("out", env.to.clone())
        } else {
            ("in", env.from.clone())
        };
        Some(HistoryEntry {
            ts: env.ts,
            msg_id: env.msg_id,
            node,
            direction: direction.to_string(),
            from: env.from,
            from_agent: env.from_agent,
            to: env.to,
            to_agent: env.to_agent,
            kind: env.kind,
            action: env.action,
            payload: env.payload,
        })
    }

    fn write_agents_snapshot(data_dir: &std::path::Path, agents: &HashMap<String, AgentRecord>) {
        let mut v: Vec<AgentRecord> = agents.values().cloned().collect();
        v.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        if let Err(e) = store::write_json_atomic(&data_dir.join("agents.json"), &v) {
            tracing::error!(error = %e, "router: failed to persist agents.json");
        }
    }

    fn persist_agents(&self) {
        if !self.cfg.delivery.persist {
            return;
        }
        let agents = self.agents.lock().unwrap();
        Self::write_agents_snapshot(&self.cfg.data_dir, &agents);
    }

    /// Rewrite the journal from live state (called by maintenance).
    async fn compact_journal(&self) {
        let mut recs = Vec::new();
        for env in self.inbox.lock().await.iter() {
            recs.push(store::Rec::add_env("inbox", env.clone()));
        }
        {
            let map = self.offline.lock().await;
            for q in map.values() {
                for e in q.iter() {
                    recs.push(store::Rec::add_offline(e.env.clone(), e.exp_wall));
                }
            }
        }
        {
            let p = self.pending.lock().unwrap();
            for pend in p.values() {
                let mut r = store::Rec::add_pending(pend.env.clone(), pend.tries);
                r.wall = Some(pend.sent_wall);
                recs.push(r);
            }
        }
        {
            let lp = self.local_pending.lock().unwrap();
            for (msg_id, (_, wall)) in lp.iter() {
                let mut r = store::Rec::add_id("local", msg_id);
                r.wall = Some(*wall);
                recs.push(r);
            }
        }
        {
            let d = self.dedup.lock().unwrap();
            for id in d.order.iter() {
                recs.push(store::Rec::add_id("seen", id));
            }
        }
        self.journal.compact(&recs);
    }

    /// Append one journal record and watch the churn counter.
    fn j(&self, r: store::Rec) {
        self.journal.record(&r);
        if self.journal_ops.fetch_add(1, Ordering::Relaxed) >= COMPACT_AFTER_OPS {
            self.journal_ops.store(0, Ordering::Relaxed);
            self.compact_due.store(true, Ordering::Relaxed);
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
        let removed = self.registry.remove(node_id).is_some();
        // A node that left takes its agent list with it: drop the cached
        // fleet view for it so ghosts never linger on /web.
        let cached = self
            .fleet_agents
            .lock()
            .unwrap()
            .remove(node_id)
            .unwrap_or_default();
        if removed {
            tracing::info!(node_id, agents = cached.len(), "router: peer unregistered");
            let ev = Envelope::event(&self.cfg.node_id, "leave", node_id, node_id);
            let _ = self.events.send(ev);
        }
    }

    /// Force-close a live peer link (user "forgot" an online node).
    /// Dropping the sink closes its send channel, which ends the peer task.
    pub fn close_peer(&self, node_id: &str) {
        self.registry.remove(node_id);
        self.fleet_agents.lock().unwrap().remove(node_id);
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

        // Local delivery: addressed to this very node (agents on this machine).
        if env.to == self.cfg.node_id {
            let _ = self.handle_incoming(&env).await;
            return Ok(env.msg_id);
        }

        let is_broadcast = env.to == "*" || env.to.is_empty();

        if is_broadcast {
            self.record_history(&env, "out", "*");
            let sinks: Vec<PeerSink> = self.registry.all();
            for sink in sinks {
                self.send_raw(&sink, &env).await;
            }
            // Deliver to our own local subscribers too.
            self.deliver_local(env.clone()).await;
            return Ok(env.msg_id);
        }

        self.record_history(&env, "out", &env.to);
        match self.registry.get(&env.to) {
            Some(sink) => {
                if self.send_raw(&sink, &env).await {
                    self.register_pending(&env, 0);
                } else {
                    self.enqueue_offline(&env).await;
                }
            }
            None => self.enqueue_offline(&env).await,
        }
        Ok(env.msg_id)
    }

    /// Re-stamp an aged envelope's timestamp + signature before outbound
    /// send: messages that sat in the offline/pending queues across a
    /// restart or reconnect would otherwise be rejected by the strict-mode
    /// peer's ±60s replay window. `msg_id` stays the same, so the receiver's
    /// dedup still protects against duplicates.
    fn refresh_stale_ts(&self, env: &mut Envelope) {
        if self.cfg.security.mode != "strict" || env.sig.is_none() {
            return;
        }
        if let Some(age) = proto::ts_age_seconds(&env.ts) {
            if age > RESTAMP_AFTER_SECONDS {
                env.ts = proto::now_rfc3339();
                env.sig = Some(envelope_sign(
                    &self.cfg.security.cluster_token,
                    &env.from,
                    &env.msg_id,
                    &env.ts,
                ));
            }
        }
    }

    async fn send_raw(&self, sink: &PeerSink, env: &Envelope) -> bool {
        let mut env = env.clone();
        self.refresh_stale_ts(&mut env);
        let text = match serde_json::to_string(&env) {
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

    fn register_pending(&self, env: &Envelope, tries: u32) {
        self.pending.lock().unwrap().insert(
            env.msg_id.clone(),
            Pending {
                peer_id: env.to.clone(),
                env: env.clone(),
                sent_at: Instant::now(),
                tries,
                sent_wall: store::now_wall(),
            },
        );
        self.j(store::Rec::add_pending(env.clone(), tries));
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
            self.j(store::Rec::add_id("seen", &env.msg_id));
            self.record_history(env, "in", &env.from);
            self.deliver_local(env.clone()).await;
            if env.to != "*" {
                self.local_pending
                    .lock()
                    .unwrap()
                    .insert(env.msg_id.clone(), (Instant::now(), store::now_wall()));
                self.j(store::Rec::add_id("local", &env.msg_id));
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
                if let Some(dropped) = inbox.pop_front() {
                    self.j(store::Rec::del("inbox", &dropped.msg_id));
                    tracing::warn!(msg_id = %dropped.msg_id, "router: inbox overflow, dropped oldest");
                }
            }
            self.j(store::Rec::add_env("inbox", env.clone()));
            inbox.push_back(env);
        } else {
            let _ = self.events.send(env);
        }
    }

    // ---- acks ---------------------------------------------------------------

    /// Remote ACKed one of our point-to-point messages.
    pub fn on_ack(&self, msg_id: &str) {
        if self.pending.lock().unwrap().remove(msg_id).is_some() {
            self.j(store::Rec::del("pending", msg_id));
            tracing::debug!(msg_id, "router: delivery acked");
        }
    }

    /// Local agent ACKed a delivered message (`/v1/ack-inbox`).
    pub fn ack_local(&self, msg_id: &str) -> bool {
        let acked = self.local_pending.lock().unwrap().remove(msg_id).is_some();
        if acked {
            self.j(store::Rec::del("local", msg_id));
        }
        acked
    }

    // ---- maintenance --------------------------------------------------------

    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            let mut counter: u64 = 0;
            loop {
                tick.tick().await;
                counter += 1;
                this.retry_pending().await;
                this.flush_offline().await;
                this.evict_expired_offline().await;
                this.drain_inbox().await;
                this.evict_local_pending();
                if this.agent_ttl > 0 && counter.is_multiple_of(5) {
                    this.evict_stale_agents();
                }
                if this.compact_due.swap(false, Ordering::Relaxed) {
                    this.compact_journal().await;
                }
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
                        self.register_pending(&pend.env, pend.tries + 1);
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
        let exp_wall = store::now_wall() + ttl as i64;
        let mut map = self.offline.lock().await;
        let queue = map.entry(env.to.clone()).or_default();
        if queue.len() >= OFFLINE_MAX_PER_PEER {
            if let Some(dropped) = queue.pop_front() {
                self.j(store::Rec::del("offline", &dropped.env.msg_id));
                tracing::warn!(msg_id = %dropped.env.msg_id, "router: offline queue overflow, dropped oldest");
            }
        }
        self.j(store::Rec::add_offline(env.clone(), exp_wall));
        queue.push_back(OfflineEntry {
            env: env.clone(),
            expires_at: Instant::now() + Duration::from_secs(ttl),
            exp_wall,
        });
    }

    async fn flush_offline(&self) {
        let mut to_send: Vec<(String, Envelope)> = Vec::new();
        {
            let mut map = self.offline.lock().await;
            for (peer_id, queue) in map.iter_mut() {
                if self.registry.get(peer_id).is_some() {
                    while let Some(entry) = queue.pop_front() {
                        self.j(store::Rec::del("offline", &entry.env.msg_id));
                        to_send.push((peer_id.clone(), entry.env));
                    }
                }
            }
            map.retain(|_, q| !q.is_empty());
        }
        for (peer_id, env) in to_send {
            if let Some(sink) = self.registry.get(&peer_id) {
                if self.send_raw(&sink, &env).await {
                    self.register_pending(&env, 0);
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
            let msg_id = env.msg_id.clone();
            if let Err(e) = self.events.send(env) {
                inbox.push_front(e.0);
                break;
            }
            // Delivered live; only now remove the durable copy. If we crash
            // in between, the message is delivered again after restart
            // (at-least-once; receiver dedup guards against double work).
            self.j(store::Rec::del("inbox", &msg_id));
        }
    }

    fn evict_local_pending(&self) {
        let now = Instant::now();
        let mut pending = self.local_pending.lock().unwrap();
        pending.retain(|_, (t, _)| now.duration_since(*t) < LOCAL_PENDING_TTL);
    }

    /// Auto-clean registered agents that are offline and have not refreshed
    /// (re-registered / reconnected) within `agent_stale_ttl_seconds`.
    fn evict_stale_agents(&self) {
        let now = store::now_wall();
        let ttl = self.agent_ttl;
        if ttl <= 0 {
            return;
        }
        let mut removed: Vec<String> = Vec::new();
        {
            let mut agents = self.agents.lock().unwrap();
            agents.retain(|k, rec| {
                if rec.online || now - rec.last_seen <= ttl {
                    true
                } else {
                    removed.push(k.clone());
                    false
                }
            });
        }
        if !removed.is_empty() {
            tracing::info!(agents = ?removed, "router: auto-cleaned stale agents");
            self.persist_agents();
            self.sync_push_agents();
        }
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

    pub fn inbox_len(&self) -> usize {
        self.inbox.try_lock().map(|i| i.len()).unwrap_or(0)
    }

    /// Queue depths persisted on disk (for /web and operators).
    pub fn durable(&self) -> bool {
        self.cfg.delivery.persist
    }

    // ---- agent registry (M7) ------------------------------------------------

    pub fn register_agent(
        &self,
        agent_id: &str,
        name: Option<String>,
        capabilities: Vec<String>,
    ) -> AgentRecord {
        let online = self.clients.lock().unwrap().contains_key(agent_id);
        let rec = AgentRecord {
            agent_id: agent_id.to_string(),
            name,
            capabilities,
            online,
            node_id: self.cfg.node_id.clone(),
            last_seen: store::now_wall(),
        };
        self.agents
            .lock()
            .unwrap()
            .insert(agent_id.to_string(), rec.clone());
        self.persist_agents();
        self.sync_push_agents();
        rec
    }

    pub fn unregister_agent(&self, agent_id: &str) -> bool {
        let removed = self.agents.lock().unwrap().remove(agent_id).is_some();
        if removed {
            self.persist_agents();
            self.sync_push_agents();
        }
        removed
    }

    pub fn local_agents(&self) -> Vec<AgentRecord> {
        let agents = self.agents.lock().unwrap();
        let mut v: Vec<AgentRecord> = agents.values().cloned().collect();
        v.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        v
    }

    /// Agents across the whole fleet (local + cached from peers).
    pub fn fleet_agents(&self) -> Vec<AgentRecord> {
        let mut out = self.local_agents();
        let remote = self.fleet_agents.lock().unwrap();
        for list in remote.values() {
            out.extend(list.iter().cloned());
        }
        out.sort_by(|a, b| {
            (a.node_id.as_str(), a.agent_id.as_str())
                .cmp(&(b.node_id.as_str(), b.agent_id.as_str()))
        });
        out
    }

    /// A local WS subscriber connected (its `client` = agent id).
    pub fn client_online(&self, client_id: &str) {
        {
            let mut c = self.clients.lock().unwrap();
            *c.entry(client_id.to_string()).or_insert(0) += 1;
        }
        // Any connected client refreshes its registration's staleness clock.
        if let Some(rec) = self.agents.lock().unwrap().get_mut(client_id) {
            rec.last_seen = store::now_wall();
        }
        self.refresh_agent_status();
    }

    pub fn client_offline(&self, client_id: &str) {
        {
            let mut c = self.clients.lock().unwrap();
            if let Some(n) = c.get_mut(client_id) {
                *n -= 1;
                if *n == 0 {
                    c.remove(client_id);
                }
            }
        }
        self.refresh_agent_status();
    }

    fn refresh_agent_status(&self) {
        let clients = self.clients.lock().unwrap();
        let mut changed = false;
        {
            let mut agents = self.agents.lock().unwrap();
            for rec in agents.values_mut() {
                let on = clients.contains_key(&rec.agent_id);
                if rec.online != on {
                    rec.online = on;
                    changed = true;
                }
            }
        }
        if changed {
            self.sync_push_agents();
        }
    }

    /// Cache the agent list reported by a peer node.
    pub fn on_agents_update(&self, from_node: &str, agents: Vec<AgentRecord>) {
        self.fleet_agents
            .lock()
            .unwrap()
            .insert(from_node.to_string(), agents);
    }

    /// An `agents_update` envelope addressed to `to` (None if no agents here).
    pub fn agents_update_envelope(&self, to: &str) -> Option<Envelope> {
        let agents = self.local_agents();
        if agents.is_empty() {
            return None;
        }
        let payload = serde_json::json!({ "agents": agents });
        Some(Envelope::agents_update(
            &self.cfg.node_id,
            to,
            payload,
            None,
            None,
        ))
    }

    /// Push our latest agent table to every connected peer.
    pub fn sync_push_agents(&self) {
        let Some(env) = self.agents_update_envelope("*") else {
            return;
        };
        let Ok(text) = serde_json::to_string(&env) else {
            return;
        };
        for sink in self.registry.all() {
            let text = text.clone();
            tokio::spawn(async move {
                let _ = sink.tx.send(Frame::Text(text)).await;
            });
        }
    }

    // ---- conversation history (M7) -------------------------------------------

    pub fn record_history(&self, env: &Envelope, direction: &str, node: &str) {
        if !self.cfg.history.enabled {
            return;
        }
        if env.kind == "ctrl" || env.kind == "event" {
            return;
        }
        let entry = HistoryEntry {
            ts: env.ts.clone(),
            msg_id: env.msg_id.clone(),
            node: node.to_string(),
            direction: direction.to_string(),
            from: env.from.clone(),
            from_agent: env.from_agent.clone(),
            to: env.to.clone(),
            to_agent: env.to_agent.clone(),
            kind: env.kind.clone(),
            action: env.action.clone(),
            payload: env.payload.clone(),
        };
        self.push_history(entry);
    }

    /// Push into the ring buffer and (when configured) the durable JSONL file.
    fn push_history(&self, entry: HistoryEntry) {
        {
            let mut h = self.history.lock().unwrap();
            if h.len() >= self.cfg.history.len.max(1) {
                h.pop_front();
            }
            h.push_back(entry.clone());
        }
        let file = &self.cfg.history.file;
        if !file.is_empty() && file != "-" {
            if let Ok(line) = serde_json::to_string(&entry) {
                store::append_history(
                    &self.cfg.data_dir,
                    file,
                    &line,
                    self.cfg.history.max_file_mb,
                );
            }
        }
    }

    pub fn history(&self) -> Vec<HistoryEntry> {
        let h = self.history.lock().unwrap();
        h.iter().cloned().collect()
    }

    /// Record a file transfer event into the human-visible history.
    #[allow(clippy::too_many_arguments)]
    pub fn record_file_event(
        &self,
        direction: &str,
        from: &str,
        to: &str,
        file_name: &str,
        size: u64,
        saved_path: &str,
    ) {
        if !self.cfg.history.enabled {
            return;
        }
        let entry = HistoryEntry {
            ts: crate::proto::now_rfc3339(),
            msg_id: crate::proto::new_id(),
            node: from.to_string(),
            direction: direction.to_string(),
            from: from.to_string(),
            from_agent: None,
            to: to.to_string(),
            to_agent: None,
            kind: "file".to_string(),
            action: "file".to_string(),
            payload: serde_json::json!({
                "file_name": file_name,
                "size": size,
                "saved_path": saved_path,
            }),
        };
        self.push_history(entry);
    }
}
