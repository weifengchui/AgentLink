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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
/// v0.5 §7.1: bound of the msg_id -> trace_id continuation map.
const TRACE_MAP_CAP: usize = 8192;
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

#[cfg(test)]
mod tests {
    use super::{cap_matches, tasks_projection, HistoryEntry, TaskFilter};
    use serde_json::json;

    fn task_entry(seq: u64, payload: serde_json::Value, from: &str, to: &str) -> HistoryEntry {
        HistoryEntry {
            ts: format!("2026-10-17T0{}:00:00+08:00", seq % 10),
            msg_id: format!("m{seq}"),
            node: "n1".to_string(),
            direction: "in".to_string(),
            from: "n1".to_string(),
            from_agent: Some(from.to_string()),
            to: "n1".to_string(),
            to_agent: Some(to.to_string()),
            kind: "msg".to_string(),
            action: "send".to_string(),
            seq,
            reply_to: None,
            trace_id: None,
            payload,
        }
    }

    #[test]
    fn cap_exact_and_wildcard() {
        assert!(cap_matches("cap:node:reboot", "cap:node:reboot"));
        assert!(cap_matches("cap:node:*", "cap:node:reboot"));
        assert!(!cap_matches("cap:node:reboot", "cap:node:*"));
        assert!(cap_matches("cap:node:*", "cap:node:admin:deep"));
        assert!(!cap_matches("cap:edit:py", "cap:edit:js"));
        assert!(!cap_matches("cap:node", "cap:node:reboot"));
        assert!(cap_matches("*", "cap:whatever:x"));
    }

    #[test]
    fn tasks_projection_full_lifecycle() {
        let entries = vec![
            task_entry(
                1,
                json!({"topic": "task_spec", "task_id": "t1", "type": "deploy.demo",
                        "intent": "smoke", "correlation": "batchX"}),
                "orch",
                "worker",
            ),
            task_entry(
                2,
                json!({"topic": "task_accepted", "task_id": "t1"}),
                "worker",
                "orch",
            ),
            task_entry(
                3,
                json!({"topic": "task_progress", "task_id": "t1"}),
                "worker",
                "orch",
            ),
            task_entry(
                4,
                json!({"topic": "task_progress", "task_id": "t1"}),
                "worker",
                "orch",
            ),
            task_entry(
                5,
                json!({"topic": "task_succeeded", "task_id": "t1"}),
                "worker",
                "orch",
            ),
            // non-task chatter + specless junk must be ignored
            task_entry(6, json!({"topic": "probe", "task_id": "t9"}), "a", "b"),
            task_entry(7, json!({"topic": "task_progress"}), "a", "b"),
        ];
        let f = TaskFilter::default();
        let tasks = tasks_projection(&entries, &f);
        assert_eq!(tasks.len(), 1);
        let t = &tasks[0];
        assert_eq!(t.task_id, "t1");
        assert_eq!(t.state, "succeeded");
        assert!(t.terminal);
        assert_eq!(t.correlation.as_deref(), Some("batchX"));
        assert_eq!(t.task_type.as_deref(), Some("deploy.demo"));
        assert_eq!(t.progress_count, 2);
        assert_eq!(t.message_count, 5);
        assert_eq!(t.seq, 5);
        assert!(t.spec.is_some());
    }

    #[test]
    fn tasks_projection_state_machine_edges() {
        let mk = |seq: u64, topic: &str, id: &str| {
            task_entry(
                seq,
                json!({"topic": topic, "task_id": id, "correlation": "c"}),
                "o",
                "w",
            )
        };
        let entries = vec![
            mk(1, "task_spec", "a"),
            mk(2, "task_accepted", "a"),
            mk(3, "task_cancel", "a"), // cancel without confirmation
            mk(4, "task_spec", "b"),
            mk(5, "task_declined", "b"),
            mk(6, "task_spec", "c"),
            mk(7, "task_failed", "c"), // failed, no later accepted -> stays failed
            mk(8, "task_spec", "d"),
            mk(9, "task_failed", "d"),
            mk(10, "task_accepted", "d"), // retry re-dispatch -> running
            mk(11, "task_accepted", "e"), // no spec in ring: still appears
        ];
        let f = TaskFilter::default();
        let tasks = tasks_projection(&entries, &f);
        let by = |id: &str| tasks.iter().find(|t| t.task_id == id).unwrap();
        assert_eq!(by("a").state, "cancelling");
        assert!(!by("a").terminal);
        assert_eq!(by("b").state, "declined");
        assert!(by("b").terminal);
        assert_eq!(by("c").state, "failed");
        assert!(by("c").terminal);
        assert_eq!(by("d").state, "running");
        assert!(!by("d").terminal);
        // specless task: null type/correlation, state from the trail alone
        assert_eq!(by("e").state, "running");
        assert!(by("e").spec.is_none());
        assert!(by("e").correlation.is_none());
        assert_eq!(by("e").task_type, None);
        // hard terminal is sticky: a late message cannot resurrect it
        let entries2 = vec![
            mk(1, "task_spec", "b"),
            mk(2, "task_declined", "b"),
            mk(3, "task_accepted", "b"),
        ];
        let tasks2 = tasks_projection(&entries2, &f);
        assert_eq!(tasks2[0].state, "declined");
    }

    #[test]
    fn tasks_projection_filters() {
        let spec = |seq: u64, id: &str, corr: &str| {
            task_entry(
                seq,
                json!({"topic": "task_spec", "task_id": id, "correlation": corr, "type": "x.y"}),
                if corr == "batchX" { "orch" } else { "other" },
                if corr == "batchX" {
                    "worker"
                } else {
                    "elsewhere"
                },
            )
        };
        let entries = vec![
            spec(1, "t1", "batchX"),
            task_entry(
                2,
                json!({"topic": "task_succeeded", "task_id": "t1"}),
                "worker",
                "orch",
            ),
            spec(3, "t2", "batchX"),
            task_entry(
                4,
                json!({"topic": "task_declined", "task_id": "t2"}),
                "worker",
                "orch",
            ),
            spec(5, "t3", "batchY"),
            task_entry(
                6,
                json!({"topic": "task_progress", "task_id": "t3"}),
                "elsewhere",
                "other",
            ),
        ];
        let ids = |ts: Vec<super::TaskView>| -> Vec<String> {
            ts.into_iter().map(|t| t.task_id).collect()
        };
        let states = vec!["succeeded".to_string(), "failed".to_string()];
        let base = TaskFilter::default();
        assert_eq!(
            ids(tasks_projection(&entries, &base)),
            vec!["t3", "t2", "t1"]
        ); // newest-first
        let f = TaskFilter {
            correlation: Some("batchX"),
            ..Default::default()
        };
        assert_eq!(ids(tasks_projection(&entries, &f)), vec!["t2", "t1"]);
        let f = TaskFilter {
            states: Some(&states),
            ..Default::default()
        };
        assert_eq!(ids(tasks_projection(&entries, &f)), vec!["t1"]);
        let f = TaskFilter {
            task_id: Some("t2"),
            ..Default::default()
        };
        assert_eq!(ids(tasks_projection(&entries, &f)), vec!["t2"]);
        let f = TaskFilter {
            agent: Some("worker"),
            ..Default::default()
        };
        assert_eq!(ids(tasks_projection(&entries, &f)), vec!["t2", "t1"]);
    }
}

/// Capability string matching (v0.5 kernel list §3). Capability grammar:
/// `cap:<domain>:<action>[:...]`, colon-hierarchical. A held capability may
/// end with a `*` segment that wildcards everything below it
/// (`cap:node:*` satisfies `cap:node:reboot`). A bare `*` satisfies anything.
/// Scheduling/matching policy itself is the AgentMesh paid layer; the core
/// only answers "which online agents satisfy the requirement".
pub fn cap_matches(have: &str, need: &str) -> bool {
    if have == need || have == "*" {
        return true;
    }
    let mut seg_h: Vec<&str> = have.split(':').collect();
    if seg_h.last().copied() != Some("*") {
        return false;
    }
    seg_h.pop();
    let seg_n: Vec<&str> = need.split(':').collect();
    seg_h.len() <= seg_n.len() && seg_n.starts_with(&seg_h)
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
    /// Append-only ring sequence (v0.5 §1). Monotonic per node, seeded at
    /// boot from the restored history so it survives restarts. Legacy lines
    /// written before v0.5 carry 0.
    #[serde(default)]
    pub seq: u64,
    /// Reply chain link: msg_id of the message this one answers (if any).
    /// Used by the console star-map to group messages into topics. Absent in
    /// history lines written before v0.4.16 (serde default = None).
    #[serde(default)]
    pub reply_to: Option<String>,
    /// v0.5 §7.1: trace id of the message (absent for lines written by
    /// older nodes and for events without a message context).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// Incremental event-stream read result for `/v1/events` (v0.5 §1).
#[derive(Debug)]
pub struct EventsView {
    pub events: Vec<HistoryEntry>,
    /// Watermark: the seq the next appended event will carry.
    pub next_seq: u64,
    /// Oldest seq still in the ring (None when the ring is empty).
    pub oldest_seq: Option<u64>,
    /// True when `since` points below the ring after a rotation: events were
    /// lost, caller must rebuild from full `/v1/history` (HTTP 409).
    pub gap: bool,
}

/// Filter for the `/v1/tasks` projection (v0.5 kernel list §4). All fields
/// are optional; unset fields match everything.
#[derive(Debug, Default)]
pub struct TaskFilter<'a> {
    /// Exact `task_spec.correlation` match.
    pub correlation: Option<&'a str>,
    /// Keep tasks whose current state is one of these (empty = any).
    pub states: Option<&'a [String]>,
    /// Exact `task_id` match.
    pub task_id: Option<&'a str>,
    /// Tasks this agent participates in (as from_agent/to_agent of any
    /// message in the trajectory).
    pub agent: Option<&'a str>,
    /// Max tasks per response; applied by [`Router::tasks_view`] so the
    /// caller can still report `total` beyond the page.
    pub limit: usize,
}

/// One task projected from its message trajectory (v0.5 §4). Derived view
/// only — the history ring stays the single append-only source of truth.
#[derive(Debug, Clone, Serialize)]
pub struct TaskView {
    pub task_id: String,
    pub state: String,
    #[serde(rename = "type")]
    pub task_type: Option<String>,
    pub intent: Option<String>,
    pub correlation: Option<String>,
    pub from_agent: Option<String>,
    pub to_agent: Option<String>,
    /// The `task_spec` payload as recorded (already observer-sanitized by
    /// the redaction pipeline, §2). Null when the ring holds no spec.
    pub spec: Option<serde_json::Value>,
    /// Seq of the task's most recent message in the ring.
    pub seq: u64,
    pub created_ts: String,
    pub last_ts: String,
    pub progress_count: u64,
    pub message_count: u64,
    pub terminal: bool,
}

/// Task-protocol topics (docs/task-protocol.md §1). Anything else in a
/// payload (plain `topic: "probe"` chatter, etc.) is not a task message.
pub fn is_task_topic(topic: &str) -> bool {
    matches!(
        topic,
        "task_spec"
            | "task_accepted"
            | "task_declined"
            | "task_progress"
            | "task_succeeded"
            | "task_failed"
            | "task_cancel"
            | "task_cancelled"
            | "task_rollback"
            | "task_rolled_back"
            | "task_ack"
            | "task_go"
    )
}

/// States after which no later message changes the projection again
/// (protocol §1/§3: these are the executor-authoritative terminals).
fn is_hard_terminal(state: &str) -> bool {
    matches!(
        state,
        "succeeded" | "declined" | "cancelled" | "rolled_back"
    )
}

/// Project task messages from the history ring into task objects, replaying
/// the protocol §3 state machine per `task_id` in ring (seq) order.
///
/// Kernel-side simplification, per v0.5 list §4: a `task_failed` report
/// shows as `failed` unless a later `task_accepted` arrives (retry/re-
/// dispatch — the orchestrator may re-dispatch via §4 reconcile, and that
/// next attempt's accepted moves the task back to `running`). `cancelling`
/// is a kernel-visible intermediate for a `task_cancel` instruction whose
/// `task_cancelled` confirmation has not landed yet. `task_progress` never
/// changes state; `monitor`-mode tasks that never report succeeded are the
/// orchestrator's semantics — the kernel projects the message trail as-is.
/// Returns matches sorted newest-first by (seq, ts); `TaskFilter::limit` is
/// *not* applied here — see [`Router::tasks_view`].
pub fn tasks_projection(entries: &[HistoryEntry], f: &TaskFilter) -> Vec<TaskView> {
    #[derive(Default)]
    struct Acc {
        state: String,
        task_type: Option<String>,
        intent: Option<String>,
        correlation: Option<String>,
        from_agent: Option<String>,
        to_agent: Option<String>,
        spec: Option<serde_json::Value>,
        seq: u64,
        created_ts: String,
        last_ts: String,
        progress_count: u64,
        message_count: u64,
    }

    let mut order: Vec<String> = Vec::new();
    let mut accs: HashMap<String, Acc> = HashMap::new();
    for e in entries {
        let topic = e
            .payload
            .get("topic")
            .and_then(|t| t.as_str())
            .unwrap_or("");
        if !is_task_topic(topic) {
            continue;
        }
        let Some(task_id) = e.payload.get("task_id").and_then(|t| t.as_str()) else {
            continue; // no id to group by: not projectable
        };
        if task_id.is_empty() {
            continue;
        }
        let acc = accs.entry(task_id.to_string()).or_insert_with(|| {
            order.push(task_id.to_string());
            Acc {
                state: "pending".to_string(),
                created_ts: e.ts.clone(),
                ..Default::default()
            }
        });
        acc.message_count += 1;
        acc.seq = acc.seq.max(e.seq);
        acc.last_ts = e.ts.clone();
        match topic {
            "task_spec" => {
                if acc.spec.is_none() {
                    acc.spec = Some(e.payload.clone());
                    acc.task_type = e
                        .payload
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    acc.intent = e
                        .payload
                        .get("intent")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    acc.correlation = e
                        .payload
                        .get("correlation")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    acc.from_agent = e.from_agent.clone();
                    acc.to_agent = e.to_agent.clone();
                }
            }
            _ => {
                if acc.from_agent.is_none() {
                    acc.from_agent = e.from_agent.clone();
                }
                if acc.to_agent.is_none() {
                    acc.to_agent = e.to_agent.clone();
                }
            }
        }
        if topic == "task_progress" {
            acc.progress_count += 1;
        } else if !is_hard_terminal(&acc.state) {
            let next = match topic {
                "task_accepted" => Some("running"),
                "task_succeeded" => Some("succeeded"),
                "task_declined" => Some("declined"),
                "task_cancelled" => Some("cancelled"),
                "task_rolled_back" => Some("rolled_back"),
                "task_failed" => Some("failed"),
                "task_cancel" => Some("cancelling"),
                _ => None,
            };
            if let Some(s) = next {
                acc.state = s.to_string();
            }
        }
    }

    let mut tasks: Vec<TaskView> = order
        .into_iter()
        .filter_map(|id| {
            let a = accs.remove(&id)?;
            Some(TaskView {
                terminal: is_hard_terminal(&a.state) || a.state == "failed",
                task_id: id,
                state: a.state,
                task_type: a.task_type,
                intent: a.intent,
                correlation: a.correlation,
                from_agent: a.from_agent,
                to_agent: a.to_agent,
                spec: a.spec,
                seq: a.seq,
                created_ts: a.created_ts,
                last_ts: a.last_ts,
                progress_count: a.progress_count,
                message_count: a.message_count,
            })
        })
        .collect();

    if let Some(c) = f.correlation {
        tasks.retain(|t| t.correlation.as_deref() == Some(c));
    }
    if let Some(states) = f.states {
        if !states.is_empty() {
            tasks.retain(|t| states.iter().any(|s| s == &t.state));
        }
    }
    if let Some(id) = f.task_id {
        tasks.retain(|t| t.task_id == id);
    }
    if let Some(agent) = f.agent {
        tasks.retain(|t| {
            t.from_agent.as_deref() == Some(agent) || t.to_agent.as_deref() == Some(agent)
        });
    }
    // Newest activity first: seq is the append-only ring order; ts breaks
    // ties for legacy entries (seq == 0).
    tasks.sort_by(|a, b| b.seq.cmp(&a.seq).then_with(|| b.last_ts.cmp(&a.last_ts)));
    tasks
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
    /// Next sequence number for history/event entries (v0.5 §1).
    next_seq: AtomicU64,
    /// Count of ring evictions this process has performed; > 0 means events
    /// below the current ring head are no longer streamable (v0.5 §1, 409).
    evicted: AtomicU64,
    /// Observer-side payload sanitizer (v0.5 §2). Applied when history
    /// entries are built — never on the delivery path.
    redactor: crate::redact::Redactor,
    /// v0.5 §7.1: msg_id -> trace_id continuation map (bounded; a miss only
    /// degrades to deriving a fresh trace id, never to data loss).
    trace_map: Mutex<crate::trace::TraceMap>,
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

        // v0.5 §1: resume the append-only sequence above whatever the
        // restored ring already carries, so seq survives restarts.
        let seed_seq = history.iter().map(|e| e.seq).max().unwrap_or(0) + 1;

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
            next_seq: AtomicU64::new(seed_seq),
            evicted: AtomicU64::new(0),
            redactor: crate::redact::Redactor::from_config(&cfg.redact),
            trace_map: Mutex::new(crate::trace::TraceMap::new(TRACE_MAP_CAP)),
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
            seq: 0,
            reply_to: env.reply_to,
            trace_id: env.trace_id,
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
        // v0.5 §7.1 structured twin (debug level: default text output of the
        // existing fleet stays byte-identical). `from` carries the peer node.
        tracing::debug!(
            trace_id = "-", correlation_id = "-", msg_id = "-",
            from = %sink.node_id, to = "-", from_agent = "-", to_agent = "-",
            node_id = %self.cfg.node_id, topic = "peer",
            "al.peer.up"
        );
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
            tracing::debug!(
                trace_id = "-", correlation_id = "-", msg_id = "-",
                from = %node_id, to = "-", from_agent = "-", to_agent = "-",
                node_id = %self.cfg.node_id, topic = "peer",
                "al.peer.down"
            );
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

    /// Resolve the trace id for an envelope and inject it into the frame
    /// metadata (`trace_id`), so forwarding and local delivery carry it on
    /// to downstream agents (v0.5 §7.1). Idempotent: an already-injected
    /// envelope simply keeps its id. Injection is protocol-safe because the
    /// envelope HMAC covers `from|msg_id|ts` only and serde ignores unknown
    /// fields on read, so legacy peers still verify and route the frame.
    fn trace_for(&self, env: &mut Envelope) -> String {
        let t = crate::trace::resolve_and_remember(&mut self.trace_map.lock().unwrap(), env);
        env.trace_id = Some(t.clone());
        t
    }

    /// Send an envelope addressed to a specific peer or `"*"`.
    pub async fn send(&self, mut env: Envelope) -> anyhow::Result<String> {
        env.v = PROTOCOL_VERSION;
        env.from = self.cfg.node_id.clone();
        if env.msg_id.is_empty() {
            return Err(anyhow::anyhow!("envelope needs a msg_id"));
        }
        // Trace metadata is resolved *before* signing; the signature input
        // does not include it, so strict-mode verification is unaffected.
        let trace_id = self.trace_for(&mut env);
        crate::trace::log_event(
            tracing::Level::DEBUG,
            "message.send",
            &self.cfg.node_id,
            &trace_id,
            Some(&env),
        );
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
            let _ = self.handle_incoming(&mut env).await;
            return Ok(env.msg_id);
        }

        let is_broadcast = env.to == "*" || env.to.is_empty();

        if is_broadcast {
            self.record_history(&env, "out", "*");
            let sinks: Vec<PeerSink> = self.registry.all();
            for sink in sinks {
                if self.send_raw(&sink, &env).await {
                    crate::trace::log_event(
                        tracing::Level::DEBUG,
                        "message.forward",
                        &self.cfg.node_id,
                        &trace_id,
                        Some(&env),
                    );
                }
            }
            // Deliver to our own local subscribers too.
            self.deliver_local(env.clone()).await;
            return Ok(env.msg_id);
        }

        self.record_history(&env, "out", &env.to);
        match self.registry.get(&env.to) {
            Some(sink) => {
                if self.send_raw(&sink, &env).await {
                    crate::trace::log_event(
                        tracing::Level::DEBUG,
                        "message.forward",
                        &self.cfg.node_id,
                        &trace_id,
                        Some(&env),
                    );
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
    pub async fn handle_incoming(&self, env: &mut Envelope) -> bool {
        if !self.verify_incoming(env) {
            tracing::warn!(msg_id = %env.msg_id, from = %env.from, "router: incoming rejected (signature or timestamp)");
            let trace_id = crate::trace::dash(&env.trace_id);
            crate::trace::log_event(
                tracing::Level::DEBUG,
                "message.reject",
                &self.cfg.node_id,
                &trace_id,
                Some(env),
            );
            return false;
        }
        // Continue or start the trace chain for this inbound frame. A frame
        // from a legacy upstream may arrive without trace_id: we derive or
        // map it here, and the injected value rides along into history and
        // local delivery (downstream agents can echo it back).
        let trace_id = self.trace_for(env);
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
            crate::trace::log_event(
                tracing::Level::DEBUG,
                "message.incoming",
                &self.cfg.node_id,
                &trace_id,
                Some(env),
            );
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
        let trace_id = crate::trace::dash(&env.trace_id);
        if self.events.receiver_count() == 0 {
            let mut inbox = self.inbox.lock().await;
            if inbox.len() >= INBOX_MAX {
                if let Some(dropped) = inbox.pop_front() {
                    self.j(store::Rec::del("inbox", &dropped.msg_id));
                    tracing::warn!(msg_id = %dropped.msg_id, "router: inbox overflow, dropped oldest");
                }
            }
            self.j(store::Rec::add_env("inbox", env.clone()));
            crate::trace::log_event(
                tracing::Level::DEBUG,
                "message.inbox",
                &self.cfg.node_id,
                &trace_id,
                Some(&env),
            );
            inbox.push_back(env);
        } else {
            crate::trace::log_event(
                tracing::Level::DEBUG,
                "message.deliver",
                &self.cfg.node_id,
                &trace_id,
                Some(&env),
            );
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
                crate::trace::log_event(
                    tracing::Level::DEBUG,
                    "message.fail",
                    &self.cfg.node_id,
                    &crate::trace::dash(&pend.env.trace_id),
                    Some(&pend.env),
                );
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
        crate::trace::log_event(
            tracing::Level::DEBUG,
            "message.defer",
            &self.cfg.node_id,
            &crate::trace::dash(&env.trace_id),
            Some(env),
        );
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
        tracing::debug!(
            trace_id = "-", correlation_id = "-", msg_id = "-",
            from = %self.cfg.node_id, to = "-", from_agent = "-",
            to_agent = %agent_id, node_id = %self.cfg.node_id, topic = "registry",
            agent = ?rec.name, "al.agent.register"
        );
        self.persist_agents();
        self.sync_push_agents();
        rec
    }

    pub fn unregister_agent(&self, agent_id: &str) -> bool {
        let removed = self.agents.lock().unwrap().remove(agent_id).is_some();
        if removed {
            tracing::debug!(
                trace_id = "-", correlation_id = "-", msg_id = "-",
                from = %self.cfg.node_id, to = "-", from_agent = %agent_id,
                to_agent = "-", node_id = %self.cfg.node_id, topic = "registry",
                "al.agent.unregister"
            );
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
            seq: 0,
            reply_to: env.reply_to.clone(),
            trace_id: env.trace_id.clone(),
            // v0.5 §2: observer copy is sanitized; the delivered frame above
            // (routed from `env` directly) still carries the original payload.
            payload: self.redactor.redact_value(&env.payload),
        };
        self.push_history(entry);
    }

    /// Push into the ring buffer and (when configured) the durable JSONL file.
    fn push_history(&self, mut entry: HistoryEntry) {
        entry.seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut h = self.history.lock().unwrap();
            if h.len() >= self.cfg.history.len.max(1) {
                h.pop_front();
                self.evicted.fetch_add(1, Ordering::Relaxed);
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
                    self.cfg.history.archive_days,
                );
                // v0.5 §7.1: history 落盘 as a structured event (debug level).
                let t = crate::trace::dash(&entry.trace_id);
                let corr =
                    crate::trace::payload_field(&entry.payload, "correlation").unwrap_or("-");
                let topic = crate::trace::payload_field(&entry.payload, "topic").unwrap_or("-");
                tracing::debug!(
                    trace_id = %t, correlation_id = %corr, msg_id = %entry.msg_id,
                    from = %entry.from, to = %entry.to,
                    from_agent = %crate::trace::dash(&entry.from_agent),
                    to_agent = %crate::trace::dash(&entry.to_agent),
                    node_id = %self.cfg.node_id, topic = %topic,
                    direction = %entry.direction, seq = entry.seq,
                    "al.history.persist"
                );
                if self.cfg.history.archive_keep > 0 {
                    store::prune_archives(&self.cfg.data_dir, self.cfg.history.archive_keep);
                }
            }
        }
    }

    pub fn history(&self) -> Vec<HistoryEntry> {
        let h = self.history.lock().unwrap();
        h.iter().cloned().collect()
    }

    /// Result of an incremental event-stream read (v0.5 §1, `/v1/events`).
    pub fn events_view(
        &self,
        since: u64,
        agent: Option<&str>,
        types: Option<&[String]>,
        limit: usize,
    ) -> EventsView {
        let h = self.history.lock().unwrap();
        let next_seq = self.next_seq.load(Ordering::Relaxed);
        let oldest_seq = h.front().map(|e| e.seq);
        let evicted = self.evicted.load(Ordering::Relaxed) > 0;
        // A requested `since` below the ring head after a rotation means the
        // caller missed events; they must rebuild from full history (409).
        let gap = since > 0 && evicted && oldest_seq.is_none_or(|o| since < o);
        let mut events: Vec<HistoryEntry> = Vec::new();
        if !gap {
            for e in h.iter() {
                if e.seq < since {
                    continue;
                }
                if let Some(a) = agent {
                    if e.from_agent.as_deref() != Some(a) && e.to_agent.as_deref() != Some(a) {
                        continue;
                    }
                }
                if let Some(ts) = types {
                    if !ts.is_empty() && !ts.contains(&e.kind) {
                        continue;
                    }
                }
                events.push(e.clone());
                if events.len() >= limit {
                    break;
                }
            }
        }
        EventsView {
            events,
            next_seq,
            oldest_seq,
            gap,
        }
    }

    /// Task-resource read for `/v1/tasks` (v0.5 §4): pure projection over
    /// the history ring — no independent storage, the append-only ring stays
    /// the single source of truth. Returns the page (clamped by
    /// `TaskFilter::limit`) plus `total`, the match count before paging.
    pub fn tasks_view(&self, f: &TaskFilter) -> (Vec<TaskView>, usize) {
        let h = self.history.lock().unwrap();
        let entries: Vec<HistoryEntry> = h.iter().cloned().collect();
        drop(h);
        let all = tasks_projection(&entries, f);
        let total = all.len();
        let limit = f.limit.max(1);
        (all.into_iter().take(limit).collect(), total)
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
        from_agent: Option<&str>,
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
            from_agent: from_agent.map(|s| s.to_string()),
            to: to.to_string(),
            to_agent: None,
            kind: "file".to_string(),
            action: "file".to_string(),
            seq: 0,
            reply_to: None,
            trace_id: None,
            payload: self.redactor.redact_value(&serde_json::json!({
                "file_name": file_name,
                "size": size,
                "saved_path": saved_path,
            })),
        };
        self.push_history(entry);
    }
}
