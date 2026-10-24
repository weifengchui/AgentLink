// SPDX-License-Identifier: Apache-2.0
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

    pub fn is_empty(&self) -> bool {
        self.map.read().unwrap().is_empty()
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
    /// v0.7 §4.2 多租户：agent 所属 namespace（缺省 = "default"；"*" = 共享池）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        cap_matches, lockdown_blocks_frame, tasks_projection, Config, Envelope, HistoryEntry,
        Router, TaskFilter, HELLO_AGENT,
    };
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn router_hello_self_reply() {
        // v0.5 收官：__node__ hello 由内核自应答，不投递本地 agent、不进 inbox。
        let mut cfg = Config::generate(
            Some("router-hello-test".to_string()),
            Some("tok".to_string()),
            Some("open".to_string()),
        );
        cfg.data_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("router-hello-test");
        cfg.history.enabled = true;
        cfg.history.file = String::new();
        cfg.delivery.persist = false;
        let node = cfg.node_id.clone();
        let r = Arc::new(Router::new(&cfg));

        let req = Envelope::business(
            &node,
            &node,
            "text",
            "hello",
            json!({ "probe": "p1" }),
            None,
            None,
            None,
            Some("cli".to_string()),
            Some(HELLO_AGENT.to_string()),
        );
        let msg_id = req.msg_id.clone();
        let dup = req.clone();
        r.send(req).await.unwrap();
        // 重复帧（同 msg_id）不应产生第二次应答。
        let mut dup = dup;
        r.handle_incoming(&mut dup).await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let h = r.history();
        let replies: Vec<_> = h.iter().filter(|e| e.action == "hello_reply").collect();
        assert_eq!(replies.len(), 1, "hello_reply 应恰好一条: {h:?}");
        let reply = &replies[0];
        assert_eq!(reply.reply_to.as_deref(), Some(msg_id.as_str()));
        assert_eq!(reply.from_agent.as_deref(), Some(HELLO_AGENT));
        assert_eq!(reply.to_agent.as_deref(), Some("cli"));
        assert_eq!(reply.payload["probe"], json!("p1"));
        assert_eq!(reply.payload["name"], json!("router-hello-test"));
        assert!(reply.payload["version"].is_string());
        assert!(reply.payload["protocol"].is_number());
        // hello 帧与应答都留审计，但都不进 inbox。
        assert!(h.iter().any(|e| e.action == "hello" && e.direction == "in"));
        assert_eq!(r.inbox_len(), 0);
    }

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
            thread: None,
            trace_id: None,
            payload,
            blocked: None,
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
    fn lockdown_blocks_frame_rules() {
        // v0.5 §4.5 薄档：业务帧拦截，控制面帧放行；HTTP 查询面不经此判定，天然畅通。
        assert!(lockdown_blocks_frame("msg"));
        assert!(lockdown_blocks_frame("file"));
        assert!(lockdown_blocks_frame("task"));
        assert!(!lockdown_blocks_frame("ctrl"));
        assert!(!lockdown_blocks_frame("event"));
    }

    #[test]
    fn tasks_projection_blocked_frames_marked_not_replayed() {
        let mk = |seq: u64, topic: &str, blocked: bool| {
            let mut e = task_entry(
                seq,
                json!({"topic": topic, "task_id": "t9", "type": "deploy.demo",
                        "intent": "i", "correlation": "c9"}),
                "worker",
                "orch",
            );
            e.blocked = if blocked { Some(true) } else { None };
            e
        };
        let spec = mk(1, "task_spec", false);
        let blocked_acc = mk(2, "task_accepted", true);
        // 被拦的 accept 不推进状态机：任务保持 pending，但 trail 里有它（带 blocked 标记）。
        let out = tasks_projection(&[spec.clone(), blocked_acc.clone()], &TaskFilter::default());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].state, "pending");
        assert_eq!(out[0].blocked_count, 1);
        assert_eq!(out[0].trail.len(), 2);
        assert!(out[0].trail[1].blocked);
        assert_eq!(out[0].message_count, 2);
        // 熔断解除后重发的 accept + succeeded 正常推进到终态，trail 完整保留被拦那一帧。
        let acc = mk(3, "task_accepted", false);
        let done = mk(4, "task_succeeded", false);
        let out2 = tasks_projection(&[spec, blocked_acc, acc, done], &TaskFilter::default());
        assert_eq!(out2[0].state, "succeeded");
        assert!(out2[0].terminal);
        assert_eq!(out2[0].blocked_count, 1);
        assert_eq!(out2[0].trail.len(), 4);
        assert_eq!(out2[0].trail[0].topic, "task_spec");
        assert_eq!(out2[0].trail[3].topic, "task_succeeded");
        assert!(!out2[0].trail[3].blocked);
    }

    #[tokio::test]
    async fn router_lockdown_gates_send_and_incoming() {
        let mut cfg = Config::generate(
            Some("router-lockdown-test".to_string()),
            Some("tok".to_string()),
            Some("trust".to_string()),
        );
        cfg.data_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("router-lockdown-test");
        cfg.history.enabled = true;
        cfg.history.file = String::new();
        cfg.delivery.persist = false;
        let node = cfg.node_id.clone();
        let r = Router::new(&cfg);
        assert!(!r.lockdown_on());

        let biz = || {
            Envelope::business(
                &node,
                &node,
                "msg",
                "send",
                json!({ "content": "hello" }),
                None,
                None,
                None,
                Some("a1".to_string()),
                Some("b1".to_string()),
            )
        };
        // 未熔断：业务帧畅通。
        r.send(biz()).await.unwrap();
        assert_eq!(r.blocked_count(), 0);

        // 熔断开启：本机发出侧业务帧被拦，带审计标记并计数。
        r.set_lockdown(true);
        let blocked = biz();
        let err = r.send(blocked.clone()).await.unwrap_err().to_string();
        assert!(err.contains("lockdown"), "err={err}");
        let (on, out, _) = r.lockdown_stats();
        assert!(on);
        assert_eq!(out, 1);
        assert_eq!(r.blocked_count(), 1);
        let h = r.history();
        assert!(h
            .iter()
            .any(|e| e.blocked == Some(true) && e.msg_id == blocked.msg_id));

        // 控制面帧照常放行，不计数。
        let ctrl = Envelope::business(
            &node,
            &node,
            "ctrl",
            "status",
            json!({ "ok": true }),
            None,
            None,
            None,
            None,
            None,
        );
        r.send(ctrl).await.unwrap();
        assert_eq!(r.lockdown_stats().1, 1);

        // 入站业务帧同样被拦：handle_incoming 返回 false，不进投递链。
        let mut peer = Envelope::business(
            "peer-node",
            &node,
            "msg",
            "send",
            json!({ "content": "x" }),
            None,
            None,
            None,
            None,
            Some("b1".to_string()),
        );
        assert!(!r.handle_incoming(&mut peer).await);
        assert_eq!(r.lockdown_stats().2, 1);

        // 恢复后新业务帧放行；被拦帧不补投（此后不再新增 blocked 记录）。
        r.set_lockdown(false);
        r.send(biz()).await.unwrap();
        let h2 = r.history();
        assert_eq!(h2.iter().filter(|e| e.blocked == Some(true)).count(), 2);
        assert!(h2.iter().any(|e| e.blocked.is_none() && e.action == "send"));
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
    fn tasks_projection_as_of_time_travel() {
        let mk = |seq: u64, topic: &str| {
            task_entry(
                seq,
                json!({"topic": topic, "task_id": "t1", "correlation": "c"}),
                "w",
                "o",
            )
        };
        let entries = vec![
            mk(1, "task_spec"),
            mk(2, "task_accepted"),
            mk(3, "task_progress"),
            mk(4, "task_succeeded"),
        ];
        // 缺省：全环回放，现状语义不变
        let t = &tasks_projection(&entries, &TaskFilter::default())[0];
        assert_eq!(t.state, "succeeded");
        assert_eq!(t.message_count, 4);
        // ts 截断到 accepted 帧（task_entry ts 按 seq 递增）：running
        let f = TaskFilter {
            as_of: Some("2026-10-17T02:00:00+08:00"),
            ..Default::default()
        };
        let t = &tasks_projection(&entries, &f)[0];
        assert_eq!(t.state, "running");
        assert_eq!(t.message_count, 2);
        assert_eq!(t.trail.len(), 2);
        assert!(!t.terminal);
        // seq 截断到 spec 帧：pending
        let f = TaskFilter {
            as_of_seq: Some(1),
            ..Default::default()
        };
        let t = &tasks_projection(&entries, &f)[0];
        assert_eq!(t.state, "pending");
        assert_eq!(t.message_count, 1);
        // 双截断取交集：as_of 覆盖全环、seq 截到 progress 帧
        let f = TaskFilter {
            as_of: Some("2026-10-17T09:00:00+08:00"),
            as_of_seq: Some(3),
            ..Default::default()
        };
        let t = &tasks_projection(&entries, &f)[0];
        assert_eq!(t.state, "running");
        assert_eq!(t.message_count, 3);
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

/// Capability string matching. Capability grammar:
/// `cap:<domain>:<action>[:...]`, colon-hierarchical. A held capability may
/// end with a `*` segment that wildcards everything below it
/// (`cap:node:*` satisfies `cap:node:reboot`). A bare `*` satisfies anything.
/// Scheduling/matching policy itself is the closed-source paid layer; the core
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
    /// 消息组（v0.5.8 R2）：同一主题多回合往来的显式线程键（Envelope.thread
    /// 原样落账）。星图 buildTopics 优先按它归组，无则回退 reply_to 链根。
    /// Absent in history lines written before v0.5.8 (serde default = None).
    #[serde(default)]
    pub thread: Option<String>,
    /// v0.5 §7.1: trace id of the message (absent for lines written by
    /// older nodes and for events without a message context).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    /// v0.5 §4.5 薄档：该帧被 lockdown 熔断拦截、**未投递/转发**。缺省
    /// （None）＝正常帧。审计可见性标记：history / `/v1/events` / /web 均透出。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<bool>,
}

/// Reserved agent id for the node's built-in self-responder (v0.5 收官：
/// hello-world 自检). Frames addressed to `__node__` are answered by the
/// kernel itself and never reach local agents or the inbox.
pub const HELLO_AGENT: &str = "__node__";

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

/// Filter for the `/v1/tasks` projection (v0.5). All fields
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
    /// v0.7 §4.2 多租户：只看该 namespace 的任务（`None` = 全部）。
    pub namespace: Option<&'a str>,
    /// v0.8 包⑦ 事件溯源 + time-travel：只回放 `ts <= as_of`（RFC3339）的
    /// 帧再做协议 §3 投影；`None` = 回放全环（现状语义逐字节不变）。
    pub as_of: Option<&'a str>,
    /// v0.8 包⑦：只回放 `seq <= as_of_seq` 的帧；与 `as_of` 同给取交集。
    pub as_of_seq: Option<u64>,
}

/// 任务 spec 的 namespace（缺省/空 = `"default"`）。
pub fn task_namespace(spec: Option<&serde_json::Value>) -> &str {
    spec.and_then(|s| s.get("namespace"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
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
    /// 轨迹中被 lockdown 拦截（未投递/转发）的帧数（v0.5 §4.5 薄档）。
    pub blocked_count: u64,
    /// 帧轨迹（timeline 数据源）：按环序（seq 升序）的 task_* 消息清单，
    /// 供 /web『任务』页画 发起→确认→进度→终态 横向时间线。
    /// 被拦帧也在 trail 里、带 `blocked: true`，但不参与状态机回放。
    pub trail: Vec<TaskTrailItem>,
    pub terminal: bool,
}

/// One message on a task's trail (a timeline node). Deliberately thin:
/// topic + ts + direction, not full payloads — the history view has those.
#[derive(Debug, Clone, Serialize)]
pub struct TaskTrailItem {
    pub seq: u64,
    pub ts: String,
    pub topic: String,
    pub msg_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_agent: Option<String>,
    /// "in"/"out" relative to this node (the observing bus).
    pub direction: String,
    /// true = 该帧被 lockdown 拦截、未投递（v0.5 §4.5）。常显序列化，
    /// 便于 /web timeline 与冒烟断言无需区分缺省。
    pub blocked: bool,
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

/// v0.5 §4.5 薄档 lockdown 的**转发判定**（纯函数，单测覆盖）：
/// lockdown 熔断只拦**业务帧**——agent→agent 消息与任务协议帧（kind 为
/// `msg`/`text`/`file` 等业务种类）；**控制面帧**（`ctrl`：heartbeat/ack/
/// agents_update/注册/agent_status 等，以及 `event`：presence 广播）放行。
/// HTTP 查询面（`/v1/lockdown` 本身、`/v1/history*`、`/v1/events`、
/// `/v1/tasks`、`/web`）不经此判定，天然不受影响。
pub fn lockdown_blocks_frame(kind: &str) -> bool {
    kind != "ctrl" && kind != "event"
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
        blocked_count: u64,
        trail: Vec<TaskTrailItem>,
    }

    let mut order: Vec<String> = Vec::new();
    let mut accs: HashMap<String, Acc> = HashMap::new();
    for e in entries {
        if let Some(cap) = f.as_of_seq {
            if e.seq > cap {
                continue;
            }
        }
        if let Some(t) = f.as_of {
            // 截断生效时，不可解析的 ts 无法证明早于 as_of：不入回放。
            match proto::ts_le(&e.ts, t) {
                Some(true) => {}
                _ => continue,
            }
        }
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
        let blocked = e.blocked.unwrap_or(false);
        acc.message_count += 1;
        if blocked {
            acc.blocked_count += 1;
        }
        acc.seq = acc.seq.max(e.seq);
        acc.last_ts = e.ts.clone();
        acc.trail.push(TaskTrailItem {
            seq: e.seq,
            ts: e.ts.clone(),
            topic: topic.to_string(),
            msg_id: e.msg_id.clone(),
            from_agent: e.from_agent.clone(),
            to_agent: e.to_agent.clone(),
            direction: e.direction.clone(),
            blocked,
        });
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
        if blocked {
            // 被拦帧只留 trail 标记：不推进状态机、不计进度（v0.5 §4.5）。
        } else if topic == "task_progress" {
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
                blocked_count: a.blocked_count,
                trail: a.trail,
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
    if let Some(ns) = f.namespace {
        tasks.retain(|t| task_namespace(t.spec.as_ref()) == ns);
    }
    // Newest activity first: seq is the append-only ring order; ts breaks
    // ties for legacy entries (seq == 0).
    tasks.sort_by(|a, b| b.seq.cmp(&a.seq).then_with(|| b.last_ts.cmp(&a.last_ts)));
    tasks
}

/// v0.7 §6.3 节点降级模式。
/// - `normal`：常规；
/// - `read_only`：维护只读（写面 503，读面/在途收敛照常）；
/// - `standalone`：连续无直连对端超过阈值，本地队列自治（仅状态标记）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeshMode {
    Normal,
    ReadOnly,
    Standalone,
}

impl MeshMode {
    pub fn as_str(self) -> &'static str {
        match self {
            MeshMode::Normal => "normal",
            MeshMode::ReadOnly => "read_only",
            MeshMode::Standalone => "standalone",
        }
    }
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
    /// 闭源能力钩子（`ha`/`chaos`/`export` 的审计追加、
    /// 延迟注入、导出旁路）。`None` = 免费层：零行为，与历史版本逐字节一致。
    hook: std::sync::OnceLock<Arc<dyn crate::core_hook::CoreHook>>,
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
    /// v0.5 §4.5 薄档一键熔断：true 时总线暂停投递/转发**业务帧**
    /// （agent→agent 消息、任务帧，即 kind ∉ {ctrl, event}）；控制面帧
    /// （status/heartbeat/注册/lockdown 本身/history/events 查询）不受影响。
    /// 运行时状态，**不落盘**——节点重启默认恢复放行（人工应急闸语义）。
    /// 恢复后**不补投**被拦帧（v0.5 薄档；被拦帧只留审计标记）。
    lockdown: AtomicBool,
    /// 本机发出侧（`/v1/send` → 网络/本地投递前）被拦业务帧计数。
    blocked_out: AtomicU64,
    /// 本机入站侧（WS 帧到达本节点后被拦）计数。
    blocked_in: AtomicU64,
    /// 进程启动时刻——`__node__` hello 应答携带 uptime（v0.5 收官）。
    started: Instant,
    /// v0.6 P2 wake-kit 收敛：出站 webhook 客户端（`node_offline` /
    /// `audit_alert` / `agent_message` 等消息面事件由此送出；`cfg.webhook`
    /// 与引擎共用同一配置，任务面事件仍由 reconcile::Engine 发出）。
    webhook: Option<Arc<crate::api::WebhookClient>>,
    /// v0.7 spine/relay 中继参数（enabled / spine / upstream / max_hops 快照）。
    relay: crate::config::RelayConfig,
    /// v0.7 第二波：经中继学到的**节点目录**（非直连；TTL 见
    /// `DIR_TTL_SECONDS`，由中继节点每 `DIR_SYNC_INTERVAL_SECS` 刷新）。
    /// 只用于 `/v1/peers` 展示与 fleet 的 node_online 判定，不参与拨号。
    dir: Mutex<HashMap<String, DirEntry>>,
    /// v0.7 chaos：最近断开过的直连节点（node_id → 断开时刻）。用于把
    /// `node_online` 从「不在 peer 表」细化为「明确离线」，避免未知节点
    /// 被误判离线；重连即清除。
    offline_nodes: Mutex<HashMap<String, std::time::Instant>>,
    /// v0.7 §6.3 运行时只读开关（配置值只是启动初值）。
    read_only: AtomicBool,
    /// 自动 standalone 阈值（秒；0=关闭）。
    standalone_after_secs: u64,
    /// 最近一次有直连对端（连接/断开）的时刻；standalone 计时起点。
    last_peer_seen: Mutex<Instant>,
    /// v0.7 §6.2 正在优雅下线的节点（收到 node_drain）：不参与调度，
    /// 在途任务跳过 reclaim 宽限期立即换机；重连注册即清除。
    draining: Mutex<HashSet<String>>,
    /// v0.7 §4.2 远端节点 tags（随 agents_update 同步）：ABAC 调度过滤用。
    peer_tags: Mutex<HashMap<String, Vec<String>>>,
}

/// 一条中继学到的节点目录记录。
#[derive(Clone)]
struct DirEntry {
    node_id: String,
    name: String,
    addr: String,
    /// 来源中继（保留最初 spine，多级时指向叶子侧的 spine）。
    via: String,
    hops: u8,
    /// 本节点最近一次收到/刷新该条目的时刻（展示 last_seen 用）。
    updated: std::time::Instant,
    /// **绝对过期时间**（unix 秒，由最初的中继按 TTL 设定并原样转发）。
    /// 多级级联时若各跳各自按本地接收时间续期，离线的叶子会被环路互相
    /// 刷新、永不掉线——所以过期时间必须随目录帧原样传播、只由源头续期。
    expires_wall: i64,
}

/// 目录条目存活时间：中继每 5s 刷新，30s 未见即视为离开。
const DIR_TTL_SECONDS: u64 = 30;
/// 中继目录推送周期（秒；与 maintenance 的 1s tick 对齐）。
const DIR_SYNC_INTERVAL_SECS: u64 = 5;
/// 「最近断开」记忆时长（秒）：超过后节点重新按未知处理。
const OFFLINE_MEMORY_SECONDS: u64 = 300;

/// 路由层 Prometheus 指标。
#[derive(Debug, Clone, Default)]
pub struct RouterMetrics {
    pub peers_online: usize,
    pub pending_acks: usize,
    pub offline_queue_len: usize,
    pub local_pending_count: usize,
    pub inbox_len: usize,
    pub blocked_count: u64,
}

impl Router {
    pub fn new(cfg: &Config) -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let persist = cfg.delivery.persist;
        let journal = store::Journal::open_with_sync(
            &cfg.data_dir,
            persist,
            cfg.delivery.journal_sync_ms,
            cfg.delivery.journal_sync_max_records,
        );
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
            hook: std::sync::OnceLock::new(),
            trace_map: Mutex::new(crate::trace::TraceMap::new(TRACE_MAP_CAP)),
            journal,
            journal_ops: AtomicUsize::new(0),
            // Rewrite the journal from live state on the first maintenance
            // tick so replayed duplicates/expired entries do not accumulate.
            compact_due: AtomicBool::new(persist),
            agent_ttl,
            lockdown: AtomicBool::new(false),
            blocked_out: AtomicU64::new(0),
            blocked_in: AtomicU64::new(0),
            started: Instant::now(),
            webhook: crate::api::WebhookClient::new(&cfg.webhook).map(Arc::new),
            relay: cfg.relay.clone(),
            dir: Mutex::new(HashMap::new()),
            offline_nodes: Mutex::new(HashMap::new()),
            // mesh 是透传段，degraded 按键取值（缺省 false/0，
            // 与 `DegradedConfig::default()` 口径一致）。
            read_only: AtomicBool::new(
                cfg.mesh
                    .get("degraded")
                    .and_then(|d| d.get("read_only"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            ),
            standalone_after_secs: cfg
                .mesh
                .get("degraded")
                .and_then(|d| d.get("standalone_after_seconds"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            last_peer_seen: Mutex::new(Instant::now()),
            draining: Mutex::new(HashSet::new()),
            peer_tags: Mutex::new(HashMap::new()),
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
            thread: env.thread,
            trace_id: env.trace_id,
            payload: env.payload,
            blocked: None,
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
        *self.last_peer_seen.lock().unwrap() = Instant::now();
        self.draining.lock().unwrap().remove(&sink.node_id);
        self.offline_nodes.lock().unwrap().remove(&sink.node_id);
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

    /// v0.6 P2 wake-kit 收敛：对外共享的 webhook 客户端（引擎任务面事件
    /// 与本节点消息面事件共用同一 client：同一 url/secret/retries 策略）。
    pub fn webhook_client(&self) -> Option<Arc<crate::api::WebhookClient>> {
        self.webhook.clone()
    }

    /// v0.7 §4.4：追加不可变审计记录（actor=角色/peer；detail 不得含密钥）。
    /// 钩子未挂载/关闭/链损坏/写入失败返回 None（调用方不因此改变业务语义）。
    /// v0.8 M4：HA 启用后写 `audit` 命名空间（leader 串行、多数派提交）；
    /// follower 把记录转给 leader——细节全在闭源钩子实现内部。
    pub fn audit_event(&self, event: &str, actor: &str, detail: serde_json::Value) -> Option<u64> {
        let hook = self.hook.get()?;
        hook.audit_append(event, actor, detail)
    }

    /// 挂接闭源能力钩子（启动时调用一次）。
    pub fn attach_hook(&self, hook: Arc<dyn crate::core_hook::CoreHook>) {
        let _ = self.hook.set(hook);
    }

    pub fn notify(&self, event: &str, payload: serde_json::Value) {
        // v0.7 §4.4：所有 audit_alert 同步落不可变审计链（模块内开关控制）。
        // lockdown 由 API 处理器显式写（带 admin/operator 角色，payload 同形），
        // 这里跳过以免同一次切换落两条。
        if event == "audit_alert" {
            let kind = payload
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("audit_alert");
            if kind != "lockdown" {
                self.audit_event(kind, "peer", payload.clone());
            }
        }
        if let Some(wh) = &self.webhook {
            if wh.should_fire(event) {
                let wh = wh.clone();
                let node_id = self.cfg.node_id.clone();
                let event = event.to_string();
                if tokio::runtime::Handle::try_current().is_ok() {
                    tokio::spawn(async move {
                        wh.fire(event, node_id, payload).await;
                    });
                }
            }
        }
    }

    pub fn unregister_peer(&self, node_id: &str) {
        let removed = self.registry.remove(node_id).is_some();
        if removed {
            self.mark_node_offline(node_id);
            self.peer_tags.lock().unwrap().remove(node_id);
            // standalone 计时从最后一个直连对端断开起算。
            *self.last_peer_seen.lock().unwrap() = Instant::now();
        }
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
            // v0.6 P2 wake-kit 收敛：OnNodeOffline（覆盖 WS 断开/心跳超时/
            // 发送失败/主动 forget 等所有 unregister 路径；bye 另在 discovery 侧补）。
            self.notify(
                "node_offline",
                serde_json::json!({
                    "peer_id": node_id,
                    "agents_lost": cached.len(),
                    "reason": "unregistered",
                }),
            );
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

    /// v0.5 §4.5 薄档：一键熔断开关（管理面调用；运行时状态，不落盘）。
    /// 返回切换后的新状态。切换本身写日志，供审计定位熔断窗口。
    pub fn set_lockdown(&self, on: bool) -> bool {
        let prev = self.lockdown.swap(on, Ordering::Relaxed);
        if prev != on {
            tracing::warn!(
                on,
                blocked_total = self.blocked_count(),
                "router: lockdown {}",
                if on {
                    "ON — business frames blocked"
                } else {
                    "OFF — bus resumed (blocked frames are NOT replayed)"
                }
            );
            // v0.6 P2 wake-kit 收敛：OnAuditAlert（lockdown 切换是最高级
            // 审计事实——窗口期被拦帧只留审计标记，见上）。
            self.notify(
                "audit_alert",
                serde_json::json!({
                    "kind": "lockdown",
                    "on": on,
                    "blocked_total": self.blocked_count(),
                }),
            );
        }
        on
    }

    pub fn lockdown_on(&self) -> bool {
        self.lockdown.load(Ordering::Relaxed)
    }

    /// (on, blocked_out, blocked_in)
    pub fn lockdown_stats(&self) -> (bool, u64, u64) {
        (
            self.lockdown_on(),
            self.blocked_out.load(Ordering::Relaxed),
            self.blocked_in.load(Ordering::Relaxed),
        )
    }

    pub fn blocked_count(&self) -> u64 {
        self.blocked_out.load(Ordering::Relaxed) + self.blocked_in.load(Ordering::Relaxed)
    }

    /// Send an envelope addressed to a specific peer or `"*"`.
    pub async fn send(&self, mut env: Envelope) -> anyhow::Result<String> {
        env.v = PROTOCOL_VERSION;
        env.from = self.cfg.node_id.clone();
        if env.msg_id.is_empty() {
            return Err(anyhow::anyhow!("envelope needs a msg_id"));
        }
        // v0.5 §4.5 薄档熔断：lockdown 期间业务帧在本机出口直接拦截——
        // 不投递、不入离线队列、恢复后不补投。只留一条 blocked 审计标记。
        // 控制面帧（ctrl/event）照常，HTTP 查询面不经此路径、天然畅通。
        if self.lockdown_on() && lockdown_blocks_frame(&env.kind) {
            let n = self.blocked_out.fetch_add(1, Ordering::Relaxed) + 1;
            let node = if env.to == "*" || env.to.is_empty() {
                "*"
            } else {
                &env.to
            };
            self.record_history_marked(&env, "out", node, true);
            tracing::info!(msg_id = %env.msg_id, kind = %env.kind, to = %env.to, "router: lockdown blocked outbound business frame");
            return Err(anyhow::anyhow!(
                "lockdown: 总线熔断开启，业务帧已拦截（累计 {n}，恢复后不补投）"
            ));
        }
        // v0.7 chaotic latency：出口前注入（锁已释放；不持任何 Router 锁）。
        // 延迟表在闭源钩子实现内，core 只见 trait。
        let latency_ms = self.hook.get().map_or(0, |h| h.chaos_latency_ms());
        if latency_ms > 0 {
            tokio::time::sleep(Duration::from_millis(latency_ms)).await;
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
            None => {
                // v0.7 spine/relay：目标非直连 → 依次尝试上游中继（叶子用
                // `relay.spine`；spine 用 `relay.upstream` 级联）。下一跳=中继，
                // pending 也按中继记账：逐跳 ACK，端到端由每跳重试+接收端
                // dedup 兜底。
                for next_hop in self.upstream_candidates(&env.to) {
                    let Some(sink) = self.registry.get(&next_hop) else {
                        continue;
                    };
                    if self.send_raw(&sink, &env).await {
                        crate::trace::log_event(
                            tracing::Level::DEBUG,
                            "message.relay",
                            &self.cfg.node_id,
                            &trace_id,
                            Some(&env),
                        );
                        self.register_pending_at(&env, &next_hop, 0);
                        return Ok(env.msg_id);
                    }
                }
                self.enqueue_offline(&env).await
            }
        }
        Ok(env.msg_id)
    }

    /// 候选上游中继（去重、排除最终目标）：叶子优先 `spine`，spine 可再
    /// 用 `upstream` 级联；返回顺序即尝试顺序。
    fn upstream_candidates(&self, target: &str) -> Vec<String> {
        let mut out = Vec::new();
        for cand in [&self.relay.spine, &self.relay.upstream] {
            if cand.is_empty() || cand == target || out.contains(cand) {
                continue;
            }
            out.push(cand.clone());
        }
        out
    }

    /// v0.7 spine/relay：中继转发。仅由 `conn` 在「目标为第三方」或
    /// 「广播且本节点是 spine」时调用。
    ///
    /// 单跳中继语义：
    /// - 定向（`to`=第三方 node_id）：验签 + 去重 + hops 上限校验后，向目标
    ///   直连投递（`send_raw` 保留原 `from`，strict 下由共享 token 重签时间戳），
    ///   并在本跳记账 pending（本跳逐跳重试）；目标未知则丢弃并告警。
    /// - 广播（`to="*"`）：向除入站连接外的所有直连 peer 扩散 + 本地投递。
    ///
    /// 入站连接已由 `conn` 逐跳回 ACK（叶片等待的是 spine 的收件确认）。
    pub async fn handle_transit(&self, env: &mut Envelope, ingress: Option<&str>) -> bool {
        if !self.relay.enabled {
            tracing::debug!(
                msg_id = %env.msg_id,
                to = %env.to,
                "router: frame not addressed to us, dropped (relay disabled)"
            );
            return false;
        }
        if !self.verify_incoming(env) {
            tracing::warn!(msg_id = %env.msg_id, from = %env.from, "router: transit rejected (signature or timestamp)");
            self.notify(
                "audit_alert",
                serde_json::json!({
                    "kind": "transit_rejected",
                    "msg_id": env.msg_id,
                    "from": env.from,
                }),
            );
            return false;
        }
        // 环保护：hops 逐跳累加，超过上限丢弃（防环；接收端 dedup 兜重复）。
        let hops = env.hops.unwrap_or(0).saturating_add(1);
        if hops > self.relay.max_hops.max(1) {
            tracing::warn!(
                msg_id = %env.msg_id,
                to = %env.to,
                hops,
                "router: transit dropped (hop limit)"
            );
            return false;
        }
        let fresh = {
            let mut d = self.dedup.lock().unwrap();
            d.record(&env.msg_id)
        };
        if !fresh {
            tracing::debug!(msg_id = %env.msg_id, "router: transit duplicate dropped");
            return false;
        }
        env.hops = Some(hops);

        if env.to == "*" || env.to.is_empty() {
            self.relay_broadcast(env, ingress).await;
            return true;
        }

        let trace_id = crate::trace::dash(&env.trace_id);
        self.record_history(env, "in", &env.to);
        match self.registry.get(&env.to) {
            Some(sink) => {
                if self.send_raw(&sink, env).await {
                    crate::trace::log_event(
                        tracing::Level::DEBUG,
                        "message.relay",
                        &self.cfg.node_id,
                        &trace_id,
                        Some(env),
                    );
                    self.register_pending(env, 0);
                } else {
                    tracing::warn!(msg_id = %env.msg_id, to = %env.to, "router: transit send failed");
                    return false;
                }
            }
            None => {
                // v0.7 第二波：多级级联——spine 找不到目标直连时向
                // `relay.upstream` 兜底转发（不回弹给入站连接，防两跳乒乓）。
                let up = self.relay.upstream.clone();
                let can_up = !up.is_empty()
                    && up != env.to
                    && ingress != Some(up.as_str())
                    && self.registry.get(&up).is_some();
                if can_up {
                    if let Some(sink) = self.registry.get(&up) {
                        if self.send_raw(&sink, env).await {
                            crate::trace::log_event(
                                tracing::Level::DEBUG,
                                "message.relay",
                                &self.cfg.node_id,
                                &trace_id,
                                Some(env),
                            );
                            self.register_pending_at(env, &up, 0);
                            return true;
                        }
                    }
                }
                tracing::warn!(
                    msg_id = %env.msg_id,
                    to = %env.to,
                    upstream = %up,
                    "router: transit target not a direct peer, dropped（未知路由不与离线队列混用）"
                );
                return false;
            }
        }
        true
    }

    /// 广播中继：除入站连接外全部直连 peer 各发一份 + 本地投递。
    async fn relay_broadcast(&self, env: &Envelope, ingress: Option<&str>) {
        let trace_id = crate::trace::dash(&env.trace_id);
        self.record_history(env, "in", "*");
        let sinks: Vec<PeerSink> = self.registry.all();
        for sink in sinks {
            if ingress == Some(sink.node_id.as_str()) {
                continue;
            }
            let _ = self.send_raw(&sink, env).await;
        }
        crate::trace::log_event(
            tracing::Level::DEBUG,
            "message.relay",
            &self.cfg.node_id,
            &trace_id,
            Some(env),
        );
        self.deliver_local(env.clone()).await;
    }

    /// 控制面帧（`agents_update` 等）的中继扩散：除入站连接外全部直连 peer。
    pub async fn relay_control(&self, env: &Envelope, ingress: Option<&str>) {
        if !self.relay.enabled {
            return;
        }
        let hops = env.hops.unwrap_or(0).saturating_add(1);
        if hops > self.relay.max_hops.max(1) {
            return;
        }
        let mut env = env.clone();
        env.hops = Some(hops);
        for sink in self.registry.all() {
            if ingress == Some(sink.node_id.as_str()) {
                continue;
            }
            let _ = self.send_raw(&sink, &env).await;
        }
    }

    /// 订阅总线事件广播（M2 reconcile loop 消费原文 Envelope 用）。
    /// 返回的 receiver 与 `Router::send` 出口及 peer WS 投递路径同源，
    /// 事件为未脱敏的原始 Envelope，供引擎按 `payload.task_id` 匹配表行。
    pub fn events_subscribe(&self) -> broadcast::Receiver<Envelope> {
        self.events.subscribe()
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
        self.register_pending_at(env, &env.to, tries);
    }

    /// v0.7 spine/relay：按**下一跳**（而非最终目标）记账 pending——
    /// 逐跳 at-least-once，接收端 dedup 兜底端到端去重。
    fn register_pending_at(&self, env: &Envelope, next_hop: &str, tries: u32) {
        self.pending.lock().unwrap().insert(
            env.msg_id.clone(),
            Pending {
                peer_id: next_hop.to_string(),
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
            // v0.6 P2 wake-kit 收敛：OnAuditAlert（签名/时间戳拒绝 = 疑似
            // 伪造或时钟漂移，值得外呼）。
            self.notify(
                "audit_alert",
                serde_json::json!({
                    "kind": "incoming_rejected",
                    "msg_id": env.msg_id,
                    "from": env.from,
                }),
            );
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
        // v0.5 收官（§3.1/§3.4）：hello-world 自检。发往保留 agent
        // `__node__` 的 hello 帧由内核自身应答；内核发出的 hello_reply 同样
        // 由内核消费——两向都不投递本地 agent、不进 inbox/local_pending
        // （CLI/agent 经 /v1/events 读审计）。type 走业务线 `text`（协议
        // §3.1，与节点握手的 ctrl/hello 以 type 区分）；熔断检查之前拦截——
        // lockdown 期间 hello_reply 出站照常走 send()（业务帧被拦属预期）。
        let hello_in = env.to_agent.as_deref() == Some(HELLO_AGENT);
        let hello_reply_in =
            env.action == "hello_reply" && env.from_agent.as_deref() == Some(HELLO_AGENT);
        if (hello_in || hello_reply_in) && env.kind != "ctrl" && env.kind != "event" {
            let is_new = {
                let mut dedup = self.dedup.lock().unwrap();
                dedup.record(&env.msg_id)
            };
            if is_new {
                self.j(store::Rec::add_id("seen", &env.msg_id));
                self.record_history(env, "in", &env.from);
                if env.action == "hello" {
                    self.hello_reply(env).await;
                }
            }
            return true;
        }
        // v0.5 §4.5 薄档熔断：入站业务帧同样拦截（不投递给本地 agent、
        // 不记录为已送达）；返回 false 但连接层的 ack 照常——发送方据此
        // 停止重发，被拦帧恢复后不补投。控制面帧照常。
        if self.lockdown_on() && lockdown_blocks_frame(&env.kind) {
            self.blocked_in.fetch_add(1, Ordering::Relaxed);
            self.record_history_marked(env, "in", &env.from, true);
            tracing::info!(msg_id = %env.msg_id, kind = %env.kind, from = %env.from, "router: lockdown blocked inbound business frame");
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
        // v0.6 P2 wake-kit 收敛：定向业务帧（to_agent 非空、text/file）被
        // 本节点接收即算「投递事实」——无论落离线 inbox 还是投给在册 WS
        // 客户端，都发 `agent_message`（替代 wake_hook.py 侧车的内核标准实现;
        // 广播/控制帧不唤醒，与 wake-kit `agent` 自回声过滤语义由外部端点
        // 按 from_agent 自行过滤）。
        let wake_payload = if env.to_agent.is_some() && (env.kind == "text" || env.kind == "file") {
            Some(serde_json::json!({
                "msg_id": env.msg_id,
                "from": env.from,
                "from_agent": env.from_agent,
                "to_agent": env.to_agent,
                "type": env.kind,
                "topic": env.payload.get("topic").cloned().unwrap_or_default(),
                "reply_to": env.reply_to,
                "content": env.payload.get("content").cloned().unwrap_or(env.payload.clone()),
            }))
        } else {
            None
        };
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
            if let Some(mut wp) = wake_payload {
                wp["channel"] = serde_json::Value::String("inbox".into());
                self.notify("agent_message", wp);
            }
        } else {
            crate::trace::log_event(
                tracing::Level::DEBUG,
                "message.deliver",
                &self.cfg.node_id,
                &trace_id,
                Some(&env),
            );
            let _ = self.events.send(env);
            if let Some(mut wp) = wake_payload {
                wp["channel"] = serde_json::Value::String("live".into());
                self.notify("agent_message", wp);
            }
        }
    }

    // ---- acks ---------------------------------------------------------------

    /// v0.5 收官：构造并发送 `__node__` hello 应答帧。远端请求走正常
    /// `send()`（strict 签名、trace 续链、history 记账免费获得）；本机自
    /// 探针则直接记账回环——若仍调 send() 会形成 async 递归（E0733），
    /// 且应答帧本就被 handle_incoming 的 hello_reply_in 分支消费，
    /// 绕开投递路径与事件环记账等效、更干净。
    async fn hello_reply(&self, req: &Envelope) {
        let payload = serde_json::json!({
            "node_id": self.cfg.node_id,
            "name": self.cfg.name,
            "version": crate::api::VERSION,
            "protocol": PROTOCOL_VERSION,
            "uptime_seconds": self.started.elapsed().as_secs(),
            "probe": req.payload.get("probe").cloned().unwrap_or(serde_json::Value::Null),
        });
        let reply = Envelope::business(
            &self.cfg.node_id,
            &req.from,
            "text",
            "hello_reply",
            payload,
            Some(30),
            Some(req.msg_id.clone()),
            None,
            Some(HELLO_AGENT.to_string()),
            req.from_agent.clone(),
        );
        if reply.to == self.cfg.node_id {
            self.record_history(&reply, "in", &self.cfg.node_id);
            return;
        }
        // send→handle_incoming→hello_reply→send 是编译期递归环，本边必须
        // 装箱断链（E0733）；仅远端请求走到这里，热路径无额外开销。
        if let Err(e) = Box::pin(self.send(reply)).await {
            tracing::debug!(error = %e, "router: hello_reply send failed (lockdown or offline)");
        }
    }

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

    /// 本节点 node_id（供 M2 reconcile loop 派发 task_spec 时标识发送方）。
    pub fn node_id(&self) -> &str {
        &self.cfg.node_id
    }

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
                // v0.7 第二波：目录过期清理 + 中继周期推送（仅 spine 推送）。
                if counter.is_multiple_of(DIR_SYNC_INTERVAL_SECS) {
                    this.dir_prune();
                    if this.relay.enabled {
                        this.sync_push_dir();
                    }
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
                        // v0.7 spine/relay：重试沿用原下一跳（pend.peer_id），
                        // 不能回退成 env.to（最终目标常非直连）。
                        self.register_pending_at(&pend.env, &pend.peer_id, pend.tries + 1);
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

    /// v0.7 §6.3：当前降级模式（纯计算，不改状态）。
    /// 只读优先级最高；standalone 仅在「阈值 >0 且无直连对端且静默超时」时成立。
    pub fn mesh_mode(&self) -> MeshMode {
        if self.read_only.load(Ordering::Relaxed) {
            return MeshMode::ReadOnly;
        }
        if self.standalone_after_secs == 0 || self.peers_online() > 0 {
            return MeshMode::Normal;
        }
        let last = *self.last_peer_seen.lock().unwrap();
        if last.elapsed().as_secs() >= self.standalone_after_secs {
            MeshMode::Standalone
        } else {
            MeshMode::Normal
        }
    }

    /// 运行时切换只读（返回切换前状态）。standalone 不可手工设置。
    pub fn set_read_only(&self, on: bool) -> bool {
        self.read_only.swap(on, Ordering::Relaxed)
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    pub fn standalone_after_secs(&self) -> u64 {
        self.standalone_after_secs
    }

    /// 最近一次直连对端活动距今秒数（无则从进程启动算）。
    pub fn last_peer_seen_secs(&self) -> u64 {
        self.last_peer_seen.lock().unwrap().elapsed().as_secs()
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

    /// 路由层 Prometheus 指标快照（供 /v1/metrics 端点消费）。
    pub fn router_metrics(&self) -> RouterMetrics {
        RouterMetrics {
            peers_online: self.registry.len(),
            pending_acks: self.pending.lock().unwrap().len(),
            offline_queue_len: self
                .offline
                .try_lock()
                .map(|m| m.values().map(|q| q.len()).sum())
                .unwrap_or(0),
            local_pending_count: self.local_pending.lock().unwrap().len(),
            inbox_len: self.inbox.try_lock().map(|i| i.len()).unwrap_or(0),
            blocked_count: self.blocked_count(),
        }
    }

    // ---- agent registry (M7) ------------------------------------------------

    pub fn register_agent(
        &self,
        agent_id: &str,
        name: Option<String>,
        capabilities: Vec<String>,
        namespace: Option<String>,
    ) -> AgentRecord {
        let online = self.clients.lock().unwrap().contains_key(agent_id);
        let rec = AgentRecord {
            agent_id: agent_id.to_string(),
            name,
            capabilities,
            online,
            node_id: self.cfg.node_id.clone(),
            last_seen: store::now_wall(),
            namespace,
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

    /// v0.7 §4.2：按 agent_id 查 namespace（本机优先，其次 fleet 缓存）。
    /// 未注册/未知 → None（调用方决定 fail-open/fail-closed）。
    pub fn agent_namespace(&self, agent_id: &str) -> Option<String> {
        if let Some(a) = self.agents.lock().unwrap().get(agent_id) {
            return Some(
                a.namespace
                    .clone()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "default".to_string()),
            );
        }
        for agents in self.fleet_agents.lock().unwrap().values() {
            if let Some(a) = agents.iter().find(|a| a.agent_id == agent_id) {
                return Some(
                    a.namespace
                        .clone()
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "default".to_string()),
                );
            }
        }
        None
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

    /// 查找 `agent_id` 所在节点（本机优先，再舰队在线节点）。
    /// 供 M2 reconcile loop 把 `task_spec` 派发到目标执行方所在节点。
    pub fn find_agent_node(&self, agent_id: &str) -> Option<String> {
        if let Some(rec) = self.agents.lock().unwrap().get(agent_id) {
            return Some(rec.node_id.clone());
        }
        let remote = self.fleet_agents.lock().unwrap();
        for list in remote.values() {
            if let Some(rec) = list.iter().find(|r| r.agent_id == agent_id && r.online) {
                return Some(rec.node_id.clone());
            }
        }
        None
    }

    /// v0.6 P2 预热池：agent 是否在线（本地注册与船队缓存同优先序，按
    /// online 判定——find_agent_node 对本机注册不滤 online，不能直接复用）。
    pub fn agent_online(&self, agent_id: &str) -> bool {
        if let Some(rec) = self.agents.lock().unwrap().get(agent_id) {
            return rec.online;
        }
        let fleet = self.fleet_agents.lock().unwrap();
        for list in fleet.values() {
            if let Some(rec) = list.iter().find(|r| r.agent_id == agent_id) {
                return rec.online;
            }
        }
        false
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
    /// 兼容旧调用（仅测试/嵌入方）：等价于空 tags 的 with_tags。
    #[allow(dead_code)]
    pub fn on_agents_update(&self, from_node: &str, agents: Vec<AgentRecord>) {
        self.on_agents_update_with_tags(from_node, agents, Vec::new());
    }

    /// v0.7 §4.2：agent 表与节点 tags 一起同步（旧节点 payload 无 tags →
    /// 空标签；tag 型 deny 不命中，tag 型 allow 会拒绝——见 abac 模块注释）。
    pub fn on_agents_update_with_tags(
        &self,
        from_node: &str,
        agents: Vec<AgentRecord>,
        tags: Vec<String>,
    ) {
        self.fleet_agents
            .lock()
            .unwrap()
            .insert(from_node.to_string(), agents);
        self.peer_tags
            .lock()
            .unwrap()
            .insert(from_node.to_string(), tags);
    }

    /// 远端节点 tags 快照（node_id → tags）。
    pub fn peer_tags(&self) -> HashMap<String, Vec<String>> {
        self.peer_tags.lock().unwrap().clone()
    }

    /// An `agents_update` envelope addressed to `to` (None if no agents here).
    pub fn agents_update_envelope(&self, to: &str) -> Option<Envelope> {
        let agents = self.local_agents();
        if agents.is_empty() {
            return None;
        }
        let payload = serde_json::json!({
            "agents": agents,
            "tags": self.cfg.tags,
        });
        let mut env = Envelope::agents_update(&self.cfg.node_id, to, payload, None, None);
        self.sign_control(&mut env);
        Some(env)
    }

    /// v0.8 HA：构造一条已签名的复制帧（`ctrl/ha`）。
    /// 旧节点收到未知 `ctrl` 帧会直接忽略（conn 底部分支），零破坏。
    pub fn ha_envelope(&self, to: &str, payload: serde_json::Value) -> Envelope {
        self.signed_ctrl_envelope(to, "ha", payload)
    }

    /// v0.8 M4：构造任意已签名控制面帧（`ctrl/<action>`）。
    pub fn signed_ctrl_envelope(
        &self,
        to: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Envelope {
        let mut env = Envelope::business(
            &self.cfg.node_id,
            to,
            "ctrl",
            action,
            payload,
            None,
            None,
            None,
            None,
            None,
        );
        self.sign_control(&mut env);
        env
    }

    /// v0.8 M4：尽力转发一条控制面帧（非阻塞；连接不存在/队列满返回 false）。
    /// 仅用于 follower → leader 的业务转发（任务事件/审计/KV），丢帧由调用方
    /// 的业务幂等/重试语义兜底。
    pub fn forward_ctrl(&self, to: &str, action: &str, payload: serde_json::Value) -> bool {
        let Some(sink) = self.peer_sink(to) else {
            return false;
        };
        let env = self.signed_ctrl_envelope(to, action, payload);
        let Ok(text) = serde_json::to_string(&env) else {
            return false;
        };
        sink.tx.try_send(Frame::Text(text)).is_ok()
    }

    /// v0.8 M4：把 follower 收到的事件注入本节点事件总线（leader 引擎消费）。
    pub fn inject_event(&self, env: Envelope) {
        if self.events.receiver_count() == 0 {
            return;
        }
        let _ = self.events.send(env);
    }

    /// 取某个对端的连接句柄（HA 复制帧发送用）。
    pub fn peer_sink(&self, node_id: &str) -> Option<PeerSink> {
        self.registry.get(node_id)
    }

    /// 控制面帧签名（v0.7 第二波）：strict 模式下所有新发出的控制面帧都带
    /// `sig`（HMAC 覆盖 from|msg_id|ts，与业务帧同一把 cluster_token）。
    /// 未开启 strict 时不签（保持 open 模式零开销）。
    fn sign_control(&self, env: &mut Envelope) {
        if self.cfg.security.mode != "strict" {
            return;
        }
        env.sig = Some(envelope_sign(
            &self.cfg.security.cluster_token,
            &env.from,
            &env.msg_id,
            &env.ts,
        ));
    }

    /// 控制面帧校验（v0.7 第二波）：
    /// - open 模式：放行；
    /// - strict + 帧带 `sig`：**总是**验签（篡改即拒）；
    /// - strict + 无 `sig`：`security.control_auth=false`（缺省）放行以兼容
    ///   未升级节点；开启后一律拒绝。
    pub fn verify_control(&self, env: &Envelope) -> bool {
        if self.cfg.security.mode != "strict" {
            return true;
        }
        match &env.sig {
            Some(_) => self.verify_incoming(env),
            None => !self.cfg.security.control_auth,
        }
    }

    // ---- node directory (v0.7 第二波) ---------------------------------------

    /// 合并对端中继来的节点目录。直连 peer / 本机 / 空 id 一律忽略
    /// （直连信息以 PeerTable 为准，避免目录覆盖真实在线态）。
    pub fn dir_merge(&self, nodes: &[serde_json::Value], via: &str) {
        let mut d = self.dir.lock().unwrap();
        for n in nodes {
            let id = n.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
            if id.is_empty() || id == self.cfg.node_id || self.registry.get(id).is_some() {
                continue;
            }
            let name = n
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or(id)
                .to_string();
            let addr = n
                .get("addr")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let origin = n
                .get("via")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(via)
                .to_string();
            let hops = n
                .get("hops")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                .min(u8::MAX as u64) as u8;
            // 绝对过期时间：源头设定（now+TTL），中继原样转发；缺失则本地兜底。
            let now = store::now_wall();
            let expires_wall = n
                .get("expires")
                .and_then(|v| v.as_i64())
                .unwrap_or(now + DIR_TTL_SECONDS as i64)
                .min(now + DIR_TTL_SECONDS as i64);
            tracing::debug!(
                node_id = %id,
                provided = ?n.get("expires"),
                effective = expires_wall,
                via = %origin,
                hops,
                "router: dir_merge"
            );
            d.insert(
                id.to_string(),
                DirEntry {
                    node_id: id.to_string(),
                    name,
                    addr,
                    via: origin,
                    hops,
                    updated: std::time::Instant::now(),
                    expires_wall,
                },
            );
        }
    }

    /// 条目是否仍在有效期内（绝对过期时间，见 `DirEntry::expires_wall`）。
    fn dir_entry_live(e: &DirEntry) -> bool {
        store::now_wall() <= e.expires_wall
    }

    /// 清理过期目录条目（中继每 5s 刷新，30s 未见即离开）。
    pub fn dir_prune(&self) {
        let mut d = self.dir.lock().unwrap();
        d.retain(|_, e| Self::dir_entry_live(e));
    }

    /// 目录里的节点是否「新鲜」（用于 fleet 的 node_online 判定）。
    pub fn dir_online(&self, node_id: &str) -> bool {
        self.dir
            .lock()
            .unwrap()
            .get(node_id)
            .map(Self::dir_entry_live)
            .unwrap_or(false)
    }

    /// 节点级在线判定（v0.7 chaos reclaim）：
    /// - 本机恒在线；直连 peer 表/中继目录新鲜 → 在线；
    /// - **最近断开过的直连节点**（5 分钟记忆）→ 明确离线；
    /// - 其余未知节点按在线处理（保持既有调度语义；真死访存交由发送侧
    ///   离线队列/目录 TTL 兜底）。
    pub fn node_online(&self, node_id: &str) -> bool {
        if node_id == self.cfg.node_id {
            return true;
        }
        // 已宣告 drain 的节点：即使 WS 还没断，也不再接受新派发/宽限。
        if self.draining.lock().unwrap().contains(node_id) {
            return false;
        }
        if self.registry.get(node_id).is_some() || self.dir_online(node_id) {
            return true;
        }
        let offline = self.offline_nodes.lock().unwrap();
        match offline.get(node_id) {
            Some(at) => at.elapsed().as_secs() > OFFLINE_MEMORY_SECONDS,
            None => true,
        }
    }

    /// v0.7 §6.2：收到对端优雅下线通告。标记后立即不可调度、在途任务
    /// 跳过 reclaim 宽限期；重连注册会清除。返回首次标记（重复通告幂等）。
    pub fn on_node_drain(&self, node_id: &str) -> bool {
        if node_id == self.cfg.node_id {
            return false;
        }
        let first = self.draining.lock().unwrap().insert(node_id.to_string());
        self.mark_node_offline(node_id);
        if first {
            tracing::info!(node_id, "router: peer drain announced（graceful shutdown）");
            self.audit_event(
                "node.drain",
                "peer",
                serde_json::json!({ "node_id": node_id, "at": crate::store::now_wall() }),
            );
            self.notify(
                "node_draining",
                serde_json::json!({ "node_id": node_id, "at": crate::store::now_wall() }),
            );
        }
        first
    }

    pub fn is_draining(&self, node_id: &str) -> bool {
        self.draining.lock().unwrap().contains(node_id)
    }

    /// v0.7 §6.2：进程退出前向所有直连 peer 宣告 drain（尽力而为，
    /// 失败不影响退出）。控制面帧按既有规则签名。
    pub fn broadcast_drain(&self) {
        let mut env = Envelope::business(
            &self.cfg.node_id,
            "*",
            "ctrl",
            "node_drain",
            serde_json::json!({ "node_id": self.cfg.node_id }),
            None,
            None,
            None,
            None,
            None,
        );
        self.sign_control(&mut env);
        self.broadcast_envelope(&env);
        tracing::info!("router: drain announced to peers（graceful shutdown）");
    }

    /// 标记节点离线（直连断开时调用；重连注册会清除）。chaos 冒烟也用它
    /// 模拟「节点已死但引擎尚未从 peer 表观测到」的边界。
    pub fn mark_node_offline(&self, node_id: &str) {
        self.offline_nodes
            .lock()
            .unwrap()
            .insert(node_id.to_string(), std::time::Instant::now());
    }

    /// `/v1/peers` 的加性视图：经中继学到的节点（`relayed=true`、`via`）。
    pub fn dir_snapshot(&self) -> Vec<crate::peer::PeerView> {
        let d = self.dir.lock().unwrap();
        let mut out: Vec<crate::peer::PeerView> = d
            .values()
            .filter(|e| Self::dir_entry_live(e))
            .map(|e| crate::peer::PeerView {
                node_id: e.node_id.clone(),
                name: e.name.clone(),
                addr: e.addr.clone(),
                mtls_addr: None,
                tags: Vec::new(),
                online: true,
                last_seen_seconds_ago: e.updated.elapsed().as_secs(),
                direction: "relay".to_string(),
                load: None,
                relayed: Some(true),
                via: Some(e.via.clone()),
            })
            .collect();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        out
    }

    /// 构造目录帧：本机直连 peer + 已学到的目录（带 hops，供下游中继判断）。
    pub fn node_dir_envelope(&self) -> Option<Envelope> {
        let mut nodes = Vec::new();
        let now = store::now_wall();
        for s in self.registry.all() {
            nodes.push(serde_json::json!({
                "node_id": s.node_id,
                "name": s.name,
                "addr": s.addr,
                "via": self.cfg.node_id,
                "hops": 0,
                "expires": now + DIR_TTL_SECONDS as i64,
            }));
        }
        {
            let d = self.dir.lock().unwrap();
            for e in d.values() {
                if !Self::dir_entry_live(e) || e.hops.saturating_add(1) > self.relay.max_hops.max(1)
                {
                    continue;
                }
                nodes.push(serde_json::json!({
                    "node_id": e.node_id,
                    "name": e.name,
                    "addr": e.addr,
                    "via": e.via,
                    "hops": e.hops.saturating_add(1),
                    // 绝对过期时间原样转发（只由源头续期）。
                    "expires": e.expires_wall,
                }));
            }
        }
        if nodes.is_empty() {
            return None;
        }
        tracing::debug!(
            nodes = %serde_json::to_string(&nodes).unwrap_or_default(),
            "router: node_dir outbound"
        );
        let mut env = Envelope::business(
            &self.cfg.node_id,
            "*",
            "ctrl",
            "node_dir",
            serde_json::json!({ "nodes": nodes }),
            None,
            None,
            None,
            None,
            None,
        );
        self.sign_control(&mut env);
        Some(env)
    }

    /// 中继目录：把本节点已知目录推给所有直连 peer（maintenance 周期调用）。
    pub fn sync_push_dir(&self) {
        let Some(env) = self.node_dir_envelope() else {
            return;
        };
        self.broadcast_envelope(&env);
    }

    /// 新 peer 注册后立刻推一份目录（不必等下一个 5s 周期）。
    pub fn push_dir_to(&self, node_id: &str) {
        let Some(env) = self.node_dir_envelope() else {
            return;
        };
        self.broadcast_env_to(node_id, &env);
    }

    /// 收目录帧：验签 → 合并。返回是否接受。
    pub fn on_node_dir(&self, env: &Envelope, _ingress: Option<&str>) -> bool {
        if !self.verify_control(env) {
            tracing::warn!(
                from = %env.from,
                msg_id = %env.msg_id,
                "router: node_dir rejected (control frame signature)"
            );
            self.notify(
                "audit_alert",
                serde_json::json!({
                    "kind": "control_frame_rejected",
                    "action": "node_dir",
                    "from": env.from,
                    "msg_id": env.msg_id,
                }),
            );
            return false;
        }
        let nodes = env
            .payload
            .get("nodes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        self.dir_merge(&nodes, &env.from);
        true
    }

    /// 向单个 peer 发一个已构造好的信封（fire-and-forget，断线不影响调用方）。
    fn broadcast_env_to(&self, node_id: &str, env: &Envelope) {
        let Some(sink) = self.registry.get(node_id) else {
            return;
        };
        let Ok(text) = serde_json::to_string(env) else {
            return;
        };
        tokio::spawn(async move {
            let _ = sink.tx.send(Frame::Text(text)).await;
        });
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

    /// Broadcast a raw envelope to every connected peer (fire-and-forget).
    /// Used by internal control-plane cycles (v0.6 P1 lease 差异上报).
    pub fn broadcast_envelope(&self, env: &Envelope) {
        let Ok(text) = serde_json::to_string(env) else {
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
        self.record_history_marked(env, direction, node, false);
    }

    /// History write with the v0.5 §4.5 lockdown audit marker (`blocked`).
    fn record_history_marked(&self, env: &Envelope, direction: &str, node: &str, blocked: bool) {
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
            thread: env.thread.clone(),
            trace_id: env.trace_id.clone(),
            // v0.5 §2: observer copy is sanitized; the delivered frame above
            // (routed from `env` directly) still carries the original payload.
            payload: self.redactor.redact_value(&env.payload),
            blocked: if blocked { Some(true) } else { None },
        };
        self.push_history(entry);
    }

    /// Push into the ring buffer and (when configured) the durable JSONL file.
    fn push_history(&self, mut entry: HistoryEntry) {
        entry.seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        // 任务协议帧（已脱敏的同一份 entry）旁路喂给外部导出。
        // submit 走 try_send 非阻塞；缺省钩子未挂载/导出关闭时零开销。
        if let Some(h) = self.hook.get() {
            if h.export_enabled() {
                let topic = entry
                    .payload
                    .get("topic")
                    .and_then(|t| t.as_str())
                    .unwrap_or("");
                if is_task_topic(topic) {
                    if let Ok(v) = serde_json::to_value(&entry) {
                        h.export_frame(v);
                    }
                }
            }
        }
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
            thread: None,
            trace_id: None,
            payload: self.redactor.redact_value(&serde_json::json!({
                "file_name": file_name,
                "size": size,
                "saved_path": saved_path,
            })),
            blocked: None,
        };
        self.push_history(entry);
    }
}

#[cfg(test)]
mod relay_tests {
    use super::{Config, Envelope, PeerSink, Router};
    use crate::conn::Frame;
    use serde_json::json;
    use std::sync::Arc;

    fn router_with(relay_enabled: bool, spine: &str, max_hops: u8) -> (Config, Arc<Router>) {
        router_custom(relay_enabled, spine, "", max_hops)
    }

    fn router_custom(
        relay_enabled: bool,
        spine: &str,
        upstream: &str,
        max_hops: u8,
    ) -> (Config, Arc<Router>) {
        let mut cfg = Config::generate(
            Some("relay-test".to_string()),
            Some("tok".to_string()),
            Some("open".to_string()), // open 模式：免签名，专注路由语义
        );
        cfg.data_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("relay-test");
        cfg.delivery.persist = false;
        cfg.relay.enabled = relay_enabled;
        cfg.relay.spine = spine.to_string();
        cfg.relay.upstream = upstream.to_string();
        cfg.relay.max_hops = max_hops;
        let r = Arc::new(Router::new(&cfg));
        (cfg, r)
    }

    fn attach(r: &Router, node_id: &str) -> tokio::sync::mpsc::Receiver<Frame> {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        r.register_peer(PeerSink {
            node_id: node_id.to_string(),
            name: node_id.to_string(),
            addr: "test".to_string(),
            tx,
        });
        rx
    }

    fn frame_env(to: &str, hops: Option<u8>) -> Envelope {
        let mut env = Envelope::business(
            "nodeA",
            to,
            "text",
            "send",
            json!({"content": "hi"}),
            None,
            None,
            None,
            Some("a1".into()),
            Some("b1".into()),
        );
        env.hops = hops;
        env
    }

    #[tokio::test]
    async fn transit_dropped_when_relay_disabled() {
        let (_cfg, r) = router_with(false, "", 2);
        let mut env = frame_env("nodeB", None);
        assert!(!r.handle_transit(&mut env, Some("nodeA")).await);
        assert_eq!(r.history().len(), 0, "禁用中继不应写 history");
    }

    #[tokio::test]
    async fn transit_forwards_to_direct_target_with_hop_increment() {
        let (_cfg, r) = router_with(true, "", 2);
        let mut rx = attach(&r, "nodeB");
        let mut env = frame_env("nodeB", None);
        assert!(r.handle_transit(&mut env, Some("nodeA")).await);
        let text = match rx.recv().await.expect("帧应被转发") {
            Frame::Text(t) => t,
            other => panic!("非文本帧: {other:?}"),
        };
        let fwd: Envelope = serde_json::from_str(&text).unwrap();
        assert_eq!(fwd.to, "nodeB", "目标保持最终节点");
        assert_eq!(fwd.from, "nodeA", "中继不改写 from（透明转发）");
        assert_eq!(fwd.hops, Some(1), "hops 逐跳 +1");
        // 本跳 pending 按下一跳记账。
        assert!(r.history().iter().any(|h| h.direction == "in"));
    }

    #[tokio::test]
    async fn transit_rejects_beyond_hop_limit() {
        let (_cfg, r) = router_with(true, "", 2);
        let _rx = attach(&r, "nodeB");
        let mut env = frame_env("nodeB", Some(2)); // 已 2 跳
        assert!(!r.handle_transit(&mut env, Some("nodeA")).await);
    }

    #[tokio::test]
    async fn transit_unknown_target_is_dropped_not_queued() {
        let (_cfg, r) = router_with(true, "", 2);
        let mut env = frame_env("ghost-node", None);
        assert!(!r.handle_transit(&mut env, Some("nodeA")).await);
        // 未知路由不与离线队列混用（离线队列按最终目标堆积，跨网段永不出队）。
        assert!(r.offline.lock().await.is_empty());
    }

    #[tokio::test]
    async fn send_routes_via_spine_when_target_not_direct() {
        let (_cfg, r) = router_with(false, "spine1", 2);
        let mut rx = attach(&r, "spine1");
        r.send(frame_env("far-node", None)).await.unwrap();
        let text = match rx.recv().await.expect("应经 spine 转投") {
            Frame::Text(t) => t,
            other => panic!("非文本帧: {other:?}"),
        };
        let fwd: Envelope = serde_json::from_str(&text).unwrap();
        assert_eq!(fwd.to, "far-node", "最终目标不变");
        // pending 按 spine 记账（逐跳重试）。
        let pending = r.pending.lock().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending.values().next().unwrap().peer_id, "spine1");
    }

    // ---- v0.7 第二波：级联 / 目录 / 控制面验签 ------------------------------

    #[tokio::test]
    async fn send_uses_upstream_when_spine_empty() {
        let (_cfg, r2) = router_custom(false, "", "up1", 3);
        let mut rx = attach(&r2, "up1");
        r2.send(frame_env("far-node", None)).await.unwrap();
        let text = match rx.recv().await.expect("应经 upstream 级联") {
            Frame::Text(t) => t,
            other => panic!("非文本帧: {other:?}"),
        };
        let fwd: Envelope = serde_json::from_str(&text).unwrap();
        assert_eq!(fwd.to, "far-node");
        assert_eq!(
            r2.pending.lock().unwrap().values().next().unwrap().peer_id,
            "up1",
            "pending 按 upstream 记账"
        );
    }

    #[tokio::test]
    async fn transit_unknown_target_falls_back_to_upstream() {
        let (_cfg, r2) = router_custom(true, "", "up1", 3);
        let mut rx = attach(&r2, "up1");
        let mut env = frame_env("ghost-node", None);
        assert!(r2.handle_transit(&mut env, Some("ingress1")).await);
        let text = match rx.recv().await.expect("未知目标应兜底到 upstream") {
            Frame::Text(t) => t,
            other => panic!("非文本帧: {other:?}"),
        };
        let fwd: Envelope = serde_json::from_str(&text).unwrap();
        assert_eq!(fwd.hops, Some(1), "级联转发同样逐跳累加");
        assert_eq!(fwd.to, "ghost-node");
    }

    #[tokio::test]
    async fn transit_does_not_bounce_back_to_ingress_upstream() {
        let (_cfg, r2) = router_custom(true, "", "up1", 3);
        let _rx = attach(&r2, "up1");
        let mut env = frame_env("ghost-node", None);
        // 入站连接就是 upstream → 不得再弹回（两跳乒乓）。
        assert!(!r2.handle_transit(&mut env, Some("up1")).await);
    }

    fn router_strict(control_auth: bool) -> (Config, Arc<Router>) {
        let mut cfg = Config::generate(
            Some("ctrl-auth-test".to_string()),
            Some("tok-ctrl".to_string()),
            Some("strict".to_string()),
        );
        cfg.data_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("relay-test");
        cfg.delivery.persist = false;
        cfg.security.control_auth = control_auth;
        let r = Arc::new(Router::new(&cfg));
        (cfg, r)
    }

    #[tokio::test]
    async fn control_auth_default_tolerates_unsigned_but_verifies_signed() {
        let (cfg, r) = router_strict(false);
        assert!(
            r.verify_control(&frame_env("nodeB", None)),
            "缺省容忍未签名"
        );
        // 带签名且正确 → 通过。
        let mut signed = frame_env("nodeB", None);
        signed.sig = Some(crate::proto::envelope_sign(
            &cfg.security.cluster_token,
            &signed.from,
            &signed.msg_id,
            &signed.ts,
        ));
        assert!(r.verify_control(&signed));
        // 带签名但被篡改 → 拒绝。
        let mut tampered = signed.clone();
        tampered.msg_id = "deadbeef".to_string();
        assert!(!r.verify_control(&tampered), "签名不覆盖篡改后的 msg_id");
    }

    #[tokio::test]
    async fn control_auth_strict_rejects_unsigned() {
        let (_cfg, r) = router_strict(true);
        assert!(
            !r.verify_control(&frame_env("nodeB", None)),
            "control_auth 开启必须签名"
        );
        let mut signed = Envelope::business(
            "nodeA",
            "*",
            "ctrl",
            "node_dir",
            json!({"nodes": []}),
            None,
            None,
            None,
            None,
            None,
        );
        signed.sig = Some(crate::proto::envelope_sign(
            &_cfg.security.cluster_token,
            &signed.from,
            &signed.msg_id,
            &signed.ts,
        ));
        assert!(r.verify_control(&signed));
    }

    #[tokio::test]
    async fn node_online_semantics_local_unknown_offline_dir() {
        let (_cfg, r) = router_with(true, "", 2);
        assert!(r.node_online(r.node_id()), "本机恒在线");
        assert!(
            r.node_online("never-seen"),
            "未知节点按在线处理（保持既有语义）"
        );
        r.mark_node_offline("gone-node");
        assert!(!r.node_online("gone-node"), "明确离线");
        // 目录新鲜 → 在线（中继叶子）。
        let env = Envelope::business(
            "spine1",
            "*",
            "ctrl",
            "node_dir",
            json!({"nodes": [{"node_id": "leaf9", "name": "leaf9", "addr": "x",
                              "via": "spine1", "hops": 1,
                              "expires": crate::store::now_wall() + 30}]}),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(r.on_node_dir(&env, None));
        assert!(r.node_online("leaf9"));
    }

    #[tokio::test]
    async fn node_dir_merges_skips_self_and_direct_peers() {
        let (_cfg, r) = router_with(true, "", 2);
        let _rx = attach(&r, "direct1");
        let self_id = r.node_id().to_string();
        let env = Envelope::business(
            "spine1",
            "*",
            "ctrl",
            "node_dir",
            json!({"nodes": [
                {"node_id": self_id, "name": "self", "addr": "x", "via": "spine1", "hops": 0},
                {"node_id": "direct1", "name": "direct", "addr": "x", "via": "spine1", "hops": 0},
                {"node_id": "leaf2", "name": "leaf2", "addr": "10.0.0.9:8765", "via": "spine1", "hops": 1}
            ]}),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(r.on_node_dir(&env, None));
        let snap = r.dir_snapshot();
        assert_eq!(snap.len(), 1, "本机与直连 peer 不进目录");
        assert_eq!(snap[0].node_id, "leaf2");
        assert_eq!(snap[0].relayed, Some(true));
        assert_eq!(snap[0].via.as_deref(), Some("spine1"));
        assert!(r.dir_online("leaf2"));
        assert!(!r.dir_online("nobody"));
        // 绝对过期时间：中继转发时原样携带，过期条目即使被反复转发也不再新鲜
        // （多级级联下防止环路互相续期）。
        let expired = Envelope::business(
            "spine1",
            "*",
            "ctrl",
            "node_dir",
            serde_json::json!({"nodes": [
                {"node_id": "leaf3", "name": "leaf3", "addr": "x", "via": "spine1",
                 "hops": 1, "expires": crate::store::now_wall() - 5}
            ]}),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(r.on_node_dir(&expired, None));
        assert!(!r.dir_online("leaf3"), "过期条目不得被视为在线");
        assert!(!r.dir_snapshot().iter().any(|v| v.node_id == "leaf3"));
    }
}

#[cfg(test)]
mod mode_tests {
    use super::{MeshMode, Router};
    use crate::config::Config;
    use std::time::{Duration, Instant};

    fn router_with(standalone_after: u64, read_only: bool) -> Router {
        let mut cfg = Config::generate(
            Some("mode-test".to_string()),
            Some("tok".to_string()),
            Some("open".to_string()),
        );
        cfg.mesh = serde_json::json!({
            "degraded": {
                "read_only": read_only,
                "standalone_after_seconds": standalone_after,
            }
        });
        Router::new(&cfg)
    }

    #[test]
    fn threshold_zero_keeps_normal_and_read_only_wins() {
        let r = router_with(0, false);
        assert_eq!(r.mesh_mode(), MeshMode::Normal);
        r.set_read_only(true);
        assert_eq!(r.mesh_mode(), MeshMode::ReadOnly);
        r.set_read_only(false);
        assert_eq!(r.mesh_mode(), MeshMode::Normal);
    }

    #[test]
    fn drain_marks_offline_and_reconnect_clears() {
        use super::PeerSink;
        use tokio::sync::mpsc;

        let r = router_with(0, false);
        let (tx, _rx) = mpsc::channel(4);
        r.register_peer(PeerSink {
            node_id: "n2".to_string(),
            name: "n2".to_string(),
            addr: "127.0.0.1:2".to_string(),
            tx,
        });
        assert!(r.node_online("n2"));
        assert!(r.on_node_drain("n2"));
        assert!(!r.node_online("n2"), "drain 后不再参与调度");
        assert!(r.is_draining("n2"));
        assert!(!r.on_node_drain("n2"), "重复通告幂等");
        // 重连注册清除 draining。
        let (tx2, _rx2) = mpsc::channel(4);
        r.register_peer(PeerSink {
            node_id: "n2".to_string(),
            name: "n2".to_string(),
            addr: "127.0.0.1:2".to_string(),
            tx: tx2,
        });
        assert!(!r.is_draining("n2"));
        assert!(r.node_online("n2"));
    }

    #[test]
    fn standalone_after_peer_silence() {
        let r = router_with(30, false);
        assert_eq!(r.mesh_mode(), MeshMode::Normal);
        // 模拟「最后一个对端已断开 31s」（单调钟，无 sleep）。
        *r.last_peer_seen.lock().unwrap() =
            Instant::now().checked_sub(Duration::from_secs(31)).unwrap();
        assert_eq!(r.mesh_mode(), MeshMode::Standalone);
        // 只读优先级更高，退出只读后回到 standalone。
        r.set_read_only(true);
        assert_eq!(r.mesh_mode(), MeshMode::ReadOnly);
        r.set_read_only(false);
        assert_eq!(r.mesh_mode(), MeshMode::Standalone);
    }
}
