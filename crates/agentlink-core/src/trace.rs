// SPDX-License-Identifier: Apache-2.0
//! trace_id resolution and structured log-field helpers.
//!
//! Rules:
//! 1. An inbound envelope that already carries `trace_id` keeps it (agent
//!    echoed the chain, or an upstream node injected it).
//! 2. Else, if it is a `reply_to` answer and this node has seen the parent
//!    message, the parent's trace_id is reused (kernel-side map).
//! 3. Else the kernel derives a fresh id from the message: `t-` + the first
//!    8 chars of `msg_id` (uuid-simple hex, stable per message).
//!
//! The resolved id is *injected* into the envelope metadata (`trace_id`
//! field) before forwarding/delivery so downstream agents can echo it.
//! Injection is protocol-safe: the envelope HMAC covers `from|msg_id|ts`
//! only, never the extra field, and legacy nodes ignore unknown fields.

use serde_json::Value;

use crate::proto::Envelope;

/// Bounded msg_id -> trace_id map used for `reply_to` chain continuation.
/// Insertion-ordered eviction (LRU-ish); the map is a log-side convenience,
/// so a miss only degrades to "derive a fresh trace", never to data loss.
pub struct TraceMap {
    entries: std::collections::HashMap<String, String>,
    order: std::collections::VecDeque<String>,
    cap: usize,
}

impl TraceMap {
    pub fn new(cap: usize) -> Self {
        TraceMap {
            entries: std::collections::HashMap::new(),
            order: std::collections::VecDeque::new(),
            cap: cap.max(1),
        }
    }

    pub fn get(&self, msg_id: &str) -> Option<&String> {
        self.entries.get(msg_id)
    }

    pub fn put(&mut self, msg_id: &str, trace_id: &str) {
        if self.entries.contains_key(msg_id) {
            return;
        }
        self.entries
            .insert(msg_id.to_string(), trace_id.to_string());
        self.order.push_back(msg_id.to_string());
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.entries.remove(&old);
            }
        }
    }
}

/// Derive the kernel-side trace id for a message without one.
pub fn derive(msg_id: &str) -> String {
    let n = msg_id.len().min(8);
    format!("t-{}", &msg_id[..n])
}

/// Pure resolution step (no injection): incoming field wins, then the
/// `reply_to` map hit, then a fresh derivation from `msg_id`.
pub fn resolve(
    incoming: Option<&str>,
    reply_to: Option<&str>,
    map: &TraceMap,
    msg_id: &str,
) -> String {
    if let Some(t) = incoming {
        if !t.is_empty() {
            return t.to_string();
        }
    }
    if let Some(parent) = reply_to {
        if let Some(t) = map.get(parent) {
            return t.clone();
        }
    }
    derive(msg_id)
}

/// Resolve and remember: returns the trace id and records `msg_id -> trace`
/// so replies seen later on this node continue the same chain.
pub fn resolve_and_remember(map: &mut TraceMap, env: &Envelope) -> String {
    let t = resolve(
        env.trace_id.as_deref(),
        env.reply_to.as_deref(),
        map,
        &env.msg_id,
    );
    map.put(&env.msg_id, &t);
    t
}

/// Best-effort string field of a task payload (`topic` / `correlation`),
/// `-` when absent so JSON lines always carry the key.
pub fn payload_field<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// One-line display value for optional log fields.
pub fn dash(o: &Option<String>) -> String {
    o.clone().unwrap_or_else(|| "-".to_string())
}

/// Emit one structured business event. Every message-level event carries the
/// full field set (trace_id / correlation_id / msg_id / from / to /
/// from_agent / to_agent / node_id / topic); non-message events pass the
/// relevant subset with `-` placeholders.
#[allow(clippy::too_many_arguments)]
pub fn log_event(
    level: tracing::Level,
    event: &str,
    node_id: &str,
    trace_id: &str,
    env: Option<&Envelope>,
) {
    let (msg_id, from, to, from_agent, to_agent, corr, topic) = match env {
        Some(e) => (
            e.msg_id.clone(),
            e.from.clone(),
            e.to.clone(),
            dash(&e.from_agent),
            dash(&e.to_agent),
            payload_field(&e.payload, "correlation")
                .unwrap_or("-")
                .to_string(),
            payload_field(&e.payload, "topic")
                .unwrap_or("-")
                .to_string(),
        ),
        None => (
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ),
    };
    match level {
        tracing::Level::WARN => tracing::warn!(
            trace_id = %trace_id, correlation_id = %corr, msg_id = %msg_id,
            from = %from, to = %to, from_agent = %from_agent, to_agent = %to_agent,
            node_id = %node_id, topic = %topic,
            "al.{event}"
        ),
        tracing::Level::DEBUG => tracing::debug!(
            trace_id = %trace_id, correlation_id = %corr, msg_id = %msg_id,
            from = %from, to = %to, from_agent = %from_agent, to_agent = %to_agent,
            node_id = %node_id, topic = %topic,
            "al.{event}"
        ),
        _ => tracing::info!(
            trace_id = %trace_id, correlation_id = %corr, msg_id = %msg_id,
            from = %from, to = %to, from_agent = %from_agent, to_agent = %to_agent,
            node_id = %node_id, topic = %topic,
            "al.{event}"
        ),
    }
}

// ---- tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env_with(msg_id: &str, reply_to: Option<&str>, trace_id: Option<&str>) -> Envelope {
        let mut e = Envelope::business(
            "nodeA",
            "nodeB",
            "text",
            "send",
            json!({"topic": "task_spec", "correlation": "c1"}),
            None,
            reply_to.map(|s| s.to_string()),
            None,
            Some("alice".to_string()),
            Some("bob".to_string()),
        );
        e.msg_id = msg_id.to_string();
        e.trace_id = trace_id.map(|s| s.to_string());
        e
    }

    #[test]
    fn resolve_reuses_envelope_trace() {
        let map = TraceMap::new(8);
        let t = resolve(Some("t-given"), None, &map, "deadbeefcafe0000");
        assert_eq!(t, "t-given");
    }

    #[test]
    fn resolve_continues_reply_chain_from_map() {
        let mut map = TraceMap::new(8);
        map.put("aaaabbbb11112222", "t-aaaabbbb");
        let t = resolve(None, Some("aaaabbbb11112222"), &map, "ccccdddd33334444");
        assert_eq!(t, "t-aaaabbbb");
    }

    #[test]
    fn resolve_derives_from_msg_id_when_unknown() {
        let map = TraceMap::new(8);
        let t = resolve(None, Some("never-seen"), &map, "1234567890abcdef");
        assert_eq!(t, "t-12345678");
        assert_eq!(resolve(None, None, &map, "ffffffff"), "t-ffffffff");
    }

    #[test]
    fn empty_incoming_trace_is_ignored() {
        let map = TraceMap::new(8);
        assert_eq!(resolve(Some(""), None, &map, "abcd1234ef"), "t-abcd1234");
    }

    #[test]
    fn map_is_bounded_with_insertion_eviction() {
        let mut map = TraceMap::new(2);
        map.put("m1", "t-1");
        map.put("m2", "t-2");
        map.put("m3", "t-3");
        assert!(map.get("m1").is_none());
        assert_eq!(map.get("m2").map(String::as_str), Some("t-2"));
        assert_eq!(map.get("m3").map(String::as_str), Some("t-3"));
    }

    #[test]
    fn resolve_and_remember_injects_then_replies_chain() {
        let mut map = TraceMap::new(16);
        let spec = env_with("specid01", None, None);
        let t = resolve_and_remember(&mut map, &spec);
        assert_eq!(t, "t-specid01");
        assert_eq!(map.get("specid01").map(String::as_str), Some("t-specid01"));
        // A reply that does not echo trace_id still continues via reply_to.
        let reply = env_with("replyid1", Some("specid01"), None);
        assert_eq!(resolve_and_remember(&mut map, &reply), "t-specid01");
        // A reply that echoes trace_id wins outright.
        let echoed = env_with("replyid2", Some("specid01"), Some("t-echo"));
        assert_eq!(resolve_and_remember(&mut map, &echoed), "t-echo");
    }

    #[test]
    fn trace_id_field_is_additive_and_optional() {
        // Legacy wire JSON without trace_id deserializes fine...
        let legacy = r#"{"v":1,"msg_id":"m1","from":"a","to":"b","type":"text","action":"send","payload":{},"ts":"2026-01-01T00:00:00Z"}"#;
        let e: Envelope = serde_json::from_str(legacy).unwrap();
        assert!(e.trace_id.is_none());
        // ...and an unset trace_id never appears in serialized bytes, so
        // frames not touched by the trace kernel stay byte-identical.
        let out = serde_json::to_string(&e).unwrap();
        assert!(!out.contains("trace_id"));
        let mut e2 = e;
        e2.trace_id = Some("t-m1".into());
        assert!(serde_json::to_string(&e2).unwrap().contains("t-m1"));
    }

    #[test]
    fn json_layer_line_carries_the_full_field_set() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct SharedBuf(Arc<Mutex<Vec<u8>>>);
        impl Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedBuf {
            type Writer = SharedBuf;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
            .with_writer(SharedBuf(buf.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let env = env_with("msg99", None, Some("t-specid01"));
            log_event(
                tracing::Level::INFO,
                "message.incoming",
                "nodeA",
                "t-specid01",
                Some(&env),
            );
        });
        let raw = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        let line: serde_json::Value = match serde_json::from_str(raw.trim()) {
            Ok(v) => v,
            Err(e) => panic!("not a json log line: {raw} ({e})"),
        };
        let fields = &line["fields"];
        for key in [
            "trace_id",
            "correlation_id",
            "msg_id",
            "from",
            "to",
            "from_agent",
            "to_agent",
            "node_id",
            "topic",
        ] {
            assert!(fields.get(key).is_some(), "missing field {key} in {raw}");
        }
        assert_eq!(fields["trace_id"], "t-specid01");
        assert_eq!(fields["correlation_id"], "c1");
        assert_eq!(fields["topic"], "task_spec");
        assert_eq!(fields["from_agent"], "alice");
        assert_eq!(fields["to_agent"], "bob");
        assert_eq!(fields["node_id"], "nodeA");
        assert_eq!(fields["message"], "al.message.incoming");
        assert_eq!(line["level"], "INFO");
    }

    #[test]
    fn payload_field_extracts_or_none() {
        let p = json!({"topic": "task_spec", "correlation": "", "n": 3});
        assert_eq!(payload_field(&p, "topic"), Some("task_spec"));
        assert_eq!(payload_field(&p, "correlation"), None);
        assert_eq!(payload_field(&p, "missing"), None);
        assert_eq!(payload_field(&p, "n"), None);
    }
}
