// SPDX-License-Identifier: Apache-2.0
//! Wire protocol types (see `docs/protocol.md`).

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Message protocol version carried in every business frame.
pub const PROTOCOL_VERSION: u32 = 1;
/// Discovery protocol version.
pub const DISCOVERY_VERSION: u32 = 1;

/// Current time as RFC3339 (UTC).
pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| String::new())
}

/// v0.8 包⑦ time-travel：调用方给的截断时刻必须能按 RFC3339 解析，
/// 否则接口 400，而不是静默回放全环。
pub fn is_rfc3339(s: &str) -> bool {
    OffsetDateTime::parse(s, &Rfc3339).is_ok()
}

/// `a <= b`（RFC3339 时刻比较，含边界）。任一侧解析失败返回 `None`。
pub fn ts_le(a: &str, b: &str) -> Option<bool> {
    let pa = OffsetDateTime::parse(a, &Rfc3339).ok()?;
    let pb = OffsetDateTime::parse(b, &Rfc3339).ok()?;
    Some(pa <= pb)
}

/// A fresh random identifier for `msg_id` / `file_id` / nonce.
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// HMAC-SHA256 over `node_id|nonce|v`, hex encoded. Used for the handshake.
pub fn sign(token: &str, node_id: &str, nonce: &str, v: u32) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(token.as_bytes()).expect("HMAC accepts any key length");
    mac.update(format!("{node_id}|{nonce}|{v}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time byte comparison.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---- discovery --------------------------------------------------------------

/// UDP broadcast discovery frame. Contains no IP: receivers use the datagram
/// source address instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMessage {
    pub discovery: u32,
    /// `announce` or `bye`.
    pub action: String,
    pub node_id: String,
    pub name: String,
    /// Peer WebSocket port.
    pub port: u16,
    /// mTLS 节点间 TLS 端口（v0.6 P1）：仅当本节点开启 mTLS 时携带；对端据此
    /// 以 `wss://` 拨号。老节点忽略未知字段，故为加性协议。
    #[serde(default)]
    pub mtls_port: Option<u16>,
    /// Machine-level capability labels.
    #[serde(default)]
    pub tags: Vec<String>,
    pub ts: String,
}

impl DiscoveryMessage {
    pub fn new(
        action: &str,
        node_id: &str,
        name: &str,
        port: u16,
        mtls_port: Option<u16>,
        tags: &[String],
    ) -> Self {
        DiscoveryMessage {
            discovery: DISCOVERY_VERSION,
            action: action.to_string(),
            node_id: node_id.to_string(),
            name: name.to_string(),
            port,
            mtls_port,
            tags: tags.to_vec(),
            ts: now_rfc3339(),
        }
    }
}

// ---- handshake --------------------------------------------------------------

/// First frame exchanged on every peer WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    #[serde(rename = "type")]
    pub kind: String,
    pub action: String,
    pub v: u32,
    pub node_id: String,
    pub name: String,
    pub nonce: String,
    pub auth: String,
    /// 本节点当前信任锚（集群 CA 证书）的 SHA-256 指纹；无 CA 时为 None。
    /// 加性字段（v0.6 P1 CA 自动分发）：老节点忽略，新节点据此判断是否需要
    /// 向对端推送集群 CA。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_fp: Option<String>,
    /// 本节点是否参与 mTLS（`mtls.enabled`）。加性字段。用于区分「已开 mTLS
    /// 但尚无 CA（等待领取）」与「未开 mTLS」——只有前者才应被推送 CA。
    #[serde(default)]
    pub mtls_enabled: bool,
    /// 本机硬件指纹（v0.6 P1 新节点准入）。加性字段；缺省 None（老节点）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hw_fp: Option<String>,
    /// 准入令牌证明：HMAC(bootstrap_token, node_id|nonce|v)（v0.6 P1 准入）。
    /// 加性字段；缺省 None（未启用准入/老节点）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot: Option<String>,
}

impl Hello {
    pub fn new(node_id: &str, name: &str, token: &str) -> Self {
        let nonce = new_id();
        Hello {
            kind: "ctrl".to_string(),
            action: "hello".to_string(),
            v: PROTOCOL_VERSION,
            node_id: node_id.to_string(),
            name: name.to_string(),
            auth: sign(token, node_id, &nonce, PROTOCOL_VERSION),
            nonce,
            ca_fp: None,
            mtls_enabled: false,
            hw_fp: None,
            boot: None,
        }
    }

    /// 附加本节点的 mTLS 状态（CA 指纹 + 是否参与 mTLS）。
    pub fn with_mtls(mut self, ca_fp: Option<String>, mtls_enabled: bool) -> Self {
        self.ca_fp = ca_fp;
        self.mtls_enabled = mtls_enabled;
        self
    }

    /// Verify the peer proved knowledge of the shared cluster token.
    pub fn verify(&self, token: &str) -> bool {
        let expected = sign(token, &self.node_id, &self.nonce, self.v);
        constant_time_eq(self.auth.as_bytes(), expected.as_bytes())
    }
}

// ---- CA distribution (v0.6 P1 自动分发) --------------------------------------

/// 集群 CA 分发帧（`ctrl/ca_sync`）：把一个受信 CA（证书 + 私钥）推送给尚未
/// 持有的对端。仅在 strict 模式、且对端已通过集群令牌握手后才会被接受；接收
/// 方据指纹做冲突保护（已有不同 CA 时拒绝覆盖）。
///
/// 安全说明：本帧含 CA 私钥。仅发往完成了集群令牌握手的对端——即同一信任域。
/// 与既有对称的 `cluster_token` 模型一致（持有令牌者本就可对任意 node_id 伪造
/// 握手），故不引入额外的越权面。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaSync {
    #[serde(rename = "type")]
    pub kind: String,
    pub action: String,
    pub v: u32,
    pub node_id: String,
    pub ca_cert: String,
    pub ca_key: String,
    pub fingerprint: String,
    pub ts: String,
}

impl CaSync {
    pub fn new(node_id: &str, ca_cert: &str, ca_key: &str, fingerprint: &str) -> Self {
        CaSync {
            kind: "ctrl".to_string(),
            action: "ca_sync".to_string(),
            v: PROTOCOL_VERSION,
            node_id: node_id.to_string(),
            ca_cert: ca_cert.to_string(),
            ca_key: ca_key.to_string(),
            fingerprint: fingerprint.to_string(),
            ts: now_rfc3339(),
        }
    }
}

// ---- message envelope -------------------------------------------------------

/// The unified message envelope. `payload` is opaque to AgentLink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u32,
    pub msg_id: String,
    pub from: String,
    pub to: String,
    /// Source agent id (optional; multiple agents may share one node).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_agent: Option<String>,
    /// Target agent id on the destination node (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_agent: Option<String>,
    #[serde(rename = "type")]
    pub kind: String,
    pub action: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    /// Thread id (v0.5.8 消息组): explicit topic key shared by every message
    /// of one conversation thread (e.g. `upgrade-v0.5.8-<node>`). Additive
    /// metadata with the same compatibility rules as `reply_to`/`trace_id`:
    /// serde default on read, and legacy nodes re-serialize envelopes from
    /// their own struct, so forwarding through an *un*upgraded node drops the
    /// field (expected — upgrade the orchestrating side first).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// Trace id (v0.5 §7.1): either supplied by the sending agent or
    /// injected by the kernel so downstream agents keep the chain. Additive
    /// metadata — the envelope HMAC covers only `from|msg_id|ts`, never this
    /// field, and legacy nodes ignore unknown fields on read (serde default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    /// 中继跳数（v0.7 spine/relay）：由中继节点逐跳 +1，超过 `relay.max_hops`
    /// 即丢弃（防环）。加性字段——旧节点 serde default 忽略、转发时按其自身
    /// 结构重序列化会丢弃（与 trace_id/thread 同兼容规则）；签名不覆盖本字段
    /// （同 trace_id 口径）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hops: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
}

impl Envelope {
    /// An application-level heartbeat control frame (M1).
    pub fn heartbeat(node_id: &str) -> Self {
        Envelope {
            v: PROTOCOL_VERSION,
            msg_id: new_id(),
            from: node_id.to_string(),
            to: "*".to_string(),
            from_agent: None,
            to_agent: None,
            kind: "ctrl".to_string(),
            action: "heartbeat".to_string(),
            payload: serde_json::Value::Null,
            ts: now_rfc3339(),
            reply_to: None,
            thread: None,
            trace_id: None,
            ttl_seconds: None,
            hops: None,
            sig: None,
        }
    }

    /// Lease 心跳/Full Status 帧（v0.6 P1）：`Some(cpu)` = Full Status（payload
    /// 带 cpu 0..=100），`None` = 裸心跳（payload 空）。加性 ctrl 帧——旧节点
    /// 按「未知 ctrl 帧」直接忽略（conn 底部分支），零破坏。
    pub fn lease(node_id: &str, cpu: Option<u32>) -> Self {
        Envelope {
            v: PROTOCOL_VERSION,
            msg_id: new_id(),
            from: node_id.to_string(),
            to: "*".to_string(),
            from_agent: None,
            to_agent: None,
            kind: "ctrl".to_string(),
            action: "lease".to_string(),
            payload: match cpu {
                Some(c) => serde_json::json!({ "cpu": c }),
                None => serde_json::Value::Null,
            },
            ts: now_rfc3339(),
            reply_to: None,
            thread: None,
            trace_id: None,
            ttl_seconds: None,
            hops: None,
            sig: None,
        }
    }

    /// Build a business envelope with a fresh `msg_id` and timestamp.
    #[allow(clippy::too_many_arguments)]
    pub fn business(
        node_id: &str,
        to: &str,
        kind: &str,
        action: &str,
        payload: Value,
        ttl_seconds: Option<u64>,
        reply_to: Option<String>,
        thread: Option<String>,
        from_agent: Option<String>,
        to_agent: Option<String>,
    ) -> Self {
        Envelope {
            v: PROTOCOL_VERSION,
            msg_id: new_id(),
            from: node_id.to_string(),
            to: to.to_string(),
            from_agent,
            to_agent,
            kind: kind.to_string(),
            action: action.to_string(),
            payload,
            ts: now_rfc3339(),
            reply_to,
            thread,
            trace_id: None,
            ttl_seconds,
            hops: None,
            sig: None,
        }
    }

    /// Delivery acknowledgement for `ref_id`.
    pub fn ack(node_id: &str, ref_id: &str) -> Self {
        Envelope::business(
            node_id,
            "*",
            "ctrl",
            "ack",
            json!({ "ref": ref_id }),
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// Peer up/down event for local subscribers (never sent over the wire).
    pub fn event(node_id: &str, action: &str, peer_id: &str, peer_name: &str) -> Self {
        Envelope::business(
            node_id,
            "*",
            "event",
            action,
            json!({ "node_id": peer_id, "name": peer_name }),
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// Agent directory push frame between nodes (`ctrl/agents_update`).
    #[allow(clippy::too_many_arguments)]
    pub fn agents_update(
        node_id: &str,
        to: &str,
        payload: Value,
        from_agent: Option<String>,
        to_agent: Option<String>,
    ) -> Self {
        Envelope::business(
            node_id,
            to,
            "ctrl",
            "agents_update",
            payload,
            None,
            None,
            None,
            from_agent,
            to_agent,
        )
    }
}

/// Signature used on business frames: HMAC-SHA256 over `node_id|msg_id|ts`.
pub fn envelope_sign(token: &str, node_id: &str, msg_id: &str, ts: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(token.as_bytes()).expect("HMAC accepts any key length");
    mac.update(format!("{node_id}|{msg_id}|{ts}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Validate the timestamp windows for replay protection (±`skew_seconds`).
pub fn ts_within_skew(ts: &str, skew_seconds: i64) -> bool {
    let Some(age) = ts_age_seconds(ts) else {
        return false;
    };
    age.abs() <= skew_seconds
}

/// Seconds elapsed since `ts` (negative when `ts` is in the future);
/// `None` if unparseable.
pub fn ts_age_seconds(ts: &str) -> Option<i64> {
    let t = OffsetDateTime::parse(ts, &Rfc3339).ok()?;
    Some((OffsetDateTime::now_utc() - t).whole_seconds())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_mtls_port_is_additive() {
        // 老节点广播（无 mtls_port 字段）必须可解析，且为 None（向后兼容）。
        let legacy = r#"{"discovery":1,"action":"announce","node_id":"a","name":"n",
                         "port":8765,"tags":[],"ts":"2026-01-01T00:00:00Z"}"#;
        let m: DiscoveryMessage = serde_json::from_str(legacy).unwrap();
        assert_eq!(m.mtls_port, None);

        // 新节点广播携带 mtls_port。
        let msg = DiscoveryMessage::new("announce", "a", "n", 8765, Some(8766), &[]);
        let dumped = serde_json::to_string(&msg).unwrap();
        assert!(dumped.contains("\"mtls_port\":8766"));
        let back: DiscoveryMessage = serde_json::from_str(&dumped).unwrap();
        assert_eq!(back.mtls_port, Some(8766));
    }

    #[test]
    fn hello_mtls_fields_are_additive() {
        // 老节点握手帧（无 ca_fp / mtls_enabled）必须可解析，且安全回落。
        let legacy = r#"{"type":"ctrl","action":"hello","v":1,"node_id":"a","name":"n",
                         "nonce":"x","auth":"deadbeef"}"#;
        let h: Hello = serde_json::from_str(legacy).unwrap();
        assert_eq!(h.ca_fp, None);
        assert!(!h.mtls_enabled);

        // 新节点：with_mtls 携带指纹与开关；None 指纹不落 JSON。
        let plain = Hello::new("a", "n", "tok");
        assert!(serde_json::to_string(&plain)
            .unwrap()
            .contains("\"mtls_enabled\":false"));
        assert!(!serde_json::to_string(&plain).unwrap().contains("ca_fp"));

        let withfp = Hello::new("a", "n", "tok").with_mtls(Some("abc".into()), true);
        let dumped = serde_json::to_string(&withfp).unwrap();
        assert!(dumped.contains("\"ca_fp\":\"abc\""));
        assert!(dumped.contains("\"mtls_enabled\":true"));
        assert!(withfp.verify("tok"));
    }

    #[test]
    fn ca_sync_roundtrip() {
        let frame = CaSync::new("a", "CERT", "KEY", "fp");
        let dumped = serde_json::to_string(&frame).unwrap();
        let back: CaSync = serde_json::from_str(&dumped).unwrap();
        assert_eq!(back.action, "ca_sync");
        assert_eq!(back.ca_cert, "CERT");
        assert_eq!(back.ca_key, "KEY");
        assert_eq!(back.fingerprint, "fp");
    }

    #[test]
    fn hello_admission_fields_are_additive() {
        // 老帧（无 hw_fp / boot）可解析为 None —— 与 v0.5.9 节点互通。
        let legacy = r#"{"type":"ctrl","action":"hello","v":1,"node_id":"a","name":"n",
                         "nonce":"x","auth":"deadbeef"}"#;
        let h: Hello = serde_json::from_str(legacy).unwrap();
        assert_eq!(h.hw_fp, None);
        assert_eq!(h.boot, None);

        // 新帧：字段在非 None 时序列化并可往返。
        let mut sealed = Hello::new("a", "n", "tok");
        sealed.hw_fp = Some("aa11".into());
        sealed.boot = Some("beef".into());
        let dumped = serde_json::to_string(&sealed).unwrap();
        assert!(dumped.contains("\"hw_fp\":\"aa11\""));
        assert!(dumped.contains("\"boot\":\"beef\""));
        let back: Hello = serde_json::from_str(&dumped).unwrap();
        assert_eq!(back.hw_fp.as_deref(), Some("aa11"));
        assert_eq!(back.boot.as_deref(), Some("beef"));

        // 缺省构造不落这两个键（对老节点最省字节）。
        let plain = serde_json::to_string(&Hello::new("a", "n", "tok")).unwrap();
        assert!(!plain.contains("hw_fp"));
        assert!(!plain.contains("\"boot\""));
    }
}
