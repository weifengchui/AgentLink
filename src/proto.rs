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
    /// Machine-level capability labels.
    #[serde(default)]
    pub tags: Vec<String>,
    pub ts: String,
}

impl DiscoveryMessage {
    pub fn new(action: &str, node_id: &str, name: &str, port: u16, tags: &[String]) -> Self {
        DiscoveryMessage {
            discovery: DISCOVERY_VERSION,
            action: action.to_string(),
            node_id: node_id.to_string(),
            name: name.to_string(),
            port,
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
        }
    }

    /// Verify the peer proved knowledge of the shared cluster token.
    pub fn verify(&self, token: &str) -> bool {
        let expected = sign(token, &self.node_id, &self.nonce, self.v);
        constant_time_eq(self.auth.as_bytes(), expected.as_bytes())
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
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
            ttl_seconds: None,
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
            ttl_seconds,
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
    let Ok(t) = OffsetDateTime::parse(ts, &Rfc3339) else {
        return false;
    };
    let now = OffsetDateTime::now_utc();
    let delta = (now - t).whole_seconds();
    delta.abs() <= skew_seconds
}
