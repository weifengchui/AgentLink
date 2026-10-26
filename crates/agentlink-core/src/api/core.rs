// SPDX-License-Identifier: Apache-2.0
//! 传输面 handlers：send / peers / files / history / config / web / ws 等本地 API 与对等通道。

//! Local HTTP API consumed by agents on the same machine, plus the peer-facing
//! WebSocket endpoint.
//!
//! A single listener serves both; `/v1/*` requires the local API token while
//! `/peer/ws` authenticates via the cluster token during the handshake.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::Path as UrlPath;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast;

use super::bridge::{MeshTaskMetrics, UploadAdmit};
use super::{
    authorized_role, forbidden, read_only_guard, token_namespace, unauthorized, valid_tenant_name,
    AppState, JsonAny, SharedState, VERSION,
};
use crate::config::{Config, Role};
use crate::conn;
use crate::proto::{constant_time_eq, Envelope};

/// Live-adjustable identity fields (name / tags), synced without restart.
#[derive(Debug, Clone)]
pub struct Runtime {
    pub name: String,
    pub tags: Vec<String>,
}

/// Liveness probe. Intentionally unauthenticated.
pub(super) async fn health(State(state): State<SharedState>) -> impl IntoResponse {
    // v0.6 M1：features 动态化（设计 §2.2）。默认态（mesh 关/license 无效，
    // state.mesh=None）输出数组与 0.5.9 **逐字符一致**——单测
    // `health_features_default_pins_059` 钉死，编排方按旧数组硬编码不受影响；
    // 仅 mesh 生效（引擎态真实装配）时追加 "engine"。
    let mut features: Vec<&str> =
        vec!["events", "redact", "tasks", "json-log", "lockdown", "hello"];
    if state.bridge().is_some_and(|b| b.engine_active()) {
        features.push("engine");
    }
    Json(json!({
        "ok": true,
        "version": VERSION,
        "node_id": state.config.node_id,
        "name": state.display_name(),
        "tags": state.tags(),
        "uptime_seconds": state.uptime_seconds(),
        // v0.5 §5: capability probe for orchestrators — endpoints listed
        // here exist on this node; absence means "degrade to legacy polling".
        // "json-log" (v0.5 §7.1): structured JSON logging with trace_id
        // injection is available (`log.format` = json / `--log-format json`).
        // "lockdown" (v0.5 §4.5): 一键熔断端点存在。
        "features": features,
    }))
}

pub(super) async fn metrics_get(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized().into_response();
    };
    if !role.allows("/v1/metrics") {
        return forbidden().into_response();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r.into_response();
    }

    let router_m = state.router.router_metrics();

    // Mesh 层任务指标（仅 mesh 启用时可用）。
    let MeshTaskMetrics {
        tasks_total,
        tasks_running,
        tasks_pending,
        tasks_succeeded,
        tasks_failed,
        tasks_declined,
        tasks_cancelled,
        scheduled_nodes,
        node_quota,
    } = state.bridge().map(|b| b.task_metrics()).unwrap_or_default();

    let mut out = String::new();

    // ---- 路由层指标 ----
    out.push_str("# HELP agentlink_peers_online Number of online mesh peers.\n");
    out.push_str("# TYPE agentlink_peers_online gauge\n");
    out.push_str(&format!(
        "agentlink_peers_online {}\n",
        router_m.peers_online
    ));

    out.push_str("# HELP agentlink_pending_acks Number of messages waiting for ACK.\n");
    out.push_str("# TYPE agentlink_pending_acks gauge\n");
    out.push_str(&format!(
        "agentlink_pending_acks {}\n",
        router_m.pending_acks
    ));

    out.push_str("# HELP agentlink_offline_queue_len Number of messages in offline queues.\n");
    out.push_str("# TYPE agentlink_offline_queue_len gauge\n");
    out.push_str(&format!(
        "agentlink_offline_queue_len {}\n",
        router_m.offline_queue_len
    ));

    out.push_str("# HELP agentlink_local_pending_count Number of locally pending messages.\n");
    out.push_str("# TYPE agentlink_local_pending_count gauge\n");
    out.push_str(&format!(
        "agentlink_local_pending_count {}\n",
        router_m.local_pending_count
    ));

    out.push_str("# HELP agentlink_inbox_len Number of messages in local inbox.\n");
    out.push_str("# TYPE agentlink_inbox_len gauge\n");
    out.push_str(&format!("agentlink_inbox_len {}\n", router_m.inbox_len));

    out.push_str("# HELP agentlink_blocked_count Total number of lockdown-blocked frames.\n");
    out.push_str("# TYPE agentlink_blocked_count counter\n");
    out.push_str(&format!(
        "agentlink_blocked_count {}\n",
        router_m.blocked_count
    ));

    // ---- Lease 差异上报（v0.6 P1）：仅 online 且对端发过 Full Status 的节点
    //      产出 series；未启用 lease 的集群零新增 series（导出形态不变）。
    let peers_snapshot = state.peers.snapshot();
    let with_load: Vec<(&str, u32)> = peers_snapshot
        .iter()
        .filter(|p| p.online)
        .filter_map(|p| p.load.as_ref().map(|l| (p.node_id.as_str(), l.cpu)))
        .collect();
    if !with_load.is_empty() {
        out.push_str(
            "# HELP agentlink_peer_cpu_percent Peer CPU utilization (lease full status).\n",
        );
        out.push_str("# TYPE agentlink_peer_cpu_percent gauge\n");
        for (node, cpu) in &with_load {
            out.push_str(&format!(
                "agentlink_peer_cpu_percent{{node=\"{node}\"}} {cpu}\n"
            ));
        }
    }

    // ---- Mesh/引擎层任务指标 ----
    out.push_str("# HELP agentlink_tasks_total Total number of tasks in engine table.\n");
    out.push_str("# TYPE agentlink_tasks_total gauge\n");
    out.push_str(&format!("agentlink_tasks_total {}\n", tasks_total));

    out.push_str("# HELP agentlink_tasks_running Number of tasks in running state.\n");
    out.push_str("# TYPE agentlink_tasks_running gauge\n");
    out.push_str(&format!("agentlink_tasks_running {}\n", tasks_running));

    out.push_str("# HELP agentlink_tasks_pending Number of tasks in pending state.\n");
    out.push_str("# TYPE agentlink_tasks_pending gauge\n");
    out.push_str(&format!("agentlink_tasks_pending {}\n", tasks_pending));

    out.push_str("# HELP agentlink_tasks_succeeded Number of tasks in succeeded state.\n");
    out.push_str("# TYPE agentlink_tasks_succeeded gauge\n");
    out.push_str(&format!("agentlink_tasks_succeeded {}\n", tasks_succeeded));

    out.push_str("# HELP agentlink_tasks_failed Number of tasks in failed state.\n");
    out.push_str("# TYPE agentlink_tasks_failed gauge\n");
    out.push_str(&format!("agentlink_tasks_failed {}\n", tasks_failed));

    out.push_str("# HELP agentlink_tasks_declined Number of tasks in declined state.\n");
    out.push_str("# TYPE agentlink_tasks_declined gauge\n");
    out.push_str(&format!("agentlink_tasks_declined {}\n", tasks_declined));

    out.push_str("# HELP agentlink_tasks_cancelled Number of tasks in cancelled state.\n");
    out.push_str("# TYPE agentlink_tasks_cancelled gauge\n");
    out.push_str(&format!("agentlink_tasks_cancelled {}\n", tasks_cancelled));

    out.push_str(
        "# HELP agentlink_scheduled_nodes Number of nodes scheduled by Mesh (quota used).\n",
    );
    out.push_str("# TYPE agentlink_scheduled_nodes gauge\n");
    out.push_str(&format!("agentlink_scheduled_nodes {}\n", scheduled_nodes));

    out.push_str("# HELP agentlink_node_quota License node quota (max schedulable nodes).\n");
    out.push_str("# TYPE agentlink_node_quota gauge\n");
    out.push_str(&format!("agentlink_node_quota {}\n", node_quota));

    Response::builder()
        .status(StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(out))
        .unwrap()
}

/// Node status. Requires a valid local API token.
pub(super) async fn status(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/status") {
        return forbidden();
    }

    let peers = state.peers.snapshot();
    let online = state.router.peers_online();
    let offline_queue = state.router.offline_queue_len().await;

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "version": VERSION,
            "node_id": state.config.node_id,
            "name": state.display_name(),
            "listen_port": state.config.listen_port,
            "listen_addr": local_ip_address::local_ip()
                .map(|ip| format!("{}:{}", ip, state.config.listen_port))
                .unwrap_or_else(|_| format!("0.0.0.0:{}", state.config.listen_port)),
            "uptime_seconds": state.uptime_seconds(),
            "peers_known": peers.len(),
            "peers_online": online,
            "pending_acks": state.router.pending_acks(),
            "offline_queue": offline_queue,
            "inbox": state.router.inbox_len(),
            "durable": state.router.durable(),
            "local_pending": state.router.local_pending_count(),
            "active_transfers": state.files.active(),
            "mode": state.router.mesh_mode().as_str(),
            "read_only": state.router.is_read_only(),
            "guardrails": state
                .bridge()
                .map(|b| b.guardrails_status())
                .unwrap_or_else(crate::api::default_guardrails_status),
        })),
    )
}

/// List known peers and their online state.
pub(super) async fn peers(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/peers") {
        return forbidden();
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "peers": merged_peer_views(&state) })),
    )
}

/// 直连 peer（PeerTable）+ v0.7 第二波「经中继学到的目录」合并视图。
/// 直连条目优先（同 node_id 时不追加）；目录条目带 `relayed=true`/`via`。
pub(super) fn merged_peer_views(state: &AppState) -> Vec<crate::peer::PeerView> {
    let mut peers = state.peers.snapshot();
    let known: std::collections::HashSet<String> =
        peers.iter().map(|p| p.node_id.clone()).collect();
    let self_id = &state.config.node_id;
    for d in state.router.dir_snapshot() {
        if d.node_id != *self_id && !known.contains(&d.node_id) {
            peers.push(d);
        }
    }
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    peers
}

/// Persist the current suppress list under the node's data dir.
pub(super) fn persist_forgotten(state: &AppState) -> Vec<String> {
    let ids = state.peers.suppressed();
    let path = state.config.data_dir.join("forgotten_peers.json");
    let _ = std::fs::write(
        &path,
        serde_json::to_string_pretty(&ids).unwrap_or_default(),
    );
    ids
}

/// Forget a (ghost) peer: removes it from the table and suppresses
/// re-discovery until that node handshakes live again.
pub(super) async fn peers_forget(
    State(state): State<SharedState>,
    headers: HeaderMap,
    UrlPath(node_id): UrlPath<String>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/peers/:node_id/forget") {
        return forbidden();
    }
    let removed = state.peers.forget(&node_id);
    let ids = persist_forgotten(&state);
    state.router.close_peer(&node_id);
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "removed": removed, "suppressed": ids })),
    )
}

pub(super) async fn peers_unforget(
    State(state): State<SharedState>,
    headers: HeaderMap,
    UrlPath(node_id): UrlPath<String>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/peers/:node_id/unforget") {
        return forbidden();
    }
    state.peers.unforget(&node_id);
    let ids = persist_forgotten(&state);
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "suppressed": ids })),
    )
}

// ---- messaging --------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct SendRequest {
    to: String,
    #[serde(rename = "type")]
    kind: String,
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    reply_to: Option<String>,
    /// 消息组（v0.5.8 R2）：显式线程键，同 thread 的消息在星图归为一个话题组。
    #[serde(default)]
    thread: Option<String>,
    /// Sender agent id (multiple agents may share this node).
    #[serde(default)]
    from_agent: Option<String>,
    /// Target agent id on the destination node.
    #[serde(default)]
    to_agent: Option<String>,
    /// Optional trace id (v0.5 §7.1): echo a previously injected value to
    /// continue a chain explicitly; when absent the kernel resolves it.
    #[serde(default)]
    trace_id: Option<String>,
}

/// Send a message to a peer (`node_id` or name) or broadcast to `"*"`.
pub(super) async fn send(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<SendRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/send") {
        return forbidden();
    }
    // v0.7 §4.2：绑定 namespace 的 token 不能借 from_agent/to_agent 跨租户
    // 冒充（仅对已知 agent 生效；未知 agent 由注册方责任，见文档残留）。
    if let Some(scope) = token_namespace(&state, &headers) {
        for id in [req.from_agent.as_deref(), req.to_agent.as_deref()]
            .into_iter()
            .flatten()
        {
            if let Some(ns) = state.router.agent_namespace(id) {
                if ns != "*" && ns != scope {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(json!({ "ok": false, "error": "agent namespace mismatch" })),
                    );
                }
            }
        }
    }
    let to = match resolve_target(&state, &req.to) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    };
    let mut payload = if req.payload.is_null() {
        json!({})
    } else {
        req.payload
    };
    // v0.7 §4.3：发送护栏（PII / API Key / 注入 / topic 白名单）。
    // 只扫本地 API 发送路径；引擎内部帧与节点间转发不重复扫描。
    if state.bridge().is_some_and(|b| b.guard_enabled()) {
        if let Some(b) = state.bridge() {
            let verdict = b.guard_send(&payload);
            if !verdict.allowed {
                state.router.audit_event(
                    "guardrail.blocked",
                    role.as_str(),
                    json!({
                        "from_agent": req.from_agent,
                        "to_agent": req.to_agent,
                        "type": req.kind,
                        "action": req.action,
                        "reason": verdict.reason,
                        "findings": verdict.findings,
                        "topic": payload.get("topic").and_then(|v| v.as_str()),
                    }),
                );
                state.router.notify(
                    "guardrail_alert",
                    json!({
                        "kind": "blocked",
                        "source": "send",
                        "findings": verdict.findings,
                    }),
                );
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({
                        "ok": false,
                        "error": "guardrail blocked",
                        "reason": verdict.reason,
                        "findings": verdict.categories,
                    })),
                );
            }
            if verdict.redacted {
                state.router.audit_event(
                    "guardrail.redacted",
                    role.as_str(),
                    json!({
                        "from_agent": req.from_agent,
                        "to_agent": req.to_agent,
                        "type": req.kind,
                        "action": req.action,
                        "findings": verdict.findings,
                        "topic": payload.get("topic").and_then(|v| v.as_str()),
                    }),
                );
                payload = verdict.payload;
            }
        }
    }
    let mut env = Envelope::business(
        &state.config.node_id,
        &to,
        &req.kind,
        &req.action,
        payload,
        req.ttl_seconds,
        req.reply_to,
        req.thread.filter(|t| !t.trim().is_empty()),
        req.from_agent,
        req.to_agent,
    );
    env.trace_id = req.trace_id.filter(|t| !t.is_empty());
    match state.router.send(env).await {
        Ok(msg_id) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "msg_id": msg_id })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

// ---- file sending -----------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct SendFileRequest {
    to: String,
    file_path: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

/// Send a local file to a peer via a dedicated transfer connection.
pub(super) async fn send_file(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<SendFileRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/send-file") {
        return forbidden();
    }
    let to = match resolve_target(&state, &req.to) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    };
    if to == "*" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "cannot send a file to broadcast" })),
        );
    }
    match crate::file::send_file(&state, &to, Path::new(&req.file_path), req.ttl_seconds).await {
        Ok(file_id) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "file_id": file_id, "to": to })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

/// 解码 percent-encoding（前端 encodeURIComponent 产物）。
/// HTTP 头只允许 ISO-8859-1，非 ASCII 文件名必须编码后传输；
/// 不含 `%` 或序列非法时原样返回，兼容直接发明文头名的旧客户端。
fn percent_decode(s: &str) -> String {
    if !s.contains('%') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Browser-style file upload: raw bytes in the body, file name in `X-File-Name`.
#[derive(Debug, Deserialize)]
pub(super) struct UploadParams {
    to: String,
    #[serde(default)]
    to_agent: Option<String>,
    /// Sender agent id (web console identity selector); recorded in history only.
    #[serde(default)]
    from_agent: Option<String>,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

pub(super) async fn send_file_upload(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(params): Query<UploadParams>,
    body: Bytes,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/send-file-upload") {
        return forbidden();
    }
    if body.len() > state.config.files.max_file_size as usize {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({ "ok": false, "error": format!(
                "file too large: {} bytes (limit {})", body.len(), state.config.files.max_file_size
            ) })),
        );
    }
    let to = match resolve_target(&state, &params.to) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    };
    if to == "*" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "cannot send a file to broadcast" })),
        );
    }
    let _ = params.to_agent; // files are node-level delivery for now
    let raw_name = headers
        .get("x-file-name")
        .and_then(|v| v.to_str().ok())
        .map(percent_decode)
        .unwrap_or_else(|| "upload.bin".to_string());
    let Some(name) = crate::file::sanitize_name(&raw_name) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "unsafe file name" })),
        );
    };

    // v0.8 §3.1：namespace 磁盘配额——入口受理时按请求体大小记账。
    // 缺省 enabled=false / limit=0：不记账、行为零变化。
    if let Some(b) = state.bridge() {
        let ns = token_namespace(&state, &headers).unwrap_or_else(|| "default".to_string());
        let add = body.len() as u64;
        match b.upload_admit(&state, ns.clone(), add).await {
            UploadAdmit::Pass => {}
            UploadAdmit::HaDown(reason) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "ok": false,
                        "error": "ha unavailable",
                        "reason": reason,
                    })),
                );
            }
            UploadAdmit::Reject { limit, current } => {
                state.router.audit_event(
                    "quota.reject",
                    role.as_str(),
                    json!({
                        "namespace": ns,
                        "reason": "disk_quota",
                        "limit": limit,
                        "current": current,
                        "incoming": add,
                        "file_name": name,
                    }),
                );
                state.router.notify(
                    "quota_alert",
                    json!({
                        "namespace": ns,
                        "reason": "disk_quota",
                        "limit": limit,
                        "current": current,
                        "incoming": add,
                    }),
                );
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "ok": false,
                        "error": "namespace disk quota exceeded",
                        "reason": "disk_quota",
                        "namespace": ns,
                        "limit": limit,
                        "current": current,
                        "incoming": add,
                    })),
                );
            }
        }
    }

    let dir = state
        .config
        .data_dir
        .join("uploads")
        .join(&crate::proto::new_id()[..8]);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }
    // Keep the original name as the file's basename so the receiver stores it
    // under the correct name (uniqueness comes from the directory).
    let path = dir.join(&name);
    if let Err(e) = tokio::fs::write(&path, body.as_ref()).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }

    let result = crate::file::send_transfer(
        &state,
        &to,
        &path,
        params.ttl_seconds,
        params.from_agent.as_deref(),
    )
    .await;
    let _ = tokio::fs::remove_file(&path).await;
    let _ = tokio::fs::remove_dir(&dir).await;
    match result {
        Ok(file_id) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "file_id": file_id, "to": to, "file_name": name })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

/// Resolve `to` into a `node_id` or `"*"`.
pub(super) fn resolve_target(state: &AppState, to: &str) -> Result<String, String> {
    // 本机自身 node_id 始终可达（即使不符合 12 位十六进制格式）。
    if to == state.config.node_id {
        return Ok(to.to_string());
    }
    if to == "*" {
        return Ok("*".to_string());
    }
    if to.len() == 12 && to.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(to.to_string());
    }
    for p in state.peers.snapshot() {
        if p.name == to {
            return Ok(p.node_id);
        }
    }
    Err(format!(
        "unknown target '{to}' (not a node_id or known peer name)"
    ))
}

#[derive(Debug, Deserialize)]
pub(super) struct AckInboxRequest {
    msg_id: String,
}

/// Acknowledge a locally delivered message (`/v1/ack-inbox`).
pub(super) async fn ack_inbox(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<AckInboxRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/ack-inbox") {
        return forbidden();
    }
    let acked = state.router.ack_local(&req.msg_id);
    (StatusCode::OK, Json(json!({ "ok": true, "acked": acked })))
}

// ---- agents & conversation history (M7) -------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct RegisterAgentRequest {
    agent_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    /// v0.7 §4.2：所属 namespace（缺省 = token 绑定值，否则 "default"；"*"=共享池）。
    #[serde(default)]
    namespace: Option<String>,
}

/// Register (or refresh) a local AI agent. Capabilities are broadcast to peers.
pub(super) async fn agents_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<RegisterAgentRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/agents") {
        return forbidden();
    }
    if let Some(r) = read_only_guard(&state) {
        return r;
    }
    let id = req.agent_id.trim();
    let valid = !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if !valid {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "agent_id must be 1-64 chars of [A-Za-z0-9_.-]" })),
        );
    }
    // v0.7 §4.2 多租户：token 绑定 namespace 时强制落在该 namespace；
    // 未显式给 namespace 则自动继承 token 的 namespace。
    let scope = token_namespace(&state, &headers);
    let namespace = match req.namespace {
        Some(ns) => ns.trim().to_string(),
        None => scope.clone().unwrap_or_else(|| "default".to_string()),
    };
    if !valid_agent_namespace(&namespace) {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "ok": false, "error": "namespace must be 1-64 chars of [A-Za-z0-9_.-] or '*'" }),
            ),
        );
    }
    if let Some(scope) = scope.as_deref() {
        if namespace != scope {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "ok": false, "error": "token is bound to a different namespace" })),
            );
        }
    }
    // 同名 agent 跨 namespace 不允许静默接管（refresh 仅限同 namespace）。
    if let Some(existing) = state
        .router
        .local_agents()
        .into_iter()
        .find(|a| a.agent_id == id)
    {
        let own = existing
            .namespace
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("default");
        if own != namespace {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "ok": false,
                    "error": format!("agent_id {id} already belongs to namespace {own}"),
                })),
            );
        }
    }
    let rec = state.router.register_agent(
        id,
        req.name.map(|s| s.trim().to_string()),
        req.capabilities,
        Some(namespace),
    );
    (StatusCode::OK, Json(json!({ "ok": true, "agent": rec })))
}

/// Remove a local agent (its registration vanishes from the fleet view too).
pub(super) async fn agents_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<RegisterAgentRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/agents") {
        return forbidden();
    }
    if let Some(r) = read_only_guard(&state) {
        return r;
    }
    // v0.7 §4.2：绑定 namespace 的 token 不能注销其它租户的 agent。
    let id = req.agent_id.trim();
    if let Some(scope) = token_namespace(&state, &headers) {
        match state.router.agent_namespace(id) {
            Some(ns) if ns == "*" || ns == scope => {}
            Some(_) => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "ok": false, "error": "agent namespace mismatch" })),
                )
            }
            // 绑定 token 不能借此探测/操作未知 agent。
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "ok": false, "error": format!("agent {id} not found") })),
                )
            }
        }
    }
    let removed = state.router.unregister_agent(id);
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "removed": removed })),
    )
}

/// List agents registered on this node.
pub(super) async fn agents_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/agents") {
        return forbidden();
    }
    let scope = token_namespace(&state, &headers);
    let agents: Vec<crate::router::AgentRecord> = state
        .router
        .local_agents()
        .into_iter()
        .filter(|a| agent_in_scope(a, scope.as_deref()))
        .collect();
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "node": { "node_id": state.config.node_id, "name": state.config.name },
            "agents": agents,
        })),
    )
}

/// List agents across the whole LAN fleet (local + synced from peers).
#[derive(serde::Deserialize)]
pub(super) struct FleetAgentsQuery {
    #[serde(default)]
    requires: Option<String>,
}

pub(super) async fn fleet_agents(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(q): Query<FleetAgentsQuery>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/fleet/agents") {
        return forbidden();
    }
    let mut names: HashMap<String, String> = HashMap::new();
    let mut addrs: HashMap<String, String> = HashMap::new();
    for p in merged_peer_views(&state) {
        names.insert(p.node_id.clone(), p.name.clone());
        addrs.insert(p.node_id.clone(), p.addr.clone());
    }
    names.insert(state.config.node_id.clone(), state.display_name());
    addrs.insert(
        state.config.node_id.clone(),
        format!("127.0.0.1:{}", state.config.listen_port),
    );
    // Ghost guard: an agent can only be online if its NODE is online. Remote
    // agent caches used to keep `online:true` forever after the owner node
    // went away (new identity/reboot), confusing the fleet view.
    // v0.7 第二波：非直连节点以中继目录的新鲜度判定（30s 未见即离线）。
    let node_online = |nid: &str| {
        nid == state.config.node_id || state.peers.is_online(nid) || state.router.dir_online(nid)
    };
    // v0.5: optional capability query (open-source = query only,
    // no scheduling). `?requires=cap:node:reboot,cap:edit:py` keeps only ONLINE
    // agents whose capabilities satisfy every requirement (hierarchical,
    // trailing `*` wildcard — see router::cap_matches).
    let requires: Vec<String> = q
        .requires
        .as_deref()
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_default();
    let scope = token_namespace(&state, &headers);
    let agents: Vec<serde_json::Value> = state
        .router
        .fleet_agents()
        .iter()
        .filter(|a| agent_in_scope(a, scope.as_deref()))
        .filter(|a| {
            requires.is_empty()
                || (a.online
                    && node_online(&a.node_id)
                    && requires.iter().all(|need| {
                        a.capabilities
                            .iter()
                            .any(|have| crate::router::cap_matches(have, need))
                    }))
        })
        .map(|a| {
            let mut v = serde_json::to_value(a).unwrap_or(serde_json::Value::Null);
            if let serde_json::Value::Object(m) = &mut v {
                m.insert(
                    "online".to_string(),
                    serde_json::json!(a.online && node_online(&a.node_id)),
                );
                m.insert(
                    "node_name".to_string(),
                    serde_json::json!(names
                        .get(&a.node_id)
                        .cloned()
                        .unwrap_or_else(|| a.node_id.clone())),
                );
                m.insert(
                    "node_addr".to_string(),
                    serde_json::json!(addrs.get(&a.node_id).cloned().unwrap_or_default()),
                );
            }
            v
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "agents": agents, "requires": requires })),
    )
}

/// Recent bus traffic (ring buffer) for the human view.
pub(super) async fn history_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/history") {
        return forbidden();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r;
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "history": state.router.history() })),
    )
}

/// `/v1/events` query params (v0.5 §1).
#[derive(Debug, Deserialize)]
pub(super) struct EventsQuery {
    /// Return events with `seq >= since`; pass the `next_seq` from the
    /// previous response to continue without loss on reconnect. Omitted or 0
    /// returns whatever the ring still holds.
    #[serde(default)]
    since: Option<u64>,
    /// Only events involving this agent id (as `from_agent` or `to_agent`).
    #[serde(default)]
    agent: Option<String>,
    /// Comma-separated frame types, matched against the envelope `type`
    /// (e.g. `text,file`). ctrl/heartbeat frames are never in this view.
    #[serde(default)]
    types: Option<String>,
    /// Max events per response (default 500, clamped to 5000).
    #[serde(default)]
    limit: Option<usize>,
}

/// Control-plane event stream for orchestrators (v0.5 §1): incremental,
/// append-only pull over business frames with per-node `seq`. Payloads are
/// sanitized through the redaction pipeline (§2) before output. When `since`
/// falls below the ring after a rotation the response is 409 with the oldest
/// available seq, and the caller rebuilds from full `/v1/history`.
pub(super) async fn events_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/events") {
        return forbidden();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r;
    }
    let since = q.since.unwrap_or(0);
    let limit = q.limit.unwrap_or(500).clamp(1, 5000);
    let agent = q
        .agent
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty());
    let types: Option<Vec<String>> = q.types.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    });
    let view = state
        .router
        .events_view(since, agent.as_deref(), types.as_deref(), limit);
    if view.gap {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "ok": false,
                "error": "since_before_history",
                "oldest_seq": view.oldest_seq,
                "next_seq": view.next_seq,
                "hint": "events below the ring were rotated out; rebuild from /v1/history",
            })),
        );
    }
    let events: Vec<serde_json::Value> = view
        .events
        .iter()
        .map(|e| json!({ "seq": e.seq, "ts": e.ts, "frame": e }))
        .collect();
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "events": events,
            "next_seq": view.next_seq,
            "oldest_seq": view.oldest_seq,
        })),
    )
}

// ---- v0.5 §4.5 薄档：一键熔断 lockdown --------------------------------------

/// `POST /v1/lockdown` body. `on` is required: this is a deliberate
/// management switch, no implicit defaults.
#[derive(Debug, Deserialize)]
pub(super) struct LockdownRequest {
    on: bool,
}

pub(super) fn lockdown_json(on: bool, out: u64, inn: u64) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "on": on,
        // 总被拦帧数（发出侧 + 入站侧），/web 顶栏与冒烟断言都用它。
        "blocked_count": out + inn,
        "blocked_out": out,
        "blocked_in": inn,
        "replay_on_resume": false,
    }))
}

/// `GET /v1/lockdown` — current fuse state + cumulative blocked counts.
/// Requires the local API (management) token, like every admin surface here.
pub(super) async fn lockdown_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return (StatusCode::UNAUTHORIZED, lockdown_json(false, 0, 0));
    };
    if !role.allows("/v1/lockdown") {
        return (StatusCode::FORBIDDEN, lockdown_json(false, 0, 0));
    }
    let (on, out, inn) = state.router.lockdown_stats();
    (StatusCode::OK, lockdown_json(on, out, inn))
}

/// `POST /v1/lockdown {on:true|false}` — flip the bus fuse.
///
/// v0.5 薄档语义（务必读 docs/task-protocol.md）：
/// - on：总线暂停投递/转发**业务帧**（agent→agent 消息、任务帧）；
///   **控制面帧**（status/heartbeat/注册/ack、lockdown 本身、history/
///   events/tasks 查询）不受影响。
/// - 被拦帧只留审计标记（history `/v1/events` 的 `blocked: true`、
///   `/v1/tasks` trail 的 `blocked`），并计入 `blocked_count`。
/// - off 恢复后**不补投**被拦帧（`replay_on_resume: false`）。
/// - 状态不落盘：节点重启默认恢复放行（人工应急闸）。
pub(super) async fn lockdown_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<LockdownRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return (StatusCode::UNAUTHORIZED, lockdown_json(false, 0, 0));
    };
    if !role.allows("/v1/lockdown") {
        return (StatusCode::FORBIDDEN, lockdown_json(false, 0, 0));
    }
    state.router.set_lockdown(req.on);
    let (on, out, inn) = state.router.lockdown_stats();
    state.router.audit_event(
        "lockdown.set",
        role.as_str(),
        json!({ "on": on, "blocked_out": out, "blocked_in": inn }),
    );
    (StatusCode::OK, lockdown_json(on, out, inn))
}

/// Conversation-history footprint for the web console. Metadata only, and it
/// never mutates records — history is append/roll-over, never deleted.
pub(super) async fn history_stats_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/history/stats") {
        return forbidden();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r;
    }
    let h = &state.config.history;
    if !h.enabled || h.file.is_empty() || h.file == "-" {
        return (
            StatusCode::OK,
            Json(json!({ "ok": false, "error": "history disabled" })),
        );
    }
    let mut v = crate::store::history_stats(
        &state.config.data_dir,
        &h.file,
        h.max_file_mb,
        h.archive_days,
        h.archive_keep,
    );
    v["ok"] = json!(true);
    (StatusCode::OK, Json(v))
}

#[derive(Debug, Deserialize)]
pub struct HistorySearchQuery {
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    from_node: String,
    #[serde(default)]
    from_agent: String,
    #[serde(default)]
    to_node: String,
    #[serde(default)]
    to_agent: String,
    #[serde(default)]
    who: String,
    #[serde(default)]
    direction: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    q: String,
    #[serde(default)]
    since: String,
    #[serde(default)]
    until: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    max_mb: Option<u64>,
    #[serde(default)]
    scope: String,
}

/// Disk-backed conversation-history search with filters (sender / receiver /
/// time window / keyword / max results). Streams the JSONL backwards, so hits
/// near the tail of a huge log return fast. Read-only.
pub(super) async fn history_search_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(qs): Query<HistorySearchQuery>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized().into_response();
    };
    if !role.allows("/v1/history/search") {
        return forbidden().into_response();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r.into_response();
    }
    let h = &state.config.history;
    if !h.enabled || h.file.is_empty() || h.file == "-" {
        return (
            StatusCode::OK,
            Json(json!({ "ok": false, "error": "history disabled" })),
        )
            .into_response();
    }
    let f = crate::store::HistSearch {
        from: qs.from,
        to: qs.to,
        from_node: qs.from_node,
        from_agent: qs.from_agent,
        to_node: qs.to_node,
        to_agent: qs.to_agent,
        who: qs.who,
        direction: qs.direction,
        kind: qs.kind,
        q: qs.q,
        since: qs.since,
        until: qs.until,
        limit: qs.limit.unwrap_or(200).clamp(1, 2000),
        max_bytes: qs.max_mb.unwrap_or(128).clamp(1, 2048) * 1_048_576,
        scope_all: qs.scope != "active",
    };
    let dir = state.config.data_dir.clone();
    let file = h.file.clone();
    let v = tokio::task::spawn_blocking(move || crate::store::history_search(&dir, &file, &f))
        .await
        .unwrap_or_else(|_| json!({ "ok": false, "error": "search panicked" }));
    (StatusCode::OK, Json(v)).into_response()
}

/// Identity colors for the console (see `store::COLORS_FILE`).
pub(super) async fn colors_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/colors") {
        return forbidden();
    }
    let dir = state.config.data_dir.clone();
    let m = tokio::task::spawn_blocking(move || crate::store::load_colors(&dir))
        .await
        .unwrap_or_default();
    (StatusCode::OK, Json(json!({ "ok": true, "colors": m })))
}

/// Replace the whole color map. Keys `node:<id>` / `agent:<node>|<agent>`,
/// values `#rrggbb`. Colors live on the node, so every viewer sees the same
/// palette without touching any agent's own config.
pub(super) async fn colors_put(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(body): JsonAny<serde_json::Value>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/colors") {
        return forbidden();
    }
    let Some(map) = body.get("colors").and_then(|c| c.as_object()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "expected {\"colors\": {...}}" })),
        );
    };
    if map.len() > 2000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "too many entries (max 2000)" })),
        );
    }
    for (k, v) in map {
        let hex = v.as_str().unwrap_or("");
        let good_hex = hex.len() == 7
            && hex.starts_with('#')
            && hex.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit);
        let good_key = (k.starts_with("node:") || k.starts_with("agent:")) && k.len() <= 200;
        if !good_hex || !good_key {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": format!("bad entry {k}={hex}") })),
            );
        }
    }
    let dir = state.config.data_dir.clone();
    let m = map.clone();
    match tokio::task::spawn_blocking(move || crate::store::save_colors(&dir, &m)).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": "join error" })),
        ),
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct HistoryExportQuery {
    /// `json` (raw JSONL, default) or `text` (human readable).
    #[serde(default)]
    format: String,
    /// `all` (archives + active, default), `active`, or `archive:<name>`.
    #[serde(default = "default_export_scope")]
    scope: String,
}

pub(super) fn default_export_scope() -> String {
    "all".to_string()
}

/// Stream the conversation history out as a downloadable attachment.
///
/// Export is strictly read-only: the node copies records out and deletes
/// nothing, so an operator can archive to taste (grep-able text, or JSONL for
/// re-ingest) whenever the console warns about size.
pub(super) async fn history_export_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(q): Query<HistoryExportQuery>,
) -> Response {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized().into_response();
    };
    if !role.allows("/v1/history/export") {
        return forbidden().into_response();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r.into_response();
    }
    let h = &state.config.history;
    if !h.enabled || h.file.is_empty() || h.file == "-" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "history disabled" })),
        )
            .into_response();
    }
    let files = crate::store::history_export_files(&state.config.data_dir, &h.file, &q.scope);
    if files.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "no history files to export" })),
        )
            .into_response();
    }
    let as_text = matches!(q.format.as_str(), "text" | "txt" | "md");
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let scope_tag = q
        .scope
        .replace("archive:", "")
        .replace('/', "_")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '.')
        .collect::<String>();
    let scope_tag = if scope_tag.is_empty() {
        "all".to_string()
    } else {
        scope_tag
    };
    let ext = if as_text { "txt" } else { "jsonl" };
    let name = format!("agentlink-history-{scope_tag}-{secs}.{ext}");

    struct Step {
        files: Vec<PathBuf>,
        idx: usize,
        file: Option<tokio::fs::File>,
        carry: Vec<u8>,
        text: bool,
    }
    let step = Step {
        files,
        idx: 0,
        file: None,
        carry: Vec::new(),
        text: as_text,
    };
    let stream = futures_util::stream::unfold(step, |mut s| async move {
        loop {
            if s.file.is_none() {
                if s.idx >= s.files.len() {
                    if s.carry.is_empty() {
                        return None;
                    }
                    let out = std::mem::take(&mut s.carry);
                    return Some((Ok::<_, std::io::Error>(Bytes::from(out)), s));
                }
                s.file = tokio::fs::File::open(&s.files[s.idx]).await.ok();
                s.idx += 1;
                if s.file.is_none() {
                    continue;
                }
            }
            let mut buf = vec![0u8; 256 * 1024];
            let read = match s.file.as_mut() {
                Some(f) => f.read(&mut buf).await,
                None => return None,
            };
            match read {
                Ok(0) => {
                    s.file = None;
                    continue;
                }
                Ok(n) => {
                    buf.truncate(n);
                    if !s.text {
                        let mut out = std::mem::take(&mut s.carry);
                        out.extend_from_slice(&buf);
                        return Some((Ok::<_, std::io::Error>(Bytes::from(out)), s));
                    }
                    s.carry.extend_from_slice(&buf);
                    let mut chunk: Vec<u8> = Vec::new();
                    let mut consumed = 0usize;
                    while let Some(rel) = s.carry[consumed..].iter().position(|b| *b == b'\n') {
                        let line =
                            String::from_utf8_lossy(&s.carry[consumed..consumed + rel]).to_string();
                        consumed += rel + 1;
                        chunk.extend_from_slice(
                            crate::store::history_line_to_text(&line).as_bytes(),
                        );
                        chunk.push(b'\n');
                    }
                    s.carry.drain(..consumed);
                    if chunk.is_empty() {
                        continue;
                    }
                    return Some((Ok::<_, std::io::Error>(Bytes::from(chunk)), s));
                }
                Err(e) => return Some((Err(e), s)),
            }
        }
    });

    let mut resp = Response::new(Body::from_stream(stream));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(if as_text {
            "text/plain; charset=utf-8"
        } else {
            "application/x-ndjson; charset=utf-8"
        }),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")) {
        resp.headers_mut()
            .insert(axum::http::header::CONTENT_DISPOSITION, v);
    }
    resp
}

/// Download a file that this node received (served from `files.recv_dir`).
pub(super) async fn file_download(
    UrlPath(file_name): UrlPath<String>,
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Response {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized().into_response();
    };
    if !role.allows("/v1/files/:file_name") {
        return forbidden().into_response();
    }
    if let Some(r) = namespace_global_read_forbidden(&state, &headers) {
        return r.into_response();
    }
    let Some(name) = crate::file::sanitize_name(&file_name) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "invalid file name" })),
        )
            .into_response();
    };
    let path = state.config.files.recv_dir_resolved().join(&name);
    if !path.is_file() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "file not found" })),
        )
            .into_response();
    }
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let mut resp = Response::new(Body::from(bytes));
            resp.headers_mut().insert(
                "Content-Type",
                HeaderValue::from_static("application/octet-stream"),
            );
            resp.headers_mut().insert(
                "Content-Disposition",
                HeaderValue::from_static("attachment"),
            );
            resp.headers_mut().insert(
                "X-Content-Type-Options",
                HeaderValue::from_static("nosniff"),
            );
            resp
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": "read failed" })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct OpenFolderRequest {
    file: String,
}

/// Open the folder (highlighting the file) in the OS file manager.
/// Local-only (loopback) + token required, since it triggers an OS action.
pub(super) async fn open_folder(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<OpenFolderRequest>,
) -> Response {
    if !addr.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "ok": false, "error": "remote address; not allowed" })),
        )
            .into_response();
    }
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized().into_response();
    };
    if !role.allows("/v1/open-folder") {
        return forbidden().into_response();
    }
    let Some(name) = crate::file::sanitize_name(&req.file) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "invalid file name" })),
        )
            .into_response();
    };
    let path = state.config.files.recv_dir_resolved().join(&name);
    let dir = path.parent().map(|p| p.to_path_buf());
    let Some(dir) = dir else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "no folder" })),
        )
            .into_response();
    };
    if !dir.is_dir() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "folder not found" })),
        )
            .into_response();
    }
    let opened = if path.is_file() {
        open_in_folder(&path)
    } else {
        open_dir(&dir)
    };
    (StatusCode::OK, Json(json!({ "ok": opened }))).into_response()
}

/// Windows: `explorer /select,<file>`; Linux: open parent dir; macOS: `open -R`.
pub(super) fn open_in_folder(path: &Path) -> bool {
    #[cfg(target_os = "windows")]
    {
        Command::new("explorer")
            .arg("/select,")
            .arg(path)
            .spawn()
            .is_ok()
    }
    #[cfg(target_os = "linux")]
    {
        path.parent()
            .and_then(|d| Command::new("xdg-open").arg(d).spawn().ok())
            .is_some()
    }
    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg("-R").arg(path).spawn().is_ok()
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

pub(super) fn open_dir(dir: &Path) -> bool {
    #[cfg(target_os = "windows")]
    {
        Command::new("explorer").arg(dir).spawn().is_ok()
    }
    #[cfg(target_os = "linux")]
    {
        Command::new("xdg-open").arg(dir).spawn().is_ok()
    }
    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg(dir).spawn().is_ok()
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

// ---- config view / edit (web 配置页) ---------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct ConfigUpdateRequest {
    name: Option<String>,
    tags: Option<Vec<String>>,
    log_level: Option<String>,
    history_len: Option<usize>,
    max_file_size: Option<u64>,
    static_peers: Option<Vec<String>>,
    cluster_token: Option<String>,
    auto_open_web: Option<bool>,
}

/// Mask secrets so the config view never leaks full tokens.
pub(super) fn mask_secret(s: &str) -> String {
    if s.len() <= 8 {
        "****".to_string()
    } else {
        format!("{}…{}", &s[..4], &s[s.len() - 2..])
    }
}

/// Read the effective configuration (secrets masked).
pub(super) async fn config_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/config") {
        return forbidden();
    }
    let c = &state.config;
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "config": {
                "name": state.display_name(),
                "tags": state.tags(),
                "node_id": c.node_id,
                "listen_port": c.listen_port,
                "data_dir": c.data_dir,
                "discovery": {
                    "enabled": c.discovery.enabled,
                    "methods": c.discovery.methods,
                    "broadcast_port": c.discovery.broadcast_port,
                    "announce_interval_seconds": c.discovery.announce_interval_seconds,
                    "heartbeat_interval_seconds": c.discovery.heartbeat_interval_seconds,
                    "offline_timeout_seconds": c.discovery.offline_timeout_seconds,
                    "static_peers": c.discovery.static_peers,
                },
                "security": {
                    "mode": c.security.mode,
                    "cluster_token": mask_secret(&c.security.cluster_token),
                },
                "local_api": {
                    "host": c.local_api.host,
                    "token": mask_secret(&c.local_api.token),
                },
                "files": {
                    "max_file_size": c.files.max_file_size,
                    "chunk_size": c.files.chunk_size,
                    "recv_dir": c.files.recv_dir,
                    "max_concurrent": c.files.max_concurrent,
                },
                "log": { "level": c.log.level, "format": c.log.format, "file": c.log.file },
                "history": { "enabled": c.history.enabled, "len": c.history.len, "file": c.history.file,
                    "max_file_mb": c.history.max_file_mb, "archive_days": c.history.archive_days, "archive_keep": c.history.archive_keep },
                "web": { "auto_open": c.web.auto_open },
            }
        })),
    )
}

/// Update configuration. `name`/`tags` apply live; most other fields persist to
/// the config file and take effect after a node restart.
pub(super) async fn config_put(
    State(state): State<SharedState>,
    headers: HeaderMap,
    JsonAny(req): JsonAny<ConfigUpdateRequest>,
) -> impl IntoResponse {
    let Some(role) = authorized_role(&state, &headers) else {
        return unauthorized();
    };
    if !role.allows("/v1/config") {
        return forbidden();
    }
    if let Some(r) = read_only_guard(&state) {
        return r;
    }
    // 审计只记「改了哪些键」，不记任何值（cluster_token 等密钥绝不入链）。
    let mut changed: Vec<&str> = Vec::new();
    if req.name.is_some() {
        changed.push("name");
    }
    if req.tags.is_some() {
        changed.push("tags");
    }
    if req.log_level.is_some() {
        changed.push("log_level");
    }
    if req.history_len.is_some() {
        changed.push("history_len");
    }
    if req.max_file_size.is_some() {
        changed.push("max_file_size");
    }
    if req.static_peers.is_some() {
        changed.push("static_peers");
    }
    if req.cluster_token.is_some() {
        changed.push("cluster_token");
    }
    if req.auto_open_web.is_some() {
        changed.push("auto_open_web");
    }
    let name = req
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from);
    if let Some(n) = &name {
        if n.len() > 64 {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "name too long (max 64)" })),
            );
        }
    }
    if let Err(e) = state.set_identity(name.clone(), req.tags.clone()) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }

    // Reload the file (already contains updated name/tags) and merge the rest.
    let mut cfg = match Config::load(&state.config_file) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e.to_string() })),
            );
        }
    };
    if let Some(l) = &req.log_level {
        if !matches!(l.as_str(), "debug" | "info" | "warn" | "error") {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "log_level must be debug|info|warn|error" })),
            );
        }
        cfg.log.level = l.clone();
    }
    if let Some(h) = req.history_len {
        cfg.history.len = h.max(1);
    }
    if let Some(m) = req.max_file_size {
        cfg.files.max_file_size = m.max(1);
    }
    if let Some(sp) = req.static_peers {
        cfg.discovery.static_peers = sp;
    }
    if let Some(t) = req.cluster_token {
        let t = t.trim().to_string();
        if !t.is_empty() && t != cfg.security.cluster_token {
            cfg.security.cluster_token = t;
        }
    }
    if let Some(a) = req.auto_open_web {
        cfg.web.auto_open = a;
    }
    if let Err(e) = cfg.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }
    // mesh/abac/guardrails/oidc/chaos/ha 等闭源段经桥校验，
    // 非法配置同样拒存（口径与拆分前一致）。
    if let Some(b) = state.bridge() {
        if let Err(e) = b.validate_closed_config(&cfg) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": e })),
            );
        }
    }
    if let Err(e) = cfg.save(&state.config_file) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        );
    }
    state
        .router
        .audit_event("config.update", role.as_str(), json!({ "keys": changed }));
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "config": "saved",
            "restart_hint": "端口/广播端口/安全模式/日志格式/历史条数/最大文件/static_peers 保存后重启生效；名称与标签已即时生效",
        })),
    )
}

/// Auto-provision the local API token for the browser, but ONLY for requests
/// coming from the loopback interface (the same machine). Remote clients get
/// 403, so a LAN visitor cannot read it.
pub(super) async fn web_setup(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
) -> Response {
    if !addr.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "ok": false, "error": "remote address; token is not exposed" })),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "token": state.config.local_api.token })),
    )
        .into_response()
}

/// Human-friendly dashboard page (no assets; token is entered in-session and
/// sent via `X-Auth-Token` header, never in the URL).
pub(super) async fn web_page(State(state): State<SharedState>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert("Cache-Control", "no-store".parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    (
        StatusCode::OK,
        headers,
        Html(web_html(state.config.history.len)),
    )
        .into_response()
}

/// 消息星图（v0.4.16）：总览页 iframe 嵌入；数据源 /v1/history/search + 拓扑接口。
pub(super) async fn starmap_page() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert("Cache-Control", "no-store".parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    (
        StatusCode::OK,
        headers,
        Html(include_str!("../../web/starmap.html")),
    )
        .into_response()
}

pub(super) fn web_html(history_len: usize) -> String {
    const HTML: &str = r#"<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AgentLink · Console v__VER__</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;background:#0b0e14;color:#dbe2ea;font-size:13px}
a{color:#4f8cff;text-decoration:none}
.hidden{display:none!important}
#app{display:grid;grid-template-columns:var(--sw,220px) 8px 1fr;min-height:100vh}
#sidebar{background:#0e1219;border-right:1px solid #1c2330;padding:16px 12px;position:sticky;top:0;height:100vh;overflow:auto}
#logo{display:flex;align-items:center;gap:8px;padding:4px 8px 14px;font-weight:700;font-size:15px;letter-spacing:.2px}
#logo .dot{width:10px;height:10px;border-radius:50%;background:#4f8cff;box-shadow:0 0 10px #4f8cff}
#sidebar .nav{margin:2px 0;padding:8px 10px;border-radius:8px;cursor:pointer;color:#9aa7b8;display:flex;align-items:center;gap:8px}
#sidebar .nav:hover{background:#141b26;color:#e6edf5}
#sidebar .nav.active{background:#16233b;color:#7fb0ff}
/* R5（10-16 第四轮）：侧栏拖拽把手（拖动调宽 · 单击折叠/展开）与折叠态 */
#side-resize{cursor:col-resize;display:flex;align-items:center;justify-content:center;position:relative;z-index:6}
#side-resize::before{content:"";position:absolute;top:0;bottom:0;left:3px;width:1px;background:transparent;transition:background .18s,box-shadow .18s}
#side-resize:hover::before{background:#4f8cff;box-shadow:0 0 8px rgba(79,140,255,.9)}
.rz-ic{width:5px;height:30px;border-radius:3px;background:#2a3547;transition:height .22s,background .22s,box-shadow .22s}
#side-resize:hover .rz-ic{height:52px;background:#4f8cff;box-shadow:0 0 12px rgba(79,140,255,.85);animation:rzp 1.1s ease-in-out infinite alternate}
@keyframes rzp{from{transform:scaleY(1)}to{transform:scaleY(1.16)}}
#sidebar .ful{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
#sidebar .sho{display:none}
#sidebar.coll .ful,#sidebar.coll #logo .lgn,#sidebar.coll .ver-badge{display:none}
#sidebar.coll .sho{display:inline}
#sidebar.coll .nav{justify-content:center;font-size:14px;padding:8px 0;gap:0}
#sidebar.coll #logo{justify-content:center;padding-left:0;padding-right:0;gap:0}
#main{display:flex;flex-direction:column;min-width:0}
#topbar{display:flex;align-items:center;gap:14px;padding:12px 20px;border-bottom:1px solid #1c2330;position:sticky;top:0;background:rgba(11,14,20,.92);backdrop-filter:blur(6px);z-index:5}
#topbar .t{font-weight:600}
#topbar .sub{color:#7d8aa0;font-size:12px}
/* R6（10-16 第四轮）：顶栏合并为一条全局状态栏，统计胶囊紧凑化 */
#topbar .stat{display:inline-flex;align-items:baseline;gap:7px;background:#10151d;border:1px solid #1c2330;border-radius:10px;padding:3px 12px;font-size:11px;color:#7d8aa0;white-space:nowrap}
#topbar .stat b{font-size:14px;color:#dbe2ea;font-weight:700;font-family:ui-monospace,Consolas,monospace}
.spacer{flex:1}
.badge{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:20px;font-size:11px;line-height:1.4}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.on{background:rgba(63,185,80,.12);color:#3fb950}.on .dot{background:#3fb950}
.off{background:rgba(248,81,73,.12);color:#f85149}.off .dot{background:#f85149}
.warn{background:rgba(210,153,34,.12);color:#d29922}
.content{padding:20px 24px;width:100%;max-width:none}
.card{background:#10151d;border:1px solid #1c2330;border-radius:12px;padding:16px;margin-bottom:16px}
.card h2{margin:0 0 12px;font-size:13px;color:#9aa7b8;font-weight:600;letter-spacing:.4px;text-transform:uppercase}
table{width:100%;border-collapse:collapse}
th{font-size:11px;color:#7d8aa0;text-align:left;padding:6px 8px;border-bottom:1px solid #1c2330;font-weight:500}
td{padding:8px;border-bottom:1px solid #171e29;vertical-align:middle}
tr:hover td{background:#131a24}
.chip{display:inline-block;padding:1px 8px;border-radius:12px;font-size:11px;background:#182238;color:#9db4dd;margin:1px 2px}
#agents-body tr.local td{background:#12203a;box-shadow:inset 0 0 0 0 transparent}
#agents-body tr.local td:first-child{box-shadow:inset 3px 0 0 #4f8cff}
#agents-body tr.local:hover td{background:#182a4c}
.self-chip{display:inline-block;padding:1px 7px;border-radius:10px;font-size:10px;background:#234a86;color:#a8cbff;margin-right:6px}
.msg{border-left:3px solid #263041;padding:8px 12px;margin:8px 0;background:#0d1219;border-radius:0 8px 8px 0}
.msg .meta{color:#7d8aa0;font-size:11px;margin-bottom:2px}
.msg .pay{color:#9cc5ff;word-break:break-all;white-space:pre-wrap}
.msg.out{border-left-color:#4f8cff}
.msg.in{border-left-color:#3fb950}
select#to,select#to2{width:340px;max-width:340px}
.ver-badge{font-size:11px;color:#a8cbff;background:#16233b;border:1px solid #26436f;border-radius:12px;padding:1px 9px;letter-spacing:.3px}
.hide-in{position:absolute;left:-9999px;width:1px;height:1px;opacity:0}
.dz{margin-top:8px;padding:10px 12px;border:1px dashed #2b3648;border-radius:10px;color:#7d8aa0;font-size:12px;text-align:center;transition:.15s}
.dz.drag{border-color:#4f8cff;background:#12203a;color:#a8cbff}
.dz a{color:#7fb0ff}
.att-chip{display:inline-flex;align-items:center;gap:7px;padding:2px 10px;border-radius:14px;background:#182238;color:#9db4dd;font-size:12px}
.idc{display:inline-flex;align-items:center;justify-content:flex-end;background:var(--nc,#22344f);border-radius:13px;padding:2px 2px 2px 14px;margin-right:4px;vertical-align:middle;max-width:100%}
.idc .idci{display:inline-block;border-radius:999px;padding:0 9px;font-weight:600;font-size:12px;line-height:18px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:320px;vertical-align:middle}
/* R3（10-17 第七轮）：悬停双层胶囊 → 内胶囊左右缓晃一次复位，hover 响应微反馈 */
@keyframes cap-nudge{0%,100%{transform:translateX(0)}18%{transform:translateX(-5px)}42%{transform:translateX(4px)}64%{transform:translateX(-2.5px)}82%{transform:translateX(1.5px)}}
.idc:hover .idci{animation:cap-nudge .7s cubic-bezier(.34,1.2,.64,1) 1}
@media (prefers-reduced-motion:reduce){.idc:hover .idci{animation:none}}
.idn{opacity:.62;font-size:11px;margin-right:2px}
#cf-colors{display:flex;flex-direction:column;gap:7px;margin-top:6px}
#cf-colors .cfrow{display:flex;align-items:center;gap:12px}
#cf-colors .cfrow .idc{min-width:0}
#cf-colors .cfrow .idci{max-width:100%}
#cf-colors input[type=color]{width:34px;height:26px;padding:1px;background:#10151d;border:1px solid #2a3550;border-radius:6px;cursor:pointer}
.cfauto{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#9aa7b8;white-space:nowrap;cursor:pointer}
.lk{background:none;border:none;color:#7fb0ff;cursor:pointer;padding:0;font-size:12px;text-decoration:underline;font-family:inherit}
.lk.red{color:#f85149;text-decoration:none;font-size:13px}
optgroup{color:#7d8aa0;font-style:normal;font-weight:600}
.row select{width:auto}
.msg .meta{font-size:12px}
.lbl{color:#8b949e;font-size:11px;margin-right:2px}
.fl{position:relative;color:#7d8aa0;font-size:11px}
.tool{display:none;gap:4px;margin-left:6px}
.fl:hover .tool{display:inline-flex}
.tbtn{display:inline-block;padding:1px 8px;border-radius:8px;font-size:11px;text-decoration:none;color:#9cc5ff;background:#182238;border:1px solid #263041;cursor:pointer;font-family:inherit;margin-left:2px}
.chip-btn{display:inline-block;padding:2px 10px;border-radius:14px;font-size:11px;background:#16233b;color:#9db4dd;border:1px solid #263041;cursor:pointer;margin:2px}
.chip-btn:hover{border-color:#4f8cff;color:#e6edf5}
.msg.file .pay{color:#7ee787}
.row{display:flex;gap:8px;align-items:center;margin:8px 0;flex-wrap:wrap}
input,textarea,select{background:#0b1017;color:#dbe2ea;border:1px solid #263041;border-radius:8px;padding:8px 10px;font-size:13px;font-family:inherit;outline:none}
input:focus,textarea:focus,select:focus{border-color:#4f8cff}
button{background:#1a2a45;color:#e6edf5;border:1px solid #2b4a7c;border-radius:8px;padding:8px 14px;font-size:13px;cursor:pointer;font-family:inherit}
button.primary{background:#2f6fe4;border-color:#2f6fe4}
button:hover{filter:brightness(1.12)}
button:active{filter:brightness(.95)}
textarea{width:100%;resize:vertical}
.hint{color:#5f6b7f;font-size:12px;margin:2px 0 6px}
.cfg-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.cfg-grid label{display:block;font-size:12px;color:#9aa7b8;margin:6px 0 4px}
input,textarea,select{width:100%}
row input{width:auto}
#modal{position:fixed;inset:0;background:rgba(4,6,10,.8);display:flex;align-items:center;justify-content:center;z-index:50;backdrop-filter:blur(4px)}
#modal .box{background:#121823;border:1px solid #263041;border-radius:14px;padding:24px;width:360px;box-shadow:0 20px 60px #0008}
#modal h3{margin:0 0 6px}
.switch{position:relative;display:inline-block;width:38px;height:20px;margin-left:8px;vertical-align:middle}
.switch input{display:none}
.slider{position:absolute;inset:0;background:#263041;border-radius:20px;transition:.2s}
.slider:before{content:"";position:absolute;width:14px;height:14px;left:3px;top:3px;background:#dbe2ea;border-radius:50%;transition:.2s}
.switch input:checked+.slider{background:#2f6fe4}
.switch input:checked+.slider:before{transform:translateX(18px)}
th.srt{cursor:pointer;user-select:none}
th.srt:hover{color:#dbe2ea}
.msg{user-select:text}
.msg .pay{user-select:text}
.msg .det{color:#67748a;font-size:11px;margin-top:6px;line-height:1.6;word-break:break-all;background:#0b1017;border:1px solid #1c2330;border-radius:8px;padding:6px 8px}
#toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:#1c2a44;color:#cfe3ff;border:1px solid #2f6fe4;padding:8px 18px;border-radius:20px;font-size:13px;opacity:0;transition:opacity .25s;pointer-events:none;z-index:90}
/* ---------- v0.5 §3.2 任务 Timeline · §4.5 一键熔断 ---------- */
#p-tasks .tk{border:1px solid #1c2330;border-radius:10px;background:#0b0f16;padding:9px 11px;margin:8px 0}
#p-tasks .tk:hover{border-color:#2f81f7aa}
#p-tasks .hd{display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:12px}
#p-tasks .tid{font-family:ui-monospace,Consolas,monospace;font-size:12px;color:#9cc5ff;word-break:break-all}
#p-tasks .tl{display:flex;align-items:flex-start;margin-top:9px;overflow-x:auto;padding-bottom:2px}
#p-tasks .tn{display:flex;flex-direction:column;align-items:center;min-width:58px}
#p-tasks .pt{width:12px;height:12px;border-radius:50%;background:#263041;border:2px solid #3a4a63;flex:none}
#p-tasks .ln{width:26px;height:2px;background:#263041;margin:5px 0 0;flex:none}
#p-tasks .tn.ok .pt{background:#3fb950;border-color:#3fb950}
#p-tasks .tn.bad .pt{background:#f85149;border-color:#f85149}
#p-tasks .tn.wn .pt{background:#d29922;border-color:#d29922}
#p-tasks .tn.info .pt{background:#38bdf8;border-color:#38bdf8}
#p-tasks .tn.blk .pt{background:#2a1010;border-color:#f85149;border-style:dashed}
#p-tasks .lb{font-size:11px;color:#9aa7b8;margin-top:4px;white-space:nowrap}
#p-tasks .lb2{font-size:10px;color:#66707f;margin-top:1px;white-space:nowrap}
.blkchip{background:#3d1212;color:#ffb4b4;border:1px solid #f85149;font-family:inherit;font-size:11px;padding:2px 7px;border-radius:6px;display:inline-block}
.lkd-btn{background:#121722;color:#cdd6e4;border:1px solid #2f81f7;border-radius:8px;padding:4px 10px;font-size:12px;font-family:inherit;cursor:pointer}
.lkd-btn:hover{background:#182135}
.lkd-btn.on{background:#5c1a1a;border-color:#f85149;color:#ffd7d7}
.msg.blk{border-left-color:#f85149;background:#160e0e}
</style></head>
<body>
<div id="modal"><div class="box">
<h3>AgentLink</h3>
<p class="hint">输入本机 <code>agentlink.json</code> 里 <code>local_api.token</code> 的值。Token 只保存在当前浏览器会话，不放进地址栏。</p>
<input id="token-input" style="width:100%" placeholder="local_api.token">
<div class="row"><button class="primary" onclick="saveToken()">进入控制台</button><button id="sso-btn" hidden onclick="location.href='/v1/auth/oidc/login'">企业 SSO 登录</button></div>
</div></div>
<div id="agent-modal" hidden style="position:fixed;inset:0;background:rgba(4,6,10,.8);align-items:center;justify-content:center;z-index:60;backdrop-filter:blur(4px)">
  <div class="box">
    <h3>编辑 Agent</h3>
    <p class="hint">提示：也可在列表中「双击名称」就地改名（回车保存）。</p>
    <label>所属节点</label><div id="ag-node" class="hint" style="margin-bottom:8px"></div>
    <label>agent_id（身份，不可改）</label><input id="ag-id" readonly>
    <label>名称 name（改后全网同步）</label><input id="ag-name">
    <label>能力 capabilities（逗号分隔）</label><input id="ag-caps">
    <div class="row"><button class="primary" onclick="saveAgentEdit()">保存</button><button onclick="hideAgentEdit()">取消</button></div>
    <div id="ag-res" class="hint"></div>
  </div>
</div>
<div id="app" class="hidden">
<aside id="sidebar">
  <div id="logo"><span class="dot"></span><span class="lgn">AgentLink</span> <span class="ver-badge" title="当前节点版本">v__VER__</span></div>
  <div class="nav active" data-p="overview" onclick="show('overview')"><span class="sho">总</span><span class="ful">总览</span></div>
  <div class="nav" data-p="nodes" onclick="show('nodes')"><span class="sho">节</span><span class="ful">节点</span></div>
  <div class="nav" data-p="agents" onclick="show('agents')"><span class="sho">智</span><span class="ful">Agent</span></div>
  <div class="nav" data-p="chat" onclick="show('chat')"><span class="sho">话</span><span class="ful">对话 / 发送</span></div>
  <div class="nav" data-p="tasks" onclick="show('tasks')"><span class="sho">任</span><span class="ful">任务</span></div>
  <div class="nav" data-p="config" onclick="show('config')"><span class="sho">配</span><span class="ful">配置</span></div>
</aside>
<div id="side-resize" title="拖动调宽 · 单击折叠/展开"><span class="rz-ic"></span></div>
<div id="main">
  <div id="topbar">
    <div style="display:flex;align-items:baseline;gap:10px"><div class="t" id="tb-self">—</div><div class="sub" id="tb-meta">—</div></div>
    <span class="stat" title="在线节点 / 已知节点（含本机）"><span>在线节点</span><b id="m-nodes">0</b></span>
    <span class="stat" title="内存中最近对话条数"><span>历史消息</span><b id="m-history">0</b></span>
    <span class="stat" title="进行中的文件传输"><span>传输中</span><b id="m-transfer">0</b></span>
    <span class="stat" title="v0.5 §4.5 总线熔断：拦业务帧（agent 消息/任务帧）；控制面帧与 history/events/tasks 查询不受影响；恢复后不补投被拦帧"><span>熔断</span><b id="lkd-state">—</b><span class="badge off" id="lkd-badge">拦0</span></span>
    <button class="lkd-btn" id="lkd-btn" title="一键熔断 / 解除熔断（均需二次确认）" onclick="toggleLockdown()">🔒 熔断</button>
    <div class="spacer"></div>
    <span class="sub">自动刷新 <label class="switch"><input type="checkbox" id="auto-refresh" checked><span class="slider"></span></label></span>
    <button onclick="refresh();pokeStar()">刷新</button>
    <button id="logout-btn" hidden title="退出当前 SSO 会话并清除浏览器 token" onclick="doLogout()">退出 SSO</button>
  </div>
  <div class="content">
    <section id="p-overview">
      <div class="card"><iframe id="sm-fr" src="/v1/starmap" title="消息星图" style="width:100%;height:72vh;min-height:540px;border:1px solid #1c2b40;border-radius:12px;background:#04070d" allow="fullscreen"></iframe></div>
      <div class="card"><h2>最近对话（总览 · 分页与查询）</h2>
        <div class="row"><input id="ov-kw" placeholder="搜索：内容 / 节点 / agent…" style="flex:2" oninput="ovGo(1)">
        <select id="ov-size" onchange="ovGo(1)"><option value="10">10</option><option value="20">20</option><option value="30" selected>30</option><option value="50">50</option></select></div>
        <div class="row"><span class="hint" id="ov-stats" style="flex:1">对话记录：—</span><select id="ov-scope"><option value="all">全部（含归档）</option><option value="active">仅当前文件</option></select><button onclick="exportHist('ov','json')">导出 JSON</button><button onclick="exportHist('ov','text')">导出 文本</button></div>
        <div id="ov-hist"></div>
        <div class="row"><button onclick="ovGo(OV.page-1)">上一页</button><span class="hint" id="ov-info"></span><button onclick="ovGo(OV.page+1)">下一页</button></div>
      </div>
    </section>
    <section id="p-nodes" class="hidden">
      <div class="card"><h2>节点 Peers</h2><table><thead><tr><th class="srt" onclick="pSortBy('name')">名称<span id="ps-name"></span></th><th class="srt" onclick="pSortBy('node_id')">node_id<span id="ps-node_id"></span></th><th>地址</th><th>标签</th><th class="srt" onclick="pSortBy('online')">状态<span id="ps-online"></span></th><th>操作</th></tr></thead><tbody id="peers-body"></tbody></table></div>
    </section>
    <section id="p-agents" class="hidden">
      <div class="card"><h2>Agent（全网）</h2><table><thead><tr><th class="srt" onclick="aSortBy('node')">节点<span id="as-node"></span></th><th class="srt" onclick="aSortBy('id')">agent_id<span id="as-id"></span></th><th class="srt" onclick="aSortBy('name')">名字<span id="as-name"></span></th><th>能力</th><th class="srt" onclick="aSortBy('online')">状态<span id="as-online"></span></th><th>操作</th></tr></thead><tbody id="agents-body"></tbody></table></div>
    </section>
    <section id="p-chat" class="hidden">
      <div class="card"><h2>发送消息 / 文件</h2>
                <div class="row"><span class="lbl">目标</span><select id="to2"></select></div>
                <div class="row"><span class="lbl">发件身份</span><select id="from2" onchange="try{localStorage.fromagent=this.value}catch(e){}"></select><span class="hint">本机注册的 agent；出站消息与文件的历史记录带上它，对话记录里发端才会显示双层胶囊。选「整机」则只显节点名。</span></div>
        <div class="row"><span class="lbl">消息组</span><input id="grp2" placeholder="主题短名（可选，如 upgrade-v0.5.8；同组消息在星图归为一个话题）" style="flex:1"><span class="hint">留空则按 reply_to 链归组（旧行为不变）。</span></div>
        <div class="row"><textarea id="payload2" rows="2" placeholder='{"content":"你好"}　可只发附件；在框内 Ctrl+V 可直接贴图'></textarea></div>
        <div class="dz" id="dz2">📎 拖拽文件或文件夹到此　·　<button type="button" class="lk" onclick="el('file2').click()">选文件</button>　·　<button type="button" class="lk" onclick="el('dir2').click()">选目录（自动打包 zip）</button><input type="file" id="file2" class="hide-in" multiple><input type="file" id="dir2" class="hide-in" webkitdirectory multiple></div>
        <div class="row"><button class="primary" onclick="doSendAll('2')">发送</button><span id="attinfo2"></span></div>
        <div id="sendres2" class="hint"></div>
      </div>
      <div class="card"><h2>对话记录（内存显示最近 __LEN__ 条 · 下方可按条件查磁盘全量）</h2>
        <div class="row"><input id="ch-kw" placeholder="搜索：内容 / 节点 / agent…" style="flex:2" oninput="chGo(1)">
        <select id="ch-size" onchange="chGo(1)"><option value="20">20</option><option value="50" selected>50</option><option value="100">100</option></select></div>
        <div class="row">
          <select id="ch-from" title="发送方" onchange="schedSearch()" style="flex:1;min-width:220px"></select>
          <select id="ch-to" title="接收方" onchange="schedSearch()" style="flex:1;min-width:220px"></select>
        </div>
        <div class="row">
          <span style="display:inline-flex;align-items:center;gap:6px;white-space:nowrap"><input type="datetime-local" id="ch-since" title="起始时间（本机时区，服务端按 UTC 过滤）" onchange="schedSearch()"><span class="hint">→</span><input type="datetime-local" id="ch-until" title="结束时间" onchange="schedSearch()"></span>
          <select id="ch-limit" title="最多返回条数" onchange="schedSearch()"><option value="50">最多 50</option><option value="200" selected>最多 200</option><option value="1000">最多 1000</option><option value="2000">最多 2000</option></select>
          <label class="hint" style="font-size:12px"><input type="checkbox" id="ch-all" onchange="schedSearch()">含归档</label>
          <button class="primary" onclick="histSearch()">查询</button>
          <button onclick="histReset()">重置</button>
        </div>
        <div class="hint" id="ch-qmeta"></div>
        <div class="row"><span class="hint" id="ch-stats" style="flex:1">对话记录：—</span><select id="ch-scope"><option value="all">全部（含归档）</option><option value="active">仅当前文件</option></select><button onclick="exportHist('ch','json')">导出 JSON</button><button onclick="exportHist('ch','text')">导出 文本</button></div>
        <div id="history"></div>
        <div class="row"><button onclick="chGo(CH.page-1)">上一页</button><span class="hint" id="ch-info"></span><button onclick="chGo(CH.page+1)">下一页</button></div>
      </div>
    </section>
    <section id="p-tasks" class="hidden">
      <div class="card"><h2>任务 · 历史投影 / 引擎表（v0.5 §3.2 + v0.8 包⑥ · /v1/tasks）</h2>
        <div class="row"><select id="tk-src" title="数据源：history 帧轨迹投影 vs mesh 引擎表权威态" onchange="loadTasks(true)" style="min-width:170px"><option value="">历史投影（帧轨迹）</option><option value="engine">引擎表（Mesh 写面）</option></select>
        <input id="tk-asof" placeholder="时间旅行 as_of（RFC3339）" title="只回放该时刻之前的任务帧（v0.8 包⑦，仅历史投影）" style="min-width:200px" onkeydown="if(event.key==='Enter')loadTasks(true)"><input id="tk-corr" placeholder="correlation 批次过滤（回车查询）" style="flex:1" onkeydown="if(event.key==='Enter')loadTasks(true)">
        <select id="tk-state" title="任务状态" onchange="loadTasks(true)"><option value="">全部状态</option><option value="pending">待接</option><option value="running">运行中</option><option value="succeeded">已成功</option><option value="failed">已失败</option><option value="declined">已拒绝</option><option value="cancelled">已取消</option><option value="cancelling">取消中</option><option value="rolled_back">已回滚</option></select>
        <select id="tk-from" title="发起方（节点 / agent）" onchange="tkRender()" style="flex:1;min-width:220px"></select>
        <select id="tk-to" title="承接方（节点 / agent）" onchange="tkRender()" style="flex:1;min-width:220px"></select>
        <input id="tk-kw" placeholder="关键词：task_id / type / intent / 进度" style="flex:1" oninput="tkRender()"></div>
        <div class="row"><button class="primary" onclick="loadTasks(true)">查询</button><button onclick="tkReset()">重置</button><span class="hint" id="tk-meta" style="flex:1"></span></div>
        <div class="hint">时间线：发起(task_spec) → 接受 → 进度/状态 → 终态；红虚线点 🚫 = 熔断期间被拦帧（计入 blocked，不推进状态机、恢复后不补投）。点「发起/承接」即按对应节点/agent 过滤。</div>
        <div id="tk-list"><span class="hint">加载中…</span></div>
      </div>
    </section>
    <section id="p-config" class="hidden">
      <div class="card"><h2>对话记录颜色标识</h2>
        <div class="hint" style="margin-bottom:8px">配色体系：外层深色胶囊＝节点（按 node_id 哈希固定，改名不漂移），内嵌亮色小胶囊＝agent（色相以所在节点为基准偏移，一眼看出归属）。手动选色保存到本节点 colors.json，全网查看者共享；勾回「自动色」即恢复算法色。</div>
        <div id="cf-colors"><span class="hint">加载中…</span></div>
      </div>
      <div class="card"><h2>节点配置</h2>
        <div class="cfg-grid">
          <div><label>节点名称（改后即时生效）</label><input id="cf-name"></div>
          <div><label>标签 tags（逗号分隔）</label><input id="cf-tags"></div>
          <div><label>日志级别</label><select id="cf-log"><option>debug</option><option>info</option><option>warn</option><option>error</option></select></div>
          <div><label>历史条数（重启生效）</label><input id="cf-hist" type="number" min="10"></div>
          <div><label>最大文件 MB（重启生效）</label><input id="cf-fmax" type="number" min="1"></div>
        </div>
        <p class="hint" style="margin-top:12px">静态节点 static_peers（每行一个 IP:端口，重启生效）：</p>
        <textarea id="cf-peers" rows="3"></textarea>
        <div class="row"><span class="hint">启动自动打开 Web</span><label class="switch"><input type="checkbox" id="cf-open"><span class="slider"></span></label></div>
        <div class="row" style="margin-top:6px"><button class="primary" onclick="saveConfig()">保存配置</button><span id="cf-res" class="hint"></span></div>
        <p class="hint" id="cf-restart"></p>
      </div>
      <div class="card"><h2>只读信息</h2><div id="cf-ro" class="hint"></div></div>
    </section>
  </div>
</div>
</div>
<div id="toast"></div>

<script>
const $=id=>document.getElementById(id);
const LS_KEY='al_token';
let TOKEN=sessionStorage.getItem(LS_KEY)||'';
const HDR=()=>({'X-Auth-Token':TOKEN});
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
/* ---------- v0.5 §3.2 任务 Timeline · §4.5 一键熔断（声明须早于 boot） ---------- */
const TK_ST={'pending':'待接','running':'运行中','succeeded':'已成功','failed':'已失败','declined':'已拒绝','cancelled':'已取消','cancelling':'取消中','rolled_back':'已回滚'};
const TK_TP={'task_spec':['发起','wn'],'task_ack':['确认',''],'task_go':['放行','ok'],'task_accepted':['接受','ok'],'task_progress':['进度','info'],'task_succeeded':['成功','ok'],'task_failed':['失败','bad'],'task_declined':['拒绝','bad'],'task_cancel':['取消令','bad'],'task_cancelled':['已取消','bad'],'task_rollback':['回滚','bad'],'task_rolled_back':['已回滚','bad']};
let TASKS=[],TKSRC='',AGID={},LKD={on:false,blocked_count:0,blocked_out:0,blocked_in:0};
function renderLkd(){const b=el('lkd-state'),bg=el('lkd-badge'),btn=el('lkd-btn');if(!b||!bg||!btn)return;
 if(LKD.on){b.textContent='开';b.style.color='#f85149';btn.classList.add('on');btn.textContent='🔒 熔断中 · 点击解除';bg.className='badge off';}
 else{b.textContent='关';b.style.color='#3fb950';btn.classList.remove('on');btn.textContent='🔒 熔断';bg.className='badge on';}
 bg.textContent='拦'+LKD.blocked_count;
 bg.title='熔断累计被拦帧：本机发出 '+LKD.blocked_out+' · 入站 '+LKD.blocked_in+'（恢复后不补投）';}
async function toggleLockdown(){
 const msg=LKD.on?'解除熔断？\n恢复业务帧投递与转发；此前被拦的帧不会补投（v0.5 §4.5 薄档）。'
  :'开启熔断？\n总线立即暂停投递/转发业务帧（agent→agent 消息、任务帧）；\n控制面帧（状态/心跳/注册）与 history / events / tasks 查询不受影响；\n恢复后不补投被拦帧。';
 if(!confirm(msg))return;
 try{const d=await j('/v1/lockdown',{method:'POST',body:JSON.stringify({on:!LKD.on})});
  LKD={on:!!d.on,blocked_count:d.blocked_count||0,blocked_out:d.blocked_out||0,blocked_in:d.blocked_in||0};
  renderLkd();toast(LKD.on?'熔断已开启：业务帧停投/停转发（控制面不受影响）':'熔断已解除：恢复投递，被拦帧不补投');refresh();
 }catch(e){alert('熔断操作失败：'+(e.message||e));}}
async function loadTasks(force){
 if(!force&&(!el('p-tasks')||el('p-tasks').classList.contains('hidden')))return; // 面板隐藏不轮询（省 token）
 const c=val('tk-corr'),s=val('tk-state'),src=val('tk-src'),ao=val('tk-asof');TKSRC=src;
 let q='/v1/tasks?limit=200';if(src)q+='&source='+encodeURIComponent(src);if(c)q+='&correlation='+encodeURIComponent(c);if(s)q+='&state='+encodeURIComponent(s);if(ao)q+='&as_of='+encodeURIComponent(ao);
 let d;try{d=await j(q);}catch(e){const m=el('tk-meta');if(m)m.textContent='任务加载失败：'+(e.message||e);return}
 TASKS=d.tasks||[];
 if(src==='engine'){el('tk-meta').textContent='引擎表：共 '+d.total+' 行（本页 '+TASKS.length+'）· 声明式写面权威态（含 GitOps 目录任务）';}
 else{const bc=TASKS.reduce((a,t)=>a+(t.blocked_count||0),0);
 el('tk-meta').textContent='共 '+d.total+' 个任务（本页 '+TASKS.length+'）· 被拦任务帧 '+bc+' · 数据由 history 投影派生'+(ao?' · 时间旅行至 '+ao:'');}
 tkRender();}
function tkReset(){for(const i of ['tk-src','tk-asof','tk-corr','tk-from','tk-to','tk-kw'])if(el(i))el(i).value='';el('tk-state').value='';loadTasks(true);}
function tkEpMatch(v,t){if(!v)return true;
 if(v.startsWith('agent:')){const rest=v.slice(6);const i=rest.indexOf(':');const id=rest.slice(i+1);return t.from_agent===id||t.to_agent===id;}
 return AGID[t.from_agent]===v||AGID[t.to_agent]===v;}
function tkCorr(c){el('tk-corr').value=c;loadTasks(true);}
function tkEp(id,node,agent){const x=el(id);const v='agent:'+node+':'+agent;
 if([...x.options].some(o=>o.value===v))x.value=v;tkRender();}
function tkRender(){const box=el('tk-list');if(!box)return;
 if(TKSRC==='engine'){tkRenderEngine(box);return;}
 const f=val('tk-from'),t=val('tk-to'),kw=val('tk-kw').toLowerCase();
 const list=TASKS.filter(x=>tkEpMatch(f,x)&&tkEpMatch(t,x)&&(!kw||JSON.stringify([x.task_id,x.type,x.intent,x.correlation,(x.progress||[]).map(p=>p.detail)]).toLowerCase().includes(kw)));
 if(!list.length){box.innerHTML='<span class="hint">暂无匹配任务（任务帧：task_spec / task_accepted / task_progress / task_result 等，由 history 投影派生）</span>';return}
 box.innerHTML=list.map(x=>{
  const bcls=x.state==='succeeded'?'on':(x.state==='failed'||x.state==='declined'||x.state==='rolled_back')?'off':'';
  const corr=x.correlation?`<button class="tbtn" title="按此 correlation 过滤整批任务" onclick="tkCorr('${esc(x.correlation)}')">批次 ${esc(String(x.correlation).slice(0,10))}</button>`:'';
  const bc=x.blocked_count?`<span class="blkchip" title="熔断期间被拦的任务帧（trail 带 blocked 标记，不推进状态机、恢复后不补投）">🚫 拦${x.blocked_count}</span>`:'';
  const fn=AGID[x.from_agent]||'',tn=AGID[x.to_agent]||'';
  const fromChip=fn?`<button class="tbtn" title="按发起方节点/agent过滤" onclick="tkEp('tk-from','${esc(fn)}','${esc(x.from_agent)}')">发起 ${esc(whoa(fn,x.from_agent)||x.from_agent)} @${esc(who(fn))}</button>`:(x.from_agent?`<span class="sub">发起 ${esc(x.from_agent)}</span>`:'<span class="sub">发起 ?</span>');
  const toChip=tn?`<button class="tbtn" title="按承接方节点/agent过滤" onclick="tkEp('tk-to','${esc(tn)}','${esc(x.to_agent)}')">承接 ${esc(whoa(tn,x.to_agent)||x.to_agent)} @${esc(who(tn))}</button>`:(x.to_agent?`<span class="sub">承接 ${esc(x.to_agent)}</span>`:'');
  const tl=(x.trail||[]).map((tr,i)=>{
   const tp=TK_TP[tr.topic]||[tr.topic,''];
   const c=tr.blocked?'blk':tp[1];
   const lb2=esc(String(tr.ts||'').slice(11,19))+(tr.blocked?' 🚫':'');
   const title=esc(String(tr.ts||''))+' · '+(tr.blocked?'熔断已拦（未投递/转发，不补投）':esc(tr.topic+' / '+tr.direction))+' · '+esc(tr.from_agent||'?')+' → '+esc(tr.to_agent||'*')+' · '+esc(tr.msg_id);
   return (i?'<div class="ln"></div>':'')+`<div class="tn ${c}" title="${title}"><div class="pt"></div><div class="lb">${esc(tp[0])}</div><div class="lb2">${lb2}</div></div>`;
  }).join('');
  return `<div class="tk"><div class="hd"><span class="badge ${bcls}">${esc(TK_ST[x.state]||x.state)}${x.total>1?' '+x.done+'/'+x.total:''}</span><span class="tid">${esc(x.type||'?')} · ${esc(x.task_id)}</span>${corr}${fromChip}${toChip}${bc}<span class="sub">${x.message_count} 帧</span></div>${x.intent?`<div class="sub" style="margin:6px 0 0">${esc(x.intent)}</div>`:''}<div class="tl">${tl}</div></div>`;
 }).join('');}
function tkRenderEngine(box){const kw=val('tk-kw').toLowerCase();
 const list=TASKS.filter(x=>!kw||JSON.stringify([x.task_id,x.type,x.intent,x.to_agent,x.correlation]).toLowerCase().includes(kw));
 if(!list.length){box.innerHTML='<span class="hint">引擎表暂无匹配行（POST /v1/tasks 声明任务或经 GitOps 目录入表）</span>';return}
 box.innerHTML=list.map(x=>{
  const term=['succeeded','failed','declined','cancelled','rolled_back'].includes(x.state);
  const bcls=x.state==='succeeded'?'on':term?'off':'';
  const cancel=term?'':`<button class="tbtn" title="请求撤销（引擎表置 cancelling）" onclick="tkCancel('${esc(x.task_id)}')">撤销</button>`;
  const corr=x.correlation?`<button class="tbtn" title="按此 correlation 过滤" onclick="tkCorr('${esc(x.correlation)}')">批次 ${esc(String(x.correlation).slice(0,10))}</button>`:'';
  const ns=esc((x.spec&&x.spec.namespace)||'default');
  const an=x.assignee_node?`<span class="sub">派至 ${esc(x.assignee_node)}</span>`:'';
  const dl=x.deadline?`<span class="sub">⏰ ${esc(String(x.deadline).slice(0,19).replace('T',' '))}</span>`:'';
  return `<div class="tk"><div class="hd"><span class="badge ${bcls}">${esc(TK_ST[x.state]||x.state)}</span><span class="tid">${esc(x.type||'?')} · ${esc(x.task_id)}</span>${corr}<span class="sub">租户 ${ns}</span><span class="sub">尝试 ${x.attempts||0}</span>${an}${dl}${cancel}<span class="sub">${esc(x.from_agent||'?')} → ${esc(x.to_agent||'*')}</span></div>${x.intent?`<div class="sub" style="margin:6px 0 0">${esc(x.intent)}</div>`:''}</div>`;
 }).join('');}
async function tkCancel(id){if(!confirm('请求撤销引擎任务 '+id+'？'))return;
 try{await j('/v1/tasks/'+encodeURIComponent(id)+'/cancel',{method:'POST'});toast('已请求撤销');loadTasks(true);}catch(e){alert('撤销失败：'+(e.message||e));}}
function fmt(ts){try{return new Date(ts).toLocaleString();}catch{return ts;}}
let HIST=[],CHQ=null,COLORS={},OV={page:1},CH={page:1},NAMES={},AN={},AGENTS=[],SELF='';
let PEERS=[],ST={},ASORT={k:'node',d:1},PSORT={k:'name',d:1},EXP=new Set(),LAST={ag:'',pe:'',op:'',hi:'',qt:'',cf:'',ch2:''};
// 用户正在选择文本时跳过所有 DOM 重写：选择/复制不再被 2 秒自动刷新打断
function selecting(){const s=window.getSelection();return s&&s.rangeCount>0&&!s.isCollapsed;}
// 用户正在操作控件（下拉展开、输入中、颜色选择器打开）：冻结本轮所有 DOM 重写，防止刷新冲掉控件状态
function ctlBusy(){const a=document.activeElement;if(!a)return false;const t=a.tagName;if(t!=='SELECT'&&t!=='INPUT'&&t!=='TEXTAREA')return false;if(a.id==='auto-refresh'||a.closest('#topbar'))return false;return true;}
function srtIcon(pfx,cur,k){const e=el(pfx+k);if(e)e.textContent=cur.k===k?(cur.d>0?' ↑':' ↓'):'';}
function aSortBy(k){if(ASORT.k===k)ASORT.d=-ASORT.d;else ASORT={k:k,d:1};renderAgents();}
function pSortBy(k){if(PSORT.k===k)PSORT.d=-PSORT.d;else PSORT={k:k,d:1};renderPeers();}
function who(n){return NAMES[n]||n;}
function whoa(n,a){return a?AN[n+'|'+a]||a:''}
function dirLab(m){if(m.kind==='file')return '📄 文件';if(m.direction==='out')return '⬆ 发出';if(m.direction==='in')return '⬇ 收到';return '• 本机';}
function fullPay(m){const p=m.payload;if(p&&typeof p==='object'&&typeof p.content==='string')return p.content;try{return JSON.stringify(p,null,2);}catch(e){return String(p);}}
function tgExp(id){if(EXP.has(id))EXP.delete(id);else EXP.add(id);ovGo(OV.page);chGo(CH.page);}
function toast(s){const t=el('toast');if(!t)return;t.textContent=s;t.style.opacity='1';clearTimeout(t._h);t._h=setTimeout(()=>{t.style.opacity='0';},1400);}
function copyMsg(id){const m=HIST.find(x=>x.msg_id===id);if(!m)return;const p=m.payload||{};let t;if(m.kind==='file'){t=[p.file_name,(p.size!==undefined?humanSize(p.size):''),p.saved_path].filter(Boolean).join(' | ');}else{t=(p&&typeof p==='object'&&typeof p.content==='string')?p.content:JSON.stringify(p,null,2);}copyText(t);}
function copyText(t){if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(t).then(()=>toast('已复制到剪贴板'),()=>fbCopy(t));}else{fbCopy(t);}}
function fbCopy(t){const ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.left='-9999px';document.body.appendChild(ta);ta.focus();ta.select();try{document.execCommand('copy');toast('已复制到剪贴板');}catch(e){toast('复制失败');}ta.remove();}
function fnv(s){let h=2166136261;for(let i=0;i<s.length;i++){h^=s.charCodeAt(i);h=Math.imul(h,16777619)>>>0;}return h;}
function fm32(x){x^=x>>>16;x=Math.imul(x,0x85ebca6b);x^=x>>>13;x=Math.imul(x,0xc2b2ae35);x^=x>>>16;return x>>>0;}
// 双层胶囊配色（用户 10-16 规格）：外层=节点深色容器（左侧留粗弧），内嵌小胶囊=agent 亮色。
// 层级铁律：节点比 agent 深一档。节点色按 node_id 哈希固定（改名/刷新不漂移）。
// agent 配色（10-16 第三轮 R1）：色相/饱和/亮度全部只由 agent_id 哈希派生——同 id 全网同色；
// S/L 扰动位让不同 id 撞同色相时仍可肉眼区分。与 web/starmap.html 必须逐字一致。
function nodeHue(n){return fnv('N|'+String(n))%360;}
function nodeCol(n){return COLORS['node:'+n]||('hsl('+nodeHue(n)+',46%,30%)');}
function nodeHi(n){return COLORS['node:'+n]||('hsl('+nodeHue(n)+',58%,66%)');}
function agHue(a){return fm32(fnv('|uo'+String(a)))%360;}
function agSat(a){return 46+(fm32(fnv('S|'+String(a)))%28);}
function agLit(a){return 56+(fm32(fnv('L|'+String(a)))%24);}
function agCol(n,a){return COLORS['agent:'+n+'|'+a]||('hsl('+agHue(a)+','+agSat(a)+'%,'+agLit(a)+'%)');}
// 手选色可能是深 hex——按亮度给胶囊内文字配可读前景
function onCol(c){if(typeof c==='string'&&c[0]==='#'&&c.length===7){const r=parseInt(c.substr(1,2),16),g=parseInt(c.substr(3,2),16),b=parseInt(c.substr(5,2),16);return (0.299*r+0.587*g+0.114*b)>150?'#0b1420':'#eef4fb';}return '#0b1420';}
function colFor(n,a){return a?agCol(n,a):nodeHi(n);}
function chip(n,a){
  if(a)return `<span class="idc" style="--nc:${nodeCol(n)}" title="${esc(String(n))} / ${esc(String(a))}"><span class="idci" style="background:${agCol(n,a)};color:${onCol(agCol(n,a))}">${esc(whoa(n,a)||a)}</span></span><span class="idn" style="color:${nodeHi(n)};opacity:.85" title="${esc(String(n))}">${esc(who(n))}</span>`;
  return `<span style="color:${nodeHi(n)}" title="${esc(String(n))}">●</span><span class="idn">${esc(who(n))}</span>`;}
function histRow(m){
  const dir=(m.direction==='out'?'out':m.direction==='in'?'in':'');
  const open=EXP.has(m.msg_id);
  const cpy=`<button class="tbtn" title="复制消息内容" onclick="copyMsg('${esc(m.msg_id)}')">复制</button>`;
  if(m.kind==='file'){
    const nm=esc((m.payload&&m.payload.file_name)||'file');
    const sp=(m.payload&&m.payload.saved_path)||'';
    const sz=humanSize(m.payload&&m.payload.size||0);
    const local=(m.direction==='in'||m.direction==='local')&&sp;
    let head;
    if(local){
      const dl=encodeURIComponent(nm);
      head=`<span class="fl">📄 <a href="/v1/files/${dl}" target="_blank" rel="noopener">${nm}</a> <span class="tool"><a class="tbtn" href="/v1/files/${dl}" target="_blank" rel="noopener">打开文件</a><button class="tbtn" onclick="openFl('${dl}')">打开文件夹</button></span></span>`;
    }else{head=`📤 ${nm}`;}
    const ftg=`<button class="tbtn" onclick="tgExp('${esc(m.msg_id)}')">${open?'收起':'详情'}</button>`;
    const fdet=open?`<div class="det">msg_id: ${esc(m.msg_id)} · ${esc(m.direction)} · from ${esc(m.from)}${m.from_agent?' / '+esc(m.from_agent):''} → to ${esc(m.to)}${m.to_agent?' / '+esc(m.to_agent):''} · size: ${esc(String((m.payload&&m.payload.size)||0))} B${sp?` · saved_path: ${esc(sp)}`:''} · ts: ${esc(m.ts)}</div>`:'';
    return `<div class="msg file"><div class="meta">${fmt(m.ts)} · ${dirLab(m)} · ${chip(m.from,m.from_agent)} → ${chip(m.to,m.to_agent)} · ${cpy}${ftg}</div><div class="pay">${head} · ${sz}</div>${fdet}</div>`;
  }
  const full=fullPay(m);
  const long=full.length>260;
  const showTxt=open?full:(long?full.slice(0,260)+'…':full);
  const tg=long?`<button class="tbtn" onclick="tgExp('${esc(m.msg_id)}')">${open?'收起':'展开'}</button>`:'';
  const det=open?`<div class="det">msg_id: ${esc(m.msg_id)} · ${esc(m.direction)} · from ${esc(m.from)}${m.from_agent?' / '+esc(m.from_agent):''} → to ${esc(m.to)}${m.to_agent?' / '+esc(m.to_agent):''} · 类型: ${esc(m.kind)}/${esc(m.action)} · ts: ${esc(m.ts)}</div>`:'';
  const blk=m.blocked?`<span class="blkchip" title="v0.5 §4.5：熔断期间被拦，未投递/转发，恢复后不补投">🚫 熔断已拦</span> · `:'';
  return `<div class="msg ${dir}${m.blocked?' blk':''}"><div class="meta">${fmt(m.ts)} · ${dirLab(m)} · ${blk}${chip(m.from,m.from_agent)} → ${chip(m.to,m.to_agent)} · <span class="k">${esc(m.kind)}/${esc(m.action)}</span> · ${cpy}${tg}</div><div class="pay">${esc(showTxt)}</div>${det}</div>`;
}
function humanSize(n){if(!n)return '';if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(1)+' MB';}
function filterHist(kw,arr){if(!kw)return arr.slice();return arr.filter(m=>JSON.stringify({f:m.from,a:m.from_agent,t:m.to,ta:m.to_agent,k:m.kind,p:m.payload}).toLowerCase().includes(kw.toLowerCase()));}
function renderList(elx,info,arr,page,size){
  const total=arr.length,pages=Math.max(1,Math.ceil(total/size));
  if(page<1)page=1; if(page>pages)page=pages;
  const part=arr.slice().reverse().slice((page-1)*size,page*size);
  elx.innerHTML=part.map(histRow).join('')||'<p class="hint">无记录</p>';
  info.textContent=`第 ${page}/${pages} 页 · 共 ${total} 条`;
  return page;
}
function ovGo(p){OV.page=renderList(el('ov-hist'),el('ov-info'),filterHist(el('ov-kw').value,HIST),p,parseInt(el('ov-size').value)||20);}
function chGo(p){CH.page=renderList(el('history'),el('ch-info'),filterHist(el('ch-kw').value,CHQ||HIST),p,parseInt(el('ch-size').value)||50);}
async function openFl(nm){try{await j('/v1/open-folder',{method:'POST',body:JSON.stringify({file:decodeURIComponent(nm)})});}catch(e){}}
function findAg(nodeId,agentId){return AGENTS.find(x=>x.node_id===nodeId&&x.agent_id===agentId)||{};}
function editAgent(nodeId,agentId){const a=findAg(nodeId,agentId);el('ag-id').value=a.agent_id||'';el('ag-node').textContent=a.node_name||nodeId;el('ag-name').value=a.name||'';el('ag-caps').value=(a.capabilities||[]).join(', ');el('ag-res').textContent='';const m=el('agent-modal');m.hidden=false;m.style.display='flex';}
function hideAgentEdit(){const m=el('agent-modal');m.hidden=true;m.style.display='none';}
async function saveAgentEdit(){
  const body={agent_id:el('ag-id').value.trim(),name:el('ag-name').value.trim(),capabilities:el('ag-caps').value.split(/[,，]/).map(x=>x.trim()).filter(Boolean)};
  try{const d=await j('/v1/agents',{method:'POST',body:JSON.stringify(body)});el('ag-res').textContent=JSON.stringify(d);hideAgentEdit();refresh();}catch(e){el('ag-res').textContent='保存失败: '+(e.message||e);}
}
function inlineName(td,nodeId,agentId){
  const a=findAg(nodeId,agentId); if(!a.agent_id) return;
  const old=el('ed-input');
  if(old) return; // 已有输入框在编辑中
  const inp=document.createElement('input');
  inp.id='ed-input'; inp.value=a.name||''; td.innerHTML=''; td.appendChild(inp);
  inp.focus(); inp.select();
  let done=false;
  function close(){
    if(done) return; done=true; el('ed-input')&&el('ed-input').parentNode&&(el('ed-input').remove());
  }
  inp.addEventListener('keydown',ev=>{
    if(ev.key==='Enter'){ ev.preventDefault(); const v=inp.value.trim(); close(); if(v&&v!==(a.name||'')){ saveName(nodeId,agentId,v); } else { refresh(); } }
    else if(ev.key==='Escape'){ close(); refresh(); }
  });
  inp.addEventListener('blur',()=>{ const v=inp.value.trim(); close(); if(v&&v!==(a.name||'')){ saveName(nodeId,agentId,v); } else { refresh(); } });
}
async function saveName(nodeId,agentId,newName){
  const a=findAg(nodeId,agentId);
  try{
    const d=await j('/v1/agents',{method:'POST',body:JSON.stringify({agent_id:agentId,name:newName,capabilities:a.capabilities||[]})});
    refresh();
  }catch(e){ alert('改名失败: '+(e.message||e)); refresh(); }
}
async function delAgent(nodeId,agentId){if(nodeId!==SELF){alert('远端 Agent 只能在其所属节点删除');return;}if(!confirm('删除 Agent '+agentId+'？'))return;try{const d=await j('/v1/agents',{method:'DELETE',body:JSON.stringify({agent_id:agentId})});alert(JSON.stringify(d));refresh();}catch(e){alert('删除失败: '+(e.message||e));}}
async function forgetPeer(id){if(!confirm('遗忘节点 '+id+' ？\n将从列表移除并忽略自动发现；它下次主动握手连上时会自动回来。'))return;try{await j('/v1/peers/'+id+'/forget',{method:'DELETE'});refresh();}catch(e){alert('失败: '+(e.message||e));}}
function renameNode(btn){if(el('rn-input'))return;const td=btn.closest('tr').children[0];const inp=document.createElement('input');inp.id='rn-input';inp.value=(ST&&ST.name)||'';inp.style.width='150px';td.textContent='';td.appendChild(inp);inp.focus();inp.select();
 inp.onkeydown=e=>{if(e.key==='Enter'){inp.onblur=null;const v=inp.value;inp.remove();saveNodeName(v);}else if(e.key==='Escape'){inp.onblur=null;refresh();}};
 inp.onblur=()=>{const v=inp.value;inp.remove();saveNodeName(v);};}
async function saveNodeName(v){const n=String(v||'').trim();if(!n){refresh();return;}try{await j('/v1/config',{method:'PUT',body:JSON.stringify({name:n})});refresh();}catch(e){alert('改名失败: '+(e.message||e));}}
function pick(id,ag){
  const v=ag?('agent:'+id+':'+ag):id;
  const t=el('to'),t2=el('to2');
  if(t){t.value=v;} if(t2){t2.value=v;}
}
// 合并下拉的值 -> {to, to_agent}；agent:<node_id>:<agent_id> 自动带出机器
function targetOf(sel){
  sel=(sel||'').trim();
  if(sel.startsWith('agent:')){const rest=sel.slice(6);const i=rest.indexOf(':');if(i>0)return {to:rest.slice(0,i),to_agent:rest.slice(i+1)||undefined};}
  return {to:sel,to_agent:undefined};
}
function saveToken(){TOKEN=$('token-input').value.trim();sessionStorage.setItem(LS_KEY,TOKEN);enter();}
/* R7（10-16 第四轮）：401 不再整页 reload——就地弹回 token 层并停轮询 */
function needToken(){TOKEN='';$('modal').classList.remove('hidden');$('app').classList.add('hidden');
 const lb=$('logout-btn');if(lb)lb.hidden=true;
 const ti=$('token-input');if(ti){ti.value='';ti.focus();}toast('token 无效，请重新输入');}
function enter(){$('modal').classList.add('hidden');$('app').classList.remove('hidden');
 const lb=$('logout-btn');if(lb)lb.hidden=!TOKEN.startsWith('al_sess_');
 refresh();loadConfig();}
async function doLogout(){let d={};try{const r=await fetch('/v1/auth/logout',{method:'POST',headers:HDR()});d=await r.json();}catch(e){}
 try{sessionStorage.removeItem(LS_KEY);}catch(e){}TOKEN='';
 // RP-initiated logout：服务端给了 IdP 登出地址就跳过去（IdP 侧会话一并失效）
 if(d&&d.end_session_url){location.href=d.end_session_url;return;}
 location.reload();}
async function boot(){
 // OIDC 回调：#token=... 只进 sessionStorage，随后从地址栏抹掉（不发给服务端/Referer）
 try{
  const h=new URLSearchParams(location.hash.startsWith('#')?location.hash.slice(1):'');
  const t=h.get('token');
  if(t){TOKEN=t;sessionStorage.setItem(LS_KEY,t);history.replaceState(null,'',location.pathname+location.search);enter();return;}
 }catch(e){}
 // SSO session 优先于本机 setup，否则 127.0.0.1 会自动把 Admin token 盖回去
 if(TOKEN&&TOKEN.startsWith('al_sess_')){enter();return;}
 // 自动取本机 token（仅 127.0.0.1 可成功）；失败则退回手动输入
 try{
  const r=await fetch('/web/setup');
  const d=await r.json();
  if(d&&d.ok){TOKEN=d.token;sessionStorage.setItem(LS_KEY,TOKEN);enter();return;}
 }catch(e){}
 if(TOKEN){enter();return;}
 // 仅在服务端开启浏览器 OIDC 登录时显示 SSO 按钮
 try{
  const r=await fetch('/v1/auth/oidc/status');
  const d=await r.json();
  if(d&&d.browser_login){const b=$('sso-btn');if(b)b.hidden=false;}
 }catch(e){}
 $('modal').classList.remove('hidden');
}
boot();
async function j(p,opt){const o=opt||{};const h={...HDR(),...(o.headers||{})};if(o.body&&typeof o.body==='string'&&!h['Content-Type']&&!h['content-type'])h['Content-Type']='application/json';const r=await fetch(p,{...o,headers:h});const txt=await r.text();let d={};try{d=txt?JSON.parse(txt):{};}catch{d={error:txt.slice(0,300)};}if(r.status===401){
 // SSO session 过期：先用服务端保存的 refresh token 静默续期一次，再重试原请求
 if(!o.__retried&&TOKEN&&TOKEN.startsWith('al_sess_')){
  try{const rr=await fetch('/v1/auth/oidc/refresh',{method:'POST',headers:HDR()});
   if(rr.ok){const rd=await rr.json();if(rd&&rd.ok)return await j(p,{...o,__retried:true});}}catch(e){}}
 sessionStorage.removeItem(LS_KEY);needToken();throw new Error('401');}if(!r.ok)throw new Error('HTTP '+r.status+' '+((d&&(d.error||d.err))||txt.slice(0,200)||r.statusText));return d;}
function show(panel){document.querySelectorAll('.nav').forEach(n=>n.classList.toggle('active',n.dataset.p===panel));document.querySelectorAll('.content>section').forEach(s=>s.classList.add('hidden'));$('p-'+panel).classList.remove('hidden');if(panel==='config')renderColorCfg();if(panel==='tasks')loadTasks(true);}
function input(id){return ($(id)&&$(id).value||'').trim();}
function el(id){return document.getElementById(id);}
function val(id){const e=el(id);return e?(e.value||'').trim():'';}
const ATT={'':[],'2':[]};
function fmtSize(n){n=Number(n)||0;if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(2)+' MB';}
function setAtt(sfx,f){ATT[sfx]=f?[f]:[];renderAtt(sfx);}
function addAtt(sfx,fs){const arr=Array.isArray(fs)?fs:Array.from(fs||[]);if(!arr.length)return;for(const x of arr)ATT[sfx].push(x);renderAtt(sfx);}
function delAtt(sfx,i){ATT[sfx].splice(i,1);const a=el('file'+sfx);if(a)a.value='';renderAtt(sfx);}
function clearAtt(sfx){sfx=sfx||'';ATT[sfx]=[];const a=el('file'+sfx);if(a)a.value='';const b=el('dir'+sfx);if(b)b.value='';renderAtt(sfx);}
function renderAtt(sfx){const box=el('attinfo'+sfx);if(!box)return;const L=ATT[sfx]||[];
  box.innerHTML=L.length?L.map((f,i)=>`<span class="att-chip">📎 ${esc(f.name)} · ${fmtSize(f.size)}${f.__zip?'（目录打包）':''} <button type="button" class="lk red" title="移除该附件" onclick="delAtt('${sfx}',${i})">✕</button></span>`).join('')+` <span class="hint">共 ${L.length} 个 · ${fmtSize(L.reduce((s,f)=>s+Number(f.size||0),0))}</span>`:'<span class="hint">无附件（可多选、可叠加拖拽/粘贴）</span>';}
const CRC_T=(()=>{const t=new Uint32Array(256);for(let i=0;i<256;i++){let c=i;for(let k=0;k<8;k++)c=(c&1)?(0xEDB88320^(c>>>1)):(c>>>1);t[i]=c>>>0;}return t;})();
function crc32(u){let c=0xFFFFFFFF;for(let i=0;i<u.length;i++)c=CRC_T[(c^u[i])&255]^(c>>>8);return (c^0xFFFFFFFF)>>>0;}
function dosT(d){return ((d.getHours()<<11)|(d.getMinutes()<<5)|(d.getSeconds()>>1))&65535;}
function dosD(d){return (((d.getFullYear()-1980)<<9)|((d.getMonth()+1)<<5)|d.getDate())&65535;}
// 极简 zip（store 不压缩，UTF-8 文件名），浏览器端把目录/多文件合成一个附件
async function zipStore(entries){const enc=new TextEncoder(),parts=[],cen=[];let off=0;const now=new Date(),dt=dosT(now),dd=dosD(now);
  for(const e of entries){const nm=enc.encode(e.name),u=e.u8,crc=crc32(u);
    const lh=new DataView(new ArrayBuffer(30));
    lh.setUint32(0,0x04034b50,true);lh.setUint16(4,20,true);lh.setUint16(6,0x0800,true);lh.setUint16(10,dt,true);lh.setUint16(12,dd,true);
    lh.setUint32(14,crc,true);lh.setUint32(18,u.length,true);lh.setUint32(22,u.length,true);lh.setUint16(26,nm.length,true);
    parts.push(lh.buffer,nm,u);
    const ch=new DataView(new ArrayBuffer(46));
    ch.setUint32(0,0x02014b50,true);ch.setUint16(4,20,true);ch.setUint16(6,20,true);ch.setUint16(8,0x0800,true);
    ch.setUint16(12,dt,true);ch.setUint16(14,dd,true);ch.setUint32(16,crc,true);ch.setUint32(20,u.length,true);ch.setUint32(24,u.length,true);
    ch.setUint16(28,nm.length,true);ch.setUint32(42,off,true);
    cen.push(ch.buffer,nm);off+=30+nm.length+u.length;}
  let csz=0;for(const c of cen)csz+=c.byteLength;
  const end=new DataView(new ArrayBuffer(22));
  end.setUint32(0,0x06054b50,true);end.setUint16(8,entries.length,true);end.setUint16(10,entries.length,true);
  end.setUint32(12,csz,true);end.setUint32(16,off,true);
  return new Blob([...parts,...cen,end.buffer],{type:'application/zip'});}
function readFileU8(f){return new Promise((res,rej)=>{const r=new FileReader();r.onload=()=>res(new Uint8Array(r.result));r.onerror=()=>rej(r.error);r.readAsArrayBuffer(f);});}
async function walkEntry(entry,base,out){out=out||[];base=base||'';
  if(entry.isFile){const f=await new Promise(r=>entry.file(r));out.push({name:base+entry.name,u8:await readFileU8(f)});}
  else if(entry.isDirectory){const rd=entry.createReader();let batch;do{batch=await new Promise((res,rej)=>rd.readEntries(res,rej));for(const b of batch)await walkEntry(b,base+entry.name+'/',out);}while(batch&&batch.length);}
  return out;}
async function doSendAll(sfx){
  sfx=sfx||'';
  const res=el('sendres'+sfx);
  const tgt=targetOf(val('to'+sfx)); if(!tgt.to){res.textContent='请选择目标';return;}
  const fam=sfx==='2'?(val('from2')||'').trim():'';  // 发件身份（第九轮 R1）
  const grp=sfx==='2'?(val('grp2')||'').trim():'';   // 消息组（第十五轮 R2）
  const raw=val('payload'+sfx), atts=ATT[sfx]||[];
  if(!raw&&!atts.length){res.textContent='消息内容和附件都是空的';return;}
  const done=[];
  if(raw){let payload;try{payload=JSON.parse(raw);}catch{payload={content:raw};}
    if(atts.length&&!payload.file)payload.note='附件：'+atts.map(a=>a.name).join('、');
    try{await j('/v1/send',{method:'POST',body:JSON.stringify({to:tgt.to,to_agent:tgt.to_agent,from_agent:fam||undefined,thread:grp||undefined,type:'text',action:'send',payload})});done.push('消息 ok');res.textContent='消息 ok，处理附件中…';}
    catch(e){res.textContent='发送失败: '+(e.message||e);return;}}
  let okn=0;
  for(let i=0;i<atts.length;i++){const att=atts[i];const q=new URLSearchParams({to:tgt.to});if(tgt.to_agent)q.set('to_agent',tgt.to_agent);if(fam)q.set('from_agent',fam);
    res.textContent=`上传附件 ${i+1}/${atts.length}：${att.name}（${fmtSize(att.size)}）`;
    try{const d=await j('/v1/send-file-upload?'+q,{method:'POST',headers:{'X-File-Name':encodeURIComponent(att.name)},body:att});
      okn++;done.push('📎'+att.name+' ok'+(d.file_id?' #'+String(d.file_id).slice(0,8):''));}
    catch(e){done.push('📎'+att.name+' 失败: '+(e.message||e));}}
  if(atts.length)clearAtt(sfx);
  res.textContent=done.join(' · ')||'完成';refresh();pokeStar();}
function onPaste(e,sfx){const cd=e.clipboardData;if(!cd)return;let f=null;
  for(const it of (cd.items||[])){if(it.kind==='file'&&it.type&&it.type.indexOf('image/')===0){f=it.getAsFile();break;}}
  if(!f){for(const x of (cd.files||[])){if(x.type&&x.type.indexOf('image/')===0){f=x;break;}}}
  if(!f)return;e.preventDefault();
  const ts=new Date().toISOString().slice(0,19).replace(/[-:T]/g,'');
  const named=(f.name&&f.name!=='image.png')?f:new File([f],'image-'+ts+'.png',{type:f.type||'image/png'});
  addAtt(sfx,[named]);el('sendres'+sfx).textContent='已附上剪贴板图片 '+named.name;}
async function packFromInput(inp,sfx,label){const fs=Array.from(inp.files||[]);if(!fs.length)return;
  const res=el('sendres'+sfx);const top=(fs[0].webkitRelativePath||'').split('/')[0]||label||'folder';
  res.textContent='打包目录中… '+fs.length+' 个文件';
  try{const es=[];for(const x of fs)es.push({name:x.webkitRelativePath||x.name,u8:new Uint8Array(await x.arrayBuffer())});
    const blob=await zipStore(es);const zf=new File([blob],top+'.zip',{type:'application/zip'});zf.__zip=true;
    addAtt(sfx,[zf]);res.textContent=`已打包 ${es.length} 个文件 → ${top}.zip（${fmtSize(zf.size)}）`;}
  catch(e){res.textContent='打包失败: '+(e.message||e);}}
async function onDrop(e,sfx){e.preventDefault();const dz=el('dz'+sfx);if(dz)dz.classList.remove('drag');const res=el('sendres'+sfx);
  const its=(e.dataTransfer&&e.dataTransfer.items)?Array.from(e.dataTransfer.items):[];
  const ents=its.map(i=>i.webkitGetAsEntry?i.webkitGetAsEntry():null).filter(Boolean);
  const dirs=ents.filter(x=>x.isDirectory);
  if(dirs.length){const made=[];
    for(const d of dirs){res.textContent='打包目录中… '+d.name;
      try{const es=await walkEntry(d,'',[]);if(!es.length){res.textContent='目录是空的';continue;}
        const blob=await zipStore(es);const zf=new File([blob],d.name+'.zip',{type:'application/zip'});zf.__zip=true;
        addAtt(sfx,[zf]);made.push(d.name+'.zip');}
      catch(err){res.textContent='目录打包失败: '+(err.message||err);}}
    if(made.length)res.textContent='已打包 '+made.length+' 个目录 → '+made.join('、');return;}
  const fl=(e.dataTransfer&&e.dataTransfer.files)?Array.from(e.dataTransfer.files):[];
  if(fl.length){addAtt(sfx,fl);res.textContent='';return;}
  if(ents.some(x=>x.isFile)){const fe=ents.find(x=>x.isFile);const f=await new Promise(r=>fe.file(r));addAtt(sfx,[f]);res.textContent='';return;}
  res.textContent='没识别到文件';}
function bindPanel(sfx){
  const fi=el('file'+sfx);if(fi)fi.onchange=()=>{addAtt(sfx,Array.from(fi.files||[]));fi.value='';};
  const di=el('dir'+sfx);if(di)di.onchange=()=>packFromInput(di,sfx);
  const dz=el('dz'+sfx);
  if(dz){['dragenter','dragover'].forEach(ev=>dz.addEventListener(ev,e=>{e.preventDefault();dz.classList.add('drag');}));
    ['dragleave','dragend'].forEach(ev=>dz.addEventListener(ev,()=>dz.classList.remove('drag')));
    dz.addEventListener('drop',e=>onDrop(e,sfx));}
  const ta=el('payload'+sfx);if(ta)ta.addEventListener('paste',e=>onPaste(e,sfx));
  renderAtt(sfx);}
bindPanel('');bindPanel('2');
function renderAgents(){
  ['node','id','name','online'].forEach(k=>srtIcon('as-',ASORT,k));
  const rank=a=>(a.node_id===SELF?0:1);
  const list=AGENTS.slice().sort((a,b)=>{
    const r=rank(a)-rank(b); if(r) return r;
    let c=0;
    if(ASORT.k==='node')c=(a.node_name||a.node_id).localeCompare(b.node_name||b.node_id,'zh')||a.agent_id.localeCompare(b.agent_id);
    else if(ASORT.k==='id')c=a.agent_id.localeCompare(b.agent_id);
    else if(ASORT.k==='name')c=(a.name||'').localeCompare(b.name||'','zh');
    else if(ASORT.k==='online')c=(b.online?1:0)-(a.online?1:0)||a.agent_id.localeCompare(b.agent_id);
    return c*ASORT.d;});
  let ag=''; list.forEach(a=>{const k=`'${esc(a.node_id)}','${esc(a.agent_id)}'`;const loc=a.node_id===SELF;
  ag+=`<tr${loc?' class="local"':''}><td>${loc?'<span class="self-chip">本机</span>':''}${esc(a.node_name||a.node_id)}</td><td>${esc(a.agent_id)}</td><td${loc?` class="ed-name" title="双击改名，回车保存" ondblclick="inlineName(this,${k})"`:''}>${esc(a.name)}</td><td>${(a.capabilities||[]).map(c=>`<span class="chip">${esc(c)}</span>`).join('')}</td><td><span class="badge ${a.online?'on':'off'}"><span class="dot"></span>${a.online?'在线':'离线'}</span></td><td>${loc?`<button class="tbtn" onclick="editAgent(${k})">改名</button> <button class="tbtn" onclick="delAgent(${k})">删除</button>`:`<span class="hint" title="远端 Agent：请在其所属节点上改名/删除（本机 API 只管本机注册）">远端</span>`}</td></tr>`;});
  if(ag!==LAST.ag){$('agents-body').innerHTML=ag;LAST.ag=ag;}
}
function renderPeers(){
  ['name','node_id','online'].forEach(k=>srtIcon('ps-',PSORT,k));
  const rank=p=>(ST.node_id&&p.node_id===ST.node_id?0:1);
  const merged=[{node_id:ST.node_id,name:ST.name||ST.node_id,addr:ST.listen_addr||'—',tags:[],online:true,last_seen_seconds_ago:0,direction:'self'},...PEERS.filter(p=>p.node_id!==ST.node_id)];
  const list=merged.slice().sort((a,b)=>{
    const r=rank(a)-rank(b); if(r) return r;
    let c=0;
    if(PSORT.k==='name')c=(a.name||'').localeCompare(b.name||'','zh');
    else if(PSORT.k==='node_id')c=a.node_id.localeCompare(b.node_id);
    else if(PSORT.k==='online')c=(b.online?1:0)-(a.online?1:0)||(a.name||'').localeCompare(b.name||'','zh');
    return c*PSORT.d;});
  let pe=''; for(const p of list){const loc=ST&&p.node_id===ST.node_id;pe+=`<tr${loc?' class="local"':''}><td>${loc?'<span class="self-chip">本机</span>':''}${esc(p.name)}</td><td>${esc(p.node_id)}</td><td>${esc(p.addr)}</td><td>${(p.tags||[]).map(t=>`<span class="chip">${esc(t)}</span>`).join('')}</td><td><span class="badge ${p.online?'on':'off'}"><span class="dot"></span>${p.online?'在线':'离线'}</span></td><td>${loc?`<button class="tbtn" title="改本机节点显示名（node_id 不变，改名后自动广播给全网）" onclick="renameNode(this)">✎ 改名</button>`:(!p.online?`<button class="tbtn" title="从列表移除且忽略自动发现，直到该节点重新握手连上" onclick="forgetPeer('${esc(p.node_id)}')">遗忘</button>`:`<span class="hint" title="远端节点的显示名请到该节点自身的 /web 页面修改">远端</span>`)}</td></tr>`;}
  if(pe!==LAST.pe){$('peers-body').innerHTML=pe;LAST.pe=pe;}
}
function histSig(){return HIST.length+'|'+(HIST.length?HIST[HIST.length-1].msg_id:'')+'|'+EXP.size;}
let LAST_HSTAT=0;
async function histStats(){if(Date.now()-LAST_HSTAT<30000)return;LAST_HSTAT=Date.now();try{const d=await j('/v1/history/stats');if(!d||d.ok!==true)return;const mb=Number(d.rotate_mb)||1024;const lim=mb*1048576;const tot=Number(d.total_bytes)||0;const act=Number(d.active_bytes)||0;const warn=act>=lim*0.8;let txt='当前 '+fmtSize(d.active_bytes)+' · 归档 '+fmtSize(d.archive_bytes)+'（'+(d.archive_count||0)+' 个）· 合计 '+fmtSize(tot)+' · 满 '+mb+' MB 或 '+(d.archive_days||0)+' 天自动归档，记录不删除';if(d.archive_keep)txt+=' · 仅保留最近 '+d.archive_keep+' 份归档';if(warn)txt+=' · 接近阈值，建议导出备份';for(const id of ['ov-stats','ch-stats']){const e=el(id);if(!e)continue;e.textContent=txt;e.style.color=warn?'#f85149':'';}}catch(err){}}
function exportHist(p,fmt){const s=el(p+'-scope');const sc=s?s.value:'all';window.location.href='/v1/history/export?format='+fmt+'&scope='+sc;}
let SCHT=null;function schedSearch(){clearTimeout(SCHT);SCHT=setTimeout(histSearch,350);}
async function histSearch(){clearTimeout(SCHT);
 const p=new URLSearchParams();
 const put=(k,id)=>{const v=val(id);if(v)p.set(k,v);};
 put('q','ch-kw');
 // 端点下拉与发送目标同口径：node_id 或 agent:<node_id>:<agent_id>，拆成精确 pair 参数
 const ep=(k,id)=>{const v=(val(id)||'').trim();if(!v)return;
  if(v.startsWith('agent:')){const rest=v.slice(6);const i=rest.indexOf(':');
   if(i>0){p.set(k+'_node',rest.slice(0,i));p.set(k+'_agent',rest.slice(i+1));return;}}
  p.set(k+'_node',v);};
 ep('from','ch-from');ep('to','ch-to');
 const sv=val('ch-since');if(sv)p.set('since',new Date(sv).toISOString());
 const uv=val('ch-until');if(uv)p.set('until',new Date(uv).toISOString());
 p.set('limit',el('ch-limit').value);
 p.set('scope',el('ch-all').checked?'all':'active');
 el('ch-qmeta').textContent='磁盘查询中…';
 try{
  const d=await j('/v1/history/search?'+p.toString());
  if(!d||d.ok!==true){el('ch-qmeta').textContent='查询失败：'+((d&&d.error)||'');return;}
  CHQ=d.matches||[];
  const kb=Math.round((d.scanned_bytes||0)/1024);
  let note='服务器命中 '+d.count+' 条 · 扫描 '+kb+' KB';
  if(d.stopped==='budget')note+=' · 达到扫描上限（可收窄时间范围或调低条数）';
  else if(d.stopped==='limit')note+=' · 已达上限截断（更早记录未扫，可提高上限）';
  else if(d.stopped==='older')note+=' · 早于起始时间自动停止';
  el('ch-qmeta').textContent=note+' · 结果在下方分页，可继续用上方关键词二次筛选';
  chGo(1);
 }catch(e){el('ch-qmeta').textContent='查询失败：'+(e.message||e);}
}
function histReset(){CHQ=null;for(const i of ['ch-from','ch-to','ch-since','ch-until','ch-kw'])if(el(i))el(i).value='';if(el('ch-all'))el('ch-all').checked=false;el('ch-qmeta').textContent='';chGo(1);}
function fillChEps(){
 let cqn='<optgroup label="筛选"><option value="">全部（不过滤）</option></optgroup><optgroup label="节点（整机）">';
 if(SELF)cqn+=`<option value="${esc(SELF)}">${esc(who(SELF))}（本机）</option>`;
 for(const p of PEERS)cqn+=`<option value="${esc(p.node_id)}">${esc(p.name)}${p.online?'':'（离线）'}</option>`;
 cqn+='</optgroup><optgroup label="Agent（节点 · agent）">';
 for(const a of AGENTS)cqn+=`<option value="agent:${esc(a.node_id)}:${esc(a.agent_id)}">${esc(a.node_name||who(a.node_id))} · ${esc(a.agent_id)}${a.online?'':'（离线）'}</option>`;
 cqn+='</optgroup>';
 if(!selecting()&&cqn!==LAST.ch2){LAST.ch2=cqn;
  [el('ch-from'),el('ch-to'),el('tk-from'),el('tk-to')].forEach(x=>{if(!x)return;const c=x.value;x.innerHTML=cqn;if(c&&[...x.options].some(o=>o.value===c))x.value=c;});}}
function renderColorCfg(){const box=el('cf-colors');if(!box||box.contains(document.activeElement))return;
 const ids=new Map();
 for(const a of AGENTS){ids.set('agent:'+a.node_id+'|'+a.agent_id,(a.node_name||who(a.node_id))+' · '+(a.name||a.agent_id));}
 for(const n of Object.keys(NAMES)){ids.set('node:'+n,'节点：'+NAMES[n]);}
 const rows=[...ids.entries()].sort((x,y)=>x[1].localeCompare(y[1]));
 const empty='<span class="hint">暂无节点/agent，等 fleet 同步后自动出现。</span>';
 if(!rows.length){if(LAST.cf!==empty){box.innerHTML=empty;LAST.cf=empty;}return;}
 const html=rows.map(([k,lbl])=>cfgRow(k,lbl)).join('');
 /* R7（10-16 第四轮）：内容不变不重写——配色区不再每 2s 整块闪重建，颜色选择器/勾选状态不再被刷掉 */
 if(html!==LAST.cf){box.innerHTML=html;LAST.cf=html;}}
function cfgPrev(k,n,a,lbl){const outer=nodeCol(n);const inner=a?agCol(n,a):nodeHi(n);
 return `<span class="idc" style="--nc:${esc(outer)}"><span class="idci" style="background:${esc(inner)};color:${onCol(inner)}">${esc(lbl)}</span></span>`;}
function cfgRow(k,lbl){const sep=k.indexOf('|');
 const n=sep>0?k.slice(6,sep):k.slice(5);const a=sep>0?k.slice(sep+1):'';const cur=COLORS[k]||'';
 const hexv=/^#[0-9a-fA-F]{6}$/.test(cur)?cur:'#5b8def';
 return `<div class="cfrow" data-k="${esc(k)}" data-n="${esc(n)}" data-a="${esc(a)}" data-lbl="${esc(lbl)}">${cfgPrev(k,n,a,lbl)}<input type="color" ${cur?'value="'+hexv+'"':''} ${cur?'':'disabled'} onchange="setColor('${k}',this.value)"><label class="cfauto" title="勾选＝跟随算法自动色"><input type="checkbox" ${cur?'':'checked'} onchange="tgAutoCol('${k}',this.checked)">自动色</label></div>`;}
function paintColorRow(k){const enc=CSS.escape?CSS.escape(k):k;
 const row=document.querySelector('#cf-colors .cfrow[data-k="'+enc+'"]');if(!row)return;const cur=COLORS[k]||'';
 const pv=row.querySelector('.idc');if(pv)pv.outerHTML=cfgPrev(k,row.dataset.n,row.dataset.a,row.dataset.lbl);
 const inp=row.querySelector('input[type="color"]');
 if(inp){inp.disabled=!cur;if(cur)inp.value=/^#[0-9a-fA-F]{6}$/.test(cur)?cur:'#5b8def';}
 const cb=row.querySelector('.cfauto input');if(cb)cb.checked=!cur;}
async function setColor(k,v){COLORS[k]=v;paintColorRow(k);await putColors();}
async function tgAutoCol(k,on){if(on)delete COLORS[k];else if(!COLORS[k])COLORS[k]='#5b8def';paintColorRow(k);await putColors();}
async function putColors(){try{await j('/v1/colors',{method:'PUT',body:JSON.stringify({colors:COLORS})});}catch(e){alert('保存颜色失败：'+(e.message||e));}}
// 第十轮 R4：手动刷新/发送成功后轻推星图 iframe（星图侧收 {al:'poke'} 合并触发一次 poll）
function pokeStar(){try{const f=el('sm-fr');if(f&&f.contentWindow)f.contentWindow.postMessage({al:'poke'},location.origin);}catch(e){}}
async function refresh(){
 const md=$('modal');if(md&&!md.classList.contains('hidden'))return; // token 层打开时停轮询
 histStats();
 try{
  if(el('ed-input')||el('rn-input'))return; // 行内改名编辑中：跳过本轮刷新，防止重绘冲掉编辑框
  const [st,peers,agents,hist,cols,lk]=await Promise.all([j('/v1/status'),j('/v1/peers'),j('/v1/fleet/agents'),j('/v1/history'),j('/v1/colors'),j('/v1/lockdown').catch(e=>null)]);
  // 用户正在选择/复制文本或操作控件：只更新内存数据，不重写 DOM、不覆盖未保存完的配色（下一轮自动恢复渲染）
  const sel=selecting()||ctlBusy();
  if(!sel)COLORS=(cols&&cols.colors)||{};
  ST=st||{}; PEERS=(peers&&peers.ok)?peers.peers:[]; AGENTS=(agents&&agents.ok)?agents.agents:[]; SELF=ST.node_id||'';
  $('tb-self').textContent=st.name;
  $('tb-meta').textContent=`${st.node_id} · ${new Date().toLocaleTimeString()}`;
  $('m-nodes').textContent=`${st.peers_online+1}/${st.peers_known+1}`;
  $('m-history').textContent=hist.history.length;
  $('m-transfer').textContent=st.active_transfers;
  if(lk&&typeof lk.on==='boolean'){LKD={on:!!lk.on,blocked_count:lk.blocked_count||0,blocked_out:lk.blocked_out||0,blocked_in:lk.blocked_in||0};renderLkd();}
  // 节点名 / agent 名映射（给人看）
  NAMES={}; AN={};
  if(st&&st.node_id){NAMES[st.node_id]=st.name||st.node_id;}
  for(const p of PEERS)NAMES[p.node_id]=p.name;
  for(const a of AGENTS){AN[a.node_id+'|'+a.agent_id]=(a.name||a.agent_id); NAMES[a.node_id]=a.node_name||NAMES[a.node_id]||a.node_id;}
  AGID={};for(const a of AGENTS)AGID[a.agent_id]=a.node_id;
  if(!sel){renderPeers();renderAgents();fillChEps();renderColorCfg();}
  // 目标下拉（纯选择）——签名相同不重写，保住控件状态
  let opn='<optgroup label="节点（发给整机）"><option value="*">* 广播（全部节点）</option>';
  for(const p of PEERS)opn+=`<option value="${esc(p.node_id)}">${esc(p.name)}${p.online?'':'（离线）'}</option>`;
  opn+='</optgroup><optgroup label="Agent（发给指定 Agent）">';
  for(const a of AGENTS)opn+=`<option value="agent:${esc(a.node_id)}:${esc(a.agent_id)}">${esc(a.node_name||a.node_id)} · ${esc(a.agent_id)}${a.online?'':'（离线）'}</option>`;
  opn+='</optgroup>';
  if(!sel&&opn!==LAST.op){LAST.op=opn;
    [el('to'),el('to2')].forEach(x=>{if(!x)return;const c=x.value;x.innerHTML=opn;if(c&&[...x.options].some(o=>o.value===c))x.value=c;});}
  // 发件身份下拉（第九轮 R1）：本机注册 agent；默认=上次选择(localStorage)，其次=首个本机 agent
  let fam='<option value="">整机（不带 agent 身份）</option>';
  const myNid=String((ST&&ST.node_id)||SELF||'');
  const locals=AGENTS.filter(a=>String(a.node_id)===myNid);
  for(const a of locals)fam+=`<option value="${esc(a.agent_id)}">${esc(a.agent_id)}${a.name&&a.name!==a.agent_id?' · '+esc(a.name):''}</option>`;
  if(!sel&&fam!==LAST.fam){LAST.fam=fam;const x=el('from2');if(x){let want='';try{want=localStorage.fromagent||''}catch(e){}
    if(!want&&locals.length)want=locals[0].agent_id;x.innerHTML=fam;if([...x.options].some(o=>o.value===want))x.value=want;}}
  HIST=hist&&hist.ok?hist.history.slice():[];
  if(!sel&&histSig()!==LAST.hi){LAST.hi=histSig();ovGo(OV.page);chGo(CH.page);}
  let qt='';
  for(const p of PEERS){if(p.online)qt+=`<button class="chip-btn" onclick="pick('${esc(p.node_id)}','')" title="${esc(p.name)}">${esc(p.name)}</button>`;}
  for(const a of AGENTS){if(a.online)qt+=`<button class="chip-btn" onclick="pick('${esc(a.node_id)}','${esc(a.agent_id)}')" title="${esc(a.node_name||a.node_id)} / ${esc(a.agent_id)}">${esc(a.node_name||a.node_id)} · ${esc(a.agent_id)}</button>`;}
  if(!sel&&qt!==LAST.qt){const qtEl=el('quick-targets'); if(qtEl){qtEl.innerHTML=qt?('快捷选择：'+qt):'';LAST.qt=qt;}}
 }catch(e){}
}
async function loadConfig(){
 try{
  const d=await j('/v1/config');
  const c=d.config;
  $('cf-name').value=c.name; $('cf-tags').value=(c.tags||[]).join(',');
  $('cf-log').value=c.log.level; $('cf-hist').value=c.history.len; $('cf-fmax').value=Math.round(c.files.max_file_size/1048576);
  $('cf-peers').value=(c.discovery.static_peers||[]).join('\n');
  $('cf-open').checked=c.web.auto_open;
  $('cf-ro').textContent=`node_id ${c.node_id} · 端口 ${c.listen_port} · 广播 ${c.discovery.broadcast_port} · 模式 ${c.security.mode} · data_dir ${c.data_dir}`;
  $('cf-restart').textContent='名称与标签立即生效；端口 / 广播端口 / 日志格式 / 历史条数 / 最大文件 / static_peers 保存后重启生效。';
 }catch(e){}
}
async function saveConfig(){
 const peers=$('cf-peers').value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);
 const body={name:$('cf-name').value.trim(),tags:$('cf-tags').value.split(/[,，]/).map(s=>s.trim()).filter(Boolean),
  log_level:$('cf-log').value,history_len:parseInt($('cf-hist').value)||500,
  max_file_size:(parseInt($('cf-fmax').value)||100)*1048576,static_peers:peers,auto_open_web:$('cf-open').checked};
 try{const d=await j('/v1/config',{method:'PUT',body:JSON.stringify(body)});$('cf-res').textContent=JSON.stringify(d);refresh();}catch(e){}
}
/* ---------- R5（10-16 第四轮）：侧栏折叠/展开 + 拖拽调宽（localStorage 持久化） ---------- */
function sideSet(px){document.documentElement.style.setProperty('--sw',px+'px');}
function sideCur(){const s=$('sidebar');return s?Math.round(s.getBoundingClientRect().width):220;}
function applySide(){const w=Math.max(150,Math.min(460,parseInt(localStorage.getItem('al_sbw')||'220',10)||220));
 const coll=localStorage.getItem('al_sbc')==='1';
 sideSet(coll?56:w);$('sidebar').classList.toggle('coll',coll);}
function initSideResize(){const rz=el('side-resize');if(!rz)return;let dr=null;
 rz.addEventListener('pointerdown',e=>{e.preventDefault();dr={x:e.clientX,base:sideCur(),moved:false};
  try{rz.setPointerCapture(e.pointerId)}catch(_){}});
 rz.addEventListener('pointermove',e=>{if(!dr)return;const dx=e.clientX-dr.x;
  if(Math.abs(dx)>3)dr.moved=true;
  if(dr.moved){const w=Math.max(150,Math.min(460,dr.base+dx));sideSet(w);
   if($('sidebar').classList.contains('coll')){$('sidebar').classList.remove('coll');localStorage.setItem('al_sbc','0');}}});
 rz.addEventListener('pointerup',()=>{if(!dr)return;const moved=dr.moved;dr=null;
  if(moved)localStorage.setItem('al_sbw',String(sideCur()));
  else{localStorage.setItem('al_sbc',($('sidebar').classList.contains('coll')?'0':'1'));applySide();}});
 rz.addEventListener('pointercancel',()=>{dr=null;});}
applySide();initSideResize();
setInterval(()=>{if($('auto-refresh').checked){refresh();loadTasks(false);}},2000);
</script></body></html>"#;
    HTML.replace("__LEN__", &history_len.to_string())
        .replace("__VER__", VERSION)
}

// ---- local subscription -----------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct SubParams {
    token: String,
    #[serde(default)]
    client: Option<String>,
}

/// `token` → WS 可见范围：
/// - `local_api.token` → `Some(None)`：全局观察者（旧行为）；
/// - 静态 `auth.tokens` + `auth.token_namespaces` 非 Admin → `Some(Some(ns))`；
/// - 浏览器 OIDC session：Admin → `Some(None)`，非 Admin → 绑定 namespace；
/// - 其余（含未绑定静态 token、Admin、OIDC access token）→ `None`（拒绝）。
///
/// OIDC **access/ID token** 不直接进 WS URL（避免写进 URL/代理日志）；先经
/// `POST /v1/ws/ticket`（请求头鉴权）换一次性票据，再 `/v1/ws?token=<ticket>`
/// 建连。浏览器登录态仍可直接用本节点短期 session token，且可随时 logout/过期。
pub fn ws_scope(state: &AppState, token: &str) -> Option<Option<String>> {
    if constant_time_eq(token.as_bytes(), state.config.local_api.token.as_bytes()) {
        return Some(None);
    }
    if let Some(session) = state
        .bridge()
        .and_then(|b| b.session_for_token(state, token))
    {
        if matches!(session.role, Role::Admin) {
            return Some(None);
        }
        return session.namespace.map(Some);
    }
    let role = state.config.auth.tokens.get(token)?;
    if matches!(role, &Role::Admin) {
        return None;
    }
    let ns = state.config.auth.token_namespaces.get(token)?;
    if ns == "*" {
        return None;
    }
    Some(Some(ns.clone()))
}

/// 帧是否属于某租户：`from_agent` 或 `to_agent` 的 namespace 精确匹配。
/// 未知 agent / `"*"` 共享池 / 无 agent 的节点级帧对租户不可见，宁可少发。
pub(super) fn envelope_in_namespace(state: &AppState, env: &Envelope, scope: &str) -> bool {
    let from = env
        .from_agent
        .as_deref()
        .and_then(|a| state.router.agent_namespace(a));
    let to = env
        .to_agent
        .as_deref()
        .and_then(|a| state.router.agent_namespace(a));
    from.as_deref() == Some(scope) || to.as_deref() == Some(scope)
}

/// Subscribe to inbound messages/events (ws://127.0.0.1:8765/v1/ws).
pub(super) async fn subscribe_ws(
    ws: WebSocketUpgrade,
    Query(params): Query<SubParams>,
    State(state): State<SharedState>,
) -> Response {
    // v0.8：先尝试一次性 WS 票据（OIDC token 不进 URL 的替代路径），
    // 未命中再走既有 token 口径（local/静态/session），行为完全兼容。
    let scope = match state.bridge().and_then(|b| b.take_ws_ticket(&params.token)) {
        Some(s) => s,
        None => match ws_scope(&state, &params.token) {
            Some(s) => s,
            None => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "error": "bad token" })),
                )
                    .into_response();
            }
        },
    };
    let client = params.client.unwrap_or_default();
    if let Some(ns) = scope.as_deref() {
        if !client.is_empty() {
            match state.router.agent_namespace(&client) {
                // 共享池 agent 会跨租户，不能拿租户 token 订阅。
                Some(agent_ns) if agent_ns == "*" || agent_ns != ns => {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(json!({ "error": "agent namespace mismatch" })),
                    )
                        .into_response();
                }
                // 未注册 agent 先放行，帧过滤仍会因未知 namespace 丢弃；
                // agent 桥可在注册前建连，保持兼容。
                _ => {}
            }
        }
    }
    ws.on_upgrade(move |socket| handle_subscriber(socket, state, client, scope))
}

/// Forward events/messages to a local WS client (an agent).
/// The `client` id is the agent id: messages carrying `to_agent` only reach the
/// matching client. `scope=Some(ns)` 的观察者/agent 只收 from/to agent 属于
/// 该 namespace 的帧。
pub(super) async fn handle_subscriber(
    socket: WebSocket,
    state: SharedState,
    client_id: String,
    scope: Option<String>,
) {
    if !client_id.is_empty() {
        state.router.client_online(&client_id);
    }
    let mut rx: broadcast::Receiver<Envelope> = state.router.subscribe();
    let (mut sender, mut receiver) = socket.split();
    loop {
        tokio::select! {
            ev = rx.recv() => {
                match ev {
                    Ok(env) => {
                        // Agent-level routing: only deliver when addressed to
                        // this client, or to the node in general.
                        // 第十轮 R1：不带 client 的连接 = 观察者（Web 星图页），可见本机全部
                        // 事件含定向消息；agent 桥恒带 client，语义不变。
                        let pass = (client_id.is_empty()
                            || env
                                .to_agent
                                .as_deref()
                                .is_none_or(|a| a == client_id.as_str()))
                            && scope
                                .as_deref()
                                .is_none_or(|ns| envelope_in_namespace(&state, &env, ns));
                        if !pass {
                            continue;
                        }
                        let text = serde_json::to_string(&env).unwrap_or_default();
                        if sender.send(WsMessage::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(lag = n, "subscriber lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    None => break,
                    Some(Ok(WsMessage::Close(_))) => break,
                    _ => {} // ignore client frames
                }
            }
        }
    }
    if !client_id.is_empty() {
        state.router.client_offline(&client_id);
    }
}

// ---- peer endpoint ----------------------------------------------------------

/// 标记：本请求来自节点间 **TLS 监听**（v0.6 P1 `mtls.require` 判定用）。
/// 仅 TLS 监听（`serve_mtls`）挂载该扩展；明文监听不挂，据此区分传输通道。
#[derive(Clone, Copy, Debug)]
pub struct PeerTlsMarker;

/// 对等传输策略强制点（v0.6 P1）：`mtls.require` 模式下，明文对等链路一律拒绝。
pub(super) fn peer_transport_policy(state: &AppState, tls: bool) -> Option<Response> {
    if state.config.mtls.require_enforced() && !tls {
        tracing::warn!("api: peer link refused: plaintext transport (mtls.require)");
        return Some(
            (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "mtls.require" })),
            )
                .into_response(),
        );
    }
    None
}

/// Accept a peer WebSocket upgrade.
pub(super) async fn peer_ws(
    ws: WebSocketUpgrade,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    tls_marker: Option<Extension<PeerTlsMarker>>,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    if let Some(resp) = peer_transport_policy(&state, tls_marker.is_some()) {
        return resp;
    }
    ws.on_upgrade(move |socket| conn::handle_inbound(socket, state, remote))
}

/// Accept a dedicated file-transfer WebSocket upgrade.
pub(super) async fn file_ws(
    ws: WebSocketUpgrade,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    tls_marker: Option<Extension<PeerTlsMarker>>,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    if let Some(resp) = peer_transport_policy(&state, tls_marker.is_some()) {
        return resp;
    }
    ws.on_upgrade(move |socket| crate::file::handle_inbound(socket, state, remote))
}

/// agent namespace 允许 `*`（共享池）。
pub(super) fn valid_agent_namespace(s: &str) -> bool {
    valid_tenant_name(s) || s == "*"
}

/// agent 是否对给定 scope 可见：同 namespace 或共享池；scope=None 全可见。
pub(super) fn agent_in_scope(a: &crate::router::AgentRecord, scope: Option<&str>) -> bool {
    let Some(scope) = scope else { return true };
    let own = a
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("default");
    own == scope || own == "*"
}

/// 绑定 namespace 的 token 不得读取全局可观测面（history/events/files/metrics）。
pub(super) fn namespace_global_read_forbidden(
    state: &AppState,
    headers: &HeaderMap,
) -> Option<(StatusCode, Json<serde_json::Value>)> {
    token_namespace(state, headers).map(|_| {
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "ok": false,
                "error": "namespace-scoped token cannot read global history/events/files/metrics; use /v1/tasks or /v1/fleet/agents"
            })),
        )
    })
}

#[cfg(test)]
mod name_header_tests {
    use super::percent_decode;

    #[test]
    fn decodes_utf8_percent_encoded_name() {
        // 前端 encodeURIComponent("任务拆解.md") 的产物
        assert_eq!(
            percent_decode("%E4%BB%BB%E5%8A%A1%E6%8B%86%E8%A7%A3.md"),
            "任务拆解.md"
        );
    }

    #[test]
    fn plain_ascii_name_passes_through() {
        assert_eq!(percent_decode("report.pdf"), "report.pdf");
        assert_eq!(percent_decode("a b"), "a b"); // 未编码空格原样保留
    }

    #[test]
    fn malformed_sequences_pass_through() {
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%2"), "%2");
    }
}
