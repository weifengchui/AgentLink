// SPDX-License-Identifier: Apache-2.0
//! Local HTTP API consumed by agents on the same machine, plus the peer-facing
//! WebSocket endpoint.
//!
//! A single listener serves both; `/v1/*` requires the local API token while
//! `/peer/ws` authenticates via the cluster token during the handshake.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, FromRequest, Request};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{async_trait, Json, Router as AxRouter};
use serde_json::json;
use tokio::sync::mpsc;

use crate::config::{Config, Role, WebhookConfig};
use crate::file::FileManager;
use crate::peer::{DiscoveredPeer, PeerTable};
use crate::proto::constant_time_eq;
use crate::router::Router;

mod bridge;
mod core;

pub use self::bridge::{
    default_guardrails_status, CaAdopt, GuardVerdict, MeshBridge, MeshTaskMetrics, SessionInfo,
    UploadAdmit,
};
pub use self::core::{ws_scope, PeerTlsMarker};

use self::core::{
    ack_inbox, agents_delete, agents_get, agents_post, colors_get, colors_put, config_get,
    config_put, events_get, file_download, file_ws, fleet_agents, health, history_export_get,
    history_get, history_search_get, history_stats_get, lockdown_get, lockdown_post, metrics_get,
    open_folder, peer_ws, peers, peers_forget, peers_unforget, send, send_file, send_file_upload,
    starmap_page, status, subscribe_ws, web_page, web_setup, Runtime,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared state for the running node.
pub struct AppState {
    pub config: Config,
    /// Path of the on-disk config file (for `PUT /v1/config` persistence).
    pub config_file: PathBuf,
    pub started_at: Instant,
    pub peers: PeerTable,
    /// Discovery -> connection manager channel.
    pub peer_tx: mpsc::Sender<DiscoveredPeer>,
    /// Message router (messaging + reliability core).
    pub router: Arc<Router>,
    /// File transfer counter.
    pub files: Arc<FileManager>,
    /// Live identity (node name / tags), editable from the web UI.
    pub runtime: std::sync::RwLock<Runtime>,
    /// 类型化扩展位：闭源运行态挂载点。
    /// 缺省为空 = 免费层，行为与历史版本逐字节一致。
    pub ext: crate::extensions::Extensions,
    /// 闭源侧能力桥（与 `ext` 中的运行态同一对象）。`None` = 免费层。
    pub(crate) bridge: Option<Arc<dyn bridge::MeshBridge>>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        config_file: PathBuf,
        peers: PeerTable,
        peer_tx: mpsc::Sender<DiscoveredPeer>,
        router: Arc<Router>,
        files: Arc<FileManager>,
    ) -> Self {
        let runtime = Runtime {
            name: config.name.clone(),
            tags: config.tags.clone(),
        };
        AppState {
            config,
            config_file,
            started_at: Instant::now(),
            peers,
            peer_tx,
            router,
            files,
            runtime: std::sync::RwLock::new(runtime),
            ext: crate::extensions::Extensions::new(),
            bridge: None,
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Current live node name.
    pub fn display_name(&self) -> String {
        self.runtime.read().unwrap().name.clone()
    }

    /// Current live tags (machine capability labels).
    pub fn tags(&self) -> Vec<String> {
        self.runtime.read().unwrap().tags.clone()
    }

    /// Update live identity and persist `name`/`tags` into the config file.
    pub fn set_identity(
        &self,
        name: Option<String>,
        tags: Option<Vec<String>>,
    ) -> anyhow::Result<()> {
        {
            let mut rt = self.runtime.write().unwrap();
            if let Some(n) = name {
                rt.name = n;
            }
            if let Some(t) = tags {
                rt.tags = t;
            }
        }
        // Persist into the on-disk config so it survives restarts.
        let mut cfg = self.config.clone();
        cfg.name = self.display_name();
        cfg.tags = self.tags();
        cfg.save(&self.config_file)?;
        Ok(())
    }

    /// 挂载闭源运行态与能力桥（启动时调用一次）。
    /// `ms` 同时进入 `ext`（按类型取运行态）与 `bridge`（trait 对象）。
    pub fn attach_bridge<T>(&mut self, ms: Arc<T>)
    where
        T: std::any::Any + Send + Sync + bridge::MeshBridge + 'static,
    {
        self.ext.insert(ms.clone());
        self.bridge = Some(ms);
    }

    /// 开源侧能力桥访问。`None` = 免费层。
    pub fn bridge(&self) -> Option<&Arc<dyn bridge::MeshBridge>> {
        self.bridge.as_ref()
    }
}

pub type SharedState = Arc<AppState>;

/// Mesh 门统一应答（写面+引擎循环+调度整体属闭源发行版，免费层一律
/// 403）。wire 文案以 `mesh license required` 为准。
pub fn mesh_required() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": "mesh license required" })),
    )
}

pub fn bad_request(msg: &str) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}

/// Build the router: local API and the peer WebSocket endpoint.
pub fn router(state: SharedState) -> AxRouter {
    // Browser uploads stream raw bytes into `Bytes`. Axum's default body limit is
    // 2 MiB, which would reject larger files with a plain-text 413 *before* the
    // handler runs, silently overriding `files.max_file_size`. Raise the limit on
    // that route to the configured ceiling (plus one byte, so a file that is one
    // byte too big still reaches the handler's own check and gets the clean JSON
    // "file too large" message instead of axum's terse text). Taken at startup:
    // changing `max_file_size` via `/v1/config` needs a node restart to move it.
    let upload_route = AxRouter::new()
        .route("/v1/send-file-upload", post(send_file_upload))
        .layer(DefaultBodyLimit::max(
            state.config.files.max_file_size as usize + 1,
        ));
    AxRouter::new()
        .route("/v1/health", get(health))
        .route("/v1/status", get(status))
        .route("/v1/peers", get(peers))
        .route(
            "/v1/peers/:node_id/forget",
            axum::routing::delete(peers_forget),
        )
        .route(
            "/v1/peers/:node_id/unforget",
            axum::routing::post(peers_unforget),
        )
        .route("/v1/send", post(send))
        .route("/v1/send-file", post(send_file))
        .route("/v1/ack-inbox", post(ack_inbox))
        .route(
            "/v1/agents",
            get(agents_get).post(agents_post).delete(agents_delete),
        )
        .route("/v1/fleet/agents", get(fleet_agents))
        .route("/v1/history", get(history_get))
        .route("/v1/events", get(events_get))
        .route("/v1/lockdown", get(lockdown_get).post(lockdown_post))
        .route("/v1/history/search", get(history_search_get))
        .route("/v1/colors", get(colors_get).put(colors_put))
        .route("/v1/history/stats", get(history_stats_get))
        .route("/v1/history/export", get(history_export_get))
        .route("/v1/files/:file_name", get(file_download))
        .route("/v1/open-folder", post(open_folder))
        .route("/v1/config", get(config_get).put(config_put))
        .route("/v1/metrics", get(metrics_get))
        .route("/web", get(web_page))
        .route("/v1/web", get(web_page))
        .route("/v1/starmap", get(starmap_page))
        .route("/web/setup", get(web_setup))
        .route("/v1/ws", get(subscribe_ws))
        .route("/peer/ws", get(peer_ws))
        .route("/peer/file/ws", get(file_ws))
        .merge(upload_route)
        .with_state(state)
}

/// 仅对等节点使用的路由（v0.6 P1 mTLS）：只挂 peer 消息与文件通道。
/// 本地 API/网页仍在明文主监听上；mTLS 开启时对端连接走独立 TLS 监听。
pub fn peer_router(state: SharedState) -> AxRouter {
    AxRouter::new()
        .route("/peer/ws", get(peer_ws))
        .route("/peer/file/ws", get(file_ws))
        .with_state(state)
}

/// v0.7 §6.3：只读模式下的写面拒绝（统一 503，读面与在途任务不受影响）。
pub fn read_only_guard(state: &AppState) -> Option<(StatusCode, Json<serde_json::Value>)> {
    if state.router.is_read_only() {
        return Some((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ok": false,
                "error": "read_only",
                "mode": "read_only",
                "hint": "节点处于只读维护模式；POST /v1/mesh/mode {\"mode\":\"normal\"} 退出",
            })),
        ));
    }
    None
}

/// 出站 webhook 客户端（v0.6 P0）。用于向外部端点发送事件通知。
/// 所有发送都是异步的（tokio::spawn），不阻塞调用方。
#[allow(dead_code)]
pub struct WebhookClient {
    http: reqwest::Client,
    config: WebhookConfig,
}

#[allow(dead_code)]
impl WebhookClient {
    pub fn new(config: &WebhookConfig) -> Option<Self> {
        if config.url.is_empty() || config.events.is_empty() {
            tracing::warn!(url = %config.url, events = %config.events, "webhook: 未配置 url 或 events，跳过");
            return None;
        }
        tracing::info!(url = %config.url, events = %config.events, "webhook: 客户端已创建");
        Some(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(config.timeout_seconds))
                .build()
                .ok()?,
            config: config.clone(),
        })
    }

    /// 判断给定事件类型是否应触发 webhook。
    pub fn should_fire(&self, event: &str) -> bool {
        self.config
            .events
            .split(',')
            .map(|e| e.trim())
            .any(|e| !e.is_empty() && e == event)
    }

    /// 异步发送 webhook（不等待响应结果，失败只记日志）。
    pub async fn fire(&self, event: String, node_id: String, payload: serde_json::Value) {
        if !self.should_fire(&event) {
            return;
        }
        let body = serde_json::json!({
            "event": event,
            "node_id": node_id,
            "ts": crate::store::now_wall(),
            "payload": payload,
        });
        // body 原文逐字节签名（收敛口径：X-Hub-Signature-256，
        // sha256=<hex>）。secret 空 = 不签名，帧内容与 P0 完全一致。
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        let signature = if self.config.secret.is_empty() {
            None
        } else {
            use hmac::Mac;
            let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(
                self.config.secret.as_bytes(),
            )
            .expect("HMAC accepts any key length");
            mac.update(&bytes);
            Some(format!(
                "sha256={}",
                hex::encode(mac.finalize().into_bytes())
            ))
        };
        let client = self.http.clone();
        let url = self.config.url.clone();
        let max_retries = self.config.max_retries;
        let retry_interval = self.config.retry_interval_seconds;
        tokio::spawn(async move {
            let mut last_err = None;
            for attempt in 0..=max_retries {
                let mut req = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(bytes.clone());
                if let Some(sig) = &signature {
                    req = req.header("X-Hub-Signature-256", sig);
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => return,
                    Ok(resp) => {
                        last_err = Some(format!("HTTP {}", resp.status()));
                    }
                    Err(e) => last_err = Some(e.to_string()),
                }
                if attempt < max_retries {
                    tokio::time::sleep(std::time::Duration::from_secs(retry_interval)).await;
                }
            }
            tracing::warn!(
                event = %event, url = %url, error = ?last_err,
                "webhook: 发送失败，已达最大重试次数"
            );
        });
    }
}

pub fn clean(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

// ---- helpers ----------------------------------------------------------------

/// Body extractor that parses JSON regardless of `Content-Type` (agents may
/// forget the header; we do not want that to break local integration).
pub struct JsonAny<T>(pub T);

#[async_trait]
impl<S, T> FromRequest<S> for JsonAny<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, _state).await.map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "bad body" })),
            )
        })?;
        let value = serde_json::from_slice(&bytes).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid JSON: {e}") })),
            )
        })?;
        Ok(JsonAny(value))
    }
}

/// 从 `X-Auth-Token` 或 `Authorization: Bearer` 取令牌。
pub fn header_token(headers: &HeaderMap) -> Option<String> {
    if let Some(t) = headers.get("x-auth-token").and_then(|v| v.to_str().ok()) {
        let t = t.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    let auth = headers.get("authorization")?.to_str().ok()?.trim();
    let (scheme, value) = auth.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") && !value.trim().is_empty() {
        Some(value.trim().to_string())
    } else {
        None
    }
}

pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 鉴权并返回调用者角色（v0.6 P0 RBAC；v0.7 支持 OIDC 中间件注入）。
/// - 未提供 token 或 token 不匹配 -> None (401)
/// - OIDC 中间件校验成功 -> 注入的可信角色
/// - 匹配 `local_api.token` 但不在 `auth.tokens` 中 -> fallback_role (缺省 Admin，兼容 0.5.9)
/// - 在 `auth.tokens` 中 -> 对应角色
pub fn authorized_role(state: &AppState, headers: &HeaderMap) -> Option<Role> {
    // 仅由 OIDC 中间件在校验成功后写入；入口已剥除同名客户端头。
    if let Some(v) = headers
        .get("x-agentlink-oidc-role")
        .and_then(|v| v.to_str().ok())
    {
        return state.bridge().and_then(|b| b.parse_injected_role(v));
    }
    let token = header_token(headers)?;
    let auth = &state.config.auth;

    // 优先匹配显式配置的 token -> 角色
    if let Some(role) = auth.tokens.get(&token) {
        return Some(*role);
    }

    // 回退：local_api.token 视为 fallback_role（缺省 Admin，兼容 0.5.9）
    if constant_time_eq(token.as_bytes(), state.config.local_api.token.as_bytes()) {
        return Some(auth.fallback_role);
    }

    // 浏览器 OIDC 登录签发的本节点短期 session（不经过 introspection）。
    if let Some(session) = state
        .bridge()
        .and_then(|b| b.session_for_token(state, &token))
    {
        return Some(session.role);
    }

    None
}

/// v0.7 §4.2：token 绑定的 namespace（未绑定 → None，即全局）。
pub fn token_namespace(state: &AppState, headers: &HeaderMap) -> Option<String> {
    if let Some(ns) = headers
        .get("x-agentlink-oidc-namespace")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        return Some(ns.to_string());
    }
    let token = header_token(headers)?;
    if let Some(session) = state
        .bridge()
        .and_then(|b| b.session_for_token(state, &token))
    {
        return session.namespace;
    }
    state
        .config
        .auth
        .token_namespaces
        .get(&token)
        .cloned()
        .filter(|s| !s.is_empty())
}

/// 租户名：1-64 字符 `[A-Za-z0-9_.-]`（任务/agent namespace 不允许 `*`）。
pub fn valid_tenant_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// 兼容 0.5.x 的 bool 鉴权：任意有效 token 即通过（角色缺省 Admin）。
#[allow(dead_code)]
fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    authorized_role(state, headers).is_some()
}

/// 检查角色是否允许访问给定端点；不允许返回 403。
#[allow(dead_code)]
fn require_role(role: Option<Role>, endpoint: &str) -> Result<Role, ()> {
    match role {
        Some(r) if r.allows(endpoint) => Ok(r),
        _ => Err(()),
    }
}

pub fn unauthorized() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "invalid or missing X-Auth-Token" })),
    )
}

#[allow(dead_code)]
pub fn forbidden() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": "insufficient role for this endpoint" })),
    )
}
