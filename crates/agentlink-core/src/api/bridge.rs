// SPDX-License-Identifier: Apache-2.0
//! 闭源能力桥。
//!
//! 开源核心（`api/core.rs`、`conn.rs` 等）只通过 [`MeshBridge`] trait 访问
//! mesh/HA/OIDC/mTLS/lease 等闭源发行版专属能力，返回值一律是本模块定义的
//! 投影类型（不暴露闭源类型）。闭源侧由其运行态实现本 trait 并挂到 `AppState::bridge`。缺省（`bridge = None`）= 免费层：
//! 行为与历史版本逐字节一致。

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{json, Value};

use super::AppState;
use crate::config::Role;
use crate::proto::{Envelope, Hello};

/// OIDC 会话的开源侧投影（仅核心鉴权所需字段）。
pub struct SessionInfo {
    pub role: Role,
    pub namespace: Option<String>,
}

/// 任务引擎指标快照（`/metrics` 用；mesh 关闭时全 0）。
#[derive(Default, Clone)]
pub struct MeshTaskMetrics {
    pub tasks_total: u64,
    pub tasks_running: u64,
    pub tasks_pending: u64,
    pub tasks_succeeded: u64,
    pub tasks_failed: u64,
    pub tasks_declined: u64,
    pub tasks_cancelled: u64,
    pub scheduled_nodes: u64,
    pub node_quota: u64,
}

/// 发送护栏判定结果（字段与闭源护栏判定结果一一对应，
/// JSON 序列化口径逐字节一致）。
pub struct GuardVerdict {
    pub allowed: bool,
    pub redacted: bool,
    pub reason: Option<String>,
    pub findings: Value,
    pub categories: Vec<String>,
    pub payload: Value,
}

/// namespace 磁盘配额准入结果。
pub enum UploadAdmit {
    Pass,
    Reject { limit: u64, current: u64 },
    HaDown(String),
}

/// 集群 CA 领取结果（闭源 mTLS 运行态领取结果的投影 + 未启用/失败态）。
pub enum CaAdopt {
    Adopted,
    Same,
    Conflict { local: String, remote: String },
    Unavailable,
    Failed(String),
}

/// 免费层（无桥）的 guardrails 状态缺省投影——与
/// 闭源护栏配置缺省值的展示口径一致。
pub fn default_guardrails_status() -> Value {
    json!({
        "enabled": false,
        "action": "redact",
        "pii": true,
        "secrets": true,
        "injection": true,
        "topics_enabled": false,
    })
}

/// 闭源能力桥。开源核心只依赖本 trait 与上述投影类型。
pub trait MeshBridge: Any + Send + Sync {
    /// 任务引擎是否生效（`mesh` 运行态存在）。
    fn engine_active(&self) -> bool;

    /// 任务引擎指标快照。
    fn task_metrics(&self) -> MeshTaskMetrics;

    /// 浏览器 OIDC 登录 session → 角色/namespace（配置关闭即 `None`）。
    fn session_for_token(&self, state: &AppState, token: &str) -> Option<SessionInfo>;

    /// 解析 OIDC 中间件注入的可信角色头。
    fn parse_injected_role(&self, raw: &str) -> Option<Role>;

    /// 消费一次性 WS 票据（命中返回其可见范围）。
    fn take_ws_ticket(&self, token: &str) -> Option<Option<String>>;

    /// 发送护栏是否启用（护栏配置归闭源侧持有）。
    /// 缺省 `false` = 不扫描，与历史免费层行为一致。
    fn guard_enabled(&self) -> bool {
        false
    }

    /// 发送护栏检查（配置由实现方内部持有）。
    fn guard_send(&self, payload: &Value) -> GuardVerdict;

    /// `/v1/status` 的 guardrails 状态投影（字段口径与历史版本一致）。
    fn guardrails_status(&self) -> Value {
        default_guardrails_status()
    }

    /// 闭源配置校验（mesh/abac/guardrails/oidc/chaos/ha；步骤 1.4）。
    /// 开源构建缺省放行；闭源实现走闭源校验器。
    fn validate_closed_config(&self, _cfg: &crate::config::Config) -> Result<(), String> {
        Ok(())
    }

    /// namespace 磁盘配额准入（异步：HA 启用时走复制状态机）。
    /// 配额开关与上限由实现方内部持有；`quota.enabled=false` 直接 `Pass`。
    fn upload_admit<'a>(
        &'a self,
        state: &'a AppState,
        namespace: String,
        add: u64,
    ) -> Pin<Box<dyn Future<Output = UploadAdmit> + Send + 'a>>;

    /// mTLS 运行态是否存在（区分「未启用静默」与「启用但未就绪」）。
    fn tls_enabled(&self) -> bool;

    /// 拨号用 TLS 客户端配置（未就绪 = `None`）。
    fn tls_client_config(&self) -> Option<Arc<rustls::ClientConfig>>;

    /// 本节点当前集群 CA 指纹。
    fn tls_ca_fp(&self) -> Option<String>;

    /// 集群 CA 证书 + 私钥 PEM（供 ca_sync 推送）。
    fn tls_ca_pems(&self) -> Option<(String, String)>;

    /// mTLS 是否就绪（可对外提供 wss）。
    fn tls_ready(&self) -> bool;

    /// 经已认证链路领取集群 CA。
    fn adopt_ca(&self, cert: &str, key: &str) -> CaAdopt;

    /// Lease 上报的当前 advertised CPU（无快照 = `None`）。
    fn advertised_cpu(&self) -> Option<u32>;

    // ---- 步骤 1.3：控制面钩子（conn/file 等开源传输路径经此调用闭源能力）----

    /// v0.6 P1 出站握手密封：填硬件指纹 + 准入令牌 HMAC（幂等，任何模式都安全）。
    fn seal_hello(&self, hello: &mut Hello);

    /// v0.6 P1 入站准入闸门。`Err(原因)` = 必须拒绝连接。
    fn admission_gate(&self, hello: &Hello) -> Result<(), String>;

    /// 由 CA PEM 实算指纹（`None` = 解析失败）。
    fn ca_fingerprint(&self, pem: &str) -> Option<String>;

    /// v0.8 HA：本节点是否 leader（未启用 HA = `false`）。
    fn ha_is_leader(&self) -> bool;

    /// v0.8 HA 复制帧：把 Raft 消息喂给本地状态机，返回待回送的
    /// `(目标节点, 消息 JSON)` 列表（未启用 HA = 空表）。
    fn ha_on_frame(&self, env: &Envelope) -> Vec<(String, Value)>;

    /// v0.8 M4：leader 侧执行复制 KV 请求，返回 `HaKvResp` 口径的应答 JSON。
    fn ha_kv_execute(&self, req: &str, op: Option<Value>) -> Value;

    /// v0.8 M4：把 `ha_kv` 应答回填等待表（超时的请求已被移除，丢弃即可）。
    fn ha_kv_deliver(&self, resp: &Value);
}
