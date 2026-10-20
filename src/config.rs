//! Configuration loading, validation and persistence.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const CURRENT_CONFIG_VERSION: u32 = 1;
pub const DEFAULT_LISTEN_PORT: u16 = 8765;
pub const DEFAULT_BROADCAST_PORT: u16 = 8766;

/// Top-level node configuration. Mirrors `docs/设计方案.md` §12.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_version")]
    pub v: u32,
    /// Stable unique identifier used for addressing. Never changes once created.
    pub node_id: String,
    /// Human friendly display name. Not used for addressing.
    pub name: String,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Machine-level capability labels (shown in /v1/peers).
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub history: HistoryConfig,
    /// Observer-side payload redaction for history/events/web views.
    /// Delivery frames to the target agent are never redacted.
    #[serde(default)]
    pub redact: RedactConfig,
    #[serde(default)]
    pub web: WebConfig,

    #[serde(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub local_api: LocalApiConfig,
    #[serde(default)]
    pub files: FilesConfig,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    #[serde(default)]
    pub log: LogConfig,
    /// AgentMesh 任务引擎配置（docs/task引擎内核化设计-2026-10-18.md §5）。
    /// 老 agentlink.json 缺整段 = 免费层（enabled=false，全默认），零破坏。
    #[serde(default)]
    pub mesh: MeshConfig,
    /// RBAC 角色/token 映射（v0.6 P0）。老配置缺此段时，local_api.token
    /// 视为 Admin 角色，行为与 0.5.9 完全一致（零破坏）。
    #[serde(default)]
    pub auth: AuthConfig,
    /// 出站 webhook 配置（v0.6 P0）。用于任务失败/节点离线/审计告警等
    /// 事件的外部通知。老配置缺此段 = 不启用 webhook，零破坏。
    #[serde(default)]
    pub webhook: WebhookConfig,
    /// mTLS 配置（v0.6 P1）。LAN 默认关闭，一键开启后节点间连接使用双向 TLS。
    #[serde(default)]
    pub mtls: MtlsConfig,
    /// Lease 心跳与阈值差异上报（v0.6 P1）。缺省关闭：零新增流量。
    /// 开启后节点按 `interval_secs` 采样本机 CPU，变化超过
    /// `cpu_threshold_pct`（±5%）才发 Full Status 帧（`ctrl/lease`），
    /// 其余沿用既有会话心跳（10~30 台阶段替代 UDP 广播风暴）。
    #[serde(default)]
    pub lease: LeaseConfig,
    /// GitOps 目录驱动（v0.6 P1）：watch 仓库 tasks/ 目录自动 apply。
    /// 缺省关闭（段可缺省）；`mesh.enabled` 未开时即便 enabled 也只告警不 spawn。
    #[serde(default)]
    pub gitops: GitopsConfig,
    /// 中继拓扑（v0.7 spine/relay）。缺省关闭 = 全网格原行为。
    #[serde(default)]
    pub relay: RelayConfig,
}

/// GitOps 目录驱动参数（v0.6 P1）。缺省 `enabled=false`——完全惰性。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitopsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 仓库内任务目录（典型 `tasks/`）。扫描根层 + 一层子目录的 `*.json`。
    #[serde(default)]
    pub dir: Option<PathBuf>,
    /// 轮询周期（秒）。实现不引入 fs-watch 依赖：内容 sha256 差分轮询。
    #[serde(default = "default_gitops_interval_secs")]
    pub interval_secs: u64,
}

fn default_gitops_interval_secs() -> u64 {
    15
}

impl Default for GitopsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: None,
            interval_secs: default_gitops_interval_secs(),
        }
    }
}

/// Lease 差异上报参数（v0.6 P1）。缺省 `enabled=false`——零流量、零行为变化。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 采样与差异判定周期（秒）。
    #[serde(default = "default_lease_interval_secs")]
    pub interval_secs: u64,
    /// CPU 变化阈值（百分比点数；设计案 §5.1 的「±5% 才发 Full Status」）。
    #[serde(default = "default_lease_cpu_threshold")]
    pub cpu_threshold_pct: u32,
    /// 两次 Full Status 之间，裸 lease 心跳帧（无负载字段）的最大间隔（秒）。
    /// 0 = 只在阈值跨越时发帧（活性完全沿用既有会话心跳）。
    #[serde(default = "default_lease_bare_secs")]
    pub bare_beat_secs: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_lease_interval_secs(),
            cpu_threshold_pct: default_lease_cpu_threshold(),
            bare_beat_secs: default_lease_bare_secs(),
        }
    }
}

fn default_lease_interval_secs() -> u64 {
    10
}

fn default_lease_cpu_threshold() -> u32 {
    5
}

fn default_lease_bare_secs() -> u64 {
    60
}

/// Mesh 总开关与 license/引擎调参入口。缺省即免费层：引擎不 spawn、
/// 写面 403，行为与 0.5.9 完全一致。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MeshConfig {
    /// 总开关。缺省 false（serde 零值），显式声明 `"enabled": true` 才进 Mesh 态。
    #[serde(default)]
    pub enabled: bool,
    /// license.toml 路径；未设置 -> `None`（节点按免费层运行，启动打 error 日志）。
    #[serde(default)]
    pub license_file: Option<PathBuf>,
    /// reconcile 引擎的调参项（拍板记录第 4 条：显式声明才有的语义全在这）。
    #[serde(default)]
    pub engine: EngineConfig,
}

/// 任务引擎内核循环参数。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// 重试预算。缺省 **0**——拍板记录第 4 条：只有显式声明才重试。
    #[serde(default)]
    pub max_retries: u32,
    /// failed→pending 重投的固定退避间隔（秒）。拍板：固定间隔，不做指数退避。
    #[serde(default = "default_engine_backoff_secs")]
    pub backoff_secs: u64,
    /// drift/declined 只告警不自动换机重派（守执行方唯一状态权威，拍板第 4 条）。
    #[serde(default = "default_true")]
    pub drift_alert_only: bool,
    /// reconcile loop 的 tick 周期（毫秒）。200ms 是 P99 调度延迟 <500ms 验收门的推论。
    #[serde(default = "default_engine_tick_ms")]
    pub tick_ms: u64,
    /// 每节点并发任务额度上限（调度打分时按不重复 node_id 记账）。
    #[serde(default = "default_engine_max_concurrent")]
    pub max_concurrent_per_node: usize,
    /// 熔断隔离（v0.6 P1）：异常节点自动置 unschedulable（冷却后自动恢复）。
    /// 缺省关闭——调度行为零变化。
    #[serde(default)]
    pub circuit: CircuitConfig,
    /// 预热池（v0.6 P2）：常驻桥 keep-warm 派发。缺省关闭——引擎零任务、
    /// 零流量，`/v1/tasks` 面板不见任何池行。
    #[serde(default)]
    pub warmpool: WarmpoolConfig,
    /// 节点失联任务回收（v0.7 chaos 第一波）。缺省关闭——调度行为零变化。
    #[serde(default)]
    pub reclaim: ReclaimConfig,
    /// SLO 告警 + 历史基线异常检测（v0.7 §7.2）。缺省关闭——零采集/零告警。
    #[serde(default)]
    pub slo: crate::slo::SloConfig,
}

/// 节点失联任务回收参数（v0.7 chaos / kill-node）。
///
/// 只回收**所在节点已离线**的 Running 任务：等宽限期（给心跳/目录收敛留
/// 余量）→ 置 Failed + 立即到期，复用既有 `max_retries` backoff 重投路径
/// 换机重派；`attempts` 达上限则保持 Failed 终态。执行方唯一状态权威不变：
/// 引擎不伪造 task_result，只做「失联超时」判定。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReclaimConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 节点离线多久后判定失联（秒）。下限 1；缺省 10（> heartbeat 3s +
    /// offline_timeout 6s 的默认收敛窗口）。
    #[serde(default = "default_reclaim_grace_secs")]
    pub grace_seconds: u64,
    /// 节点失联重派次数上限（独立于 `max_retries`——节点死亡不是执行方
    /// 业务失败，不该占用执行方的重试预算）。缺省 3；达上限保持 failed。
    #[serde(default = "default_reclaim_max_attempts")]
    pub max_attempts: u32,
}

impl Default for ReclaimConfig {
    fn default() -> Self {
        ReclaimConfig {
            enabled: false,
            grace_seconds: default_reclaim_grace_secs(),
            max_attempts: default_reclaim_max_attempts(),
        }
    }
}

fn default_reclaim_grace_secs() -> u64 {
    10
}

fn default_reclaim_max_attempts() -> u32 {
    3
}

/// 预热池参数（v0.6 P2）。把「常驻桥 = agent warmpool」形式化为引擎内置
/// keep-warm 派发循环：成员名单里的 agent 就是池位——引擎按
/// `interval_secs` 周期为每个在线成员注入 `task_type` 预热任务
/// （自带 correlation="warmpool"），回收 succeeded/failed 终态后池位空闲，
/// 下拍再补一行。真实任务到达即可派给已热的执行方（引擎 tick 上界仍
/// 200ms）。成员离线/熔断则该位跳过（find_agent_node + taint 双闸）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmpoolConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 池成员（agent_id 名单，显式声明——预热是资源动作，不做隐式能力推断）。
    #[serde(default)]
    pub members: Vec<String>,
    /// 单成员同时允许的在池任务上限（pending+running 计）。
    #[serde(default = "default_warmpool_keep_inflight")]
    pub keep_inflight: usize,
    /// keep-warm 派发周期（秒）；下限 5 秒。
    #[serde(default = "default_warmpool_interval_secs")]
    pub interval_secs: u64,
    /// 预热任务的 type（执行方 runner 据此当 no-op keep-warm 处理）。
    #[serde(default = "default_warmpool_task_type")]
    pub task_type: String,
    /// 预热任务优先级（越大越靠后，缺省 900——真任务一律先于池任务）。
    #[serde(default = "default_warmpool_priority")]
    pub priority: u64,
}

fn default_warmpool_keep_inflight() -> usize {
    1
}

fn default_warmpool_interval_secs() -> u64 {
    120
}

fn default_warmpool_task_type() -> String {
    "warmup.ping".to_string()
}

fn default_warmpool_priority() -> u64 {
    900
}

impl Default for WarmpoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            members: Vec::new(),
            keep_inflight: default_warmpool_keep_inflight(),
            interval_secs: default_warmpool_interval_secs(),
            task_type: default_warmpool_task_type(),
            priority: default_warmpool_priority(),
        }
    }
}

/// 熔断隔离参数（v0.6 P1）。窗口内 task-status failed 帧按执行节点归账，
/// 达到 `failures` 次即置 unschedulable 标签 `cooldown_secs` 秒（自动恢复）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 触发阈值：窗口内失败次数。
    #[serde(default = "default_circuit_failures")]
    pub failures: u32,
    /// 计数窗口（秒）。
    #[serde(default = "default_circuit_window_secs")]
    pub window_secs: u64,
    /// 熔断冷却时长（秒）；到期自动恢复调度资格。
    #[serde(default = "default_circuit_cooldown_secs")]
    pub cooldown_secs: u64,
}

fn default_circuit_failures() -> u32 {
    3
}

fn default_circuit_window_secs() -> u64 {
    600
}

fn default_circuit_cooldown_secs() -> u64 {
    300
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            failures: default_circuit_failures(),
            window_secs: default_circuit_window_secs(),
            cooldown_secs: default_circuit_cooldown_secs(),
        }
    }
}

/// 中继拓扑（v0.7 spine/relay）：叶子只连 spine，跨叶子帧由 spine 单跳转发。
/// 缺省全关——现有全网格行为逐字节不变。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayConfig {
    /// 本节点作为中继（spine）：接收「目标为第三方」的帧并转发，
    /// 广播帧向除入站连接外的直连 peer 扩散（hops 上限内）。
    #[serde(default)]
    pub enabled: bool,
    /// 上游中继 node_id（叶子用）。非空且目标节点非直连时，出站帧改投
    /// spine 并由其转发（spine 再按目标直连投递）；空 = 维持既有全网格语义。
    #[serde(default)]
    pub spine: String,
    /// spine 的**上游中继** node_id（多级级联用，v0.7 第二波）：
    /// 本节点作为 spine 找不到目标直连时，向 upstream 兜底转发。
    /// 叶子通常不配（叶子用 `spine`）；空 = 不级联（第一波语义）。
    #[serde(default)]
    pub upstream: String,
    /// 中继跳数上限（缺省 2：leaf → spine → leaf；跨 spine 级联需 ≥3）。
    #[serde(default = "default_relay_max_hops")]
    pub max_hops: u8,
}

fn default_relay_max_hops() -> u8 {
    2
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            spine: String::new(),
            upstream: String::new(),
            max_hops: default_relay_max_hops(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// `udp_broadcast` is the primary method, `mdns` is optional (feature gated).
    #[serde(default = "default_discovery_methods")]
    pub methods: Vec<String>,
    #[serde(default = "default_broadcast_port")]
    pub broadcast_port: u16,
    #[serde(default = "default_announce_interval")]
    pub announce_interval_seconds: u64,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default = "default_offline_timeout")]
    pub offline_timeout_seconds: u64,
    /// Fallback peers for networks where broadcast is blocked, e.g. `192.168.1.11:8765`.
    #[serde(default)]
    pub static_peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// `strict` requires a matching cluster token, `open` trusts the LAN.
    #[serde(default = "default_security_mode")]
    pub mode: String,
    #[serde(default = "default_placeholder_token")]
    pub cluster_token: String,
    /// 新节点准入（v0.6 P1，roadmap §4.1）。缺省未配置 = 不启用，
    /// 行为与从前完全一致（cluster_token 单因素）。
    #[serde(default)]
    pub admission: AdmissionConfig,
    /// 控制面帧强制验签（v0.7 第二波）。缺省 false：
    /// - 帧带 `sig` → strict 模式下**总是**校验（篡改即拒，零破坏）；
    /// - 帧不带 `sig` → 放行（兼容未升级节点 / 旧控制帧）。
    ///
    /// 开启后：`agents_update` / `node_dir` 等控制帧不带签名一律拒绝。
    ///
    /// **滚动升级**：须先把 `control_auth=true` 推到接收方，再让发送方开启/升级，
    /// 否则未签名控制帧会被丢弃（agent 表/目录不更新，不影响业务消息）。
    #[serde(default)]
    pub control_auth: bool,
    /// 不可变审计日志（v0.7 §4.4）。缺省 false：不建目录、不落盘。
    #[serde(default)]
    pub audit: AuditConfig,
}

/// 不可变审计日志配置：hash 链 append-only，落 `<data_dir>/audit/audit.jsonl`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuditConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// 新节点准入配置（v0.6 P1）：bootstrap 令牌 + 硬件指纹**双验证**。
///
/// 开启后，任何对等连接的握手（含文件通道）都必须同时满足：
/// 1. 现有 `cluster_token` 校验（strict 模式下既有的必要条件）；
/// 2. `boot` = HMAC(bootstrap_token, node_id|nonce|v) 正确（准入令牌）；
/// 3. `hw_fp`（本机硬件指纹）在 `allowed_hardware` 白名单内。
///
/// 细则：
/// - 只配置 bootstrap_token（白名单空）→ 仅验证令牌（单因素，小集群）。
/// - 只配置白名单（令牌为空/占位符）→ 仅验证指纹。
/// - 两者都未配置 → fail-closed：拒绝一切入网（握手段里视为非法配置，
///   启动时就打告警）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionConfig {
    /// 准入开关。缺省 false：不启用，与之前版本行为逐字符一致。
    #[serde(default)]
    pub enabled: bool,
    /// 准入令牌。空或占位符视为「未配置」。
    #[serde(default = "default_placeholder_token")]
    pub bootstrap_token: String,
    /// 硬件指纹白名单（SHA-256 前 16 个 hex 字符）。空 = 不校验指纹。
    #[serde(default)]
    pub allowed_hardware: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalApiConfig {
    #[serde(default = "default_local_host")]
    pub host: String,
    /// 主 API 令牌。若 `auth.tokens` 未配置，此令牌视为 Admin 角色。
    #[serde(default = "default_placeholder_token")]
    pub token: String,
}

/// RBAC 角色（v0.6 P0）。按特权从高到低排序。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// 管理员：所有端点（含配置、mesh 状态、指标）。
    #[default]
    Admin,
    /// 运维：读写任务/代理，查看状态，但不改配置。
    Operator,
    /// 只读：GET 端点。
    Viewer,
    /// 执行方：仅发送/接收任务帧（POST /v1/send 等）。
    Executor,
}

impl Role {
    /// 审计/日志用的稳定小写名。
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Operator => "operator",
            Role::Viewer => "viewer",
            Role::Executor => "executor",
        }
    }

    /// 是否允许访问给定端点。
    pub fn allows(self, endpoint: &str) -> bool {
        match self {
            Role::Admin => true,
            Role::Operator => !matches!(
                endpoint,
                "/v1/config"
                    | "/v1/mesh/status"
                    | "/v1/metrics"
                    | "/v1/lockdown"
                    | "/v1/admission"
                    | "/v1/audit/verify"
                    | "/v1/audit/export"
            ),
            Role::Viewer => matches!(
                endpoint,
                "/v1/health"
                    | "/v1/status"
                    | "/v1/peers"
                    | "/v1/tasks"
                    | "/v1/tasks/"
                    | "/v1/events"
                    | "/v1/history"
                    | "/v1/agents"
                    | "/v1/fleet/agents"
                    | "/v1/metrics"
                    | "/v1/mesh/slo"
            ),
            Role::Executor => matches!(
                endpoint,
                "/v1/send" | "/v1/send-file" | "/v1/send-file-upload" | "/v1/ack-inbox"
            ),
        }
    }
}

/// RBAC 配置（v0.6 P0）。token -> 角色映射。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// 令牌到角色的映射。未在本表中的令牌视为无访问权限。
    #[serde(default)]
    pub tokens: std::collections::HashMap<String, Role>,
    /// 未在 `tokens` 中但通过 `local_api.token` 传入的令牌，缺省角色。
    /// 缺省 Admin 以保持与 0.5.9 行为一致。
    #[serde(default = "default_auth_fallback_role")]
    pub fallback_role: Role,
}

fn default_auth_fallback_role() -> Role {
    Role::Admin
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesConfig {
    #[serde(default = "default_max_file_size")]
    pub max_file_size: u64,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u32,
    /// Not set in JSON -> `data_dir/files`. Explicit relative paths stay
    /// anchored to the config file's folder (pre-0.4.10 compatibility).
    #[serde(default)]
    pub recv_dir: Option<std::path::PathBuf>,
    #[serde(default = "default_one")]
    pub max_concurrent: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_ack_timeout")]
    pub ack_timeout_seconds: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_offline_ttl")]
    pub offline_ttl_seconds: u64,
    #[serde(default = "default_dedup_cache")]
    pub dedup_cache_size: usize,
    /// Persist message queues to `data_dir/journal.jsonl` (fsync per record)
    /// and the agent registry to `data_dir/agents.json`. Disable only for
    /// throwaway/test nodes — a restart then loses in-flight messages.
    #[serde(default = "default_true")]
    pub persist: bool,
    /// Local agents that are offline and have not re-registered within this
    /// many seconds are auto-removed (0 disables the cleanup).
    #[serde(default = "default_agent_stale_ttl")]
    pub agent_stale_ttl_seconds: u64,
    /// Journal fsync 策略（v0.7 性能项）。`0`（缺省）= 每条记录 flush+fsync
    /// （掉电零丢，v0.5 起的既有语义，行为不变）；`>0` = 批量 fsync 窗口
    /// （毫秒）：append 只把数据推进 OS 页缓存，后台线程每窗口至多 sync 一次，
    /// **掉电最多丢最后一个窗口**，换取突发派发时 P99 调度延迟从秒级回到
    /// 200ms 级（见 docs/journal-groupcommit-验证-2026-10-19.md）。
    #[serde(default)]
    pub journal_sync_ms: u64,
}

/// 出站 webhook 配置（v0.6 P0）。用于任务失败/节点离线/审计告警等
/// 事件的外部通知。所有字段缺省时 webhook 不启用。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// webhook 目标 URL（如 http://192.168.1.50:9000/webhooks/agentlink）。
    /// 空字符串表示不启用 webhook。
    #[serde(default)]
    pub url: String,
    /// 触发 webhook 的事件类型，逗号分隔（如 "task_failed,node_offline,audit_alert,
    /// agent_message,node_taint"）。空字符串表示不启用任何事件。
    #[serde(default)]
    pub events: String,
    /// HMAC-SHA256 签名密钥（wake-hook 同口径）。非空时每个 webhook 请求带
    /// `X-Hub-Signature-256: sha256=<hex>`（对 body 原文逐字节签名）；空 =
    /// 不签名（向后兼容，行为与 v0.6 P0 一致）。
    #[serde(default)]
    pub secret: String,
    /// HTTP 请求超时（秒），缺省 5 秒。
    #[serde(default = "default_webhook_timeout_seconds")]
    pub timeout_seconds: u64,
    /// 最大重试次数，缺省 3 次。
    #[serde(default = "default_webhook_max_retries")]
    pub max_retries: u32,
    /// 重试间隔（秒），缺省 10 秒。
    #[serde(default = "default_webhook_retry_interval_seconds")]
    pub retry_interval_seconds: u64,
}

fn default_webhook_timeout_seconds() -> u64 {
    5
}
fn default_webhook_max_retries() -> u32 {
    3
}
fn default_webhook_retry_interval_seconds() -> u64 {
    10
}

/// mTLS 配置（v0.6 P1）。LAN 默认关闭，一键开启后节点间连接使用
/// 双向 TLS 认证。内置 CA 一键签发节点证书，支持 24h 轮转。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtlsConfig {
    /// 是否启用 mTLS。缺省 false（LAN 明文），显式 true 后启用双向 TLS。
    #[serde(default)]
    pub enabled: bool,
    /// CA 证书路径（PEM 格式）。缺省自动生成到 data_dir/mtls/ca.pem。
    #[serde(default)]
    pub ca_cert: Option<std::path::PathBuf>,
    /// CA 私钥路径（PEM 格式）。缺省自动生成到 data_dir/mtls/ca.key。
    #[serde(default)]
    pub ca_key: Option<std::path::PathBuf>,
    /// 证书有效期（天），缺省 1 天（24h 轮转）。
    #[serde(default = "default_mtls_cert_ttl_days")]
    pub cert_ttl_days: u32,
    /// 证书轮转间隔（秒），缺省 21600s（6h），提前于 ttl 过期前重新签发。
    #[serde(default = "default_mtls_rotate_interval")]
    pub cert_rotate_interval_seconds: u64,
    /// 节点间 TLS 监听端口。缺省 = `listen_port + 1`（对等 WS 专用端口，
    /// 本地 API/网页仍在 `listen_port` 上走明文 HTTP，二者互不影响）。
    #[serde(default)]
    pub listen_port: Option<u16>,
    /// **强制模式**（v0.6 P1）：开启后拒绝一切明文对等链路——出站只拨 `wss://`
    /// （未广播 mtls 端点/明文静态条目直接拒），入站明文 `/peer/ws`、
    /// `/peer/file/ws` 一律 403。缺省 false（机会式：对端不支持 TLS 则明文）。
    /// 仅当 `enabled=true` 时实际生效；`--require` 需与 `--enable` 同用。
    #[serde(default)]
    pub require: bool,
    /// 是否允许本节点在无 CA 时**新建** CA（即集群「权威」节点）。缺省 true，
    /// 保持既有行为（首节点 `mtls --enable` 建 CA）。成员节点置 false：
    /// 不建 CA，启动后经已认证的对等链路自动领取集群 CA（CA 自动分发）。
    #[serde(default = "default_true")]
    pub create_ca: bool,
    /// 是否允许经已认证（strict 集群令牌）的对等链路**分发/领取** CA。
    /// 缺省 true。关闭后节点只能使用本地已有 CA（需人工分发文件）。
    #[serde(default = "default_true")]
    pub ca_sync: bool,
}

impl Default for MtlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert: None,
            ca_key: None,
            cert_ttl_days: default_mtls_cert_ttl_days(),
            cert_rotate_interval_seconds: default_mtls_rotate_interval(),
            listen_port: None,
            require: false,
            create_ca: true,
            ca_sync: true,
        }
    }
}

impl MtlsConfig {
    /// 强制模式是否实际生效（require 必须以 enabled 为前提）。
    pub fn require_enforced(&self) -> bool {
        self.enabled && self.require
    }
}

fn default_mtls_cert_ttl_days() -> u32 {
    1
}
fn default_mtls_rotate_interval() -> u64 {
    21600
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    /// `text` or `json`.
    #[serde(default = "default_log_format")]
    pub format: String,
    /// Optional log file path; when empty logs go to stdout only.
    #[serde(default)]
    pub file: String,
}

/// Human-visible conversation history (in-memory ring + optional JSONL file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Ring-buffer capacity (most recent N messages kept in memory).
    #[serde(default = "default_history_len")]
    pub len: usize,
    /// Append log under `data_dir`; loaded back into the ring at boot so the
    /// conversation view survives restarts. Empty string disables it.
    #[serde(default = "default_history_file")]
    pub file: String,
    /// Rotate the active file once it exceeds this many MB.
    #[serde(default = "default_history_max_mb")]
    pub max_file_mb: u64,
    /// Also roll the active file over once it is older than this many days
    /// (0 = size-based rollover only). Rolled files move to `history_archive/`
    /// and are never deleted.
    #[serde(default = "default_history_archive_days")]
    pub archive_days: u64,
    /// Keep at most this many archive files; 0 (default) keeps every archive.
    /// Set > 0 only if you deliberately want the node to discard oldest records.
    #[serde(default)]
    pub archive_keep: usize,
}

/// Observer-side payload redaction (v0.5 kernel list §2). Applied to the
/// human/audit views — history ring, `/v1/history*`, `/v1/events` and the
/// web console — never to frames delivered to the target agent. Recipients
/// see originals; observers see `<REDACTED:...>` placeholders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Extra user regexes; every match is replaced with `<REDACTED:custom>`.
    #[serde(default)]
    pub patterns: Vec<String>,
}

impl Default for RedactConfig {
    fn default() -> Self {
        RedactConfig {
            enabled: true,
            patterns: Vec::new(),
        }
    }
}

/// Web UI / browser behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfig {
    /// Open the human-facing page in the default browser on startup.
    #[serde(default = "default_true")]
    pub auto_open: bool,
}

// ---- defaults ---------------------------------------------------------------

fn default_version() -> u32 {
    CURRENT_CONFIG_VERSION
}
fn default_listen_port() -> u16 {
    DEFAULT_LISTEN_PORT
}
fn default_data_dir() -> PathBuf {
    // IMPORTANT: never default inside the node's own directory — releases
    // swap whole folders (AgentLink-vX.Y.Z) and would orphan/overwrite data.
    #[cfg(windows)]
    if let Ok(local) = std::env::var("LOCALAPPDATA") {
        return PathBuf::from(local).join("AgentLink").join("data");
    }
    #[cfg(not(windows))]
    {
        if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
            return PathBuf::from(xdg).join("agentlink");
        }
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home)
                .join(".local")
                .join("share")
                .join("agentlink");
        }
    }
    PathBuf::from("./data") // last-resort fallback
}
fn default_true() -> bool {
    true
}
fn default_discovery_methods() -> Vec<String> {
    vec!["udp_broadcast".to_string()]
}
fn default_broadcast_port() -> u16 {
    DEFAULT_BROADCAST_PORT
}
fn default_announce_interval() -> u64 {
    5
}
fn default_heartbeat_interval() -> u64 {
    10
}
fn default_offline_timeout() -> u64 {
    30
}
fn default_security_mode() -> String {
    "strict".to_string()
}
fn default_placeholder_token() -> String {
    "change-me".to_string()
}
fn default_local_host() -> String {
    // Single listener serves both the local agent API (token gated) and, from
    // M1, the peer WebSocket endpoint. Peers must be able to reach us, so we
    // bind all interfaces by default; local agents still use 127.0.0.1.
    "0.0.0.0".to_string()
}
fn default_max_file_size() -> u64 {
    100 * 1024 * 1024
}
fn default_chunk_size() -> u32 {
    64 * 1024
}
impl FilesConfig {
    /// Effective received-file directory (after `Config::normalize`).
    pub fn recv_dir_resolved(&self) -> &std::path::Path {
        self.recv_dir
            .as_deref()
            .unwrap_or(std::path::Path::new("files"))
    }
}

fn default_one() -> u32 {
    1
}
fn default_ack_timeout() -> u64 {
    5
}
fn default_max_retries() -> u32 {
    5
}
fn default_offline_ttl() -> u64 {
    300
}
fn default_dedup_cache() -> usize {
    2048
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "text".to_string()
}

fn default_history_len() -> usize {
    500
}
fn default_history_file() -> String {
    "history.jsonl".to_string()
}
fn default_history_max_mb() -> u64 {
    // Active file rolls into history_archive/ at 1 GiB; the console warns as
    // it approaches that size. Nothing is ever deleted by default.
    1024
}
fn default_history_archive_days() -> u64 {
    // Periodic rollover so a quiet node still archives regularly.
    7
}
fn default_agent_stale_ttl() -> u64 {
    3600
}

// Mesh 引擎参数默认值（拍板记录第 2/4 条）。
fn default_engine_backoff_secs() -> u64 {
    30
}
fn default_engine_tick_ms() -> u64 {
    200
}
fn default_engine_max_concurrent() -> usize {
    2
}

impl Default for HistoryConfig {
    fn default() -> Self {
        HistoryConfig {
            enabled: true,
            len: default_history_len(),
            file: default_history_file(),
            max_file_mb: default_history_max_mb(),
            archive_days: default_history_archive_days(),
            archive_keep: 0,
        }
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        WebConfig {
            auto_open: default_true(),
        }
    }
}

// ---- sub-config defaults ----------------------------------------------------

impl Default for DiscoveryConfig {
    fn default() -> Self {
        DiscoveryConfig {
            enabled: default_true(),
            methods: default_discovery_methods(),
            broadcast_port: default_broadcast_port(),
            announce_interval_seconds: default_announce_interval(),
            heartbeat_interval_seconds: default_heartbeat_interval(),
            offline_timeout_seconds: default_offline_timeout(),
            static_peers: Vec::new(),
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        SecurityConfig {
            mode: default_security_mode(),
            cluster_token: default_placeholder_token(),
            admission: AdmissionConfig::default(),
            control_auth: false,
            audit: AuditConfig::default(),
        }
    }
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        AdmissionConfig {
            enabled: false,
            bootstrap_token: default_placeholder_token(),
            allowed_hardware: Vec::new(),
        }
    }
}

impl AdmissionConfig {
    /// 准入令牌是否真的被配置过（区分空串与占位符）。
    pub fn boot_token_configured(&self) -> bool {
        !self.bootstrap_token.is_empty() && self.bootstrap_token != default_placeholder_token()
    }

    /// 指纹是否在白名单内。
    pub fn hardware_allowed(&self, hw_fp: &str) -> bool {
        self.allowed_hardware.iter().any(|f| f == hw_fp)
    }
}

impl Default for LocalApiConfig {
    fn default() -> Self {
        LocalApiConfig {
            host: default_local_host(),
            token: default_placeholder_token(),
        }
    }
}

impl Default for FilesConfig {
    fn default() -> Self {
        FilesConfig {
            max_file_size: default_max_file_size(),
            chunk_size: default_chunk_size(),
            recv_dir: None, // -> <data_dir>/files after normalize
            max_concurrent: default_one(),
        }
    }
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        DeliveryConfig {
            ack_timeout_seconds: default_ack_timeout(),
            max_retries: default_max_retries(),
            offline_ttl_seconds: default_offline_ttl(),
            dedup_cache_size: default_dedup_cache(),
            persist: true,
            agent_stale_ttl_seconds: default_agent_stale_ttl(),
            journal_sync_ms: 0,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            level: default_log_level(),
            format: default_log_format(),
            file: String::new(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_retries: 0, // 显式声明才重试（拍板第 4 条）
            backoff_secs: default_engine_backoff_secs(),
            drift_alert_only: true, // drift/declined 只告警（拍板第 4 条）
            tick_ms: default_engine_tick_ms(),
            max_concurrent_per_node: default_engine_max_concurrent(),
            circuit: CircuitConfig::default(),
            warmpool: WarmpoolConfig::default(),
            reclaim: ReclaimConfig::default(),
            slo: crate::slo::SloConfig::default(),
        }
    }
}

// ---- impl -------------------------------------------------------------------

impl Config {
    /// Build a brand new configuration with a fresh identity and tokens.
    pub fn generate(
        name: Option<String>,
        cluster_token: Option<String>,
        mode: Option<String>,
    ) -> Self {
        Config {
            v: CURRENT_CONFIG_VERSION,
            node_id: new_short_id(),
            name: name.unwrap_or_else(default_node_name),
            listen_port: DEFAULT_LISTEN_PORT,
            data_dir: default_data_dir(),
            tags: Vec::new(),
            history: HistoryConfig::default(),
            redact: RedactConfig::default(),
            web: WebConfig::default(),
            lease: LeaseConfig::default(),
            gitops: GitopsConfig::default(),
            relay: RelayConfig::default(),
            discovery: DiscoveryConfig {
                enabled: true,
                methods: default_discovery_methods(),
                broadcast_port: DEFAULT_BROADCAST_PORT,
                announce_interval_seconds: 5,
                heartbeat_interval_seconds: 10,
                offline_timeout_seconds: 30,
                static_peers: Vec::new(),
            },
            security: SecurityConfig {
                mode: mode.unwrap_or_else(default_security_mode),
                cluster_token: cluster_token.unwrap_or_else(new_secret),
                admission: AdmissionConfig::default(),
                control_auth: false,
                audit: AuditConfig::default(),
            },
            local_api: LocalApiConfig {
                host: default_local_host(),
                token: new_secret(),
            },
            files: FilesConfig {
                max_file_size: default_max_file_size(),
                chunk_size: default_chunk_size(),
                recv_dir: None,
                max_concurrent: 1,
            },
            delivery: DeliveryConfig {
                ack_timeout_seconds: 5,
                max_retries: 5,
                offline_ttl_seconds: 300,
                dedup_cache_size: 2048,
                persist: true,
                agent_stale_ttl_seconds: default_agent_stale_ttl(),
                journal_sync_ms: 0,
            },
            log: LogConfig {
                level: "info".to_string(),
                format: "text".to_string(),
                file: String::new(),
            },
            mesh: MeshConfig::default(),
            auth: AuthConfig::default(),
            webhook: WebhookConfig::default(),
            mtls: MtlsConfig::default(),
        }
    }

    /// Load configuration from a JSON file.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let mut cfg: Config = serde_json::from_str(&raw)?;
        cfg.normalize(path);
        cfg.validate()?;
        Ok(cfg)
    }

    /// Resolve data paths so they never depend on the current working
    /// directory (a logon-startup script may launch us from system32):
    /// - relative `data_dir`        -> anchored to the config file's folder
    /// - relative explicit `recv_dir` -> anchored to the config file's folder
    /// - `recv_dir` not set          -> `<data_dir>/files`
    pub fn normalize(&mut self, config_file: &Path) {
        let base = config_file
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        if self.data_dir.is_relative() {
            self.data_dir = base.join(&self.data_dir);
        }
        self.files.recv_dir = Some(match &self.files.recv_dir {
            Some(rel) if rel.is_relative() => base.join(rel),
            Some(abs) => abs.clone(),
            None => self.data_dir.join("files"),
        });
        // Upgrade-friendly default: pre-0.4.12 configs carried an explicit
        // empty `history.file`; persist the conversation view now ("- opts
        // back out, an empty string is treated the same as the default).
        if self.history.file.is_empty() {
            self.history.file = default_history_file();
        }
    }

    /// 节点间 TLS 监听端口（mTLS 开启时使用）：显式配置优先，否则 `listen_port + 1`。
    pub fn mtls_listen_port(&self) -> u16 {
        self.mtls
            .listen_port
            .unwrap_or_else(|| self.listen_port.saturating_add(1))
    }

    /// Persist configuration as pretty JSON.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let raw = serde_json::to_string_pretty(self)?;
        std::fs::write(path, format!("{raw}\n"))?;
        Ok(())
    }

    /// Basic sanity checks.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.node_id.trim().is_empty() {
            anyhow::bail!("node_id must not be empty");
        }
        if self.name.trim().is_empty() {
            anyhow::bail!("name must not be empty");
        }
        if self.listen_port == 0 {
            anyhow::bail!("listen_port must be non-zero");
        }
        if self.listen_port == self.discovery.broadcast_port {
            anyhow::bail!("listen_port and discovery.broadcast_port must differ");
        }
        if self.security.mode != "strict" && self.security.mode != "open" {
            anyhow::bail!("security.mode must be 'strict' or 'open'");
        }
        if self.log.format != "text" && self.log.format != "json" {
            anyhow::bail!("log.format must be 'text' or 'json'");
        }
        Ok(())
    }
}

/// A 12 hex character random identifier, used for `node_id`.
pub fn new_short_id() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    id[..12].to_string()
}

/// A human-friendly secret for tokens: `xxxx-xxxx-xxxx-xxxx`, avoiding
/// confusing characters, so it is easy to copy between machines.
pub fn new_secret() -> String {
    const CHARS: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut s = String::with_capacity(19);
    for (i, b) in bytes.iter().take(16).enumerate() {
        if i % 4 == 0 && i > 0 {
            s.push('-');
        }
        s.push(CHARS[(b % (CHARS.len() as u8)) as usize] as char);
    }
    s
}

fn default_node_name() -> String {
    // 节点名默认用本机局域网 IPv4，直观可辨；也可手动改 name。
    if let Ok(ip) = local_ip_address::local_ip() {
        return ip.to_string();
    }
    let host = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "host".to_string());
    let host = host.split('.').next().unwrap_or("host").to_string();
    format!("agent-{host}")
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 现网 0.5.9 形态的老 agentlink.json：完全没有 `mesh` 段。
    /// 要求反序列化成功且 mesh 全默认（enabled=false = 免费层，零行为变化）。
    #[test]
    fn mesh_section_absent_defaults_off() {
        let legacy = r#"{
            "v": 1,
            "node_id": "a1b2c3d4e5f6",
            "name": "192.168.1.7",
            "listen_port": 8765,
            "data_dir": "C:/Users/x/AppData/Local/AgentLink/data",
            "tags": ["win", "hermes"],
            "history": { "enabled": true, "len": 500, "file": "history.jsonl",
                         "max_file_mb": 1024, "archive_days": 7 },
            "redact": { "enabled": true },
            "web": { "auto_open": false },
            "discovery": { "enabled": true, "methods": ["udp_broadcast"],
                           "broadcast_port": 8766, "static_peers": [] },
            "security": { "mode": "strict", "cluster_token": "change-me" },
            "local_api": { "host": "0.0.0.0", "token": "aaaa-bbbb-cccc-dddd" },
            "files": { "max_file_size": 104857600, "chunk_size": 65536,
                       "recv_dir": "files", "max_concurrent": 1 },
            "delivery": { "ack_timeout_seconds": 5, "max_retries": 5,
                          "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                          "persist": true },
            "log": { "level": "info", "format": "text", "file": "" }
        }"#;
        let cfg: Config = serde_json::from_str(legacy).expect("老配置（无 mesh 段）必须解析成功");
        assert!(!cfg.mesh.enabled);
        assert_eq!(cfg.mesh.license_file, None);
        assert_eq!(cfg.mesh.engine.max_retries, 0);
        assert_eq!(cfg.mesh.engine.backoff_secs, 30);
        assert!(cfg.mesh.engine.drift_alert_only);
        assert_eq!(cfg.mesh.engine.tick_ms, 200);
        assert_eq!(cfg.mesh.engine.max_concurrent_per_node, 2);
        // v 号不动（P1-3 验收：不发配置版本）
        assert_eq!(cfg.v, 1);
        // 老配置没有 mtls 段：必须全默认（关闭，零行为变化）。
        assert!(!cfg.mtls.enabled);
        assert_eq!(cfg.mtls.ca_cert, None);
        assert_eq!(cfg.mtls.cert_ttl_days, 1);
        assert_eq!(cfg.mtls.cert_rotate_interval_seconds, 21600);
        // 老配置没有 admission：必须默认关闭（准入不存在，与 v0.5 行为一致）。
        assert!(!cfg.security.admission.enabled);
        assert!(!cfg.security.admission.boot_token_configured());
        assert!(cfg.security.admission.allowed_hardware.is_empty());
    }

    /// 准入段（v0.6 P1）：缺省关闭；显式配置后 token/白名单语义正确。
    #[test]
    fn admission_section_defaults_and_semantics() {
        // 无 security 段：全默认。
        let absent: Config = serde_json::from_str(r#"{"node_id":"n","name":"h"}"#).unwrap();
        assert!(!absent.security.admission.enabled);

        // 显式启用 + 白名单。
        let cfg: Config = serde_json::from_str(
            r#"{"node_id":"n","name":"h","security":{"mode":"strict",
                "cluster_token":"tok","admission":{"enabled":true,
                "bootstrap_token":"boot-tok","allowed_hardware":["aa11","bb22"]}}}"#,
        )
        .unwrap();
        let adm = &cfg.security.admission;
        assert!(adm.enabled);
        assert!(adm.boot_token_configured());
        assert!(adm.hardware_allowed("aa11"));
        assert!(adm.hardware_allowed("bb22"));
        assert!(!adm.hardware_allowed("cc33"));

        // 只给白名单（token 未配置 → 指纹单因素模式）。
        let fp_only: AdmissionConfig =
            serde_json::from_str(r#"{"enabled":true,"allowed_hardware":["aa11"]}"#).unwrap();
        assert!(!fp_only.boot_token_configured());
        assert!(fp_only.allowed_hardware.len() == 1);

        // token 用占位符等价于未配置。
        let placeholder: AdmissionConfig =
            serde_json::from_str(r#"{"enabled":true,"bootstrap_token":"change-me"}"#).unwrap();
        assert!(!placeholder.boot_token_configured());
    }

    /// Lease 段（v0.6 P1）：缺省关闭（零流量零行为变化），参数 sane 默认。
    #[test]
    fn lease_section_defaults_and_partial_fields() {
        let absent: Config = serde_json::from_str(r#"{"node_id":"n","name":"h"}"#).unwrap();
        assert!(!absent.lease.enabled);
        assert_eq!(absent.lease.interval_secs, 10);
        assert_eq!(absent.lease.cpu_threshold_pct, 5);
        assert_eq!(absent.lease.bare_beat_secs, 60);

        // 显式部分字段 + 覆盖缺省（bare_beat_secs=0 = 禁用裸心跳）。
        let partial: Config = serde_json::from_str(
            r#"{"node_id":"n","name":"h","lease":{"enabled":true,
                "interval_secs":5,"cpu_threshold_pct":3,"bare_beat_secs":0}}"#,
        )
        .unwrap();
        assert!(partial.lease.enabled);
        assert_eq!(partial.lease.interval_secs, 5);
        assert_eq!(partial.lease.cpu_threshold_pct, 3);
        assert_eq!(partial.lease.bare_beat_secs, 0);
    }

    /// mTLS 段缺省/部分字段：默认关闭，逐项回落到默认值。
    #[test]
    fn mtls_section_defaults_and_partial_fields() {
        // 完全没有 mtls 段。
        let absent: Config = serde_json::from_str(r#"{"node_id":"n","name":"h"}"#).unwrap();
        assert!(!absent.mtls.enabled);
        assert_eq!(absent.mtls.cert_ttl_days, 1);
        assert_eq!(absent.mtls.cert_rotate_interval_seconds, 21600);

        // 只写 enabled，其余回落默认。
        let partial: Config =
            serde_json::from_str(r#"{"node_id":"n","name":"h","mtls":{"enabled":true}}"#).unwrap();
        assert!(partial.mtls.enabled);
        assert_eq!(partial.mtls.cert_ttl_days, 1);
        assert_eq!(partial.mtls.cert_rotate_interval_seconds, 21600);

        // 显式自定义 ttl/窗口 + 自定义 CA 路径。
        let custom: Config = serde_json::from_str(
            r#"{"node_id":"n","name":"h","mtls":{"ca_cert":"k/root.pem","ca_key":"k/root.key",
                "cert_ttl_days":2,"cert_rotate_interval_seconds":3600}}"#,
        )
        .unwrap();
        assert!(!custom.mtls.enabled);
        assert_eq!(custom.mtls.cert_ttl_days, 2);
        assert_eq!(custom.mtls.cert_rotate_interval_seconds, 3600);
        assert_eq!(custom.mtls.ca_cert, Some(PathBuf::from("k/root.pem")));
        assert_eq!(custom.mtls.ca_key, Some(PathBuf::from("k/root.key")));

        // 默认构造（Config::generate 走这里）与 serde 默认一致。
        let d = MtlsConfig::default();
        assert!(!d.enabled);
        assert_eq!(d.cert_ttl_days, 1);
        assert_eq!(d.cert_rotate_interval_seconds, 21600);
        assert_eq!(d.listen_port, None);
        // require 缺省 false：保持机会式（零行为变化）。
        assert!(!d.require);
        assert!(!d.require_enforced());
        // require 只有在 enabled 时才实际生效。
        let pending = MtlsConfig {
            require: true,
            ..MtlsConfig::default()
        };
        assert!(pending.require);
        assert!(!pending.require_enforced());
        let enforced = MtlsConfig {
            enabled: true,
            require: true,
            ..MtlsConfig::default()
        };
        assert!(enforced.require_enforced());
        // CA 自动分发：缺省「可建 CA + 允许同步」，保持既有行为。
        assert!(d.create_ca);
        assert!(d.ca_sync);
        // 老配置（无这两个字段）也回落到默认 true（向后兼容）。
        assert!(absent.mtls.create_ca);
        assert!(absent.mtls.ca_sync);

        // 成员节点：不建 CA，仅领取。
        let member: Config = serde_json::from_str(
            r#"{"node_id":"n","name":"h","mtls":{"enabled":true,"create_ca":false,"ca_sync":true}}"#,
        )
        .unwrap();
        assert!(member.mtls.enabled);
        assert!(!member.mtls.create_ca);

        // 监听端口：缺省 = listen_port + 1；显式配置优先。
        let mut c = Config::generate(None, None, None);
        c.listen_port = 8765;
        assert_eq!(c.mtls_listen_port(), 8766);
        c.mtls.listen_port = Some(9000);
        assert_eq!(c.mtls_listen_port(), 9000);
    }

    /// mesh 段只写一部分字段：逐项缺省值必须生效（含 mesh 有段但 engine 缺、
    /// engine 只写一个字段的两种形态）。
    #[test]
    fn mesh_partial_fields_fall_back_to_defaults() {
        // 形态 A：有 mesh.enabled，无 license_file / engine。
        let a: Config =
            serde_json::from_str(r#"{"node_id":"n","name":"h","mesh":{"enabled":true}}"#).unwrap();
        assert!(a.mesh.enabled);
        assert_eq!(a.mesh.license_file, None);
        assert_eq!(a.mesh.engine.max_retries, 0);
        assert_eq!(a.mesh.engine.backoff_secs, 30);
        assert!(a.mesh.engine.drift_alert_only);
        assert_eq!(a.mesh.engine.tick_ms, 200);
        assert_eq!(a.mesh.engine.max_concurrent_per_node, 2);

        // 形态 B：engine 只显式声明 max_retries 与 backoff_secs（拍板：显式声明才重试）。
        let b: Config = serde_json::from_str(
            r#"{"node_id":"n","name":"h","mesh":{"engine":{"max_retries":3,"backoff_secs":60}}}"#,
        )
        .unwrap();
        assert!(!b.mesh.enabled); // 没写 enabled 仍是免费层
        assert_eq!(b.mesh.engine.max_retries, 3);
        assert_eq!(b.mesh.engine.backoff_secs, 60);
        assert!(b.mesh.engine.drift_alert_only);
        assert_eq!(b.mesh.engine.tick_ms, 200);

        // 形态 C：license_file 显式给出。
        let c: Config = serde_json::from_str(
            r#"{"node_id":"n","name":"h","mesh":{"enabled":true,"license_file":"license.toml"}}"#,
        )
        .unwrap();
        assert_eq!(c.mesh.license_file, Some(PathBuf::from("license.toml")));
    }

    /// 非法值必须拒解（不能静默吞掉）：enabled 写成字符串、engine 字段类型错、
    /// max_concurrent_per_node 负数，三种都要报错。
    #[test]
    fn mesh_invalid_values_are_rejected() {
        let e1 = serde_json::from_str::<Config>(
            r#"{"node_id":"n","name":"h","mesh":{"enabled":"yes"}}"#,
        );
        assert!(e1.is_err(), "enabled 非布尔必须拒解");
        let e2 = serde_json::from_str::<Config>(
            r#"{"node_id":"n","name":"h","mesh":{"engine":{"tick_ms":"fast"}}}"#,
        );
        assert!(e2.is_err(), "tick_ms 非数字必须拒解");
        let e3 = serde_json::from_str::<Config>(
            r#"{"node_id":"n","name":"h","mesh":{"engine":{"max_concurrent_per_node":-1}}}"#,
        );
        assert!(e3.is_err(), "usize 负数必须拒解");
    }

    /// generate() 出的新配置默认就在免费层，且 save/load 往返一致。
    #[test]
    fn mesh_roundtrip_and_generate_defaults() {
        let cfg = Config::generate(Some("t".into()), Some("tok".into()), Some("strict".into()));
        assert!(!cfg.mesh.enabled);
        let dir = std::env::temp_dir().join(format!("agentlink-cfg-test-{}", new_short_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("agentlink.json");
        let mut cfg2 = cfg.clone();
        cfg2.mesh.enabled = true;
        cfg2.mesh.engine.max_retries = 2;
        cfg2.save(&path).unwrap();
        let back = Config::load(&path).unwrap();
        assert!(back.mesh.enabled);
        assert_eq!(back.mesh.engine.max_retries, 2);
        assert_eq!(back.mesh.engine.backoff_secs, 30);
        std::fs::remove_dir_all(&dir).ok();
    }
}
