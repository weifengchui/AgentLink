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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalApiConfig {
    #[serde(default = "default_local_host")]
    pub host: String,
    #[serde(default = "default_placeholder_token")]
    pub token: String,
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
        }
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
            web: WebConfig::default(),
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
            },
            log: LogConfig {
                level: "info".to_string(),
                format: "text".to_string(),
                file: String::new(),
            },
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
