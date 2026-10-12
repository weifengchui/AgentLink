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
    #[serde(default = "default_recv_dir")]
    pub recv_dir: PathBuf,
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

// ---- defaults ---------------------------------------------------------------

fn default_version() -> u32 {
    CURRENT_CONFIG_VERSION
}
fn default_listen_port() -> u16 {
    DEFAULT_LISTEN_PORT
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
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
fn default_recv_dir() -> PathBuf {
    PathBuf::from("./data/files")
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
            recv_dir: default_recv_dir(),
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
                recv_dir: default_recv_dir(),
                max_concurrent: 1,
            },
            delivery: DeliveryConfig {
                ack_timeout_seconds: 5,
                max_retries: 5,
                offline_ttl_seconds: 300,
                dedup_cache_size: 2048,
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
        let cfg: Config = serde_json::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
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
    let host = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "host".to_string());
    let host = host.split('.').next().unwrap_or("host").to_string();
    format!("agent-{host}")
}
