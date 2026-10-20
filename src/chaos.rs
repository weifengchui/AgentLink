//! v0.7 chaos 第二波（§6.4）：进程内故障注入（latency / disk_full）。
//!
//! 设计约束：
//! - **默认关闭**（`mesh.chaos.enabled=false`），关闭态查询/注入零行为；
//! - 只允许 Admin 经 `POST /v1/mesh/chaos` 注入，`DELETE` 提前解除；
//! - 每个故障带 TTL（上限可配，缺省 60s），到期自动失效——不会永久留在
//!   生产路径上；
//! - `latency`：`Router::send` 在出口前 sleep，模拟网络/总线延迟；
//! - `disk_full`：`TaskStore::append` 直接返回 IO 错误（模拟 ENOSPC），
//!   不实际写满磁盘；用于验证 journal 失败回滚、API 500、恢复后继续。
//!
//! 运行态不持久化：重启即清空。审计由 API 层写 `chaos.start`/`chaos.stop`。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// 可注入的故障类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChaosKind {
    Latency,
    DiskFull,
}

impl ChaosKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ChaosKind::Latency => "latency",
            ChaosKind::DiskFull => "disk_full",
        }
    }
}

/// `mesh.chaos` 配置。缺省关闭；上限约束防止误操作造成长时间不可用。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 单次注入允许的最长持续时间（秒，1-3600，缺省 60）。
    #[serde(default = "default_max_duration")]
    pub max_duration_seconds: u64,
    /// latency 允许的最大毫秒数（1-60000，缺省 5000）。
    #[serde(default = "default_max_latency")]
    pub max_latency_ms: u64,
}

fn default_max_duration() -> u64 {
    60
}

fn default_max_latency() -> u64 {
    5000
}

impl Default for ChaosConfig {
    fn default() -> Self {
        ChaosConfig {
            enabled: false,
            max_duration_seconds: default_max_duration(),
            max_latency_ms: default_max_latency(),
        }
    }
}

/// 当前生效的故障（查询面用，时间已折算为剩余秒数）。
#[derive(Debug, Clone, Serialize)]
pub struct ActiveFault {
    pub kind: &'static str,
    pub remaining_seconds: u64,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct Active {
    until: Instant,
    latency_ms: u64,
}

/// 进程内故障表（每类最多一个，重复注入覆盖旧值）。
#[derive(Debug, Default)]
pub struct Chaos {
    faults: Mutex<HashMap<ChaosKind, Active>>,
}

impl Chaos {
    /// 注入/覆盖一个故障；`duration` 由调用方按配置上限校验。
    pub fn start(&self, kind: ChaosKind, duration: Duration, latency_ms: u64) {
        let until = Instant::now() + duration;
        let mut g = self.faults.lock().unwrap();
        g.insert(kind, Active { until, latency_ms });
    }

    /// 解除全部故障。
    pub fn clear(&self) {
        self.faults.lock().unwrap().clear();
    }

    fn active(&self, kind: ChaosKind) -> Option<Active> {
        let mut g = self.faults.lock().unwrap();
        let now = Instant::now();
        match g.get(&kind).copied() {
            Some(a) if a.until > now => Some(a),
            Some(_) => {
                g.remove(&kind);
                None
            }
            None => None,
        }
    }

    /// `Router::send` 出口延迟（毫秒；0=无）。
    pub fn latency_ms(&self) -> u64 {
        self.active(ChaosKind::Latency)
            .map(|a| a.latency_ms)
            .unwrap_or(0)
    }

    /// `TaskStore::append` 是否模拟 ENOSPC。
    pub fn disk_full(&self) -> bool {
        self.active(ChaosKind::DiskFull).is_some()
    }

    /// 查询当前生效故障（自动清理过期项）。
    pub fn snapshot(&self) -> Vec<ActiveFault> {
        let now = Instant::now();
        let mut g = self.faults.lock().unwrap();
        g.retain(|_, a| a.until > now);
        let mut v: Vec<ActiveFault> = g
            .iter()
            .map(|(k, a)| ActiveFault {
                kind: k.as_str(),
                remaining_seconds: a.until.saturating_duration_since(now).as_secs(),
                latency_ms: a.latency_ms,
            })
            .collect();
        v.sort_by_key(|a| a.kind);
        v
    }
}

/// 配置校验（`Config::validate` 调用）。
pub fn validate(cfg: &ChaosConfig) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    if !(1..=3600).contains(&cfg.max_duration_seconds) {
        anyhow::bail!("mesh.chaos.max_duration_seconds must be 1-3600");
    }
    if !(1..=60000).contains(&cfg.max_latency_ms) {
        anyhow::bail!("mesh.chaos.max_latency_ms must be 1-60000");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_and_disk_full_expire() {
        let c = Chaos::default();
        assert_eq!(c.latency_ms(), 0);
        assert!(!c.disk_full());
        c.start(ChaosKind::Latency, Duration::from_millis(60), 120);
        c.start(ChaosKind::DiskFull, Duration::from_millis(60), 0);
        assert_eq!(c.latency_ms(), 120);
        assert!(c.disk_full());
        assert_eq!(c.snapshot().len(), 2);
        std::thread::sleep(Duration::from_millis(90));
        assert_eq!(c.latency_ms(), 0);
        assert!(!c.disk_full());
        assert!(c.snapshot().is_empty(), "过期项自动清理");
    }

    #[test]
    fn clear_and_overwrite() {
        let c = Chaos::default();
        c.start(ChaosKind::Latency, Duration::from_secs(30), 100);
        c.start(ChaosKind::Latency, Duration::from_secs(30), 300);
        assert_eq!(c.latency_ms(), 300, "重复注入覆盖旧值");
        c.clear();
        assert_eq!(c.latency_ms(), 0);
        assert!(c.snapshot().is_empty());
    }

    #[test]
    fn validate_bounds_and_disabled() {
        let mut c = ChaosConfig::default();
        assert!(validate(&c).is_ok(), "关闭态不校验");
        c.enabled = true;
        c.max_duration_seconds = 0;
        assert!(validate(&c).is_err());
        c.max_duration_seconds = 60;
        c.max_latency_ms = 0;
        assert!(validate(&c).is_err());
        c.max_latency_ms = 5000;
        assert!(validate(&c).is_ok());
    }
}
