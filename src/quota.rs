//! per-namespace 资源配额（v0.7 §5.4）：准入 open/in-flight 上限 + 提交速率。
//!
//! 语义（全部 `0` = 不限制；`enabled=false` 时整段忽略）：
//! - `max_open`：该 namespace 的活跃任务上限（pending/running/cancelling）；
//! - `max_inflight`：该 namespace 同时占用的执行额度（running + 已分配
//!   未确认的 pending），引擎派发时按 namespace 截流；
//! - `max_submit_per_minute`：60 秒滑动窗口内的提交尝试上限（含被 open 上限
//!   拒绝的请求——限流保护的是入口压力）。
//!
//! 进程内计数：重启后速率窗口清空；任务计数来自引擎表（journal 重建），
//! 因此重启不会丢 open/in-flight 账。多引擎节点各自独立记账——namespace
//! 配额是**节点本地准入**，不是全局分布式配额（见文档残留）。

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

/// `mesh.quota` 配置。缺省关闭且全 0，行为零变化。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 未在 `namespaces` 中列出的 namespace 使用的默认上限（0=不限制）。
    #[serde(default)]
    pub default_max_open: usize,
    #[serde(default)]
    pub default_max_inflight: usize,
    #[serde(default)]
    pub default_max_submit_per_minute: usize,
    /// 按 namespace 覆盖默认值；字段缺省 = 继承对应 default。
    #[serde(default)]
    pub namespaces: BTreeMap<String, NamespaceQuota>,
}

/// 单个 namespace 的覆盖项（`None` = 继承 `default_*`）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceQuota {
    #[serde(default)]
    pub max_open: Option<usize>,
    #[serde(default)]
    pub max_inflight: Option<usize>,
    #[serde(default)]
    pub max_submit_per_minute: Option<usize>,
}

impl QuotaConfig {
    pub fn max_open(&self, namespace: &str) -> usize {
        if !self.enabled {
            return 0;
        }
        self.namespaces
            .get(namespace)
            .and_then(|q| q.max_open)
            .unwrap_or(self.default_max_open)
    }

    pub fn max_inflight(&self, namespace: &str) -> usize {
        if !self.enabled {
            return 0;
        }
        self.namespaces
            .get(namespace)
            .and_then(|q| q.max_inflight)
            .unwrap_or(self.default_max_inflight)
    }

    pub fn max_submit_per_minute(&self, namespace: &str) -> usize {
        if !self.enabled {
            return 0;
        }
        self.namespaces
            .get(namespace)
            .and_then(|q| q.max_submit_per_minute)
            .unwrap_or(self.default_max_submit_per_minute)
    }

    /// 已知 namespace 的并集（配置里列出的 + 调用方传入的），用于查询面。
    pub fn known_namespaces(&self, extra: impl IntoIterator<Item = String>) -> Vec<String> {
        let mut set: std::collections::BTreeSet<String> = self.namespaces.keys().cloned().collect();
        set.extend(extra);
        set.into_iter().collect()
    }
}

/// 60 秒滑动窗口提交限流器（进程内）。
#[derive(Debug, Default)]
pub struct RateLimiter {
    windows: HashMap<String, VecDeque<Instant>>,
}

impl RateLimiter {
    const WINDOW: Duration = Duration::from_secs(60);

    fn purge(&mut self, namespace: &str, now: Instant) -> &mut VecDeque<Instant> {
        let w = self.windows.entry(namespace.to_string()).or_default();
        while let Some(front) = w.front() {
            if now.saturating_duration_since(*front) >= Self::WINDOW {
                w.pop_front();
            } else {
                break;
            }
        }
        w
    }

    /// 尝试记账一次提交：`limit=0` 不限制。允许时记录并返回 true。
    pub fn allow(&mut self, namespace: &str, limit: usize, now: Instant) -> bool {
        if limit == 0 {
            return true;
        }
        let w = self.purge(namespace, now);
        if w.len() >= limit {
            return false;
        }
        w.push_back(now);
        true
    }

    /// 最近 60 秒内的提交尝试数（观测面用，不记账）。
    pub fn recent(&mut self, namespace: &str, now: Instant) -> usize {
        self.purge(namespace, now).len()
    }
}

/// 配置校验（`Config::validate` 调用）。
pub fn validate(cfg: &QuotaConfig) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    for ns in cfg.namespaces.keys() {
        if ns.is_empty()
            || ns.len() > 64
            || ns == "*"
            || !ns
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            anyhow::bail!(
                "mesh.quota.namespaces key {ns:?} must be 1-64 chars of [A-Za-z0-9_.-] (no '*')"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> QuotaConfig {
        let mut namespaces = BTreeMap::new();
        namespaces.insert(
            "team-a".to_string(),
            NamespaceQuota {
                max_open: Some(2),
                max_inflight: Some(1),
                max_submit_per_minute: None,
            },
        );
        QuotaConfig {
            enabled: true,
            default_max_open: 10,
            default_max_inflight: 5,
            default_max_submit_per_minute: 3,
            namespaces,
        }
    }

    #[test]
    fn namespace_overrides_inherit_and_defaults() {
        let c = cfg();
        assert_eq!(c.max_open("team-a"), 2);
        assert_eq!(c.max_inflight("team-a"), 1);
        assert_eq!(c.max_submit_per_minute("team-a"), 3); // 继承 default
        assert_eq!(c.max_open("team-b"), 10);
        assert_eq!(c.max_inflight("team-b"), 5);
        assert_eq!(c.max_submit_per_minute("team-b"), 3);
        let mut off = c.clone();
        off.enabled = false;
        assert_eq!(off.max_open("team-a"), 0);
        assert_eq!(off.max_inflight("team-a"), 0);
        assert_eq!(off.max_submit_per_minute("team-a"), 0);
    }

    #[test]
    fn rate_limiter_is_a_sliding_minute() {
        let mut r = RateLimiter::default();
        let t0 = Instant::now();
        assert!(r.allow("n", 2, t0));
        assert!(r.allow("n", 2, t0 + Duration::from_secs(10)));
        assert!(!r.allow("n", 2, t0 + Duration::from_secs(20)));
        // 窗口外的第一次记账过期后重新放行（10s/70s 仍在窗口内，20s/80s 过期）。
        assert!(r.allow("n", 2, t0 + Duration::from_secs(61)));
        assert_eq!(r.recent("n", t0 + Duration::from_secs(61)), 2);
        // 其它 namespace 独立。
        assert!(r.allow("m", 1, t0 + Duration::from_secs(61)));
        assert!(!r.allow("m", 1, t0 + Duration::from_secs(62)));
    }

    #[test]
    fn zero_limit_is_unlimited() {
        let mut r = RateLimiter::default();
        let t0 = Instant::now();
        for _ in 0..100 {
            assert!(r.allow("n", 0, t0));
        }
        assert_eq!(r.recent("n", t0), 0, "0=不记账");
    }

    #[test]
    fn validate_rejects_bad_namespace_and_accepts_off() {
        let mut c = cfg();
        assert!(validate(&c).is_ok());
        c.namespaces.insert(
            "*".to_string(),
            NamespaceQuota {
                max_open: Some(1),
                ..Default::default()
            },
        );
        assert!(validate(&c).is_err());
        c.enabled = false;
        assert!(validate(&c).is_ok(), "关闭态不校验（零行为）");
    }
}
