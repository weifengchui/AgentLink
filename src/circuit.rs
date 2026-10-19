//! 熔断隔离（v0.6 P1，设计案 §4.5 taint 等价物）：异常节点自动置
//! 「unschedulable」标签，冷却后自动恢复调度资格。
//!
//! 归账口径：`task_failed` 帧按**执行节点**（from_agent 所在节点 /
//! envelope from）归账；引擎自身重试（from_agent="engine"）不计入——
//! 否则引擎重投失败会错误熔断本节点。
//!
//! 触发：`window_secs` 滑动窗口内失败次数 ≥ `failures` → 该节点置
//! unschedulable `cooldown_secs` 秒（窗口内同类计数清零，避免二次即刻重熔断）。
//! 效果：dispatch 选点直接跳过熔断节点（比 exclude_tags 更硬——对任务
//! 规则不可见，异常机不接新活）。到期由「读取时惰性过期」自动恢复。
//!
//! 兼容性：缺省 `engine.circuit.enabled=false`——不记账、不过滤，
//! 调度行为与关闭态逐字节一致；熔断状态纯内存（重启即清，与批次护栏同口径）。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::CircuitConfig;

/// 单节点的熔断记账条目。
#[derive(Debug, Clone)]
struct NodeTaint {
    /// 熔断到期时刻（`None` = 观察中、未触发）。
    until: Option<Instant>,
    /// 触发原因（首个超阈值时的记录）。
    reason: String,
    /// 窗口内失败计数。
    failures: u32,
    /// 计数窗口起点（滑动以首失败锚定，超窗整体清零重计）。
    window_start: Instant,
}

/// 熔断运行态（每 mesh 引擎一份，进程内）。
#[derive(Default)]
pub struct Circuit {
    entries: HashMap<String, NodeTaint>,
}

/// 熔断记账结果（供日志/单测断言）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureVerdict {
    /// 计入但未达阈值（返回当前窗口计数）。
    Counted(u32),
    /// 达到阈值 → 新置 unschedulable（返回冷却秒数）。
    Tainted(u64),
    /// 已在熔断中（本次失败不再累计）。
    AlreadyTainted,
    /// 熔断未启用（不记账）。
    Disabled,
}

impl Circuit {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记一次失败。返回判定结果（见 [`FailureVerdict`]）。
    pub fn record_failure(
        &mut self,
        cfg: &CircuitConfig,
        node: &str,
        now: Instant,
    ) -> FailureVerdict {
        if !cfg.enabled {
            return FailureVerdict::Disabled;
        }
        let window = Duration::from_secs(cfg.window_secs.max(1));
        let cooldown = Duration::from_secs(cfg.cooldown_secs);
        let entry = self
            .entries
            .entry(node.to_string())
            .or_insert_with(|| NodeTaint {
                until: None,
                reason: String::new(),
                failures: 0,
                window_start: now,
            });
        if let Some(until) = entry.until {
            if until > now {
                return FailureVerdict::AlreadyTainted;
            }
            // 冷却已过：到期 由读取侧惰性清理，这里顺带重置窗口重计。
            entry.until = None;
            entry.failures = 0;
            entry.window_start = now;
        }
        if now.duration_since(entry.window_start) > window {
            // 滑窗重置：旧窗口的失败不再追责。
            entry.failures = 0;
            entry.window_start = now;
        }
        entry.failures += 1;
        if entry.failures >= cfg.failures.max(1) {
            entry.until = Some(now + cooldown);
            entry.reason = format!(
                "熔断隔离：{} 秒窗口内失败 {} 次（阈值 {}）",
                cfg.window_secs, entry.failures, cfg.failures
            );
            return FailureVerdict::Tainted(cfg.cooldown_secs);
        }
        FailureVerdict::Counted(entry.failures)
    }

    /// 节点当前是否被熔断（读取时惰性过期并清理）。
    pub fn is_tainted(&mut self, node: &str, now: Instant) -> bool {
        match self.entries.get(node) {
            Some(t) if t.until.is_some() => {
                if t.until.unwrap() > now {
                    true
                } else {
                    // 过期即恢复（保留条目壳会导致 map 只增不减——直接移除）。
                    self.entries.remove(node);
                    false
                }
            }
            Some(_) => false, // 观察中、未触发
            None => false,
        }
    }

    /// 当前仍在熔断中的节点视图（API/CLI 用；读取时惰性过期）。
    pub fn active(&mut self, now: Instant) -> Vec<TaintView> {
        let expired: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, t)| t.until.is_some_and(|u| u <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for node in expired {
            self.entries.remove(&node);
        }
        let mut out: Vec<TaintView> = self
            .entries
            .iter()
            .filter_map(|(node, t)| {
                t.until.filter(|u| *u > now).map(|until| TaintView {
                    node: node.clone(),
                    reason: t.reason.clone(),
                    failures: t.failures,
                    remaining_secs: until.duration_since(now).as_secs().max(1),
                })
            })
            .collect();
        out.sort_by(|a, b| a.node.cmp(&b.node));
        out
    }

    /// 观察中的节点数（测试/诊断用）。
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// `/v1/mesh/status` 里的熔断视图（v0.6 P1）。
#[derive(Debug, Clone, Serialize)]
pub struct TaintView {
    pub node: String,
    pub reason: String,
    pub failures: u32,
    pub remaining_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CircuitConfig {
        CircuitConfig {
            enabled: true,
            failures: 3,
            window_secs: 600,
            cooldown_secs: 300,
        }
    }

    fn t(offset_ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(offset_ms)
    }

    #[test]
    fn counts_then_taints_at_threshold() {
        let c = cfg();
        let mut circuit = Circuit::new();
        let t0 = t(0);
        assert_eq!(
            circuit.record_failure(&c, "n2", t0),
            FailureVerdict::Counted(1)
        );
        assert_eq!(
            circuit.record_failure(&c, "n2", t(1500)),
            FailureVerdict::Counted(2)
        );
        assert!(!circuit.is_tainted("n2", t(2000)));
        assert_eq!(
            circuit.record_failure(&c, "n2", t(2000)),
            FailureVerdict::Tainted(300)
        );
        assert!(circuit.is_tainted("n2", t(3000)));
        // 熔断中再失败：不累计（AlreadyTainted）。
        assert_eq!(
            circuit.record_failure(&c, "n2", t(4000)),
            FailureVerdict::AlreadyTainted
        );
        // 其它节点不受牵连。
        assert_eq!(
            circuit.record_failure(&c, "n3", t(4100)),
            FailureVerdict::Counted(1)
        );
        assert!(!circuit.is_tainted("n3", t(4200)));
    }

    #[test]
    fn expiry_auto_recovers() {
        let c = cfg();
        let mut circuit = Circuit::new();
        let t0 = t(0);
        circuit.record_failure(&c, "n2", t0);
        circuit.record_failure(&c, "n2", t0);
        circuit.record_failure(&c, "n2", t0);
        assert!(circuit.is_tainted("n2", t0));
        // 299.5s 后仍在熔断。
        assert!(circuit.is_tainted("n2", t(299_500)));
        // 300s 过后自动恢复。
        assert!(!circuit.is_tainted("n2", t(301_000)));
        // 恢复后窗口从零重计。
        assert_eq!(
            circuit.record_failure(&c, "n2", t(301_500)),
            FailureVerdict::Counted(1)
        );
    }

    #[test]
    fn window_resets_after_gap() {
        let c = cfg();
        let mut circuit = Circuit::new();
        let t0 = t(0);
        circuit.record_failure(&c, "n2", t0);
        circuit.record_failure(&c, "n2", t0);
        assert_eq!(
            circuit.record_failure(&c, "n2", t0),
            FailureVerdict::Tainted(300)
        );
        // 超过窗口（600s）后：冷却早已过期、旧窗口计数清零，重新从 1 计。
        let after_window = t(601_000);
        assert_eq!(
            circuit.record_failure(&c, "n2", after_window),
            FailureVerdict::Counted(1)
        );
        assert!(!circuit.is_tainted("n2", after_window));
    }

    #[test]
    fn disabled_never_records_or_blocks() {
        let mut c = cfg();
        c.enabled = false;
        let mut circuit = Circuit::new();
        for _ in 0..10 {
            let v = circuit.record_failure(&c, "n2", t(0));
            assert_eq!(v, FailureVerdict::Disabled);
        }
        assert!(!circuit.is_tainted("n2", t(1000)));
        assert_eq!(circuit.len(), 0);
    }

    #[test]
    fn active_view_lists_and_sorts() {
        let c = cfg();
        let mut circuit = Circuit::new();
        let t0 = t(0);
        for (i, node) in ["n3", "n2", "n1"].iter().enumerate() {
            let base = t((10_000 * i) as u64);
            circuit.record_failure(&c, node, base);
            circuit.record_failure(&c, node, base);
            circuit.record_failure(&c, node, base);
        }
        let view = circuit.active(t(11_000));
        assert_eq!(view.len(), 3);
        // node 字典序 + 剩余秒数非零。
        assert_eq!(
            view.iter().map(|v| v.node.as_str()).collect::<Vec<_>>(),
            vec!["n1", "n2", "n3"]
        );
        assert!(view
            .iter()
            .all(|v| v.remaining_secs > 0 && !v.reason.is_empty()));
        // 冷却结束 → 视为空（active 不再返回）。
        assert!(circuit.active(t(400_000)).is_empty());
    }

    #[test]
    fn threshold_floor_is_one() {
        let mut c = cfg();
        c.failures = 0; // 有效值取 1（防 0 阈值双触发歧义）
        let mut circuit = Circuit::new();
        assert_eq!(
            circuit.record_failure(&c, "n2", t(0)),
            FailureVerdict::Tainted(c.cooldown_secs)
        );
    }
}
