//! v0.7 SLO 告警 + 历史基线异常检测（路线图 §7.2）。
//!
//! 纯观测：不改变调度/投递语义。`mesh.engine.slo.enabled=false`（缺省）时
//! 零采集、零评估、零告警，`/v1/mesh/slo` 返回 `enabled=false` 的静态视图。
//!
//! 口径：
//! - 样本 = 引擎观测到终态的**非预热池**任务，耗时为「首次入表/派发 → 终态」；
//! - 窗口 = `window_seconds` 内的样本；`p50/p95/p99/avg` 用 nearest-rank；
//! - 错误率 = failed + declined + expired / 样本数（cancelled 不算错误）；
//! - 基线 = 全部历史样本的慢速 EWMA（alpha=0.01，跨重启持久化到
//!   `<data_dir>/slo_baseline.json`），漂移 = 窗口 p50 / 基线 > ratio（+30% 类）；
//! - 告警按 kind 冷却（`cooldown_seconds`），经 `router.notify("slo_alert")`
//!   外发，同时保留最近 50 条供 `/v1/mesh/slo` 查看。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// 窗口内最多保留的样本数（防止长窗口 + 高吞吐无界增长）。
pub const MAX_SAMPLES: usize = 4096;
/// 告警历史保留条数。
pub const MAX_ALERTS: usize = 50;
/// 基线 EWMA 平滑系数：越小越「历史」，对持续回归的敏感性越持久。
pub const BASELINE_ALPHA: f64 = 0.01;
/// 基线落盘节流（秒）。
const SAVE_INTERVAL_SECS: u64 = 60;
/// first_seen 观察表的硬上限（未终态的僵尸任务防御）。
const MAX_FIRST_SEEN: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloConfig {
    /// 缺省关闭——不采集、不评估、不告警。
    #[serde(default)]
    pub enabled: bool,
    /// 统计窗口（秒）。
    #[serde(default = "default_window_seconds")]
    pub window_seconds: u64,
    /// 窗口内最少样本数（不足只展示、不判阈值/漂移）。
    #[serde(default = "default_min_samples")]
    pub min_samples: usize,
    /// 任务耗时 P99 上限（毫秒）。
    #[serde(default = "default_p99_ms")]
    pub p99_ms: u64,
    /// 错误率上限（0.0~1.0）。
    #[serde(default = "default_error_rate")]
    pub error_rate: f64,
    /// 待调度队列深度上限。
    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,
    /// 历史基线漂移倍数（1.3 = 耗时 +30%）。
    #[serde(default = "default_baseline_ratio")]
    pub baseline_ratio: f64,
    /// 同类告警冷却（秒）。
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,
}

impl Default for SloConfig {
    fn default() -> Self {
        SloConfig {
            enabled: false,
            window_seconds: default_window_seconds(),
            min_samples: default_min_samples(),
            p99_ms: default_p99_ms(),
            error_rate: default_error_rate(),
            queue_depth: default_queue_depth(),
            baseline_ratio: default_baseline_ratio(),
            cooldown_seconds: default_cooldown_seconds(),
        }
    }
}

fn default_window_seconds() -> u64 {
    300
}
fn default_min_samples() -> usize {
    20
}
fn default_p99_ms() -> u64 {
    5000
}
fn default_error_rate() -> f64 {
    0.05
}
fn default_queue_depth() -> usize {
    500
}
fn default_baseline_ratio() -> f64 {
    1.3
}
fn default_cooldown_seconds() -> u64 {
    60
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    ts: i64,
    duration_ms: f64,
    ok: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Alert {
    pub kind: String,
    pub value: f64,
    pub threshold: f64,
    pub at: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BaselineFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    baseline_ms: Option<f64>,
    #[serde(default)]
    samples: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WindowStats {
    pub n: u64,
    pub errors: u64,
    pub error_rate: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub avg_ms: f64,
}

pub struct SloState {
    enabled: bool,
    samples: VecDeque<Sample>,
    /// 任务 id → 首次入表/派发时刻（单调钟，进程内）。
    first_seen: HashMap<String, Instant>,
    baseline: BaselineFile,
    path: Option<PathBuf>,
    alerts: VecDeque<Alert>,
    last_fire: HashMap<String, Instant>,
    last_save: Option<Instant>,
}

impl SloState {
    pub fn new(path: Option<PathBuf>, enabled: bool) -> Self {
        let baseline = path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|s| serde_json::from_str::<BaselineFile>(&s).ok())
            .unwrap_or_default();
        Self {
            enabled,
            samples: VecDeque::new(),
            first_seen: HashMap::new(),
            baseline,
            path,
            alerts: VecDeque::new(),
            last_fire: HashMap::new(),
            last_save: None,
        }
    }

    /// 测试用：切换采集开关（运行期只由装配期配置决定）。
    #[cfg(test)]
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// 任务首次入表/派发（重复调用幂等）。预热池任务由调用方跳过。
    pub fn touch(&mut self, task_id: &str) {
        if !self.enabled {
            return;
        }
        if self.first_seen.len() >= MAX_FIRST_SEEN {
            // 僵尸任务防御：清空观察表，已入表任务的最坏影响是丢一条样本。
            self.first_seen.clear();
        }
        self.first_seen
            .entry(task_id.to_string())
            .or_insert_with(Instant::now);
    }

    /// 任务终态：返回「首次入表 → 终态」耗时（毫秒）；未观测过返回 None。
    /// 取出即删除，保证同一任务只记一条样本。
    pub fn finish(&mut self, task_id: &str) -> Option<f64> {
        if !self.enabled {
            return None;
        }
        self.first_seen
            .remove(task_id)
            .map(|t| t.elapsed().as_secs_f64() * 1000.0)
    }

    pub fn record(&mut self, ts: i64, duration_ms: f64, ok: bool) {
        if !self.enabled {
            return;
        }
        self.samples.push_back(Sample {
            ts,
            duration_ms,
            ok,
        });
        while self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }
        let b = self.baseline.baseline_ms;
        self.baseline.baseline_ms = Some(match b {
            Some(old) if self.baseline.samples > 0 => {
                old * (1.0 - BASELINE_ALPHA) + duration_ms * BASELINE_ALPHA
            }
            _ => duration_ms,
        });
        self.baseline.samples = self.baseline.samples.saturating_add(1);
    }

    fn prune(&mut self, window_seconds: u64, now_ts: i64) {
        let cutoff = now_ts - window_seconds as i64;
        while self.samples.front().is_some_and(|s| s.ts < cutoff) {
            self.samples.pop_front();
        }
    }

    /// 评估阈值 + 漂移；返回本拍新触发的告警（已做同类冷却）。
    pub fn evaluate(
        &mut self,
        cfg: &SloConfig,
        now_ts: i64,
        now: Instant,
        queue_depth: usize,
    ) -> Vec<Alert> {
        if !self.enabled || !cfg.enabled {
            return Vec::new();
        }
        self.prune(cfg.window_seconds, now_ts);
        let st = window_stats(&self.samples);
        let mut checks: Vec<(&str, f64, f64, bool)> = Vec::new();
        if st.n >= cfg.min_samples.max(1) as u64 {
            checks.push((
                "p99",
                st.p99_ms,
                cfg.p99_ms as f64,
                st.p99_ms > cfg.p99_ms as f64,
            ));
            checks.push((
                "error_rate",
                st.error_rate,
                cfg.error_rate,
                st.error_rate > cfg.error_rate,
            ));
            if let Some(b) = self.baseline.baseline_ms.filter(|b| *b > 0.0) {
                let ratio = st.p50_ms / b;
                checks.push((
                    "baseline_drift",
                    ratio,
                    cfg.baseline_ratio,
                    ratio > cfg.baseline_ratio,
                ));
            }
        }
        checks.push((
            "queue_depth",
            queue_depth as f64,
            cfg.queue_depth as f64,
            queue_depth > cfg.queue_depth,
        ));

        let mut fired = Vec::new();
        for (kind, value, threshold, breach) in checks {
            if !breach {
                continue;
            }
            if let Some(at) = self.last_fire.get(kind) {
                if now.duration_since(*at).as_secs() < cfg.cooldown_seconds {
                    continue;
                }
            }
            let alert = Alert {
                kind: kind.to_string(),
                value: round1(value),
                threshold: round1(threshold),
                at: now_ts,
            };
            self.last_fire.insert(kind.to_string(), now);
            self.alerts.push_back(alert.clone());
            while self.alerts.len() > MAX_ALERTS {
                self.alerts.pop_front();
            }
            fired.push(alert);
        }
        self.maybe_save(now);
        fired
    }

    /// `/v1/mesh/slo` 视图（诊断只读；不改变基线/告警状态）。
    pub fn snapshot(
        &mut self,
        cfg: &SloConfig,
        now_ts: i64,
        queue_depth: usize,
        running: usize,
    ) -> Value {
        self.prune(cfg.window_seconds, now_ts);
        let st = window_stats(&self.samples);
        let baseline = self.baseline.baseline_ms.unwrap_or(0.0);
        let drift_ratio = if baseline > 0.0 {
            st.p50_ms / baseline
        } else {
            0.0
        };
        let enough = st.n >= cfg.min_samples.max(1) as u64;
        let mut active: Vec<&str> = Vec::new();
        if enough && st.p99_ms > cfg.p99_ms as f64 {
            active.push("p99");
        }
        if enough && st.error_rate > cfg.error_rate {
            active.push("error_rate");
        }
        if enough && baseline > 0.0 && drift_ratio > cfg.baseline_ratio {
            active.push("baseline_drift");
        }
        if queue_depth > cfg.queue_depth {
            active.push("queue_depth");
        }
        json!({
            "enabled": self.enabled && cfg.enabled,
            "window_seconds": cfg.window_seconds,
            "samples": st.n,
            "errors": st.errors,
            "error_rate": round1(st.error_rate),
            "p50_ms": round1(st.p50_ms),
            "p95_ms": round1(st.p95_ms),
            "p99_ms": round1(st.p99_ms),
            "avg_ms": round1(st.avg_ms),
            "baseline_ms": round1(baseline),
            "baseline_samples": self.baseline.samples,
            "drift_ratio": round1(drift_ratio),
            "anomaly": active.contains(&"baseline_drift"),
            "queue_depth": queue_depth,
            "running": running,
            "thresholds": {
                "min_samples": cfg.min_samples,
                "p99_ms": cfg.p99_ms,
                "error_rate": cfg.error_rate,
                "queue_depth": cfg.queue_depth,
                "baseline_ratio": cfg.baseline_ratio,
                "cooldown_seconds": cfg.cooldown_seconds,
            },
            "active": active,
            // 最近在前，最多 20 条（避免观察面过大）。
            "alerts": self.alerts.iter().rev().take(20).cloned().collect::<Vec<_>>(),
        })
    }

    fn maybe_save(&mut self, now: Instant) {
        let Some(path) = self.path.clone() else {
            return;
        };
        if let Some(t) = self.last_save {
            if now.duration_since(t).as_secs() < SAVE_INTERVAL_SECS {
                return;
            }
        }
        self.last_save = Some(now);
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let tmp = path.with_extension("json.tmp");
        if let Ok(body) = serde_json::to_string(&self.baseline) {
            if std::fs::write(&tmp, body).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
    }
}

/// 窗口统计（nearest-rank 分位；空窗口全 0）。
fn window_stats(samples: &VecDeque<Sample>) -> WindowStats {
    let mut st = WindowStats::default();
    if samples.is_empty() {
        return st;
    }
    st.n = samples.len() as u64;
    let mut durs: Vec<f64> = Vec::with_capacity(samples.len());
    let mut sum = 0.0;
    for s in samples {
        durs.push(s.duration_ms);
        sum += s.duration_ms;
        if !s.ok {
            st.errors += 1;
        }
    }
    st.error_rate = st.errors as f64 / st.n as f64;
    st.avg_ms = sum / st.n as f64;
    durs.sort_by(|a, b| a.total_cmp(b));
    st.p50_ms = percentile(&durs, 50.0);
    st.p95_ms = percentile(&durs, 95.0);
    st.p99_ms = percentile(&durs, 99.0);
    st
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: i64, ms: f64, ok: bool) -> Sample {
        Sample {
            ts,
            duration_ms: ms,
            ok,
        }
    }

    fn cfg() -> SloConfig {
        SloConfig {
            enabled: true,
            window_seconds: 60,
            min_samples: 3,
            p99_ms: 100,
            error_rate: 0.2,
            queue_depth: 2,
            baseline_ratio: 1.3,
            cooldown_seconds: 30,
        }
    }

    #[test]
    fn percentiles_nearest_rank_and_error_rate() {
        let q: VecDeque<_> = [10.0, 20.0, 30.0, 40.0, 100.0]
            .iter()
            .enumerate()
            .map(|(i, ms)| sample(1_000 + i as i64, *ms, i != 4))
            .collect();
        let st = window_stats(&q);
        assert_eq!(st.n, 5);
        assert_eq!(st.errors, 1);
        assert!((st.error_rate - 0.2).abs() < 1e-9);
        assert_eq!(st.p50_ms, 30.0);
        assert_eq!(st.p95_ms, 100.0);
        assert_eq!(st.p99_ms, 100.0);
        assert!((st.avg_ms - 40.0).abs() < 1e-9);
    }

    #[test]
    fn baseline_drift_fires_once_and_recovers_after_cooldown() {
        let mut s = SloState::new(None, true);
        // 历史基线 100ms：先记录 50 条，EWMA 收敛到 ~100。
        for i in 0..50 {
            s.record(1_000 + i, 100.0, true);
        }
        // 窗口内 3 条 200ms：p50/基线 ≈ 2.0 > 1.3。
        for i in 0..3 {
            s.record(1_100 + i, 200.0, true);
        }
        let t0 = Instant::now();
        let fired = s.evaluate(&cfg(), 1_110, t0, 0);
        let kinds: Vec<&str> = fired.iter().map(|a| a.kind.as_str()).collect();
        assert!(kinds.contains(&"baseline_drift"), "kinds: {kinds:?}");
        assert!(kinds.contains(&"p99"), "kinds: {kinds:?}");
        // 冷却内不再重复。
        let again = s.evaluate(&cfg(), 1_111, t0 + std::time::Duration::from_secs(5), 0);
        assert!(again.is_empty(), "冷却期不应重复触发: {again:?}");
        // 冷却后恢复触发。
        let later = s.evaluate(&cfg(), 1_112, t0 + std::time::Duration::from_secs(31), 0);
        assert!(!later.is_empty());
    }

    #[test]
    fn error_rate_and_queue_depth_thresholds() {
        let mut s = SloState::new(None, true);
        s.record(1_000, 10.0, true);
        s.record(1_001, 10.0, false);
        s.record(1_002, 10.0, false);
        let fired = s.evaluate(&cfg(), 1_010, Instant::now(), 5);
        let kinds: Vec<&str> = fired.iter().map(|a| a.kind.as_str()).collect();
        assert!(kinds.contains(&"error_rate"), "kinds: {kinds:?}");
        assert!(kinds.contains(&"queue_depth"), "kinds: {kinds:?}");
        assert!(!kinds.contains(&"baseline_drift"), "样本不足时不判漂移");
    }

    #[test]
    fn window_prunes_old_samples_and_min_samples_gate() {
        let mut s = SloState::new(None, true);
        s.record(1_000, 999.0, false);
        s.record(1_100, 10.0, true);
        s.record(1_101, 10.0, true);
        s.record(1_102, 10.0, true);
        // 窗口 60s：1_000 被裁掉，剩 3 条（min_samples=3），p99=10 不触发；
        // 错误率 0 不触发。
        let fired = s.evaluate(&cfg(), 1_110, Instant::now(), 0);
        assert!(fired.is_empty(), "旧样本应被窗口裁掉: {fired:?}");
        let snap = s.snapshot(&cfg(), 1_110, 0, 0);
        assert_eq!(snap["samples"], 3);
        assert_eq!(snap["errors"], 0);
        assert_eq!(snap["enabled"], true);
    }

    #[test]
    fn baseline_persists_and_reloads() {
        let dir = std::env::temp_dir().join(format!("al-slo-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("slo_baseline.json");
        let mut s = SloState::new(Some(path.clone()), true);
        // 2 条 50ms → 基线 50；触发一次评估落盘（首次评估必存）。
        s.record(1_000, 50.0, true);
        s.record(1_001, 50.0, true);
        s.evaluate(&cfg(), 1_002, Instant::now(), 0);
        let reloaded = SloState::new(Some(path), true);
        assert!((reloaded.baseline.baseline_ms.unwrap() - 50.0).abs() < 1e-6);
        assert_eq!(reloaded.baseline.samples, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disabled_state_collects_nothing() {
        let mut s = SloState::new(None, false);
        s.touch("t1");
        assert!(s.finish("t1").is_none());
        s.record(1_000, 999.0, false);
        assert!(s.evaluate(&cfg(), 1_001, Instant::now(), 100).is_empty());
        let snap = s.snapshot(&cfg(), 1_001, 100, 0);
        assert_eq!(snap["enabled"], false);
        assert_eq!(snap["samples"], 0);
    }
}
