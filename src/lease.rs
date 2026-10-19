//! Lease 心跳 + 阈值差异上报（v0.6 P1，设计案 §5.1）。
//!
//! 目标：10~30 台阶段以「差异化 Full Status」替代高频全量广播——节点按
//! `lease.interval_secs` 采样本机 CPU，变化 ≥ `cpu_threshold_pct` 才发
//! Full Status 帧（`ctrl/lease`，payload 带 cpu）；阈值内不发帧；
//! 长时间静默时补一个裸 lease 心跳（无负载字段），链路活性沿用既有会话心跳。
//!
//! 兼容性：帧为加性 ctrl 帧——旧节点在 `conn::handle_text` 底部分支按
//! 「未知 ctrl 帧」直接忽略（零破坏）。缺省 `lease.enabled=false`
//! 时本模块完全不 spawn（零流量、零行为变化）。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::api::SharedState;
use crate::config::LeaseConfig;
use crate::load;
use crate::proto::Envelope;

/// 本节点 lease 出站的进程内差异状态（每节点一份，跨会话共享）。
#[derive(Default)]
pub struct LeaseState {
    /// 最近一次**对外宣告**的 CPU 利用率（差异比较基准）。
    advertised: Mutex<Option<u32>>,
    /// 最近一帧（含裸心跳）发出时刻。
    last_sent: Mutex<Option<Instant>>,
}

impl LeaseState {
    pub fn new() -> Self {
        Self::default()
    }

    /// 最近对外宣告的 CPU（新对端注册时据此补推快照；从未宣告 → None）。
    pub fn advertised_cpu(&self) -> Option<u32> {
        *self.advertised.lock().unwrap()
    }
}

/// 差异判定 + 裸心跳节流的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseDecision {
    /// 阈值跨越/首次采样：发 Full Status（带 cpu 字段）。
    Full(u32),
    /// 裸心跳节流到期：发无负载字段的心跳帧。
    BareBeat,
    /// 本拍不发帧（阈值内且裸心跳未到期）。
    Quiet,
}

/// 纯函数：决定本拍发什么帧（供 run 循环与单测）。
///
/// - `sample=None`（平台不支持/采样失败）永不发 Full，只按裸心跳节流兜底；
/// - 首次采样（`advertised=None`）必发 Full；
/// - 变化 < 阈值 → 静默；阈值有效值取 `max(threshold, 1)`（含边界，防 0 抖动死循环）；
/// - `bare_beat_secs=0` 语义 = 禁用裸心跳（活性完全沿用既有会话心跳）。
pub fn decide(
    cfg: &LeaseConfig,
    sample: Option<u32>,
    advertised: Option<u32>,
    since_last: Option<Duration>,
) -> LeaseDecision {
    if let Some(cpu) = sample {
        let changed = match advertised {
            Some(prev) => cpu.abs_diff(prev),
            None => u32::MAX,
        };
        if changed >= cfg.cpu_threshold_pct.max(1) {
            return LeaseDecision::Full(cpu);
        }
    }
    if cfg.bare_beat_secs == 0 {
        return LeaseDecision::Quiet;
    }
    match since_last {
        None => LeaseDecision::BareBeat,
        Some(el) => {
            if el >= Duration::from_secs(cfg.bare_beat_secs) {
                LeaseDecision::BareBeat
            } else {
                LeaseDecision::Quiet
            }
        }
    }
}

/// Lease 出站循环：每 `interval_secs` 采样一次 CPU 并按差异决策发帧。
/// 阻塞的（≤200ms）采样放进 `spawn_blocking`，不占用异步执行器。
pub fn spawn(state: SharedState, shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let cfg = state.config.lease.clone();
        if !cfg.enabled {
            return;
        }
        tracing::info!(
            interval_secs = cfg.interval_secs,
            cpu_threshold_pct = cfg.cpu_threshold_pct,
            bare_beat_secs = cfg.bare_beat_secs,
            "lease: started (full status only on threshold crossing)"
        );
        let mut ticker = tokio::time::interval(Duration::from_secs(cfg.interval_secs.max(1)));
        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = ticker.tick() => {
                    let sample = tokio::task::spawn_blocking(load::cpu_percent)
                        .await
                        .unwrap_or(None);
                    let advertised = *state.lease.advertised.lock().unwrap();
                    let since_last = (*state.lease.last_sent.lock().unwrap()).map(|t| t.elapsed());
                    let decision = decide(&cfg, sample, advertised, since_last);
                    let env = match decision {
                        LeaseDecision::Full(cpu) => {
                            *state.lease.advertised.lock().unwrap() = Some(cpu);
                            tracing::debug!(
                                node_id = %cfg_id(&state),
                                cpu,
                                "lease: full status advertised"
                            );
                            Envelope::lease(cfg_id(&state), Some(cpu))
                        }
                        LeaseDecision::BareBeat => {
                            tracing::trace!("lease: bare heartbeat");
                            Envelope::lease(cfg_id(&state), None)
                        }
                        LeaseDecision::Quiet => continue,
                    };
                    *state.lease.last_sent.lock().unwrap() = Some(Instant::now());
                    state.router.broadcast_envelope(&env);
                }
            }
        }
        tracing::info!("lease: stopped");
    });
}

fn cfg_id(state: &SharedState) -> &str {
    &state.config.node_id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> LeaseConfig {
        LeaseConfig {
            enabled: true,
            interval_secs: 10,
            cpu_threshold_pct: 5,
            bare_beat_secs: 60,
        }
    }

    #[test]
    fn decide_threshold_semantics() {
        let c = cfg();
        let since = Some(Duration::from_secs(5));
        // 首次采样必发 Full（无基准可比）。
        assert_eq!(decide(&c, Some(42), None, since), LeaseDecision::Full(42));
        // 变化 ≥ 阈值（±5 个点）→ Full。
        assert_eq!(
            decide(&c, Some(48), Some(42), since),
            LeaseDecision::Full(48)
        );
        assert_eq!(
            decide(&c, Some(37), Some(42), since),
            LeaseDecision::Full(37)
        );
        // 变化 < 阈值 → 阈值内静默。
        assert_eq!(decide(&c, Some(46), Some(42), since), LeaseDecision::Quiet);
        assert_eq!(decide(&c, Some(40), Some(42), since), LeaseDecision::Quiet);
        // 采样失败 → 永不发 Full；裸心跳到期与否决定 BareBeat/Quiet。
        assert_eq!(
            decide(&c, None, Some(42), Some(Duration::from_secs(61))),
            LeaseDecision::BareBeat
        );
        assert_eq!(decide(&c, None, Some(42), since), LeaseDecision::Quiet);
        // 从没发过帧（进程刚起）且无样本 → 裸心跳兜底。
        assert_eq!(decide(&c, None, None, None), LeaseDecision::BareBeat);
        // threshold=0 的防护：按 ≥1 点比较（含边界）——该档位下 1 点抖动即发全量，
        // 属操作者显式选择；实现只防 0 点抖动死循环，不擅自加余量。
        let c0 = LeaseConfig {
            cpu_threshold_pct: 0,
            ..cfg()
        };
        assert_eq!(
            decide(&c0, Some(43), Some(42), since),
            LeaseDecision::Full(43)
        );
        assert_eq!(decide(&c0, Some(42), Some(42), since), LeaseDecision::Quiet);
        // bare_beat_secs=0 = 禁用裸心跳：静默（活性沿用既有会话心跳）。
        let c1 = LeaseConfig {
            bare_beat_secs: 0,
            ..cfg()
        };
        assert_eq!(decide(&c1, None, Some(42), None), LeaseDecision::Quiet);
    }

    #[test]
    fn lease_frame_additive_roundtrip() -> Result<(), serde_json::Error> {
        // Full 帧：payload 带 cpu。
        let v = serde_json::to_value(Envelope::lease("node-a", Some(17)))?;
        assert_eq!(v["type"], "ctrl");
        assert_eq!(v["action"], "lease");
        assert_eq!(v["payload"]["cpu"], 17);
        // 裸帧：payload 空；旧节点按未知 ctrl 帧忽略，新节点解析不炸。
        let bare = serde_json::to_value(Envelope::lease("node-a", None))?;
        assert!(bare["payload"].is_null());
        // 反序列化往返保持形状。
        let back: Envelope =
            serde_json::from_value(serde_json::to_value(Envelope::lease("node-a", Some(3)))?)?;
        assert_eq!(back.payload["cpu"], 3);
        // 缺 payload 的老帧（不存在但健壮性）解析为 Null。
        let mut legacy = serde_json::to_value(Envelope::lease("node-a", Some(5)))?;
        legacy.as_object_mut().unwrap().remove("payload");
        let l: Envelope = serde_json::from_value(legacy)?;
        assert!(l.payload.is_null());
        Ok(())
    }
}
