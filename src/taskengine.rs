//! 任务领域模型与状态机纯函数（v0.6 M1 / 设计 §2.2，编码拆包 P1-2）。
//!
//! 定位：`apply_event` 是与 `router.rs::tasks_projection`（:745）**同语义**的
//! 纯函数回放状态机——无 IO、无锁、不依赖 taskstore/license/config。协议语义
//! 正本：docs/task-protocol.md §3（状态机）与 §4（`(task_id, seq)` 幂等键）；
//! 对拍基准：router.rs 单测 `tasks_projection_blocked_frames_marked_not_replayed`
//! （:250）、`tasks_projection_full_lifecycle`（:375）、
//! `tasks_projection_state_machine_edges`（:428）。
//!
//! 内核简化口径（与投影注释一致）：
//! - `task_failed` 不是硬终态——后续新 `task_accepted`（重派/retry）把任务
//!   带回 `running`，并令 `attempts += 1`；
//! - `task_cancel` 指令只把状态推到 `cancelling`（内核可见中间态），
//!   `task_cancelled` 确认才落 `cancelled` 硬终态；
//! - `task_progress` 永不改状态，只累计进度计数并刷新心跳时间戳（喂 drift）；
//! - `task_spec` / `task_ack` / `task_go` / `task_rollback`（指令帧）不改状态
//!   （回滚终态以执行方回报的 `task_rolled_back` 为准，执行方是唯一状态权威）；
//! - 硬终态（succeeded/declined/cancelled/rolled_back/expired）粘性：
//!   迟到的任何帧都不能复活（对拍 :428 末段 declined 粘性断言）；
//! - lockdown 被拦帧（`blocked: true`）只计数、不推进状态机（对拍 :250）；
//! - 缺 `task_id` 的帧不可投影即忽略（对拍 :375 "specless junk"）；
//!   未知 task_id 的无 spec 帧（trail-only 帧，对拍 :428 的 e）按投影语义
//!   建行：字段留空、状态只由轨迹推进。M2 reconcile 接线时，"未入表的陌生
//!   手发帧不进引擎表"由调用方（订阅侧按表内 task_id 过滤）实现，本文件
//!   保持与投影一致的纯回放语义（重放/乱序到达的终态不变性需要它）。
#![allow(dead_code)] // M1 骨架：字段与接口为 P1-1/P1-5/M2 消费方预留

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::router::is_task_topic;

/// 任务状态（协议 §3 全边 + 内核可见中间态）。序列化用小写蛇形，
/// 与 `/v1/tasks` 投影的 `state` 字符串完全一致，消费方零区分成本。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// 已声明（task_spec 落表或首帧建行），尚未被认领。
    Pending,
    /// 执行方已 `task_accepted`，在跑。
    Running,
    /// `task_cancel` 指令已见、`task_cancelled` 确认未回的内核中间态。
    Cancelling,
    /// 成功终态。
    Succeeded,
    /// 失败软终态：可被后续 `task_accepted` 复活为 running（retry 边）。
    Failed,
    /// 执行方拒绝终态（协议 §1，粘性）。
    Declined,
    /// 撤销确认终态（粘性，不可复活）。
    Cancelled,
    /// 回滚确认终态（粘性）。
    RolledBack,
    /// deadline 超期终态（引擎判定写入，协议 §3"编排方判定"；粘性）。
    Expired,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Cancelling => "cancelling",
            TaskState::Succeeded => "succeeded",
            TaskState::Failed => "failed",
            TaskState::Declined => "declined",
            TaskState::Cancelled => "cancelled",
            TaskState::RolledBack => "rolled_back",
            TaskState::Expired => "expired",
        }
    }

    /// 硬终态：与 router.rs `is_hard_terminal`（:715）同集合，另加引擎自己
    /// 写入的 `expired`（投影侧永远到不了 expired，不构成对拍差异）。
    /// 进入硬终态后，`apply_event` 不再被任何后续帧推进状态。
    pub fn is_hard_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Succeeded
                | TaskState::Declined
                | TaskState::Cancelled
                | TaskState::RolledBack
                | TaskState::Expired
        )
    }

    /// 报告用终态（对拍投影 `terminal` 字段口径：硬终态 ∪ failed；
    /// expired 是协议 §3 明示终态，一并计入）。
    pub fn is_terminal(self) -> bool {
        self.is_hard_terminal() || self == TaskState::Failed
    }

    /// 可被 deadline 判定翻转为 expired 的活态集合（pending/running/cancelling）。
    pub fn is_live(self) -> bool {
        !self.is_terminal()
    }
}

/// 内核态任务记录（设计 §2.1 的模型侧，序列化形状即 P1-1 taskstore 落表用）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub state: TaskState,
    /// `task_spec.type` 原文（未见 spec 前为 None——trail-only 行）。
    #[serde(rename = "type", default)]
    pub task_type: Option<String>,
    #[serde(default)]
    pub intent: Option<String>,
    #[serde(default)]
    pub correlation: Option<String>,
    /// task_spec 帧 payload **原文**（未脱敏；本机 data_dir，重启续传要原文）。
    /// 首见 spec 定版不覆盖（对拍投影 `spec.is_none()` 门），重投 spec 只推进
    /// 水位与 attempts。
    #[serde(default)]
    pub spec: Option<Value>,
    /// deadline 原文（ISO8601，来自 spec），expired 判定的输入。
    #[serde(default)]
    pub deadline: Option<String>,
    #[serde(default)]
    pub from_agent: Option<String>,
    #[serde(default)]
    pub to_agent: Option<String>,
    /// 观测水位：已接受帧的最高协议 seq（对拍投影 `seq` 的 max 语义）。
    pub seq: u64,
    /// 派发次数：初始 0；failed→accepted（retry 复活）与更高 seq 的重投
    /// task_spec 各 +1。
    pub attempts: u64,
    /// 已见的协议 seq 集合——`(task_id, seq)` 幂等键（协议 §4，对齐
    /// executor.py SEEN）。落表持久化，重启重放天然续用。
    #[serde(default)]
    pub seen_seqs: BTreeSet<u64>,
    pub progress_count: u64,
    pub message_count: u64,
    /// 轨迹中被 lockdown 拦截（未投递/转发）的帧数（v0.5 §4.5 薄档口径）。
    pub blocked_count: u64,
    /// 最近一次 task_progress 的帧时间戳原文（maintain 心跳，drift 判定基线）。
    #[serde(default)]
    pub last_progress_ts: Option<String>,
    /// 首见帧时间戳（建行时刻）。帧不带 ts 时为 None。
    #[serde(default)]
    pub created_ts: Option<String>,
    /// 最近被接受帧的时间戳。
    #[serde(default)]
    pub last_ts: Option<String>,
    /// 最近一次被采纳的 task_spec seq（识别"重投 spec"用）。
    #[serde(default)]
    pub last_spec_seq: Option<u64>,
}

impl TaskRecord {
    fn new(task_id: &str, ts: Option<&str>) -> Self {
        TaskRecord {
            task_id: task_id.to_string(),
            state: TaskState::Pending,
            task_type: None,
            intent: None,
            correlation: None,
            spec: None,
            deadline: None,
            from_agent: None,
            to_agent: None,
            seq: 0,
            attempts: 0,
            seen_seqs: BTreeSet::new(),
            progress_count: 0,
            last_progress_ts: None,
            created_ts: ts.map(str::to_string),
            last_ts: ts.map(str::to_string),
            last_spec_seq: None,
            message_count: 0,
            blocked_count: 0,
        }
    }
}

/// 引擎任务表：task_id → 记录。BTreeMap 保证迭代定序（调度器断言友好）。
pub type TaskTable = BTreeMap<String, TaskRecord>;

/// 被丢弃帧的原因（均未对表做任何改动）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoreReason {
    /// payload.topic 不在任务协议 12 topic 全集内（普通 chatter 等）。
    NonTaskTopic,
    /// 缺 task_id 或为空串——无分组键，不可投影/入表（对拍 :375 junk 帧）。
    MissingTaskId,
    /// `(task_id, seq)` 幂等键命中已见集合——重复投递直接丢弃（协议 §4）。
    DuplicateSeq,
}

/// 一帧应用后的状态变更报告。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// 首见 task_id 建行且本帧未推进状态（典型：task_spec 首帧）。
    Created { task_id: String, state: TaskState },
    /// 状态机迁移发生（含建行即迁移：from 取建行默认 pending）。
    Transitioned {
        task_id: String,
        from: TaskState,
        to: TaskState,
    },
    /// 帧被接受但状态未变（计数/字段刷新；含被拦帧与硬终态后的迟到帧）。
    Updated { task_id: String },
    /// 帧被丢弃，表未动。
    Ignored { reason: IgnoreReason },
}

/// 把一帧总线 JSON 应用到任务表，返回状态变更（纯函数，无 IO 无锁）。
///
/// 帧形状两种都收：
/// 1. **信封形**（HistoryEntry/Envelope 风格）：`{"ts":…, "from_agent":…,
///    "blocked":…, "payload": {"topic":…,"task_id":…,"seq":…}}`；
/// 2. **裸 payload 形**：帧本身就是 `{"topic":…,"task_id":…}`。
///
/// 迁移边与 `tasks_projection` 逐一同构（router.rs :745）：
/// spec 定版字段 → accepted=running → progress×n（态不变）→
/// succeeded/failed 终报；failed 后新 accepted → running（retry，attempts+1）；
/// declined/cancelled/rolled_back/expired 粘性；cancel→cancelling→cancelled。
pub fn apply_event(records: &mut TaskTable, frame: &Value) -> ApplyOutcome {
    // ---- 帧拆解：payload 本体 + ts/blocked/from/to 外层字段 ----
    let payload = frame.get("payload").unwrap_or(frame);
    let get_str = |key: &str| {
        frame
            .get(key)
            .and_then(|v| v.as_str())
            .or_else(|| payload.get(key).and_then(|v| v.as_str()))
    };
    let topic = payload.get("topic").and_then(|v| v.as_str()).unwrap_or("");
    let task_id = match payload.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => {
            return ApplyOutcome::Ignored {
                reason: IgnoreReason::MissingTaskId,
            }
        }
    };
    if !is_task_topic(topic) {
        return ApplyOutcome::Ignored {
            reason: IgnoreReason::NonTaskTopic,
        };
    }
    let blocked = frame
        .get("blocked")
        .or_else(|| payload.get("blocked"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let ts = get_str("ts");
    // 协议 seq：非法/缺失视为无幂等键（照常应用，只推进观测量）——对齐投影
    // 对无 seq 帧不拦截的行为；executor 参考实现总带 seq。
    let seq = payload.get("seq").and_then(|v| v.as_u64());

    // ---- 幂等键 (task_id, seq)：重放丢弃，表零改动（协议 §4）----
    if let Some(rec) = records.get(&task_id) {
        if let Some(s) = seq {
            if rec.seen_seqs.contains(&s) {
                return ApplyOutcome::Ignored {
                    reason: IgnoreReason::DuplicateSeq,
                };
            }
        }
    }

    let existed = records.contains_key(&task_id);
    let rec = records
        .entry(task_id.clone())
        .or_insert_with(|| TaskRecord::new(&task_id, ts));
    let from_state = rec.state;

    // ---- 观测量刷新（对拍投影 :789-805：blocked 帧也计数、也推水位）----
    rec.message_count += 1;
    if blocked {
        rec.blocked_count += 1;
    }
    if let Some(s) = seq {
        rec.seq = rec.seq.max(s);
        rec.seen_seqs.insert(s);
    }
    if let Some(t) = ts {
        rec.last_ts = Some(t.to_string());
    }

    // ---- 字段填充（spec 首见定版不覆盖；其余帧只补空，对拍 :806-837）----
    if topic == "task_spec" {
        if rec.spec.is_none() {
            rec.spec = Some(payload.clone()); // 原文入库，不脱敏
            rec.task_type = payload
                .get("type")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            rec.intent = payload
                .get("intent")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            rec.correlation = payload
                .get("correlation")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            rec.deadline = payload
                .get("deadline")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            rec.last_spec_seq = seq;
            if rec.from_agent.is_none() {
                rec.from_agent = get_str("from_agent").map(str::to_string);
            }
            if rec.to_agent.is_none() {
                rec.to_agent = get_str("to_agent").map(str::to_string);
            }
        } else if let Some(s) = seq {
            // 重投 spec（引擎 retry/换机再派发，seq 递增，设计 §2.4 第 4 步）：
            // 不改写原文、不伪造状态，只计一次派发。
            if rec.last_spec_seq.is_none_or(|old| s > old) {
                rec.attempts += 1;
                rec.last_spec_seq = Some(s);
            }
        }
    } else {
        if rec.from_agent.is_none() {
            rec.from_agent = get_str("from_agent").map(str::to_string);
        }
        if rec.to_agent.is_none() {
            rec.to_agent = get_str("to_agent").map(str::to_string);
        }
    }

    // ---- 状态机推进（被拦帧与硬终态行都不推进，对拍 :838-856）----
    if blocked {
        // 被拦帧只留计数：不推进状态机、不计进度。
    } else if topic == "task_progress" {
        rec.progress_count += 1;
        if let Some(t) = ts {
            rec.last_progress_ts = Some(t.to_string());
        }
    } else if !rec.state.is_hard_terminal() {
        let next = match topic {
            "task_accepted" => Some(TaskState::Running),
            "task_succeeded" => Some(TaskState::Succeeded),
            "task_declined" => Some(TaskState::Declined),
            "task_cancelled" => Some(TaskState::Cancelled),
            "task_rolled_back" => Some(TaskState::RolledBack),
            "task_failed" => Some(TaskState::Failed),
            "task_cancel" => Some(TaskState::Cancelling),
            // task_spec/task_ack/task_go/task_rollback 为信息/指令帧，不改态
            //（回滚终态以执行方回报的 task_rolled_back 为准）。
            _ => None,
        };
        if let Some(to) = next {
            if to != rec.state {
                // retry 边：failed 后新 accepted 复活为 running，计一次派发。
                if rec.state == TaskState::Failed && to == TaskState::Running {
                    rec.attempts += 1;
                }
                rec.state = to;
            }
        }
    }

    let to_state = rec.state;
    if to_state != from_state {
        ApplyOutcome::Transitioned {
            task_id,
            from: from_state,
            to: to_state,
        }
    } else if !existed {
        ApplyOutcome::Created {
            task_id,
            state: to_state,
        }
    } else {
        ApplyOutcome::Updated { task_id }
    }
}

// ===========================================================================
// 给后续里程碑（M2 reconcile / P2-3 timer 动作）的纯函数接口——只判定不动作：
// 告警外发与派发策略由 loop 接线时决定（设计 §2.3/§2.6，拍板 4：
// drift/declined 只告警不自动换机重派）。
// ===========================================================================

/// ISO8601/RFC3339（含 ±hh:mm 偏移或 Z）→ Unix 秒。解析失败返回 None。
pub fn parse_iso_epoch(s: &str) -> Option<i64> {
    OffsetDateTime::parse(s, &Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

/// deadline 超期判定（协议 §3/§4"编排方判定"）：仅对活态（pending/running/
/// cancelling）且 spec 带可解析 deadline 的行成立；`now > deadline` 即超期。
/// 无 deadline / 不可解析 / 已终态 → false。纯判定，不改表。
pub fn is_expired(rec: &TaskRecord, now_epoch: i64) -> bool {
    rec.state.is_live()
        && rec
            .deadline
            .as_deref()
            .and_then(parse_iso_epoch)
            .is_some_and(|d| now_epoch > d)
}

/// 全表 deadline 扫描：把超期的活态行翻转为 `expired`（终态、粘性），返回
/// 被翻转的 task_id 列表（BTreeMap 定序）。M2 tick"引擎写表+告警"的纯函数内核。
pub fn expire_overdue(records: &mut TaskTable, now_epoch: i64) -> Vec<String> {
    let mut flipped = Vec::new();
    for rec in records.values_mut() {
        if is_expired(rec, now_epoch) {
            rec.state = TaskState::Expired;
            flipped.push(rec.task_id.clone());
        }
    }
    flipped
}

/// maintain 心跳缺勤 drift 判定（对拍 taskctl.py:38,91 与设计拍板 4）：
/// `now - 最近心跳 > 3 × interval_s` 视为 drift。**只返回告警布尔，绝不改
/// 状态、绝不换机重派**（执行方是唯一状态权威）。心跳基线缺省退化为最近
/// 一帧时间戳，再退化为建行时刻；running/cancelling 之外的态不判 drift；
/// 全无时间戳（帧不带 ts）返回 false，交上层按环外退化规则处理。
pub fn is_drifting(rec: &TaskRecord, now_epoch: i64, interval_s: i64) -> bool {
    if interval_s <= 0 || !matches!(rec.state, TaskState::Running | TaskState::Cancelling) {
        return false;
    }
    let baseline = rec
        .last_progress_ts
        .as_deref()
        .or(rec.last_ts.as_deref())
        .or(rec.created_ts.as_deref())
        .and_then(parse_iso_epoch);
    match baseline {
        Some(t) => now_epoch - t > 3 * interval_s,
        None => false,
    }
}

// ===========================================================================
// 单测：全生命周期 / retry 边 / 幂等重放 / 乱序帧 / 终态粘性 / expired /
// drift / 与 tasks_projection（router.rs :250/:375/:428）逐边对拍。
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 造 HistoryEntry 风格的信封帧（对拍 router.rs 测试的 task_entry）。
    fn frame(seq: u64, topic: &str, task_id: &str, extra: Option<Value>) -> Value {
        let mut payload = json!({"topic": topic, "task_id": task_id, "seq": seq});
        if let Some(e) = extra {
            if let (Some(o), Some(x)) = (payload.as_object_mut(), e.as_object()) {
                for (k, v) in x {
                    o.insert(k.clone(), v.clone());
                }
            }
        }
        json!({
            "ts": format!("2026-10-17T0{}:00:00+08:00", seq % 10),
            "from_agent": if topic == "task_spec" { "orch" } else { "worker" },
            "to_agent": if topic == "task_spec" { "worker" } else { "orch" },
            "payload": payload,
        })
    }

    fn replay(frames: impl IntoIterator<Item = Value>) -> TaskTable {
        let mut t = TaskTable::new();
        for f in frames {
            apply_event(&mut t, &f);
        }
        t
    }

    #[test]
    fn full_lifecycle_edges() {
        // 对拍 router.rs :375：spec→accepted→progress×2→succeeded + junk 帧。
        let frames = [
            frame(
                1,
                "task_spec",
                "t1",
                Some(json!({"type": "deploy.demo", "intent": "smoke", "correlation": "batchX"})),
            ),
            frame(2, "task_accepted", "t1", None),
            frame(3, "task_progress", "t1", None),
            frame(4, "task_progress", "t1", None),
            frame(5, "task_succeeded", "t1", None),
            // 非任务 topic、缺 task_id 的 junk：表零污染（投影同款忽略）。
            json!({"payload": {"topic": "probe", "task_id": "t9"}}),
            json!({"payload": {"topic": "task_progress"}}),
        ];
        let mut t = TaskTable::new();
        let outcomes: Vec<_> = frames.iter().map(|f| apply_event(&mut t, f)).collect();
        assert_eq!(
            outcomes[0],
            ApplyOutcome::Created {
                task_id: "t1".into(),
                state: TaskState::Pending
            }
        );
        assert_eq!(
            outcomes[1],
            ApplyOutcome::Transitioned {
                task_id: "t1".into(),
                from: TaskState::Pending,
                to: TaskState::Running
            }
        );
        assert_eq!(
            outcomes[5],
            ApplyOutcome::Ignored {
                reason: IgnoreReason::NonTaskTopic
            }
        );
        assert_eq!(
            outcomes[6],
            ApplyOutcome::Ignored {
                reason: IgnoreReason::MissingTaskId
            }
        );
        assert_eq!(t.len(), 1);
        let r = &t["t1"];
        assert_eq!(r.state, TaskState::Succeeded);
        assert!(r.state.is_terminal());
        assert_eq!(r.seq, 5);
        assert_eq!(r.progress_count, 2);
        assert_eq!(r.message_count, 5);
        assert_eq!(r.blocked_count, 0);
        assert_eq!(r.attempts, 0);
        assert_eq!(r.task_type.as_deref(), Some("deploy.demo"));
        assert_eq!(r.correlation.as_deref(), Some("batchX"));
        // spec 原文入库（未脱敏，非投影的 sanitized 面）。
        assert_eq!(r.spec.as_ref().unwrap()["intent"], "smoke");
        assert_eq!(r.from_agent.as_deref(), Some("orch"));
        assert_eq!(r.to_agent.as_deref(), Some("worker"));
    }

    #[test]
    fn retry_edge_after_failed() {
        // 对拍 router.rs :428 的 d：failed 后新 accepted → running 即 retry。
        let mut t = replay([
            frame(1, "task_spec", "d", Some(json!({"type": "x.y"}))),
            frame(2, "task_accepted", "d", None),
            frame(3, "task_failed", "d", None),
            frame(4, "task_accepted", "d", None),
        ]);
        let r = &t["d"];
        assert_eq!(r.state, TaskState::Running);
        assert!(!r.state.is_terminal()); // failed 是软终态，复活后照常
        assert_eq!(r.attempts, 1); // retry 边计一次派发
                                   // 再败再复活：attempts 单调。
        apply_event(&mut t, &frame(5, "task_failed", "d", None));
        apply_event(&mut t, &frame(6, "task_accepted", "d", None));
        assert_eq!(t["d"].attempts, 2);
        assert_eq!(t["d"].state, TaskState::Running);
        // failed 挂着的行也是终态报告口径（投影 terminal: hard ∪ failed）。
        let t2 = replay([
            frame(1, "task_spec", "c", None),
            frame(2, "task_failed", "c", None),
        ]);
        assert_eq!(t2["c"].state, TaskState::Failed);
        assert!(t2["c"].state.is_terminal() && !t2["c"].state.is_hard_terminal());
    }

    #[test]
    fn idempotent_seq_replay() {
        // 协议 §4：同 (task_id, seq) 重复投递丢弃，表不动。
        let spec = frame(1, "task_spec", "t", Some(json!({"type": "x.y"})));
        let acc = frame(2, "task_accepted", "t", None);
        let mut t = replay([spec.clone(), acc.clone()]);
        let before = t.clone();
        assert_eq!(
            apply_event(&mut t, &acc),
            ApplyOutcome::Ignored {
                reason: IgnoreReason::DuplicateSeq
            }
        );
        assert_eq!(
            apply_event(&mut t, &spec),
            ApplyOutcome::Ignored {
                reason: IgnoreReason::DuplicateSeq
            }
        );
        assert_eq!(t, before); // 深比较：计数/水位/观测量全部未动
                               // 不同 seq 的同语义帧照常推进（failed 后再 accepted ≠ 幂等冲突）。
        apply_event(&mut t, &frame(3, "task_failed", "t", None));
        assert_eq!(t["t"].state, TaskState::Failed);
    }

    #[test]
    fn out_of_order_accepted_before_spec() {
        // 乱序帧（先 accepted 后 spec）：trail-only 建行→running，spec 迟到
        // 回填字段，终态与正序回放一致（事件溯源重放的到达序不变性）。
        let mut t = replay([
            frame(2, "task_accepted", "x", None),
            frame(
                1,
                "task_spec",
                "x",
                Some(json!({"type": "deploy.demo", "deadline": "2026-10-18T07:00:00+08:00"})),
            ),
        ]);
        let r = &t["x"];
        assert_eq!(r.state, TaskState::Running);
        assert_eq!(r.task_type.as_deref(), Some("deploy.demo"));
        assert_eq!(r.deadline.as_deref(), Some("2026-10-18T07:00:00+08:00"));
        assert!(r.spec.is_some());
        assert_eq!(r.seq, 2); // 水位取 max，与帧到达序无关
        assert_eq!(r.attempts, 0); // 首见 spec 定版，不算重投
                                   // 再来一帧终报收敛。
        apply_event(&mut t, &frame(3, "task_succeeded", "x", None));
        assert_eq!(t["x"].state, TaskState::Succeeded);
    }

    #[test]
    fn cancelled_terminal_is_sticky() {
        // cancel→cancelling→cancelled；cancelled 后任何帧不可复活。
        let mut t = replay([
            frame(1, "task_spec", "k", None),
            frame(2, "task_accepted", "k", None),
            frame(3, "task_cancel", "k", None),
        ]);
        assert_eq!(t["k"].state, TaskState::Cancelling); // 未确认中间态
        assert!(!t["k"].state.is_terminal());
        apply_event(&mut t, &frame(4, "task_cancelled", "k", None));
        assert_eq!(t["k"].state, TaskState::Cancelled);
        assert!(t["k"].state.is_hard_terminal());
        let before = t.clone();
        for (i, topic) in [
            "task_accepted",
            "task_succeeded",
            "task_progress",
            "task_failed",
        ]
        .iter()
        .enumerate()
        {
            apply_event(&mut t, &frame(5 + i as u64, topic, "k", None));
        }
        assert_eq!(t["k"].state, TaskState::Cancelled); // 状态粘性
        assert_eq!(t["k"].attempts, before["k"].attempts); // 不被伪造推进
        assert!(t["k"].message_count > before["k"].message_count); // 轨迹仍计数
    }

    #[test]
    fn expired_deadline_computation() {
        // expired 计算：deadline 前不翻、超期翻、翻后粘性、无 deadline 永不。
        let dl = "2026-10-18T07:00:00+08:00";
        let d = parse_iso_epoch(dl).expect("deadline 应可解析");
        // 偏移换算自检：+08:00 的 07:00 = UTC 前一日 23:00。
        assert_eq!(parse_iso_epoch("2026-10-17T23:00:00Z"), Some(d));
        let mut t = replay([
            frame(1, "task_spec", "due", Some(json!({"deadline": dl}))),
            frame(2, "task_accepted", "due", None),
            frame(1, "task_spec", "free", None), // 无 deadline
        ]);
        assert!(!is_expired(&t["due"], d)); // 恰好到点不算超期（严格 >）
        assert!(!is_expired(&t["due"], d - 1));
        assert!(is_expired(&t["due"], d + 1));
        assert_eq!(expire_overdue(&mut t, d - 1), Vec::<String>::new()); // 未到期不动表
        assert_eq!(expire_overdue(&mut t, d + 1), vec!["due".to_string()]);
        let r = &t["due"];
        assert_eq!(r.state, TaskState::Expired);
        assert!(r.state.is_terminal());
        assert_eq!(t["free"].state, TaskState::Pending); // 无墙任务不受波及
                                                         // expired 粘性：迟到的 succeeded 不能复活（引擎表赢，设计 §2.1）。
        apply_event(&mut t, &frame(3, "task_succeeded", "due", None));
        assert_eq!(t["due"].state, TaskState::Expired);
        assert_eq!(t["due"].message_count, 3); // 仍计入轨迹
    }

    #[test]
    fn drift_warns_after_three_intervals() {
        // drift：心跳缺勤 > 3×interval 只告警（纯判定，零状态副作用）。
        let mut t = replay([
            frame(
                1,
                "task_spec",
                "m",
                Some(json!({"reconcile": {"mode": "maintain", "interval_s": 60}})),
            ),
            frame(2, "task_accepted", "m", None),
        ]);
        // 心跳基线：seq 3 的 progress 帧 ts = 2026-10-17T03:00:00+08:00。
        let hb = frame(3, "task_progress", "m", None);
        apply_event(&mut t, &hb);
        let base = parse_iso_epoch(hb["ts"].as_str().unwrap()).unwrap();
        assert!(!is_drifting(&t["m"], base + 3 * 60, 60)); // 恰好 3×interval 不告警
        assert!(is_drifting(&t["m"], base + 3 * 60 + 1, 60)); // 缺勤越限告警
        let snapshot = t["m"].clone();
        is_drifting(&t["m"], base + 9999, 60);
        assert_eq!(t["m"], snapshot); // 判定函数不改表——动作策略留给 M2
                                      // pending 行不判 drift（还没开跑）。
        let t2 = replay([frame(1, "task_spec", "p", None)]);
        assert!(!is_drifting(&t2["p"], base + 9999, 60));
    }

    #[test]
    fn projection_parity_state_machine_edges() {
        // 与 router.rs :428 `tasks_projection_state_machine_edges` 逐行对拍：
        // 同一批帧，投影断言的终值必须逐一相等。
        let mk = |seq: u64, topic: &str, id: &str| {
            frame(seq, topic, id, Some(json!({"correlation": "c"})))
        };
        let frames = vec![
            mk(1, "task_spec", "a"),
            mk(2, "task_accepted", "a"),
            mk(3, "task_cancel", "a"), // cancel 无确认 → cancelling（非终）
            mk(4, "task_spec", "b"),
            mk(5, "task_declined", "b"), // declined 终态
            mk(6, "task_spec", "c"),
            mk(7, "task_failed", "c"), // 后续无 accepted → 停 failed
            mk(8, "task_spec", "d"),
            mk(9, "task_failed", "d"),
            mk(10, "task_accepted", "d"), // retry 再派发 → running
            mk(11, "task_accepted", "e"), // 环内无 spec：trail-only 建行仍出现
        ];
        let t = replay(frames);
        assert_eq!(t["a"].state, TaskState::Cancelling);
        assert!(!t["a"].state.is_terminal());
        assert_eq!(t["b"].state, TaskState::Declined);
        assert!(t["b"].state.is_terminal());
        assert_eq!(t["c"].state, TaskState::Failed);
        assert_eq!(t["d"].state, TaskState::Running);
        assert_eq!(t["d"].attempts, 1);
        // specless 任务：投影口径 type/correlation 只从 spec 取 → 全 None，
        // 状态只由轨迹推进（correlation 虽在非 spec 帧 payload 里也不采纳）。
        assert_eq!(t["e"].state, TaskState::Running);
        assert!(t["e"].spec.is_none());
        assert!(t["e"].correlation.is_none());
        assert_eq!(t["e"].task_type, None);
        // 硬终态粘性：declined 后迟到 accepted 不改态（投影 entries2 断言）。
        let t2 = replay([
            mk(1, "task_spec", "b"),
            mk(2, "task_declined", "b"),
            mk(3, "task_accepted", "b"),
        ]);
        assert_eq!(t2["b"].state, TaskState::Declined);
    }

    #[test]
    fn blocked_frames_marked_not_replayed() {
        // 与 router.rs :250 对拍：被拦帧计数、推水位，但不推进状态机。
        let spec = frame(1, "task_spec", "t9", Some(json!({"type": "deploy.demo"})));
        let mut blocked_acc = frame(2, "task_accepted", "t9", None);
        blocked_acc["blocked"] = json!(true);
        let mut t = replay([spec, blocked_acc.clone()]);
        assert_eq!(t["t9"].state, TaskState::Pending); // 被拦 accept 不改态
        assert_eq!(t["t9"].blocked_count, 1);
        assert_eq!(t["t9"].message_count, 2);
        assert_eq!(t["t9"].seq, 2); // 水位照常推进
                                    // 熔断解除后重发的 accept + succeeded 正常收敛到终态。
        apply_event(&mut t, &frame(3, "task_accepted", "t9", None));
        apply_event(&mut t, &frame(4, "task_succeeded", "t9", None));
        assert_eq!(t["t9"].state, TaskState::Succeeded);
        assert_eq!(t["t9"].blocked_count, 1);
    }

    #[test]
    fn record_serde_roundtrip_keeps_spec_verbatim() {
        // P1-1 taskstore 的序列化前提：表行 JSON 往返无损、spec 原文未动。
        let mut t = replay([
            frame(
                1,
                "task_spec",
                "s",
                Some(json!({"type": "a.b", "params": {"k": "v"}})),
            ),
            frame(2, "task_accepted", "s", None),
        ]);
        let js = serde_json::to_string(&t["s"]).unwrap();
        let back: TaskRecord = serde_json::from_str(&js).unwrap();
        assert_eq!(back, t["s"]);
        // 状态序列化为投影同名字符串。
        assert_eq!(
            serde_json::to_value(t["s"].state).unwrap(),
            json!("running")
        );
        apply_event(&mut t, &frame(3, "task_rolled_back", "s", None));
        assert_eq!(
            serde_json::to_value(t["s"].state).unwrap(),
            json!("rolled_back")
        );
    }

    #[test]
    fn redispatched_spec_counts_attempt_not_overwrite() {
        // 引擎重投语义（设计 §2.4 第 4 步）：更高 seq 的再份 spec → attempts+1，
        // 原文不覆盖、状态不改写（状态只由执行方事件推进）。
        let mut t = replay([
            frame(1, "task_spec", "r", Some(json!({"type": "a.b"}))),
            frame(2, "task_accepted", "r", None),
            frame(3, "task_failed", "r", None),
            frame(
                4,
                "task_spec",
                "r",
                Some(json!({"type": "a.b", "seq_note": "engine re-dispatch"})),
            ),
        ]);
        let r = &t["r"];
        assert_eq!(r.state, TaskState::Failed); // 状态等 accepted，不由 spec 伪造
        assert_eq!(r.attempts, 1); // 重投计一次派发
        assert_eq!(r.spec.as_ref().unwrap().get("seq_note"), None); // 原文定版
        assert_eq!(r.seq, 4);
        apply_event(&mut t, &frame(5, "task_accepted", "r", None));
        assert_eq!(t["r"].state, TaskState::Running);
        assert_eq!(t["r"].attempts, 2); // 复活再计一次
    }
}
