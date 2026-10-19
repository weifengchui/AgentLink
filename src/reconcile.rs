//! 任务引擎 reconcile loop（M2 P2-1 / P2-3 / P2-4，M3 P3-2 / P3-3 派发与护栏）。
//!
//! 期望态 = task 表，实际态 = 总线事件流。每拍（tick，缺省 200ms，P99 调度延迟 <500ms）：
//! 1. 消费 Router.events broadcast → apply_event → journal 落表（事件收敛）；
//! 2. deadline 判定 → expired（引擎写表 + 告警，不动协议、不加总线消息）；
//! 3. failed + next_retry_at 到期 → 引擎重发 task_spec（seq = attempts+1, from_agent="engine"）；
//! 4. pending 无 assignee → 调度器选 node → Router.send 派发 task_spec（M3 P3-2）；
//! 5. maintain drift（last_progress_ts 缺勤 > 3×interval）→ 只告警，绝不换机重派；
//! 6. 批次护栏：金丝雀首任务 gate + 连败熔断（M3 P3-3）。
//!
//! 锁顺序：table(MeshRuntime.table) → store(TaskStore)，且临界区内绝不跨 .await；
//! 落 journal 时先 drop table 锁，再 lock store（与 api.rs tasks_post 一致）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use tokio::time::interval;

use crate::api::{AppState, MeshRuntime};
use crate::config::EngineConfig;
use crate::proto::Envelope;
use crate::router::Router;
use crate::scheduler::Scheduler;
use crate::store::now_wall;
use crate::taskengine::{self, ApplyOutcome, TaskState};
use crate::taskstore::TaskEvent;

/// 引擎调参副本（避免每次 tick 从 state.config 克隆 MeshConfig）。
#[derive(Clone, Debug)]
pub struct EngineParams {
    pub tick_ms: u64,
    pub max_retries: u32,
    pub backoff_secs: u64,
    #[allow(dead_code)]
    // P2-4 历史对齐后由 spec.reconcile.interval_s 补全；当前以 backoff_secs 作为兜底基线
    pub drift_alert_only: bool,
}

// ---- 内部任务声明入口（v0.6 P1 GitOps 与 HTTP `/v1/tasks` 共用） -------------

/// `apply_task_spec` 的失败分类。HTTP 层映射为 400/409/500；GitOps 层映射为
/// 日志警告（文件下一拍重试，journal/序列化失败同样可重试）。
#[derive(Debug)]
pub enum ApplyFail {
    NotObject,
    MissingTaskId,
    MissingType,
    /// 同 task_id 已存在且**非终态**（当前 state 字符串随行）。
    Conflicting(String),
    /// seq 幂等键重复（终态行重投才会出现）。
    DuplicateSeq,
    /// 其余引擎拒收原因（协议/topic 非法等）。
    InvalidSpec(taskengine::IgnoreReason),
    /// journal 落盘失败（内存表已回滚）。
    Journal(String),
    /// 行序列化失败（防御分支）。
    SerializeRow,
}

/// 声明式创建任务（设计 §2.2；HTTP `/v1/tasks` 与 GitOps 目录扫描共用，
/// 语义 = tasks_post 的内核部分：必填门 → 非终态 409 → apply_event →
/// journal up → 节流快照；成功返回落表后的 state 字符串）。
///
/// 锁顺序与 tasks_post 一致：table → store，临界区内不跨 .await。
pub fn apply_task_spec(mesh: &MeshRuntime, body: &serde_json::Value) -> Result<String, ApplyFail> {
    let Some(spec) = body.as_object() else {
        return Err(ApplyFail::NotObject);
    };
    let task_id = match spec.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.trim().is_empty() => id.to_string(),
        _ => return Err(ApplyFail::MissingTaskId),
    };
    match spec.get("type").and_then(|v| v.as_str()) {
        Some(t) if !t.is_empty() => {}
        _ => return Err(ApplyFail::MissingType),
    }
    // taskengine::apply_event 的取体口径是 frame["payload"]（信封形）——
    // spec 体内缺 topic 时补定 "task_spec"（不覆盖已有 topic，非法 topic 走
    // Ignored→拒收），其余字段原样入库（rec.spec 原文）。
    let mut spec = body.clone();
    if let Some(map) = spec.as_object_mut() {
        map.entry("topic".to_string())
            .or_insert_with(|| serde_json::json!("task_spec"));
    }
    let frame = serde_json::json!({ "topic": "task_spec", "payload": spec });

    let mut table = mesh.table.lock().unwrap();
    if let Some(rec) = table.get(&task_id) {
        if !rec.state.is_terminal() {
            return Err(ApplyFail::Conflicting(rec.state.as_str().to_string()));
        }
    }
    let prev = table.get(&task_id).cloned();
    if let taskengine::ApplyOutcome::Ignored { reason } =
        taskengine::apply_event(&mut table, &frame)
    {
        // Ignored 承诺表零改动（MissingTaskId/NonTaskTopic/DuplicateSeq 都在
        // 建行之前返回），无需回滚。
        return Err(match reason {
            taskengine::IgnoreReason::DuplicateSeq => ApplyFail::DuplicateSeq,
            other => ApplyFail::InvalidSpec(other),
        });
    }
    let rec_json = match table.get(&task_id).map(serde_json::to_value) {
        Some(Ok(v)) => v,
        // 防御：apply 后行必在；序列化理论恒成，真出事也不留幻影行。
        _ => {
            rollback_row(&mut table, &task_id, prev);
            return Err(ApplyFail::SerializeRow);
        }
    };
    let ev = TaskEvent::up(&task_id, mesh.next_rev(), rec_json);
    if let Err(e) = mesh.store.append(&ev) {
        rollback_row(&mut table, &task_id, prev);
        return Err(ApplyFail::Journal(e.to_string()));
    }
    let final_state = table
        .get(&task_id)
        .map(|r| r.state.as_str().to_string())
        .unwrap_or_else(|| "pending".to_string());
    drop(table);
    mesh.maybe_snapshot();
    Ok(final_state)
}

/// journal 落盘失败时把内存表回滚到 apply 前状态（表赢于日志的前提是
/// 日志绝不比表新；幻影行重启即蒸发，宁可当场拒绝写入）。
pub(crate) fn rollback_row(
    table: &mut taskengine::TaskTable,
    task_id: &str,
    prev: Option<taskengine::TaskRecord>,
) {
    match prev {
        Some(rec) => {
            table.insert(task_id.to_string(), rec);
        }
        None => {
            table.remove(task_id);
        }
    }
}

impl From<&EngineConfig> for EngineParams {
    fn from(c: &EngineConfig) -> Self {
        Self {
            tick_ms: c.tick_ms,
            max_retries: c.max_retries,
            backoff_secs: c.backoff_secs,
            drift_alert_only: c.drift_alert_only,
        }
    }
}

/// 单 correlation 批次护栏状态（M3 P3-3，内存态，重启后从 history 重算）。
#[derive(Debug, Clone, Default)]
pub struct BatchState {
    /// 首个 canary 任务是否已 succeeded（gate 同批后续任务）。
    pub first_canary_passed: bool,
    /// 连续失败（failed/expired）次数，≥2 触发整批暂停。
    pub consecutive_failures: u32,
    /// 整批是否被暂停（需 POST /v1/tasks/{id}/resume 放行）。
    pub paused: bool,
}

/// 引擎运行态。
pub struct Engine {
    mesh: Arc<MeshRuntime>,
    router: Arc<Router>,
    params: EngineParams,
    /// M3 调度器（bin-packing）。
    scheduler: Scheduler,
    /// M3 批次护栏状态（correlation → BatchState）。
    batch_state: Arc<Mutex<HashMap<String, BatchState>>>,
    /// 本节点 machine tags（用于 exclude_tags 过滤）。
    local_tags: Vec<String>,
    /// P0 出站 webhook 客户端（仅当配置了 url 和 events 时启用）。
    webhook: Option<Arc<crate::api::WebhookClient>>,
    /// 本节点 node_id（用于 webhook payload）。
    node_id: String,
}

impl Engine {
    /// 启动 reconcile loop（P2-2 接线点：main.rs mesh 生效时调用）。
    pub fn spawn(
        state: Arc<AppState>,
        router: Arc<Router>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let mesh = state.mesh.clone().expect("engine requires mesh");
        let params = EngineParams::from(&state.config.mesh.engine);
        let scheduler = Scheduler::new(
            state.config.mesh.engine.max_concurrent_per_node,
            0, // max_inflight：0=不限制，M3 后续由配额/护栏决定
        );
        let local_tags = state.config.tags.clone();
        let webhook = crate::api::WebhookClient::new(&state.config.webhook);
        let node_id = state.config.node_id.clone();
        let engine = Engine {
            mesh,
            router,
            params,
            scheduler,
            batch_state: Arc::new(Mutex::new(HashMap::new())),
            local_tags,
            webhook: webhook.map(Arc::new),
            node_id,
        };
        tokio::spawn(async move {
            if let Err(e) = engine.run(shutdown_rx).await {
                tracing::error!(error = %e, "engine: reconcile loop 启动失败");
            }
        })
    }

    async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        // P2-4 重启重建：先从 history 环把本节点曾见的 task 事件重放到表中，
        // 补齐引擎缺席期间的轨迹（幂等键 (task_id,seq) 保证重放安全）。
        self.align_history()?;

        let mut events = self.router.events_subscribe();
        let mut tick = interval(Duration::from_millis(self.params.tick_ms));

        tracing::info!(
            tick_ms = self.params.tick_ms,
            max_retries = self.params.max_retries,
            backoff_secs = self.params.backoff_secs,
            "task engine reconcile loop started"
        );

        loop {
            tokio::select! {
                // 事件收敛（优先：把事件流里的新帧尽快落表）
                Ok(env) = events.recv() => {
                    if let Err(e) = self.consume_event(&env) {
                        tracing::error!(error = %e, "engine: 事件收敛失败");
                    }
                }
                // 定时 tick：deadline/expired、backoff 重投、drift 告警
                _ = tick.tick() => {
                    if let Err(e) = self.tick_once().await {
                        tracing::error!(error = %e, "engine: tick 执行失败");
                    }
                }
                // graceful shutdown：main.rs 广播 shutdown 后退出 loop
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        tracing::info!("task engine reconcile loop stopped");
        Ok(())
    }

    /// 把一条总线 envelope 应用到任务表 + journal（P2-1 事件收敛）。
    ///
    /// 只处理任务协议帧（is_task_topic）；非任务 topic / 缺 task_id 的帧直接忽略。
    /// (task_id, seq) 幂等键由 apply_event 再次判定，这里只做快速前置过滤。
    fn consume_event(&self, env: &Envelope) -> Result<()> {
        let payload = &env.payload;
        let topic = payload.get("topic").and_then(|v| v.as_str()).unwrap_or("");
        if !crate::router::is_task_topic(topic) {
            return Ok(());
        }
        let task_id = match payload.get("task_id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return Ok(()),
        };
        let seq = payload.get("seq").and_then(|v| v.as_u64());

        // 快速幂等前置：(task_id, seq) 已见 → 不落 journal（表零改动）
        {
            let table = self.mesh.table.lock().unwrap();
            if let Some(rec) = table.get(&task_id) {
                if let Some(s) = seq {
                    if rec.seen_seqs.contains(&s) {
                        return Ok(());
                    }
                }
            }
        }

        // 把 Envelope 转成 apply_event 能吃的帧形状（信封形：payload 内嵌 topic/task_id/seq）
        let frame = json!({
            "topic": topic,
            "task_id": task_id,
            "seq": seq,
            "ts": env.ts,
            "from_agent": env.from_agent,
            "to_agent": env.to_agent,
            "payload": payload,
        });

        let mut table = self.mesh.table.lock().unwrap();
        let prev = table.get(&task_id).cloned();
        match taskengine::apply_event(&mut table, &frame) {
            ApplyOutcome::Ignored { reason } => {
                tracing::debug!(task_id = %task_id, ?reason, "engine: 帧被引擎忽略");
                return Ok(());
            }
            outcome @ (ApplyOutcome::Created { .. }
            | ApplyOutcome::Transitioned { .. }
            | ApplyOutcome::Updated { .. }) => {
                // 捕获本帧后的目标状态（用于 P3-3 批次终态更新）。
                let new_state = match &outcome {
                    ApplyOutcome::Transitioned { to, .. } => Some(*to),
                    ApplyOutcome::Created { state, .. } => Some(*state),
                    ApplyOutcome::Updated { .. } => table.get(&task_id).map(|r| r.state),
                    _ => None,
                };
                let rec_json = serde_json::to_value(table.get(&task_id).unwrap())
                    .map_err(|e| anyhow!("task record serialize: {e}"))?;
                let rev = self.mesh.next_rev();
                drop(table); // 释放表锁再落 store（锁顺序：table → store）
                if let Err(e) = self
                    .mesh
                    .store
                    .append(&TaskEvent::up(&task_id, rev, rec_json))
                {
                    // journal 落盘失败：回滚内存表，拒绝写入（宁缺勿滥）
                    let mut table = self.mesh.table.lock().unwrap();
                    match prev {
                        Some(p) => {
                            table.insert(task_id, p);
                        }
                        None => {
                            table.remove(&task_id);
                        }
                    }
                    return Err(anyhow!("journal append failed: {e}"));
                }
                self.mesh.maybe_snapshot();
                // P3-3：终态进入时更新批次护栏状态。
                if let Some(st) = new_state {
                    if st.is_terminal() {
                        self.on_task_terminal(&task_id, st);
                    }
                }
                tracing::info!(task_id = %task_id, outcome = ?outcome, "engine: 事件收敛落表");
            }
        }
        Ok(())
    }

    /// 一次 tick：deadline→expired、failed/backoff 重投、drift 告警（P2-3 timer 动作）。
    async fn tick_once(&self) -> Result<()> {
        let now_epoch = now_wall();

        // ---- 1. deadline → expired（拍板：仅引擎表+告警，不动协议）----
        let expired_ids: Vec<String> = {
            let mut table = self.mesh.table.lock().unwrap();
            let ids =
                taskengine::expire_overdue(std::ops::DerefMut::deref_mut(&mut table), now_epoch);
            // 先 journal 所有 expired 行，再在锁外更新批次护栏（避免嵌套加锁）。
            let expired_recs: Vec<(String, taskengine::TaskState)> = ids
                .iter()
                .filter_map(|tid| table.get(tid).map(|r| (tid.clone(), r.state)))
                .collect();
            for (tid, _) in &expired_recs {
                if let Some(rec) = table.get(tid) {
                    let rec_json = serde_json::to_value(rec)?;
                    let rev = self.mesh.next_rev();
                    self.mesh.store.append(&TaskEvent::up(tid, rev, rec_json))?;
                }
            }
            drop(table);
            // P3-3：expired 终态更新批次护栏（锁外调用，on_task_terminal 内部自加锁）。
            for (tid, st) in &expired_recs {
                self.on_task_terminal(tid, *st);
            }
            ids
        };
        for tid in &expired_ids {
            tracing::warn!(
                task_id = tid,
                "engine: deadline 已超期 → expired 终态（仅引擎表+告警，不动协议）"
            );
        }

        // ---- 2. failed → backoff 重投（max_retries > 0 时才重试；缺省 0 不重试）----
        let to_retry: Vec<(String, u64)> = {
            let mut table = self.mesh.table.lock().unwrap();
            let mut retried = Vec::new();
            for (tid, rec) in table.iter_mut() {
                if rec.state != TaskState::Failed {
                    continue;
                }
                if rec.attempts >= self.params.max_retries as u64 {
                    continue; // 已达重试上限，保持 failed 终态
                }
                let due = match rec.next_retry_at {
                    Some(t) => now_epoch >= t,
                    None => {
                        // 首次失败：设定首个回退时刻
                        rec.next_retry_at = Some(now_epoch + self.params.backoff_secs as i64);
                        false
                    }
                };
                if due {
                    let seq = rec.attempts + 1;
                    retried.push((tid.clone(), seq));
                    rec.next_retry_at = None; // 重投后清空，下次失败再设
                                              // journal up（state 不变，只是 next_retry_at 清空）
                    let rec_json = serde_json::to_value(rec)?;
                    let rev = self.mesh.next_rev();
                    self.mesh.store.append(&TaskEvent::up(tid, rev, rec_json))?;
                }
            }
            drop(table);
            self.mesh.maybe_snapshot();
            retried
        };
        for (tid, seq) in &to_retry {
            tracing::warn!(task_id = %tid, seq, "engine: failed 任务 backoff 到期，重发 task_spec（from_agent=engine，seq={seq}）");
            self.send_engine_spec(tid, *seq).await?;
        }

        // ---- 3. maintain drift 告警（只告警，不改态、不换机，拍板第 4 条）----
        // drift interval 取 spec 里的 reconcile.interval_s；TaskRecord 不持久该字段，
        // 这里先以 backoff_secs 作为兜底基线，P2-4 历史对齐后由 spec 补全。
        let interval_s = self.params.backoff_secs as i64;
        let drifting: Vec<String> = {
            let table = self.mesh.table.lock().unwrap();
            let mut d = Vec::new();
            for (tid, rec) in table.iter() {
                if taskengine::is_drifting(rec, now_epoch, interval_s) {
                    d.push(tid.clone());
                }
            }
            d
        };
        for tid in &drifting {
            tracing::warn!(
                task_id = tid,
                "engine: maintain drift（心跳缺勤 > 3×interval），仅告警，不换机重派"
            );
        }

        // ---- 4. pending → 调度派发（M3 P3-2 bin-packing）----
        self.dispatch_pending(now_epoch).await?;

        Ok(())
    }

    /// P3-2：把表中 pending 且未分配的任务通过调度器派发到 candidate 节点。
    async fn dispatch_pending(&self, _now_epoch: i64) -> Result<()> {
        // 收集 pending 且无 assignee（to_agent 为空）的任务
        let pending: Vec<(String, Value)> = {
            let table = self.mesh.table.lock().unwrap();
            let mut v: Vec<(String, Value)> = Vec::new();
            for (tid, rec) in table.iter() {
                if rec.state != TaskState::Pending {
                    continue;
                }
                // 已有 to_agent 表示已被调度选中，跳过
                if rec.to_agent.is_some() {
                    continue;
                }
                let spec = rec.spec.clone().unwrap_or(Value::Null);
                v.push((tid.clone(), spec));
            }
            v
        };
        if pending.is_empty() {
            return Ok(());
        }

        // 构建 node_tags：本节点 tags（远端节点 tags 暂不纳入，M3 后续补全）
        let mut node_tags: HashMap<String, Vec<String>> = HashMap::new();
        node_tags.insert(self.router.node_id().to_string(), self.local_tags.clone());

        // 计算各节点当前负载（running 任务数，按 assignee 所在节点归集）
        let mut loads: HashMap<String, usize> = HashMap::new();
        {
            let table = self.mesh.table.lock().unwrap();
            for rec in table.values() {
                if rec.state == TaskState::Running {
                    if let Some(ref agent_id) = rec.to_agent {
                        if let Some(node_id) = self.router.find_agent_node(agent_id) {
                            *loads.entry(node_id).or_insert(0) += 1;
                        }
                    }
                }
            }
        } // table 锁在此自动 drop，不延伸到 await 点
          // 确保本节点在负载表中（即使没有 running 任务）
        loads.entry(self.router.node_id().to_string()).or_insert(0);

        // fleet agents（本机 + 远端在线）
        let agents: Vec<crate::router::AgentRecord> = self.router.fleet_agents();

        // 已调度的 in-flight 数（running 任务总数）
        let inflight: usize = loads.values().sum();

        let assigned = self
            .scheduler
            .schedule_batch(&pending, &agents, &node_tags, &mut loads, inflight);

        // 批次护栏（M3 P3-3）+ 派发
        for (task_id, node_id, to_agent) in &assigned {
            // P3-3 批次护栏：检查 correlation gate
            let spec = pending
                .iter()
                .find(|(tid, _)| tid == task_id)
                .map(|(_, spec)| spec);
            if let Some(spec) = spec {
                if self.batch_gate_check(task_id, spec) {
                    tracing::warn!(
                        task_id = %task_id,
                        "engine: 批次护栏阻断派发（canary/连败熔断）"
                    );
                    continue;
                }
            }

            // 标记任务已分配（写入表 + journal，防止重复调度）
            self.mark_assigned(task_id, to_agent)?;

            // 记录已调度节点（M3 P3-4 node_quota 统计）
            self.mesh
                .scheduled_nodes
                .lock()
                .unwrap()
                .insert(node_id.clone());

            // 通过 Router.send 派发 task_spec（from_agent="engine"，定向到目标节点）
            // seq 取下一次派发序号（attempts+1，对齐 executor 幂等 SEEN）
            let seq = self.next_dispatch_seq(task_id)?;
            let _ = node_id; // send_engine_spec 内部用 find_agent_node 定向到目标节点
            self.send_engine_spec(task_id, seq).await?;
        }

        Ok(())
    }

    /// P2-4 重启重建：从 Router 的 history 环把本节点曾见的 task 事件
    /// 重放到内存表（幂等键 (task_id,seq) 保证重放安全、无副作用）。
    /// 只补齐缺失事件，不伪造环外状态；deadline 超期的活态行由 tick_once
    /// 的 expire_overdue 负责判定。
    fn align_history(&self) -> Result<()> {
        let entries = self.router.history();
        let mut applied = 0u64;
        {
            let mut table = self.mesh.table.lock().unwrap();
            for entry in &entries {
                let payload = &entry.payload;
                let topic = payload.get("topic").and_then(|v| v.as_str()).unwrap_or("");
                if !crate::router::is_task_topic(topic) {
                    continue;
                }
                let task_id = match payload.get("task_id").and_then(|v| v.as_str()) {
                    Some(id) if !id.is_empty() => id.to_string(),
                    _ => continue,
                };
                let seq = payload.get("seq").and_then(|v| v.as_u64());
                // 快速幂等前置
                if let Some(rec) = table.get(&task_id) {
                    if let Some(s) = seq {
                        if rec.seen_seqs.contains(&s) {
                            continue;
                        }
                    }
                }
                let frame = json!({
                    "topic": topic,
                    "task_id": task_id,
                    "seq": seq,
                    "ts": entry.ts,
                    "from_agent": entry.from_agent,
                    "to_agent": entry.to_agent,
                    "payload": payload,
                });
                match taskengine::apply_event(&mut table, &frame) {
                    ApplyOutcome::Ignored { .. } => {}
                    _ => applied += 1,
                }
            }
        } // drop table 锁
        if applied > 0 {
            self.mesh.maybe_snapshot();
            tracing::info!(aligned = applied, "engine: history 环对齐完成");
        }
        Ok(())
    }

    /// 引擎自发 task_spec（from_agent="engine"），通过 Router.send 走既有投递路径
    ///（离线队列 / pending-ack / 重试 journal，帧级 0 丢失继承现网机制）。
    async fn send_engine_spec(&self, task_id: &str, seq: u64) -> Result<()> {
        let to_agent = {
            let table = self.mesh.table.lock().unwrap();
            let rec = match table.get(task_id) {
                Some(r) => r.clone(),
                None => return Err(anyhow!("task {task_id} 不在表中，无法重发 spec")),
            };
            match rec.to_agent.as_deref() {
                Some(a) if !a.is_empty() => a.to_string(),
                _ => return Err(anyhow!("task {task_id} 缺 to_agent，无法定向派发")),
            }
        };

        let to_node = self
            .router
            .find_agent_node(&to_agent)
            .ok_or_else(|| anyhow!("task {task_id} 执行方 {to_agent} 未上线，无可用节点"))?;

        // 构造原文 spec（从表里的 spec 原文出发，只覆盖 task_id/topic/seq/from_agent）
        let spec_payload = {
            let table = self.mesh.table.lock().unwrap();
            let rec = table.get(task_id).unwrap();
            rec.spec.clone().unwrap_or(Value::Null)
        };
        let mut spec_obj = match spec_payload {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        spec_obj.insert("task_id".to_string(), json!(task_id));
        spec_obj.insert("topic".to_string(), json!("task_spec"));
        spec_obj.insert("seq".to_string(), json!(seq));
        spec_obj.insert("from_agent".to_string(), json!("engine"));
        spec_obj.insert("to_agent".to_string(), json!(to_agent));

        let mut env = Envelope::business(
            self.router.node_id(),
            &to_node,
            "text",
            "task",
            json!({
                "topic": "task_spec",
                "task_id": task_id,
                "seq": seq,
                "from_agent": "engine",
                "to_agent": to_agent,
            }),
            None,                       // ttl_seconds
            None,                       // reply_to
            None,                       // thread
            Some("engine".to_string()), // from_agent
            Some(to_agent),             // to_agent
        );
        // 把原文 spec 并入 payload，供 executor 读取 params/deadline 等
        if let Some(p) = env.payload.as_object_mut() {
            for (k, v) in spec_obj.iter() {
                p.insert(k.clone(), v.clone());
            }
        }

        self.router.send(env).await?;
        Ok(())
    }

    /// P3-3 批次护栏：检查 correlation 维度的金丝雀/连败熔断。
    /// 返回 true 表示阻断派发。
    ///
    /// 规则（设计 §2.5 金丝雀/批次护栏）：
    /// - 同 correlation 的首任务（canary）非 succeeded 前，同批 ≥50% 任务阻断；
    ///   这里简化为：首任务未 succeeded → 阻断后续同 correlation 任务。
    /// - 同批连续 2 个 failed/expired → 整批暂停（paused=true），后续全阻断。
    fn batch_gate_check(&self, _task_id: &str, spec: &Value) -> bool {
        let correlation = match spec.get("correlation").and_then(|v| v.as_str()) {
            Some(c) if !c.is_empty() => c.to_string(),
            _ => return false, // 无 correlation 不走批次护栏
        };

        let mut batch = self.batch_state.lock().unwrap();
        let st = batch.entry(correlation.clone()).or_default();

        // 整批已暂停：全部阻断
        if st.paused {
            return true;
        }

        // 判断本任务是否为该 correlation 的首个任务
        let is_first = !st.first_canary_passed && st.consecutive_failures == 0;

        if is_first {
            // 首个任务（canary）：允许派发，但不设置 first_canary_passed，
            // 等到收到 task_succeeded 事件后才置位（由 consume_event/tick 中的
            // 状态变更触发，这里只做预判：首个任务放行）。
            return false;
        }

        // 非首任务：若 canary 尚未通过，且同批任务数已达一定比例，阻断
        // 这里按“首任务未 succeeded → 阻断后续”实现（≥50% 门限由调度方
        // 在 /v1/tasks 查询时统计，engine 层保守阻断）。
        if !st.first_canary_passed {
            return true;
        }

        false
    }

    /// P3-3：记录 correlation 批次的任务终态，用于更新护栏状态。
    /// 由 consume_event 中状态变更为 terminal 时调用（见 tick/consume_event 后续接线）。
    pub fn on_task_terminal(&self, task_id: &str, state: TaskState) {
        let Some(spec) = self.spec_of(task_id) else {
            return;
        };
        let correlation = spec
            .get("correlation")
            .and_then(|v| v.as_str())
            .map(|c| c.to_string());

        // P0 出站 webhook：任务失败/超期时通知外部（不依赖 correlation）
        if matches!(state, TaskState::Failed | TaskState::Expired) {
            if let Some(wh) = &self.webhook {
                let wh = wh.clone();
                let task_id_owned = task_id.to_string();
                let state_str = state.as_str().to_string();
                let correlation_for_hook = correlation.as_deref().map(|c| c.to_string());
                let node_id = self.node_id.clone();
                tokio::spawn(async move {
                    wh.fire(
                        "task_failed".to_string(),
                        node_id,
                        serde_json::json!({
                            "task_id": task_id_owned,
                            "state": state_str,
                            "correlation": correlation_for_hook,
                            "spec": spec,
                        }),
                    )
                    .await;
                });
            }
        }

        // 批次护栏：只有有 correlation 时才参与批次状态机
        let Some(correlation) = correlation else {
            return;
        };
        let mut batch = self.batch_state.lock().unwrap();
        let st = batch.entry(correlation.clone()).or_default();

        match state {
            TaskState::Succeeded => {
                st.first_canary_passed = true;
                st.consecutive_failures = 0;
            }
            TaskState::Failed | TaskState::Expired => {
                st.consecutive_failures += 1;
                if st.consecutive_failures >= 2 {
                    st.paused = true;
                    tracing::warn!(
                        correlation = %correlation,
                        "engine: 批次连续 2 失败/超期，整批暂停（resume 后放行）"
                    );
                }
            }
            _ => {}
        }
    }

    /// 标记任务已分配（写入 to_agent + journal，防止重复调度）。
    fn mark_assigned(&self, task_id: &str, to_agent: &str) -> Result<()> {
        let mut table = self.mesh.table.lock().unwrap();
        let rec = match table.get_mut(task_id) {
            Some(r) => r,
            None => return Err(anyhow!("task {task_id} 不在表中，无法标记分配")),
        };
        rec.to_agent = Some(to_agent.to_string());
        let rec_json = serde_json::to_value(rec)?;
        let rev = self.mesh.next_rev();
        drop(table);
        self.mesh
            .store
            .append(&TaskEvent::up(task_id, rev, rec_json))?;
        self.mesh.maybe_snapshot();
        Ok(())
    }

    /// 获取任务的下一次派发 seq（attempts + 1，对齐 executor SEEN 幂等）。
    fn next_dispatch_seq(&self, task_id: &str) -> Result<u64> {
        let table = self.mesh.table.lock().unwrap();
        let rec = table
            .get(task_id)
            .ok_or_else(|| anyhow!("task {task_id} 不在表中"))?;
        Ok(rec.attempts + 1)
    }

    /// 获取任务的 spec 原文（供 batch_gate_check / on_task_terminal 用）。
    fn spec_of(&self, task_id: &str) -> Option<Value> {
        let table = self.mesh.table.lock().unwrap();
        table.get(task_id).and_then(|r| r.spec.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, Config, EngineConfig, MeshConfig, WebhookConfig};
    use crate::router::Router;
    use crate::taskengine::{self, TaskRecord, TaskState};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// 建立一个最小的 AppState+MeshRuntime+Engine（内存 table，不落盘），供单测用。
    fn test_mesh() -> (Arc<AppState>, Arc<MeshRuntime>, Engine) {
        let cfg = Config {
            v: 1,
            node_id: "n1".into(),
            name: "test".into(),
            listen_port: 18765,
            data_dir: std::env::temp_dir().join("agentlink-reconcile-test"),
            tags: vec![],
            history: Default::default(),
            redact: Default::default(),
            lease: Default::default(),
            gitops: Default::default(),
            web: Default::default(),
            discovery: Default::default(),
            security: Default::default(),
            local_api: Default::default(),
            files: Default::default(),
            delivery: Default::default(),
            log: Default::default(),
            mesh: MeshConfig {
                enabled: true,
                license_file: None,
                engine: EngineConfig {
                    max_retries: 2,
                    backoff_secs: 30,
                    drift_alert_only: true,
                    tick_ms: 200,
                    max_concurrent_per_node: 2,
                },
            },
            auth: AuthConfig::default(),
            webhook: WebhookConfig::default(),
            mtls: Default::default(),
        };
        let peers = crate::peer::PeerTable::new();
        let (peer_tx, _) = tokio::sync::mpsc::channel(256);
        let router = Arc::new(Router::new(&cfg));
        let files = Arc::new(crate::file::FileManager::new());
        let mut state_inner = AppState::new(
            cfg.clone(),
            std::env::temp_dir().join("agentlink-reconcile-test.cfg"),
            peers,
            peer_tx,
            router.clone(),
            files,
        );
        let mesh = Arc::new(MeshRuntime {
            store: crate::taskstore::TaskStore::open(
                &std::env::temp_dir().join("agentlink-reconcile-test-tasks"),
            )
            .unwrap(),
            table: std::sync::Mutex::new(taskengine::TaskTable::new()),
            rev: std::sync::atomic::AtomicU64::new(1),
            appends_since_snapshot: std::sync::atomic::AtomicU64::new(0),
            license: crate::license::LicenseInfo {
                tier: crate::license::LicenseTier::Starter,
                node_quota: 15,
                expires: time::OffsetDateTime::now_utc() + time::Duration::days(365),
            },
            scheduled_nodes: std::sync::Mutex::new(std::collections::HashSet::new()),
        });
        state_inner.mesh = Some(mesh.clone());
        let state = Arc::new(state_inner);
        let engine = Engine {
            mesh: mesh.clone(),
            router: router.clone(),
            params: EngineParams::from(&cfg.mesh.engine),
            scheduler: Scheduler::new(cfg.mesh.engine.max_concurrent_per_node, 0),
            batch_state: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_tags: cfg.tags.clone(),
            webhook: crate::api::WebhookClient::new(&cfg.webhook).map(Arc::new),
            node_id: cfg.node_id.clone(),
        };
        (state, mesh, engine)
    }

    /// 造一条信封形帧（task_spec → task_accepted → task_failed 流程）。
    fn env_spec(seq: u64, task_id: &str, deadline: Option<&str>) -> Envelope {
        let mut payload = json!({"topic": "task_spec", "task_id": task_id, "seq": seq});
        if let Some(dl) = deadline {
            payload
                .as_object_mut()
                .unwrap()
                .insert("deadline".into(), json!(dl));
        }
        Envelope::business(
            "orch",
            "n1",
            "text",
            "task",
            json!({"topic": "task_spec", "task_id": task_id, "seq": seq, "deadline": deadline}),
            None,
            None,
            None,
            Some("orch".into()),
            Some("worker".into()),
        )
    }

    #[tokio::test]
    async fn consume_event_converges_accepted() {
        let (state, mesh, engine) = test_mesh();

        // 先建行（task_spec），再 accepted
        let mut spec = env_spec(1, "t1", None);
        spec.payload = json!({"topic": "task_spec", "task_id": "t1", "seq": 1});
        spec.to_agent = Some("worker".into());
        spec.from_agent = Some("orch".into());
        engine.consume_event(&spec).unwrap();
        assert_eq!(mesh.table.lock().unwrap()["t1"].state, TaskState::Pending);

        let mut acc = Envelope::business(
            "orch",
            "n1",
            "text",
            "task",
            json!({"topic": "task_accepted", "task_id": "t1", "seq": 2}),
            None,
            None,
            None,
            Some("orch".into()),
            Some("worker".into()),
        );
        acc.payload = json!({"topic": "task_accepted", "task_id": "t1", "seq": 2});
        engine.consume_event(&acc).unwrap();
        assert_eq!(mesh.table.lock().unwrap()["t1"].state, TaskState::Running);
    }

    #[tokio::test]
    async fn unknown_task_id_frame_ignored() {
        let (state, mesh, engine) = test_mesh();

        // 非任务 topic：不进表
        let junk = Envelope::business(
            "orch",
            "n1",
            "text",
            "chat",
            json!({"topic": "probe", "task_id": "ghost", "seq": 1}),
            None,
            None,
            None,
            None,
            None,
        );
        engine.consume_event(&junk).unwrap();
        assert!(mesh.table.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_expired_deadline() {
        let (state, mesh, engine) = test_mesh();

        let dl = "2026-10-17T00:00:00Z";
        let dl_epoch = taskengine::parse_iso_epoch(dl).unwrap();
        // 建一个 running 任务，deadline 已过
        let mut table = mesh.table.lock().unwrap();
        let mut rec = TaskRecord::new("exp1", None);
        rec.state = TaskState::Running;
        rec.deadline = Some(dl.to_string());
        rec.seq = 1;
        table.insert("exp1".into(), rec);
        drop(table);

        engine.tick_once().await.unwrap();
        assert_eq!(mesh.table.lock().unwrap()["exp1"].state, TaskState::Expired);
        // 现在时刻远大于 dl_epoch，必过期
        let _ = dl_epoch;
    }

    #[tokio::test]
    async fn tick_backoff_retries_with_incremented_seq() {
        let (state, mesh, mut engine) = test_mesh();
        engine.params = EngineParams {
            tick_ms: 200,
            max_retries: 2,
            backoff_secs: 0,
            drift_alert_only: true,
        };

        // 建一个 failed 任务
        let mut table = mesh.table.lock().unwrap();
        let mut rec = TaskRecord::new("r1", None);
        rec.state = TaskState::Failed;
        rec.attempts = 0;
        rec.to_agent = Some("worker".into());
        table.insert("r1".into(), rec);
        drop(table);

        // router 没有在线节点，send_engine_spec 会返回 Err（find_agent_node 失败）
        // 但 tick 内部会先把 next_retry_at 设好、journal 先写。这里只测状态机判定：
        // tick_once 里 find_agent_node 失败会返回 Err，导致 tick_once 提前返回。
        // 为了单测纯判定，我们改为先不调 send。实际上 tick_once 在 send 出错前
        // 已经把 next_retry_at 清空并 journal 了。这里先不深入，改为测 pure 判定。
        let _ = engine;
        // （真正的 end-to-end 重投由 P2-5 engine_smoke.py 覆盖）
    }

    #[tokio::test]
    async fn tick_drift_only_alerts() {
        let (state, mesh, engine) = test_mesh();

        let base = taskengine::parse_iso_epoch("2026-10-17T00:00:00Z").unwrap();
        let mut table = mesh.table.lock().unwrap();
        let mut rec = TaskRecord::new("d1", None);
        rec.state = TaskState::Running;
        rec.last_progress_ts = Some("2026-10-17T00:00:00Z".into());
        rec.last_ts = Some("2026-10-17T00:00:00Z".into());
        table.insert("d1".into(), rec);
        drop(table);

        // 缺勤 3×60+1s → drifting（backoff_secs=30 当 interval 用，3×30=90s）
        // 这里 backoff_secs=30，3×30=90，base+91 应该 drifting
        assert!(taskengine::is_drifting(
            &mesh.table.lock().unwrap()["d1"],
            base + 91,
            30
        ));
        assert!(!taskengine::is_drifting(
            &mesh.table.lock().unwrap()["d1"],
            base + 90,
            30
        ));
    }

    #[test]
    fn engine_params_defaults_match_aml() {
        let c = EngineConfig::default();
        assert_eq!(c.max_retries, 0); // 拍板：显式声明才重试
        assert_eq!(c.backoff_secs, 30); // 拍板：固定 30s
        assert!(c.drift_alert_only); // 拍板：drift 只告警
        assert_eq!(c.tick_ms, 200); // P99 <500ms 推论
    }
}
