# tasklib 事件驱动改造验证（2026-10-17）

范围：`scripts/task-proto/` 参考编排器轮询改事件驱动（`/v1/events` 续传 + 409 降级 + 面板 `/v1/tasks`）。
分支 `feat/tasklib-events`（worktree `~/al-worktrees/al-tasklib-events`，基线 main c490870）。

## 接口

- `GET /v1/events?since=&agent=&types=&limit=`（v0.5 §1，`src/api.rs` `events_get`）
  响应 `{ok, events:[{seq, ts, frame}], next_seq, oldest_seq}`；`since` 传上一轮
  `next_seq` 增量续传。环旋转丢件回 **409**
  `{ok:false, error:"since_before_history", oldest_seq, next_seq}`。鉴权头
  `X-Auth-Token` 沿用现有 `_api` 写法。
- `GET /v1/tasks?correlation=&task_id=&agent=&state=&limit=`（v0.5 §4，`tasks_get`）
  响应 `{ok, tasks:[TaskView], total}`；TaskView 含 `state`（succeeded/declined/
  failed/cancelled/running/cancelling…）、`terminal`、`correlation`、`progress_count`。
- 特性探测：`GET /v1/health` 的 `features` 数组含 `events`（0.4.x 旧节点无该字段/
  无该端点）。
- 脱敏面（v0.5 §2，`src/redact.rs`）：history 环记录的是观察者脱敏副本，
  `/v1/history`、`/v1/events`、`/v1/tasks` 同源；投递给目标 agent 的 WS 帧是原文。
  规则命中的是**字符串值内部文本**（`key:value` 形态 token/secret/password、
  32+ 位 hex、PEM 块）→ `<REDACTED:*>`；JSON 键名与短 hex（uuid 段）不受影响。

## tasklib/taskctl 变更

- `Bus.events(since, agent, types, limit)`、`Bus.tasks(correlation, task_id, agent, state, limit)`、
  `Bus.features()/supports()` 探测缓存、`Bus.stats` 路径计数、`_api` 对非 2xx 回读
  JSON 体并带 `_http_status`（409 判定用）。
- `taskctl.wait()`：优先 `/v1/events` 增量（`agent=orchestrator`、`types=text`、
  limit 2000；截断页按已读最大 seq+1 续传）；409 → `/v1/history` 全量重建该任务
  轨迹恰一次后从 `next_seq` 续传；无 events 特性 → 静默回退 history 轮询（间隔
  0.3s 与旧行为一致；事件路径轮询间隔 0.05s）。实际传输记在
  `bus.last_wait_transport`（`events`/`events+rebuild`/`history`）。
- 匹配一律用结构键 `payload.task_id`+`to_agent`（脱敏面内可存活）；`blob()` 文档
  明确原文 blob 匹配仅限 WS 收件侧。
- `taskctl.py` 新增 `tasks` 子命令与 `panel()`：`/v1/tasks?correlation=` 面板投影。

## 冒烟环境

- 二进制：worktree 产物 `agentlink 0.5.6`
  （`CARGO_TARGET_DIR=~/agentlink-target-eventslib cargo build --release --target x86_64-unknown-linux-gnu`，
  旧 release 二进制为 0.4.19 无 events/tasks，未使用）。
- 隔离测试节点：`listen_port=18768`、`node_id=e7c19a2b4d03`（12 位小写 hex）、
  `discovery.enabled=false`、`local_api.host=127.0.0.1`、data_dir 在 scratch、
  `run --no-open`；`/v1/health` features 实测 `["events","redact","tasks"]`。
  生产 8765 与默认测试口 18765 均未触碰。
- executor/smoke：`/mnt/d/HermesSpace/LiteGuard/agentlink/venv/bin/python`。

## 场景矩阵与结果（SMOKE_RESULT 14/14 passed，连跑两轮全绿）

| # | 场景 | 结果 |
|---|---|---|
| 1-8 | happy/cap-decline/sha-decline/幂等双发/cancel/maintain-drift/金丝雀×2/旧 upgrade_notice 兼容 | PASS（原 9 断言全保留） |
| 9 | tasks_panel：/v1/tasks?correlation=batchSmoke 投影 ≥2 任务、全 terminal、含 declined+succeeded | PASS |
| 10 | redaction_struct_key_match：64 位 hex sha 在 events 视图已 `<REDACTED>`，task_id 结构键仍收敛 | PASS |
| 11 | events_path_p50：9 样本发令→wait() 返回，传输集合 ⊆ {events}，P50 判定 <500ms | PASS |
| 12 | fallback_history_polling（stub 无 events 特性）：/v1/events 零命中，history 轮询收敛 | PASS |
| 13 | events_409_history_rebuild（stub 环旋转）：409 后恰一次全量重建，transport=events+rebuild | PASS |

## P50 实测

- 第 1 轮：**p50 = 56.3 ms**（min 56.0 / max 57.7，n=9，transport=["events"]）
- 第 2 轮复跑：p50 = 59.7 ms（min 58.1 / max 61.1）
- 阈值 500ms 满足，且 evidence 断言 9 个样本全部走事件路径（无一次 history 轮询回退）。

## 遗留/风险

- 真实环旋转（生产流量打满 500 环）未在本轮真节点构造，以 stub 确定性验证降级逻辑；
  如需真机复验可把测试节点 `history.len` 调小后压流量。
- 事件路径轮询仍是短周期 pull（0.05s），内核 §1 的 subscribe/长轮询落地后可再降延迟。
