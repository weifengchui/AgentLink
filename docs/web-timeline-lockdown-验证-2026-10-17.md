# v0.5 内核验证记录：/web 任务 Timeline 视图 + 一键熔断 lockdown（2026-10-17）

范围：`docs/版本规划-AgentMesh路线图.md` §3.2（/web 任务 timeline 视图）与 §4.5（一键熔断薄档）。分支 `feat/v0.5-web-timeline`（基线 `ccdf69b`）。响应形状示例参见 `docs/tasks-view-验证-2026-10-17.md`（`/v1/tasks` 既有契约，本次只增不改）。

## 一、lockdown 薄档语义（§4.5 档定义）

- **管理面**：`POST /v1/lockdown {"on":true|false}` 开/关总线熔断；`GET /v1/lockdown` 查询状态与累计计数。两者均需本地 API 管理 token（`X-Auth-Token`，与全部管理端点同一鉴权路径 `authorized()`）。`on` 为必填字段，无隐式默认——这是刻意的人工闸。
- **拦截范围**：熔断期间总线暂停投递/转发**业务帧**——agent→agent 消息（`kind=msg/text/file`）与任务协议帧；**控制面帧**（`kind=ctrl`：heartbeat/ack/agents_update/注册/agent_status，以及 `kind=event`：presence 广播）照常放行。判定是纯函数 `router::lockdown_blocks_frame(kind)`，单测覆盖。
- **查询面不受影响**：HTTP 侧 `/v1/health`、`/v1/history*`、`/v1/events`、`/v1/tasks`、`/v1/lockdown`、`/web` 不经闸门，熔断期间照常可读（这正是运维看轨迹、做决策的通道）。
- **恢复后不补投**（薄档核心取舍）：`off` 之后新帧照常流转，此前被拦的帧**不重放、不排队**——`/v1/lockdown` 响应固定 `"replay_on_resume": false`。发送方收到的是 `/v1/send` 400（error 含 `lockdown`）；跨节点入站帧连接层照常 ack（发送方停止重发队列），业务上不落地。
- **审计可见**：被拦帧写入 history 环并带 `blocked: true` 标记（`Envelope`/`HistoryEntry` 新增 `blocked: Option<bool>`，`skip_serializing_if="Option::is_none"`，旧记录零迁移），`/v1/events` SSE 同带；`/v1/tasks` 每任务新增 `blocked_count` 与 `trail[].blocked`；顶栏徽章显示累计拦截数。
- **状态不落盘**：`lockdown` 是运行时 `AtomicBool`（连同 `blocked_out`/`blocked_in` 计数），节点重启默认恢复放行——人工应急闸语义，避免无人值守时重启即自锁死。

## 二、/web 任务 Timeline 视图（§3.2）

- 新增「任务」页签（`data-p="tasks"`），数据源 `GET /v1/tasks?correlation=&state=`，沿用现有 token 鉴权与端点注册方式；纯内嵌 HTML/CSS/JS（`src/api.rs` 常量 HTML），风格与 starmap 面板同族（同卡片/badge/tbtn/who 色点体系）。
- 每个任务一张卡片：状态徽章（含批次 done/total）、type·task_id、correlation 一键过滤、发起/承接方 chip（点击即按 节点/agent 过滤，沿用第五/六轮 history 四维过滤：批次/状态/发起方/承接方 + 关键词）、横向 timeline 按 `trail` 帧序画 `task_spec→task_accepted→task_progress→终态`，节点色：发起=琥珀、接受/成功=绿、失败/拒绝/取消=红、进度=蓝、**被拦=灰虚线+🚫**（tooltip 注明"未投递/转发，不补投"）。
- 省 token：面板隐藏时不轮询（`loadTasks(false)` 直接 return），打开页签或手动查询才拉 `/v1/tasks`；与主 `refresh()` 共用 2s 自动刷新开关。history 视图对 `blocked=true` 帧加 🚫 chip 与灰色样式。
- 顶栏常驻熔断闸：状态灯（开=红/关=绿）+「拦N」badge + `🔒 熔断` 按钮；点击弹**二次确认** `confirm()`（写明拦截范围与不补投语义），`POST /v1/lockdown` 翻转并 toast 结果；状态由每轮 `refresh()` 的 `GET /v1/lockdown` 常显（失败降级 null 不打断主视图）。

## 三、静态门与单测

- `cargo check`：0 error。
- `cargo clippy --all-targets -- -D warnings`：0 warning。
- `cargo test --bin agentlink`：**11 组全过**（基线 8 组 + 新增 3 组）：
  - `lockdown_blocks_frame_rules`：msg/file/task 拦、ctrl/event 放。
  - `tasks_projection_blocked_frames_marked_not_replayed`：被拦 accept 不推进状态机（保持 pending）、`blocked_count=1`、trail 带标记；恢复后重发帧正常推进到 `succeeded` 终态、trail 完整保留被拦帧。
  - `router_lockdown_gates_send_and_incoming`（tokio）：未熔断畅通 → 开启后 `send()` 返回含 lockdown 的 Err、`blocked_out=1`、history 落 `blocked=true` 审计、ctrl 帧放行不计数、`handle_incoming` 对入站业务帧返回 false 且 `blocked_in=1` → 恢复后新帧放行、blocked 记录不再新增（不补投）。

## 四、端到端冒烟（scripts/web-timeline-lockdown-smoke.py，隔离总线 18772）

独立 scratch 数据目录（`al-v05-lkd-testdata`），`discovery.enabled=false`、`web.open=false`，**全程未触碰生产 127.0.0.1:8765、未与在线 agent 通信**。用法 `python3 scripts/web-timeline-lockdown-smoke.py <agentlink-binary>`（Linux debug 二进制）。**12/12 PASS**：

| # | 断言 | 结果 |
|---|---|---|
| A1 | `/v1/health` `features` 含 `lockdown`（`["events","redact","tasks","lockdown"]`） | PASS |
| A2 | `GET`/`POST /v1/lockdown` 无 token 均 401 | PASS |
| A3 | 初态 `on=false`、`blocked_count=0`、`replay_on_resume=false` | PASS |
| A4 | 正常态 `task_spec→task_accepted` 投影 `running`，trail 2 帧均 `blocked=false` | PASS |
| A5 | `POST {"on":true}` 开启熔断 | PASS |
| A6 | 熔断中业务帧 `/v1/send` 400（error 含 lockdown）；`ctrl` 帧 200；`/v1/history`、`/v1/tasks` 查询畅通 | PASS |
| A7 | `blocked_count=1`、`blocked_out=1`，history 出现 `blocked:true` 审计记录 | PASS |
| A8 | 熔断中被拦 `task_progress`：state 仍 `running`、`blocked_count=1`、trail 末帧 `blocked=true`、`progress_count=0`（不推进状态机） | PASS |
| A9 | `POST {"on":false}` 恢复；`blocked_count` 保留为累计 2 | PASS |
| A10 | 恢复后新帧 200 放行；blocked 记录不再新增（不补投）；重发 `task_progress` 正常进 trail 且 `progress_count=1` | PASS |
| A11 | `GET /web` HTML 含任务页签/时间线容器/`tk-corr`/`tk-state`/熔断按钮与 `toggleLockdown`/`/v1/lockdown`/`blocked_count` 关键元素 | PASS |
| A12 | HTML 内嵌 topic 词表覆盖 `/v1/tasks` 实际返回的全部 topic（spec/accepted/progress），即渲染与数据契约一致 | PASS |

回归：`scripts/tasks-smoke.py`（旧 `/v1/tasks` 契约）17/17 PASS，确认新增字段为纯加性变更。

## 五、接口增改清单

- `GET/POST /v1/lockdown`（新）：`{"ok":true,"on":bool,"blocked_count":N,"blocked_out":N,"blocked_in":N,"replay_on_resume":false}`。
- `/v1/health`：`features` 追加 `"lockdown"`。
- `/v1/history`、`/v1/events`：条目可选新增 `blocked: true`（仅被拦帧，其余记录不变）。
- `/v1/tasks`：每个任务新增 `blocked_count: u64` 与 `trail: [{seq,ts,topic,msg_id,from_agent?,to_agent?,direction,blocked}]`（trail 常显 `blocked` bool，薄投影不带 payload 全文，详情仍在 history）。
- 内核：`Envelope/HistoryEntry` 加 `blocked: Option<bool>`（serde default，向后兼容）；`Router` 加 `lockdown/blocked_out/blocked_in` 运行时原子状态；`send()` 与 `handle_incoming()` 各插一道闸；`record_history_marked()` 支持审计标记写入。
- 未改：`conn.rs`（被拦帧照常 ack 正是"不补投"所需）、store/config/版本号。
