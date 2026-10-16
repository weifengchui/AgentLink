# v0.5 内核 §4 验证记录：/v1/tasks 任务资源视图（2026-10-17）

范围：`docs/v0.5-内核待发清单.md` §4——把 task 消息轨迹投影成任务对象（`GET /v1/tasks?correlation=&state=`），**纯派生视图**：从 history 环即时回放协议 §3 状态机，非独立存储，保持 append-only 单一事实源。§5：`/v1/health` `features` 增项 `"tasks"`。全部加性接口，不改任何现有行为。

## 单元与静态门

- `cargo test --bin agentlink`：8 组单测全过（cap_matches 1 组 + `tasks_projection` 3 组：全生命周期、状态机边界、过滤与排序）。
- `cargo fmt --all` + `cargo check` + `cargo clippy --all-targets`（target `x86_64-unknown-linux-gnu`）：0 warning、0 error。
- Windows 交叉构建（llvm-mingw，`x86_64-pc-windows-gnu` release）编译通过。

## 端到端（scripts/tasks-smoke.py，隔离总线 18767，生产 8765/18765 未触碰）

17/17 PASS（release 版 Linux 二进制，脚本自带节点起停与独立 scratch 数据目录，`python3 scripts/tasks-smoke.py <binary>`，退出码 0=全过）：

| 场景 | 结果 | 验收对应 |
|---|---|---|
| S1 `/v1/health` `features:["events","redact","tasks"]` | PASS | 清单 §5 能力探测 |
| S1b 无 token 访问 `/v1/tasks` → 401 | PASS | 本地 API token 鉴权 |
| S2 spec→accepted→progress×2→succeeded（`correlation=batchX`）投影 `succeeded` 终态；`terminal=true`、`progress_count=2`、`message_count=5`、`spec` 原样、type/intent 取自 spec | PASS | §4 验收「smoke 场景跑完后返回各任务终态」 |
| S3 spec→declined 投影 `declined` 终态 | PASS | 协议 §1 拒绝终态 |
| S4 收 `task_cancel` 未收 `task_cancelled` → 中间态 `cancelling`（`terminal=false`） | PASS | §3 状态机投影 |
| S4b 环内无 spec、只有 `task_accepted` 的 task_id 也出现：state 按轨迹推（running），type/correlation/spec 为 null | PASS | 实现要求 2 |
| S5a `task_failed` 后无 `task_accepted` → `failed` 终态 | PASS | 内核简化语义（见下） |
| S5b 响应按 `seq` 倒序 | PASS | 实现要求 3 |
| S5c `?correlation=batchX` 只命中 batchX 三任务（total=3） | PASS | §4 验收查询 |
| S5d `?state=succeeded,failed` 逗号多值过滤 | PASS | 实现要求 2 |
| S5e `?task_id` 精确命中 | PASS | 实现要求 2 |
| S5f `?agent=orch` 命中 from/to 参与方 | PASS | 实现要求 2 |
| S5g `?limit=2` 页化且 `total` 仍为过滤后全匹配数 | PASS | 分页语义 |
| S6 普通 text 消息（`topic=probe`）不进入视图（total 不变） | PASS | 纯 task topic 命中 |
| S7 全新 data_dir 只发普通消息 → `tasks` 为空数组、`ok=true`、`total=0` | PASS | 空视图语义 |

## 接口规格

`GET /v1/tasks?correlation=&state=&task_id=&agent=&limit=`（`X-Auth-Token` 鉴权；参数全部可选，缺省匹配全部；`limit` 默认 200、钳 1–2000）。

响应：`{"ok":true,"tasks":[…],"total":N}`，按任务最后一条消息 `seq`（同值时 `last_ts`）倒序；`total` 为过滤后、截断前的任务数。每个任务对象字段：

`task_id` / `state` / `type` / `intent` / `correlation` / `from_agent` / `to_agent` / `spec`（task_spec payload 原样，即 §2 观察者脱敏件；环内无 spec 时为 null）/ `seq` / `created_ts` / `last_ts` / `progress_count` / `message_count` / `terminal`。

状态取值：`pending`（初始）/ `running` / `cancelling` / `succeeded` / `declined` / `cancelled` / `rolled_back` / `failed`；`terminal=true` 的只有后四个加 `failed`。

## 实现要点

- **投影位置**：`src/router.rs` 纯函数 `tasks_projection(entries,&TaskFilter)->Vec<TaskView>`（可单测、无锁语义在 `Router::tasks_view` 单点持环锁后交给它）；handler 在 `src/api.rs` `tasks_get`，路由注册紧随 `/v1/events`。直接吃环内条目——payload 入环前已经过 redactor（§2），视图天然观察者安全。
- **topic 命中**：精确集合（协议 §1 的 12 个 `task_*` topic），`payload.task_id` 缺失/为空串的条目跳过（无法分组）。
- **状态回放**：按环序（seq 单调）逐条应用协议 §3：accepted→running；succeeded/declined/cancelled/rolled_back→对应终态且**粘滞**（硬终态后任何消息不再改写状态）；`task_progress` 只计数与刷新 `last_ts`，不改状态。
- **内核侧简化（有意的偏差，已在代码注释注明）**：
  1. `task_failed` 若无后续 `task_accepted` 即投影为 `failed`（终态展示）；协议 §3 允许预算内重试回 pending——内核不判定预算，**编排方可按协议 §4 reconcile 重派**，重派后的新 `task_accepted` 会把同 task_id 投影回 `running`。
  2. `task_cancel` 指令已发、`task_cancelled` 确认未到时投影中间态 `cancelling`；协议本身无此态名，是内核「如实投影消息轨迹」的可见性补充，编排方可按非终态处理。
  3. `monitor` 模式永不 succeeded 属编排方语义（协议 §4），内核只如实反映收到的消息。
- **from/to 聚合**：有 spec 时取 spec 条目的 `from_agent`/`to_agent`（发起→执行的权威方向）；无 spec 时取环内首见值。
- 无历史数据兼容：旧盘行 `seq=0` 时排序回退 `last_ts`。

## 待办 / 风险

- 视图只覆盖环内（及重启恢复部分）轨迹：环满被轮换掉的早期 spec 会退化为「无 spec 任务」——与 §1 事件流同源限制，编排方需全量审计时仍走 `/v1/history`。
- web console 未接 `/v1/tasks`（清单未要求）；留作后续轮。
- 参考实现 orchestrator（scripts/task-proto）切用 `/v1/tasks` 收敛任务面板属后续轮，协议不变。

复跑方法：`CARGO_TARGET_DIR=~/agentlink-target cargo build --release --target x86_64-unknown-linux-gnu && python3 scripts/tasks-smoke.py ~/agentlink-target/x86_64-unknown-linux-gnu/release/agentlink`（自带隔离节点起停与清理，端口 18767，绝不触碰生产）。
