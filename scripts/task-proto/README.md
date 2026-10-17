# task-proto — 任务协议 v1 参考实现

对 `docs/task-protocol.md` 的可执行参考：编排方、执行方、冒烟场景。纯 payload 层约定，**任何 0.4.x+ 总线均可跑**，无需内核改动。

## 快速开始（仅限隔离测试总线）

```bash
# 1. 测试总线（端口必须与生产 8765 隔离，防串入生产 mesh——铁律）
agentlink init --config test.json   # 或手写：listen_port=18765, node_id 必填, local_api.token 自定
agentlink --config test.json run --no-open

# 2. 执行者（agent=worker，capabilities 见 executor.py）
venv-python executor.py --base http://127.0.0.1:18765 --token <T>

# 3. 冒烟全场景（happy/decline/幂等/cancel/drift/金丝雀/兼容层/面板/脱敏匹配/P50/回退/409降级）
TASKPROTO_BASE=http://127.0.0.1:18765 TASKPROTO_TOKEN=<T> venv-python smoke.py evidence.json

# 手工单发
venv-python taskctl.py submit --type rollout.node_version --params '{"target_version":"0.5.0"}'
```

## 文件

| 文件 | 角色 |
|---|---|
| tasklib.py | 总线客户端封装：REST(send/register/history/events/tasks)、WS URL、spec/msg 构造、features 特性探测、回报哨兵（blob 全文匹配，仅限 WS 收件侧，见下） |
| executor.py | 参考执行者：WS 收 spec→状态机回报；能力门/SHA 门/幂等/取消/maintain 心跳/旧 upgrade_notice 兼容 |
| taskctl.py | 参考编排者：submit+wait 收敛（优先 /v1/events 事件驱动，见下）；drift 检测基于"新心跳出现时刻"（勿每次轮询刷新——必测坑）；金丝雀闸口；`tasks` 命令走 /v1/tasks 面板投影 |
| smoke.py | 14 场景端到端验证（含回退/降级 stub），退出码即结论 |

## 事件驱动路径与兼容回退（v0.5）

`taskctl.wait()` 不再默认轮询 `/v1/history` 全量：

1. **特性探测**：`Bus.features()` 缓存 `/v1/health` 的 `features` 数组；含
   `events` 才走事件路径，旧节点（0.4.x 无该端点）静默回退 history 轮询，
   行为与 v0.5 前完全一致。
2. **增量续传**：每轮 `GET /v1/events?since=<上一轮 next_seq>&agent=orchestrator&types=text`，
   只追加新帧、按 `payload.task_id` 结构键匹配；页面被 limit 截断时按已读最大
   seq+1 续传，不用 next_seq（防漏）。
3. **环旋转降级**：`/v1/events` 返回 409（`since_before_history`，since 落在环
   头之下）时，用 `/v1/history` 全量重建该任务轨迹**一次**，再从 409 响应里的
   `next_seq` 继续增量。实测传输记录在 `bus.last_wait_transport`
   （`events` / `events+rebuild` / `history`），smoke 场景 11 以此证明事件路径
   并测 P50（WSL 隔离总线实测 ~56ms）。
4. **任务面板**：批次视角用 `GET /v1/tasks?correlation=`（任务投影列表，含
   终态 `state`/`terminal`），不再从 history 手拼；`taskctl.py tasks --correlation C`。
5. **脱敏面坑（勿直接平移旧匹配逻辑）**：`/v1/events`、`/v1/history`、`/v1/tasks`
   输出都是**观察者脱敏件**——payload 字符串值命中规则（`key: token…` 形态、
   32+ 位 hex 摘要、PEM 块）会变 `<REDACTED:*>` 占位。原文 blob 匹配只在目标
   agent 的 **WS 收件侧**有效（executor 拿原文）；编排方匹配必须用不在脱敏面内
   的结构键：`payload.task_id`（uuid 短 hex 段不命中规则）、`topic`、`msg_id`。
   smoke 场景 10 用 64 位 hex sha 验证脱敏生效且结构键收敛不受影响。
6. 环旋转真实触发条件在冒烟里难构造，smoke 用 stub 总线（场景 12/13）确定性地
   验证"无 events 特性→history 轮询"与"409→恰一次全量重建后续传"。

## 已知坑（WSL 实测）

- 客户端接入是 `GET /v1/ws?token=&client=`（节点间 WS hello 握手是另一条通道，勿混）。
- `POST /v1/agents` 注册 agent（agent_id/name/capabilities），REST send 带 `from_agent` 才符合身份铁律。
- history 帧 `ts` 是 ISO 字符串（非 epoch）；解析时间直接切字符串，别拿 float 比大小。
- `agentlink --config <file> run`（不是 `-c`）；config 必填 `node_id`。
- 桥/脚本一律用带 websocket-client 的 venv python（如 LiteGuard venv），系统 python3 缺依赖。
