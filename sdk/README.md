# agentlink-adapter-sdk — Adapter SDK（v0.6 参考实现）

把宿主 agent（Hermes / Claude Code / OpenCode / …）接入标准 Task Stream 协议
（`docs/task-protocol.md`）的可执行参考实现。**纯 payload 层约定、零内核改动**，
依赖仅 stdlib + websocket-client（与 `scripts/task-proto/` 同口径）。

> 现役 `wake-hook/hermes_bridge.py` 即本 SDK 的**第一参考实现**：WS 订阅 → 收件
> → 会话唤醒 → 回信这条数据流被正式化为 `hosts/hermes.py`；桥的连接铁律
> （ping 保活、指数退避 1→30s 重连、重连重注册、msg_id 幂等）内化为
> `TaskAdapter.serve_forever()`。现役桥本体不动，继续服役。

## 目录

| 文件 | 角色 |
|---|---|
| `adapter.py` | 核心：`TaskBus`（总线客户端）+ `TaskAdapter`（task_spec 状态机）+ `CommandRunner`（CLI 型宿主底座）+ `BaseRunner`/`RunResult`（宿主契约） |
| `hosts/hermes.py` | Hermes 宿主（第一参考实现）：会话唤醒式，HMAC 签名 wake + `reply_to` 回信收集 |
| `hosts/claude_code.py` | Claude Code 宿主：`claude -p <prompt>` 非交互会话 |
| `hosts/opencode.py` | OpenCode 宿主：`opencode run <prompt>` 非交互会话 |
| `../tests/adapter_sdk_smoke.py` | 包级冒烟 10 场景（隔离端口 18983） |

## 状态机（与 docs/task-protocol.md §3 对齐）

```
task_spec ─预检不过→ task_declined(reason) [终]
         ─通过→ task_accepted ─runner 完成→ task_succeeded(result) [终]
                              ┌─ task_failed(error, retryable) ─预算内可重试(非终)
                              ├─ 期间 task_cancel → task_cancelled [终]
                              └─ 期间 task_rollback → task_rolled_back [终]
```

内建铁律（内核不再重复处理）：
- **幂等**：`(task_id, seq)` 业务级去重 + 重放回 `task_ack(note="dup-dropped")`；
- **执行者自治**：`requires` 能力门 / deadline 逾期 / runner.preflight →
  `task_declined`（终态），adapter 不替宿主做业务决定；
- **预算**：`retryable=True` 的失败不算终态，编排方在 `budget.max_retries`
  内重发 `seq+1`；`retryable=False` 即终态；
- **可追溯**：所有回报带 `reply_to`（指向收到的 spec 帧 msg_id），
  `/v1/history` 按结构键（task_id/topic）可完整重建轨迹。

## 接入三步（CLI 型宿主，如 OpenCode）

```python
import sys; sys.path.insert(0, "<repo>/sdk")
from adapter import TaskAdapter, TaskBus
from hosts.opencode import OpenCodeRunner

bus = TaskBus("http://127.0.0.1:8765", token, agent="oc-worker")
TaskAdapter(bus, OpenCodeRunner(), capabilities=["cap:shell:safe"]).serve_forever()
```

自实现宿主：继承 `BaseRunner`（或 `CommandRunner` 给 `build_command()`），
`run(spec, report, cancelled) -> RunResult` 即可；进度回调 `report(stage=…)`
自动编 `task_progress` 递增 seq。

## Hermes 宿主（第一参考实现）会话侧契约

1. `HermesRunner` 收 spec → 以 HMAC-SHA256（`X-Hub-Signature-256`，同
   wake_hook 约定）POST 网关唤醒会话，唤醒信封带 `msg_id`；
2. 会话答复经桥/编排侧回总线，帧 outer `reply_to` = 该 `msg_id`，
   payload 即任务结果（`content` 为文本命中即成 `task_succeeded.result`）；
3. 网关不可达 / 回信超时 → `task_failed(retryable=True)`。

## 预热池契约（v0.6 P2，与引擎 warmpool 呼应）

引擎 `engine.warmpool`（缺省关）按 `interval_secs` 给成员名单里的在线 agent
注入 `task_type`（缺省 `warmup.ping`）keep-warm 任务（correlation="warmpool"），
把「常驻桥 = agent warmpool」形式化：宿主 runner 收到该 type 应作 **no-op
承接**（快速回 succeeded，会话/进程保持常驻）——真实任务到达即注入已热的
会话上下文，免去冷启动。本 SDK 的 `BaseRunner` 子类建议：

```python
def run(self, spec, report, cancelled):
    if spec.get("type") == "warmup.ping":
        return RunResult(ok=True, result="warm")   # no-op keep-warm，快速回收
    ...
```

本仓冒烟 `tests/warmpool_smoke.py` 用 SDK 适配器充当池位桥（6/6）。

## 验证

`tests/adapter_sdk_smoke.py` 10/10（2026-10-19，详见
`docs/adapter-sdk-设计-2026-10-19.md`）：decline/happy/幂等/cancel/预算重发/
deadline/Hermes 端到端/不可达/超时/CLI 映射。
