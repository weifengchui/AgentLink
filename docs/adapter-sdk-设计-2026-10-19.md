# Adapter SDK 设计与验证（2026-10-19）

对应路线图 v0.6.x 条目「Adapter SDK：Hermes / Claude Code / OpenCode → 标准
Task Stream 协议；现役 hermes_bridge 转为第一参考实现【§2.3、Phase2】」，本包交付。

## 1. 范围与原则

- **零内核改动**：适配器语义全部是总线 `kind=msg` 帧 payload 层约定
  （docs/task-protocol.md），任何 0.4.x+ 节点皆可跑；单二进制形态不动。
- **执行者自治复述**：requires 能力门 / deadline / 宿主预检不过即
  `task_declined`（终态），编排方不得绕过——与 `scripts/task-proto/executor.py`
  参考执行者语义完全同构，正式化为 SDK。
- **现役桥不动**：`wake-hook/hermes_bridge.py` 继续服役；其连接铁律
  （ping 保活、指数退避 1→30s、重连重注册、msg_id 幂等、回环 API、
  导入 wake 钩子静默降级）内化为 `TaskAdapter.serve_forever()` 并以此
  作为「第一参考实现」——`hosts/hermes.py` 承接同一数据流（wake→回信）
  到任务协议语义。

## 2. 交付物

| 文件 | 内容 |
|---|---|
| `sdk/adapter.py` | `TaskBus`（总线客户端回环口径）+ `TaskAdapter`（spec 状态机：通用预检/幂等/承接/进度/终态/取消）+ `BaseRunner`/`RunResult`（宿主契约）+ `CommandRunner`（CLI 型底座：exit 0=成功、超时 retryable、on_cancel 杀子进程） |
| `sdk/hosts/hermes.py` | `HermesRunner`：HMAC-SHA256（`X-Hub-Signature-256`）唤醒 hermes 网关 → 会话经总线帧 `reply_to` 回信 → `task_succeeded(result=content)`；网关不可达/超时 `task_failed(retryable=True)` |
| `sdk/hosts/claude_code.py` | `claude -p <prompt> --output-format text` |
| `sdk/hosts/opencode.py` | `opencode run <prompt>`（`params.timeout_seconds` 逐任务覆盖超时） |
| `sdk/README.md` | SDK 契约（状态机/铁律/接入三步/Hermes 会话侧契约） |
| `tests/adapter_sdk_smoke.py` | 包级冒烟 10 场景（隔离端口 18983，数据目录即跑即清） |

## 3. 状态机与铁律（SDK 内建）

```
task_spec ─requires 缺门 / deadline 逾期 / preflight 拒─→ task_declined(reason) [终]
         ─通过→ task_accepted ──→ runner.run(report, cancelled)
                ├─ ok  ──────────→ task_succeeded(result)            [终]
                ├─ fail(retryable)→ task_failed(retryable=True)       （可以重试，非终态）
                ├─ fail(硬) ─────→ task_failed(retryable=False)       [终]
                └─ 期间 cancel/rollback → task_cancelled / rolled_back [终]
```

- 幂等：`(task_id, seq)` 业务级去重，重放回 `task_ack(note="dup-dropped")`，
  绝不二次执行；
- 预算：`retryable=True` 失败由编排在 `budget.max_retries` 内重发 `seq+1`；
- 可追溯：所有回报带 `reply_to`（收到 spec 帧的 msg_id）；
- 进度：`report(stage=…)` 自动编 `task_progress` 递增 seq。

## 4. 冒烟（10/10，隔离端口 18983）

| 场景 | 结论 |
|---|---|
| S1 能力门 declined（missing cap） | PASS |
| S2 happy：accepted→progress→succeeded（EchoRunner） | PASS |
| S3 (task_id,seq) 重放 → task_ack(dup-dropped) 且 runs=1（不二次执行） | PASS |
| S4 运行中 cancel → task_cancelled + on_cancel 回调（杀子进程锚点） | PASS |
| S5 retryable 失败 → seq+1 重发 → succeeded（预算语义） | PASS |
| S6 逾期 deadline → declined(deadline_passed) | PASS |
| S7 claude_code/opencode build_command 映射（prompt 含 intent+params） | PASS |
| S8 Hermes 端到端：HMAC 桩网关验签通过 + 会话 `reply_to` 回信 → succeeded | PASS |
| S9a 网关不可达 → task_failed(retryable=True, "wake 失败") | PASS |
| S9b 回信超时 → task_failed(retryable=True, "session 回信超时") | PASS |

存档：`tests/adapter_sdk_smoke.jsonl`。证据命令：

```bash
python3 tests/adapter_sdk_smoke.py
```

## 5. 验证过程中发现并修正的坑（备忘）

1. `handle_frame` spec 分支变量误引用（线程 target 用错 payload 变量名）——
   适配器静默吞帧的唯一硬 bug；`serve_forever` 的 `log=print` + `stats`
   观察口径是定位关键的观测面（已内建 `TaskAdapter.stats`）。
2. 冒烟 scratch 复用导致**持久化 history 污染断言**（上一轮 task_succeeded
   残留）——数据目录即跑即清（`rm -rf DATA`）。
3. 适配器线程启动与任务下发存在注册竞速——发 spec 前必须等
   `GET /v1/agents` 出现该 agent（40×0.25s 等待环）。
4. 多个 adapter 共用同一 agent_id 会抢帧——每个 adapter 独立 agent_id。

## 6. 遗留与后续（v0.7 候选）

- Engine 调度路径集成：目前 SDK 为执行方侧参考实现；引擎 `schedule_batch`
  派发给 SDK agent 即需 spec 直投 WS——已具备（本冒烟即同路径收发），
  引擎侧联动冒烟待 v0.7 评估。
- hermes_bridge 本体升级为 `from adapter import …`（收益：铁律代码合一；
  风险：动现役，需随舰队分发节奏）。
- OpenCode/Claude Code 宿主在真 agent CLI 上的实机验证（本包用映射桩 +
  真节点总线完成契约验证）。
