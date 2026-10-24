# AgentLink 任务协议 v1（task protocol）

状态：v1 草案正本，2026-10-17 定稿。本文档为公开协议规范（OSS），不随商业发行版收费。
承载：本协议完全运行在 AgentLink 总线协议 v1（docs/protocol.md）之上，**零内核改动**——任务语义全部是 `kind=msg` 帧的 `payload` 层约定。文件载荷复用总线文件通道（§5 / `/v1/send-file`）。内核原生 `subscribe_events` 属 v0.5.x 增项，见 docs/v0.5-内核待发清单.md。

## 0. 设计原则

- **声明式**：发起方声明期望状态（task_spec），执行方回报实际状态（task_status），编排方收敛差值。升级协议（upgrade_notice/ready/go/done/…）是本协议第一个内置任务类型，语义完全兼容。
- **幂等**：所有消息可按 `task_id + seq` 重放；所有指令可按 `reply_to` 关联。接收方按 `msg_id` 去重（总线已保证）。
- **可追溯**：每个任务的完整消息轨迹可从 `/v1/history?node=…&agent=…` 或总线 WS `topic=broadcast` 重建，含时间戳——天然审计源。
- **执行者自治**：执行方对高危动作保有最终否决权（`declined`/`rolled_back`），编排方不得绕过（对应升级协议"SHA 不符即拒"的先例）。

## 1. 消息类别

均为 `kind=msg`，`payload.topic` 标识类别，`payload.task_id` 必带（除 `task_spec` 首发时由发起方生成 uuid）。建议外层附 `reply_to` 指向被响应消息。

| topic | 方向 | 语义 |
|---|---|---|
| `task_spec` | 发起→执行 | 任务声明（期望状态） |
| `task_accepted` | 执行→发起 | 认领，承诺开始 |
| `task_declined` | 执行→发起 | 拒绝（附 `reason`），终态 |
| `task_progress` | 执行→发起 | 心跳/进度（周期性） |
| `task_succeeded` | 执行→发起 | 成功，附 `result`，终态 |
| `task_failed` | 执行→发起 | 失败，附 `error`；可重试时不算终态 |
| `task_cancel` | 发起→执行 | 撤销指令 |
| `task_cancelled` | 执行→发起 | 已撤销确认，终态 |
| `task_rollback` | 编排→执行 | 回滚指令（护栏联动） |
| `task_rolled_back` | 执行→发起 | 已回滚确认 |
| `task_ack` | 双向 | 对上述任意消息的送达外语义确认（总线 ack 只到帧级，此为业务级） |

## 2. task_spec 对象

```json
{
  "topic": "task_spec",
  "task_id": "uuid-v4",
  "seq": 1,
  "type": "rollout.node_version",     // 内置类型或自定义
  "intent": "把节点 X 的 agentlink 升到 0.4.20",  // 人类可读
  "params": { "target_version": "0.4.20", "sha256": "…", "file_id": "…" },
  "deadline": "2026-10-18T07:00:00+08:00",  // 可选；超期未达期望态按护栏处置
  "reconcile": { "mode": "one_shot", "interval_s": 60 },  // 见 §4
  "priority": 100,                    // 越小越优先，同队列排序用
  "budget": { "max_retries": 3, "max_cost": 100 },        // 可选；超限 task_failed
  "correlation": "batch-2026-10-17-A", // 可选；多任务共享标签
  "requires": ["cap:node:admin"],      // 可选；能力门，执行方无此能力应 decline
  "constraints": { "window": "08:00-22:00" }  // 可选；保护窗等硬约束
}
```

字段约束：`type` 命名 `域.对象动作`（小写，点分）；`params` 结构由类型 spec 定义；未知字段必须忽略（向前兼容）。

## 3. 状态机

```
pending ──(task_accepted)──▶ running ──(task_succeeded)──▶ succeeded [终]
   │                            │ ├─(task_failed, 可重试, 预算内)──▶ pending
   │                            │ ├─(task_failed, 超预算)─────────▶ failed  [终]
   ├─(task_declined)──────────────▶ declined [终]                 │
   ├─(task_cancel→task_cancelled)─▶ cancelled [终]      (task_rollback)─▶ rolled_back [终]
   └─(deadline 超期)──────────────▶ expired [终]（编排方判定）
```

执行方是唯一状态权威：编排方只观察回报并收敛，不得自行改写任务状态；失联超时走 §5 护栏，不走状态伪造。

## 4. 幂等与 reconcile

- **幂等键**：`(task_id, seq)`。同键消息重复投递，接收方丢弃并回 `task_ack`。
- **reconcile.mode**：
  - `one_shot`：达成期望态即终（升级、部署、清理）。
  - `maintain`：执行方在 `deadline` 前周期发 `task_progress` 证明在态；缺勤 > 3×`interval_s` 视为 drift，编排方告警/重派。
  - `monitor`：只采集不干预；`task_progress` 是观测值本身，永不 succeeded，直到 cancel。
- 编排方崩溃恢复：从 history/WS 重放该 task_id 全部消息即可重建状态——协议消息即事件溯源。

## 5. 护栏（编排侧义务）

1. **金丝雀**：同 `correlation` 的批量 spec，首任务 succeeded 前不得放行后续 ≥50%。
2. **连续失败熔断**：同批次连续 2 个 failed/expired → 暂停整批，等人工。
3. **回滚预算**：spec 可带 `rollback_plan`（params 内的逆操作声明）；触发 `task_rollback` 时按方案执行并核验 health。
4. **保护窗**：`constraints.window` 内禁发破坏性 `task_go` 类指令（沿用 22:00-08:00 全网先例）。
5. **限并发**：同时在跑任务数有上限（默认按节点数），对应未来令牌桶调度。

## 6. 内置类型 `rollout.node_version`（升级协议映射）

| 原 upgrade_* | 新任务消息 | 映射说明 |
|---|---|---|
| upgrade_notice | task_spec（type=rollout.node_version） | params 含 target_version/sha256/file_id/window |
| upgrade_ready | task_accepted | 预检（实算 SHA、路径、备份位）通过 |
| upgrade_declined | task_declined | reason 必填（如 sha_mismatch） |
| upgrade_go | task_go 语义=编排方 task_ack(reply_to=accepted) 或显式放行消息 `topic:"task_go"` | 分批闸口 |
| upgrade_done | task_succeeded | result 含 backup/elapsed_ms/version 核验 |
| upgrade_rolled_back | task_rolled_back | |
| upgrade_failed | task_failed | |

过渡期两套 topic 双发兼容（执行方任一命中即认），v0.5.x 收敛为单套。

## 7. 传输与寻址

- 点对点走 `/v1/send`（REST，本机）或总线 WS `action=send`；必带 `to`(node_id)+`to_agent`，出站带 `from_agent`（总线身份铁律）。
- 大载荷（安装包、报表）不塞 payload：先 `/v1/send-file`，spec 里引用 `file_id`。
- 观察通道：REST 轮询 `/v1/history`（现状）；内核 `subscribe_events`（v0.5.x）提供 `GET /v1/events?since=<seq>`，payload 层脱敏后输出，业务层可先行以 broadcast 近似。

## 8. 安全边界（商业发行版入口声明）

本协议的消息格式、状态机、语义**永久开源**。收费的是执行这些语义的治理引擎：优先级抢占、准入控制、多租户隔离、审计留存策略、自动护栏/混沌验证。节点身份沿用 cluster_token + 帧签名（协议 v1 §6），跨 trust domain 互信（mTLS）为 v0.7.x 增项。
