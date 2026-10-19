# spine/relay 中继拓扑验证（2026-10-19，v0.7 第一波）

路线图 v0.7 第一条：「spine/relay 中继拓扑 + 静态种子清单（破全网格连接数与
广播风暴）」。本包交付**单跳中继最小可用集**：叶子只连 spine，跨叶子帧由
spine 转发；静态种子清单复用既有 `discovery.static_peers` 语义（无条件拨号 +
1~30s 退避）。

## 1. 拓扑与配置

```
A(leaf) ─┐
         ├─ R(spine)          A/B: static_peers=[R地址], relay.spine=R.node_id
B(leaf) ─┘                     R:   relay.enabled=true
```

```jsonc
// spine（中继节点）
"relay": { "enabled": true, "max_hops": 2 }
// leaf（叶子）
"discovery": { "static_peers": ["127.0.0.1:18922"] },
"relay": { "enabled": false, "spine": "b1b1b1b1b1b1" }
```

缺省 `relay.enabled=false` + `spine=""` = 既有全网格行为**逐字节不变**。

## 2. 语义

| 场景 | 行为 |
|---|---|
| leaf → 非直连 node | `Router::send` 改投 `relay.spine`，并按**下一跳=spine** 记账 pending（逐跳 at-least-once） |
| spine 收到「目标为第三方」帧 | 验签 → `hops+1`（超 `max_hops` 丢）→ msg_id 去重 → 向目标直连 `send_raw`（**保留原 `from`**，透明转发）+ 本跳 pending；目标未知则丢弃告警（不与离线队列混用） |
| 逐跳 ACK | spine 收帧即回 `ctrl/ack`（conn 层）；发送端 pending 按跳清除，重试也走原下一跳（`register_pending_at`） |
| 广播 | spine 向除入站连接外的所有直连 peer 扩散 + 本地投递（hops 上限防环；叶子不扩散） |
| agent 表 | spine 把收到的 `agents_update` 转发给其它直连 peer（保留 `from`）——叶子据此看到非直连节点的 agent，跨叶子调度前提 |
| 环保护 | Envelope 新增加性字段 `hops`（serde default，旧节点忽略；签名不覆盖，同 `trace_id` 口径）+ spine 逐跳 +1、超限丢弃 + msg_id 去重 |

## 3. 改动面

- `src/config.rs`：`RelayConfig{enabled, spine, max_hops=2}` + `Config.relay`（serde default）。
- `src/proto.rs`：`Envelope.hops: Option<u8>`（加性）。
- `src/router.rs`：`Router.relay` 快照；`send` 定向分支「非直连 → 经 spine」；
  `handle_transit`（验签/去重/hops/定向转发/广播扩散）；`relay_control`
  （控制面中继）；`register_pending_at`（按下一跳记账，重试路径同步修好）。
- `src/conn.rs`：入站三态判定（定向到本机 / 中继 / 丢弃）+ 中继逐跳 ACK +
  `agents_update` 中继扩散。

## 4. 验证

### 单测（5 条，`src/router.rs::relay_tests`，cargo test 131/131）

中继禁用丢弃 / 定向转发（hops=1、from 保持）/ 超跳数丢弃 / 未知目标丢弃且
不进离线队列 / 非直连目标经 spine 且 pending 按 spine 记账。

### 冒烟（7/7，`tests/relay_smoke.py`，端口 18921-18923）

| 场景 | 结论 |
|---|---|
| S0 星型组网（R 见 2 peer；A 只见 R） | PASS |
| S1 A→B 定向经 R 中继（B 收原帧：from=A、hops=1、history 入向） | PASS |
| S2 B→A 回向 | PASS |
| S3 广播中继（R 本地投递 + 转发 B） | PASS |
| S4 未知目标丢弃不排队（R 日志有据，三节点不崩） | PASS |
| S5 agent 表中继（A 的 fleet 视图可见 B 的 agent） | PASS |
| S6 直连 A→R 正常、不经中继 | PASS |

### 零回归（relay 缺省关）

cargo test 131/131 · clippy -D warnings（default+mdns）零 · fmt 全绿；
engine_smoke 27/27、tasks 17/17、events 20/20、jsonlog 21/21、cli 13/13、
circuit 3/3、warmpool 6/6、gitops 10/10、lease 8/8、adapter 10/10、
wakekit 7/7、mtls-net 6/6、admission 9/9。

## 5. 边界与残留（后续波次）

1. **多级 spine 级联**（leaf→spine1→spine2→leaf）：`max_hops` 已支持，但
   spine1 需知道 spine2 是其「直连 peer 的上游」——当前 spine 把自己直连
   表中找不到的目标一律丢弃，级联需要 spine 间互为直连（配置成 mesh）或
   增加「上游 spine 兜底转发」开关，留待第二波。
2. **叶子节点目录**：叶子只连 spine，`/v1/peers` 看不到其它叶子（设计如此）；
   agent 表已中继可用。若要 `/v1/fleet/agents` 的 node_online 也正确，
   需要节点目录帧（peer 目录同步）——第二波。
3. **`agents_update` 未认证**（既有问题，中继会放大伪造面）：本次仅中继，
   未加验签——建议第二波给控制面帧补 `to` 校验 + 签名（调研已记录）。
4. 静态 peer 与 discovery 双拨号、多连接覆盖等既有问题未在本波触及。
