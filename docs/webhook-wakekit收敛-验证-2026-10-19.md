# webhook / wake-kit 收敛验证（2026-10-19）

v0.6 P2：把 v0.4.x 的 `wake_hook.py + wake.json` 调用链收敛进内核 webhook，
并补全 `251805a`（P0）未接线的触发点。冒烟 7/7（隔离端口 18995-18997）。

## 1. 设计

### 事件清单（`webhook.events` 逗号分隔）

| 事件 | 触发点 | payload 要点 |
|---|---|---|
| `task_failed` | P0 既有（`reconcile::on_task_terminal`） | task_id/state/correlation/spec |
| `node_offline` | `router::unregister_peer`（removed==true：覆盖 WS 断开/心跳超时/发送失败/主动 forget）+ `discovery.rs` bye 路径补口 | peer_id/agents_lost/reason="unregistered"\|"bye" |
| `audit_alert` | ① `set_lockdown` 开关 ② conn 握手拒绝（admission / cluster_token / protocol_version）③ `handle_incoming` 入站签名/时间戳拒绝 | kind/原因/地址 |
| `node_taint` | 熔断隔离的 Tainted 判决（v0.6 P1 deferred 项闭环） | node_id/cooldown_secs/reason |
| `agent_message` | `deliver_local`：定向业务帧（`to_agent` 非空且 kind∈{text,file}）被本节点**接收**即外呼——落离线 inbox 或投在册 WS 客户端都算 | msg_id/from/from_agent/to_agent/type/topic/reply_to/content/channel="inbox"\|"live" |

### 签名（wake-hook 同口径）

`webhook.secret` 非空 → 每个请求带
`X-Hub-Signature-256: sha256=HMAC-SHA256(secret, body 原文逐字节)`；
空 = 不签名，请求内容与 P0 完全一致（向后兼容零破坏）。body 即信封
`{event, node_id, ts, payload}` 的 serde 序列化原字节，重试沿用。

### 语义对照（侧车 → 内核）

| wake-kit（v0.4.x 侧车） | 内核 webhook（v0.6 P2） |
|---|---|
| 桥（hermes_bridge.py）收定向件入 inbox 后调 `wake(env)` | 任意 agent 帧（含 inbox 暂存）被节点接收即 `agent_message` |
| HMAC 通过 `wake.json.secret` 配置 | `webhook.secret` 同算法同头 |
| 只在装了侧车的 profile 生效 | 单二进制生效，**对所有 agent 生效**（外部端点按 to_agent 过滤） |
| 铁律：唤醒失败绝不阻塞收信 | notify 先过事件清单门 + tokio::spawn，永不阻塞消息路径、运行时外不 panic |
| SDK `sdk/hosts/hermes.py` 的伪造网关口径 | 同一 HMAC 头约定，两世界一致 |

**侧车处置**：官方退役（wake-hook/README.md 顶部加退役指引），既有装好的
实例继续兼容工作——收数据流不变，部署节奏交给舰队（新旧混跑期无冲突）。

## 2. 改动面

- `src/config.rs`：`WebhookConfig.secret`（serde default，老配置零破坏）。
- `src/api.rs`：`fire()` 改为字节级 body + 按需签名 + 显式 Content-Type 头。
- `src/router.rs`：Router 持有 `webhook: Option<Arc<WebhookClient>>`（同
  `cfg.webhook`），`notify()` helper + 四处事件接线；`webhook_client()` getter。
- `src/reconcile.rs`：Engine 复用 `router.webhook_client()`（消除重复客户端）；
  Tainted 判决外呼 `node_taint`。
- `src/conn.rs`：三处握手拒绝接 `audit_alert`。
- `src/discovery.rs`：bye 路径补 `node_offline`。

## 3. 冒烟（7/7，`tests/webhook_converge_smoke.py`，端口 18995-18997）

| 场景 | 结论 |
|---|---|
| S1 定向件 → `agent_message` 外呼（msg_id/to_agent/content/topic 全齐 + HMAC 验签通过） | PASS |
| S2 WS 客户端在册 → 同事件（channel=live） | PASS |
| S3 lockdown 开/关 → `audit_alert(kind=lockdown, on)` ×2 | PASS |
| S4 3×task_failed 归账 → P0 `task_failed` ×3 + 熔断 `node_taint` ×1 + `circuit_taints` 视图 | PASS |
| S5 双节点静态 peer 入网 → 杀 N2 → 心跳超时 `node_offline(peer_id=wk2)` | PASS |
| S6 无 secret → 事件照收且**无** `X-Hub-Signature-256` 头 | PASS |
| S7 全程 HMAC 验签零误报（桩内重算） | PASS |

证据命令：

```bash
python3 tests/webhook_converge_smoke.py
```

## 4. 冒烟踩坑备忘

1. **S1 前提修正**：内核广播通道存在常驻订户，`receiver_count()>0` 时
   deliver_local 走 live 分支——`channel` 是观察面而非承诺，断言核心=
   字段完整性 + 签名。
2. 静态 peer 格式与 mtls_net 冒烟一致用裸 `host:port`（`ws://` 前缀不拨号）。
3. `wait_for` 统一返回谓词最终值（list 而非 bool），避免 `len(True)`。
4. 冒烟启停前后按 `/proc` 精确清残留（`al-wakekit` 路径特征）。

## 5. 残留

- `client` 级「离线暂存→agent 上线补投」时的二次 agent_message（flush_offline
  路径）未接事件——外部端点已有投递事实，补投属节点内部行为；如需
  `agent_message_flushed` 事件，v0.7 评估。
- 本地 API 未授权（authorized_role 拒绝）不产 audit_alert（缺日志锚点），
  P2 内未添加新日志面。
