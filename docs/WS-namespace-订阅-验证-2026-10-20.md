# WS namespace 订阅验证（2026-10-20，v0.7 §4.2 第三波）

namespace 第二波把任务/agent/history/events/files/metrics 的 HTTP 面按
`token_namespaces` 隔离，但实时 WS 仍只有 `local_api.token` 全局视角。
本包补上 **静态 namespace 绑定 token 的 `/v1/ws` 订阅**：

- `local_api.token` 保持全局观察者（旧行为逐字符不变）；
- `auth.tokens` + `auth.token_namespaces` 的非 Admin 静态 token 可订阅，
  只收 `from_agent` / `to_agent` 属于本租户的帧；
- 未绑定静态 token、静态 Admin token、OIDC token 仍拒绝（401）；
- 绑定 token 带 `client=<agent>` 时，agent 必须已注册且 namespace 精确匹配；
  共享池 `"*"` agent 不允许被租户 token 订阅；
- 无 client 的观察者收本租户全部相关帧（含跨租户消息的己方一侧）；
  有 client 时叠加旧的 `to_agent == client` 过滤。

## 1. 鉴权与范围

```text
token == local_api.token                      -> 全局（scope=None）
token ∈ auth.tokens 且 role != Admin
      且 token ∈ auth.token_namespaces
      且 ns != "*"                            -> 本租户（scope=Some(ns)）
其他（未绑定/Admin 静态/OIDC/未知）            -> 401
```

- `client` 非空且 agent 已注册：
  - `agent.namespace == scope` → 允许；
  - `"*"` 或不同 namespace → 403；
  - agent 未注册 → 允许建连（兼容注册前连接），但帧因 namespace 未知被丢弃。
- 无新增配置项；复用 `auth.tokens` / `auth.token_namespaces`。
- OIDC token 暂不支持 WS：只能放 query，避免 access token 进 URL/代理日志；
  后续可做 `Sec-WebSocket-Protocol` 或一次性 ticket。

## 2. 帧过滤

对 `scope=Some(ns)` 的订阅者，一帧可见当且仅当：

```text
namespace(from_agent) == ns  OR  namespace(to_agent) == ns
```

- `namespace(agent)` 来自本机 agent 表或 fleet 缓存（`agent_namespace`）；
- 未知 agent、`"*"` 共享池、无 agent 的节点级帧（peer up/down、node_dir、
  heartbeat 等）对租户不可见——宁可少发，不猜归属；
- 跨租户消息对**双方**都可见（from 方看自己的出站，to 方看自己的入站），
  这是消息参与方的自然语义，不是广播泄漏；
- 租户观察者看不到其他租户的内部消息、全局 agent 流量与节点拓扑事件；
- 有 client 时，先做旧 `to_agent == client` 过滤，再做 namespace 过滤。

## 3. 改动面

- `src/api.rs`：
  - `ws_scope()`：local → 全局；非 Admin 且绑定 namespace 的静态 token →
    本租户；其余拒绝；
  - `subscribe_ws()`：鉴权、client/agent namespace 校验、scope 传入连接；
  - `handle_subscriber()`：新增 `scope: Option<String>` 参数，帧按
    `envelope_in_namespace()` 过滤；
  - `envelope_in_namespace()`：from/to agent 的 namespace 精确匹配。
- 无配置/协议/schema 变更；local token 路径零行为变化。
- 单测 183/183 不变（本包为集成行为，由冒烟覆盖）。

## 4. 验证

### 冒烟（5/5，`tests/ws_namespace_smoke.py`，端口 18968）

单节点（mesh 关闭，只测消息/agent 面）：`team-a-token`/`team-b-token`
绑定 team-a/team-b，`unbound-token` 未绑定，`admin-static` 为 Admin；
agent `ws-a`(team-a)、`ws-b`(team-b)、`ws-global`(`*`)。

| 场景 | 结论 |
|---|---|
| W0 local 可连；绑定 token 可连；未绑定/Admin 静态 401；跨租户/共享池 client 403 | PASS |
| W1 观察者 A 收 `{m1,m2,m4}`、B 收 `{m2,m3,m4}`；m3 不进 A、m1 不进 B、m5 双方都不进 | PASS |
| W2 A token + client=ws-a 只收 `{m1,m4}`（本租户出站 m2 不投） | PASS |
| W3 local token 观察者仍收全量 `{m1..m5}`（含全局 agent m5） | PASS |
| W4 进程存活、日志无 panic | PASS |

消息：m1 a→a、m2 a→b、m3 b→b、m4 b→a、m5 global→global。
证据：`tests/ws_namespace_smoke.jsonl`。

### 零回归

`cargo fmt --check` · `cargo clippy -- -D warnings`（default + mdns）· 
`cargo test 183/183`；acceptance 5/5（200/200）、namespace 9/9、streaming
5/5、oidc 9/9、abac 6/6、quota 7/7、engine 27/27、tasks 17/17、events 20/20；
chaos/checkpoint/audit/slo/relay 等既有套件不受本包影响。

## 5. 边界与残留

1. **OIDC token 不能订阅 WS**：仅静态 `auth.tokens` + `token_namespaces`；
   OIDC 浏览器登录与 WS 票据是后续波次。
2. **token 在 query**：与旧 local token 一致，可能进代理/访问日志；生产应
   限制 `/v1/ws` 来源（localhost/反代 TLS），后续可支持 subprotocol/ticket。
3. **仅按 agent namespace 归属**：无 from/to agent 的节点级帧对租户丢弃；
   若某任务事件缺少 agent 字段，租户看不到（宁少发不误发）。
4. **未知 agent 先放行连接**：帧过滤会丢弃，直到 agent 注册；客户端可能
   看到“连上但无数据”，属预期。
5. **共享池 agent 不能带租户 token 订阅**：`*` agent 跨租户，租户 token
   订阅直接 403；共享池流量只对消息另一侧的租户可见。
6. **跨租户消息双方可见**：这是参与方语义；敏感内容仍需应用层加密/权限。
7. **无历史回放/断点续传**：WS 是 live broadcast，断线期间帧不补；租户
   需要历史时仍走 HTTP 任务面（history/events 对绑定 token 仍 403）。
8. **无 topic/task 级订阅过滤**：租户 client 收本租户全部相关帧，不能只订
   某任务；后续可在 query 增加 `topic`/`task_id` 过滤。
9. **无连接数/速率上限**：沿用本地 API 的暴露面；公网部署需反代限流。
10. **静态 token 吊销需重启**：`auth.tokens` 变更不热加载，与现有静态
    token 语义一致。
