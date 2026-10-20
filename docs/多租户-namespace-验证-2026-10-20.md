# 多租户 namespace 验证（2026-10-20，v0.7 §4.2 第二波）

路线图 v0.7：「ABAC 策略引擎（label × node 属性 allow/deny，如 finance-data 禁入
低安全级节点）+ namespace 多租户」。上一包交付 ABAC，本包交付 **namespace
隔离 + token 绑定**。

缺省完全不启用：`auth.token_namespaces` 为空 → 所有 token 仍是全局角色，
任务/agent 的 namespace 缺省 `default`，调度行为与之前一致。

## 1. 模型

### 1.1 任务与 agent

- 任务 namespace = `task_spec.namespace`（缺省/空 = `default`）。提交时非法
  名称 → 400；非字符串/对象外字段一律按缺省处理。
- agent namespace = 注册请求 `namespace`（缺省继承 token 绑定，否则 `default`）。
  `"*"` 是显式共享池，可服务任意 namespace。
- 调度过滤：`agent.namespace == task.namespace` 或 `agent.namespace == "*"`；
  没有匹配 agent 时任务保持 `pending`，不失败、不耗 attempts。
- 节点 tags（ABAC）与 namespace 正交：先 namespace/requires/exclude_tags 过滤，
  再 ABAC 求值，最后打分。

### 1.2 token 绑定

```jsonc
"auth": {
  "tokens": { "tok-team-a": "operator", "tok-team-b": "viewer" },
  "token_namespaces": { "tok-team-a": "team-a", "tok-team-b": "team-b" }
}
```

- `tok-team-a` 提交任务时若省略 `namespace`，服务端自动注入 `team-a`；
  显式写别的 namespace → 403。
- `GET /v1/tasks`、`/v1/tasks/{id}`、cancel/delete 均按绑定 namespace 过滤；
  跨租户单查/撤销统一 404（不泄露存在性）。
- `/v1/agents`、`/v1/fleet/agents` 只返回本租户 agent（共享池对所有人可见）。
- agent 注册自动归位；显式跨租户 → 403；同名 agent 跨 namespace 接管 → 409。
- agent 注销：跨租户 403；未知 agent 404（绑定 token 不能探测）。
- `/v1/send` 的 `from_agent`/`to_agent` 若为已知 agent，必须与 token 同
  namespace（或共享池），否则 403。
- **绑定 token 不能是 Admin**，也不能是 `local_api.token`——配置校验会拒绝
  （Admin 是集群级角色；local token 还要用于 WS 与全局运维）。
- 绑定 token 读全局可观测面（`/v1/history*`、`/v1/events`、`/v1/files/:name`、
  `/v1/metrics`）→ 403；任务面 `/v1/tasks` 仍可用（已按 namespace 过滤）。
- 审计 `task.submit` 增加 `namespace` 字段（不含 payload）。

## 2. 改动面

- `src/config.rs`：`auth.token_namespaces`（serde default）+ validate（Admin/
  local token/未知 token/名称格式拒绝）+ 1 单测。
- `src/router.rs`：`AgentRecord.namespace`（加性、skip none）、
  `register_agent(..., namespace)`、`agent_namespace()`、
  `TaskFilter.namespace` + `task_namespace()` 投影过滤。
- `src/scheduler.rs`：候选过滤 `agent_serves(namespace)`（含 `*`）+ 1 单测。
- `src/api.rs`：任务提交注入/校验、任务读/撤销/删除 scope、agent 注册/注销/
  列表/fleet scope、同名接管 409、`/v1/send` 冒充检查、全局可观测面 403。
- 单测 163/163（+2：scheduler namespace、config token 校验）。

## 3. 验证

### 冒烟（9/9，`tests/namespace_smoke.py`，端口 18956-18958）

S 引擎（team-a/team-b 两个执行器）+ 绑定 token `tok-a→team-a`。

| 场景 | 结论 |
|---|---|
| S0 fleet 带 namespace；`tok-a` 只看到 team-a agent | PASS |
| S1 team-a / team-b 任务分别只落各自执行器 | PASS |
| S2 `tok-a` 跨租户提交 403；省略 namespace 自动归位 team-a 并完成 | PASS |
| S3 `tok-a` 任务列表/单查/撤销按 namespace 过滤（跨租户 404） | PASS |
| S4 team-c 无匹配时 pending；`namespace="*"` 共享池上线后接管 | PASS |
| S5 agent 注册自动归位；跨租户注册/删除 403；同名接管 409 | PASS |
| S6 `tok-a` 借 `from_agent` 冒充 team-b 发信 403，本租户 200 | PASS |
| S7 `tok-a` 读全局 history/events/files 403，`/v1/tasks` 200 | PASS |
| S8 三进程存活、日志无 panic | PASS |

证据：`tests/namespace_smoke.jsonl`。

### 零回归

`cargo fmt --check` 全绿 · `cargo clippy -- -D warnings`（default + mdns）零 ·
`cargo test 163/163`；acceptance 5/5（200/200）、chaos 6/6、checkpoint 5/5、
abac 6/6、degraded 6/6、audit 7/7、slo 7/7、adapter-sdk 10/10、relay-cascade
10/10、engine 27/27、tasks 17/17、events 20/20、streaming 5/5、gitops 10/10、
webhook 7/7、warmpool 6/6、circuit 3/3。

## 4. 边界与残留（后续波次）

1. **ID 仍是全局唯一**：`task_id` / `agent_id` 没有按 namespace 组键，跨租户
   同名会冲突（任务 409、agent 409）。多租户部署应要求租户给 ID 加前缀；复合
   主键改造会触及 journal/引擎表/CLI，留待后续。
2. **配额未按 namespace**：`node_quota` 仍是集群级；per-namespace 任务/并发/
   节点配额（路线图 §5.4）未做。
3. **隔离是授权层而非加密层**：namespace 不提供独立密钥、独立表或网络分区；
   同节点 agent 进程若绕过本地 API 直连总线，仍受集群 token 约束而非租户约束。
4. **全局元数据仍可见**：`/v1/status`、`/v1/peers`、`/v1/mesh/mode` 等只含
   拓扑/计数，未按租户过滤；绑定 token 的 Operator 可读（不含任务载荷）。
5. **GitOps 目录受信**：目录里的 `task_spec.namespace` 可写任意租户（文件系统
   是管理面）；HTTP 写面才做 token 绑定。
6. **共享池是显式信任**：`namespace="*"` 的 agent 能执行所有租户任务；只应在
   受控的公共算力上使用。
7. **WS 实时面（第三波已交付）**：`/v1/ws` 接受 `local_api.token`（全局）
   或静态 `auth.tokens` + `auth.token_namespaces` 绑定 token（仅本租户）；
   绑定 token 按 from/to agent namespace 过滤，跨租户 client/共享池 403。
   OIDC token、历史回放与 topic/task 级订阅仍未支持
   （见 `docs/WS-namespace-订阅-验证-2026-10-20.md`）。
8. **同名 agent 刷新**：同 namespace 内允许按 agent_id 覆盖注册（执行器重连
   语义）；跨 namespace 409。
