# per-namespace 配额验证（2026-10-20，v0.7 §5.4 第一波）

路线图 v0.7：「资源配额（每 namespace token/时长/磁盘）；进程隔离 Linux=cgroups
v2、Windows=Job Objects【§5.4】」。

本包先交付**任务数配额 + 提交速率**这一可验证子集：

- 准入：活跃任务数（open）上限、60 秒滑动窗口提交速率；
- 派发：namespace 同时 in-flight 上限（running + 已分配未确认）；
- 观测：Admin 可读 `/v1/mesh/quota` 的配置与实时用量；
- 拒绝：429 + 稳定 reason（`open_quota` / `rate_limited`），审计
  `quota.reject` + webhook `quota_alert`。

缺省 `mesh.quota.enabled=false` 且全 0，准入/派发行为逐字节不变。时长、磁盘、
LLM token 配额（需要计量与文件归属）留待后续波次。

## 1. 配置

```jsonc
"mesh": {
  "quota": {
    "enabled": true,
    "default_max_open": 0,                 // 0 = 不限制
    "default_max_inflight": 0,
    "default_max_submit_per_minute": 0,
    "namespaces": {
      "team-a": { "max_open": 20, "max_inflight": 4, "max_submit_per_minute": 30 },
      "team-b": { "max_inflight": 2 }       // 其余字段继承 default_*
    }
  }
}
```

- `max_open`：该 namespace 的活跃任务上限（`pending` / `running` /
  `cancelling` 计入；终态不计）。
- `max_inflight`：该 namespace 同时占用的执行额度（`running` +
  `pending` 但已分配 `to_agent`）。引擎派发时截流，多出的任务保持 `pending`。
- `max_submit_per_minute`：60 秒滑动窗口内的提交**尝试**上限（含被 open
  上限拒绝的请求——限流保护入口压力）。
- 所有 `0` 表示不限制；namespace 字段缺省继承 `default_*`。
- `"*"` 不能作为配额 key（共享池不是租户）；名称格式
  `[A-Za-z0-9_.-]{1,64}`，配置校验会拒绝非法值。

## 2. 行为

```
POST /v1/tasks
  ├─ 必填 task_id/type 通过
  ├─ rate 窗口 allow? 否 → 429 rate_limited（不落表）
  ├─ open < max_open? 否 → 429 open_quota（不落表）
  └─ 入表 pending
引擎 tick
  └─ namespace inflight < max_inflight? 否则跳过派发（保持 pending）
```

- 速率检查先于 open 检查，且**拒绝也消耗一次窗口额度**（防重试放大）。
- 拒绝响应含 `namespace` / `limit` / `current`（速率场景另有
  `retry_after_seconds: 60`），不含任务 payload。
- 审计 `quota.reject` 字段：`namespace`、`reason`、`limit`、`current`、
  `inflight`、`task_id`；webhook `quota_alert` 同源但无 task_id（降噪）。
- `GET /v1/mesh/quota`：
  - Admin 返回全局视图（省略 `scope`）：
    ```jsonc
    { "ok": true, "enabled": true,
      "defaults": {"max_open":0,"max_inflight":0,"max_submit_per_minute":0},
      "namespaces": [
        {"namespace":"team-a","max_open":2,"max_inflight":1,
         "max_submit_per_minute":10,"open":2,"inflight":1,"submit_last_minute":3}
      ] }
    ```
    包含配置里列出的 namespace + 当前表里出现的 namespace。
  - namespace 绑定 token（Operator/Viewer）返回 `scope` + **仅本租户一行**，
    便于租户自助看用量，不暴露其他租户的任务数/速率：
    ```jsonc
    { "ok": true, "enabled": true, "scope": "team-a",
      "defaults": {"max_open":0,"max_inflight":0,"max_submit_per_minute":0},
      "namespaces": [
        {"namespace":"team-a","max_open":2,"max_inflight":1,
         "max_submit_per_minute":10,"open":2,"inflight":1,"submit_last_minute":3}
      ] }
    ```
- **未绑定** namespace 的 Operator 调 `/v1/mesh/quota` → 403（与
  `/v1/mesh/status` 同口径）；绑定 token 只能看本行，不能看全局/其他租户。

## 3. 改动面

- 新增 `src/quota.rs`：`QuotaConfig` / `NamespaceQuota` / `RateLimiter`
  滑动窗口 / `validate` + 4 单测。
- `src/config.rs`：`mesh.quota`（serde default）+ validate。
- `src/scheduler.rs`：`schedule_batch_with_quota()` 在选 agent 前按
  namespace 截流，本批次新分配也计入；旧 `schedule_batch` 保留为
  零配额兼容入口 + 1 单测。
- `src/reconcile.rs`：Engine 快照 `quota_cfg`；派发前按表统计
  `namespace_inflight` 并调用带配额调度。
- `src/api.rs`：`MeshRuntime.quota_rate`；`tasks_post` 准入（429/审计/
  webhook）；`GET /v1/mesh/quota` 视图与路由。
- 单测 183/183（当前累计；配额包自身 +5）。

## 4. 验证

### 冒烟（7/7，`tests/quota_smoke.py`，端口 18963/18964）

S 引擎 + W1 持任务执行器（`team-a`）；配额：team-a open=2 / inflight=1 /
rate=10，team-rate rate=1，default 全 0。

| 场景 | 结论 |
|---|---|
| S0 配置视图正确；未绑定 Operator 403；绑定 operator 仅本租户 200 | PASS |
| S1 A running + B pending 后第 3 个提交 429 `open_quota`，未入表（GET 404） | PASS |
| S2 inflight=1：A 在跑时 B 保持 pending 且未派发；release A 后 B 才 running | PASS |
| S3 team-rate 第 2 次提交 429 `rate_limited` 且未入表；default 仍 202 | PASS |
| S4 用量面：team-a open=1/inflight=1/rate=3；team-rate rate=1；default 用量正确；绑定 token 只返回 team-a 一行 | PASS |
| S5 release B 后终态，team-a open/inflight 归零 | PASS |
| S6 两进程存活、日志无 panic | PASS |

证据：`tests/quota_smoke.jsonl`。

### 零回归

`cargo fmt --check` 全绿 · `cargo clippy -- -D warnings`（default + mdns）零 ·
`cargo test 183/183`；acceptance 5/5（200/200）、chaos kill-node 6/6、chaos
注入 8/8、checkpoint 5/5、abac 6/6、namespace 9/9、guardrails 9/9、degraded
6/6、audit 7/7、slo 7/7、oidc 9/9、adapter-sdk 10/10、relay-cascade 10/10、
engine 27/27、tasks 17/17、events 20/20、streaming 5/5、gitops 10/10、
webhook 7/7、warmpool 6/6、circuit 3/3。

## 5. 边界与残留

1. **节点本地配额，不是全局配额**：速率窗口与任务计数在各自引擎进程内；
   多引擎节点各自准入，集群总量可达 N×上限。全局配额需要共享账本（Raft/
   外部存储），留待 HA 波次。
2. **GitOps / 内部 `apply_task_spec` 绕过准入**：目录扫描直接入表，不检查
   open/rate；但引擎派发仍受 `max_inflight` 截流。文件系统视为管理面。
3. **只做任务数 + 提交速率**：时长配额（可按 deadline 派生）、磁盘配额
   （需要文件归属 namespace）、LLM token 计量（需要执行方上报 usage）尚未
   实现。
4. **租户自助视图无历史**：`/v1/mesh/quota` 只给当前 open/inflight 与 60s
   速率窗口，不提供按小时/天趋势；`defaults` 仍会暴露全局默认策略（不是
   其他租户数据）。审计/SLO 仍按全局治理面，租户不可见。
5. **重启清空速率窗口**：60 秒窗口是进程内 `Instant` 队列；任务计数从
   journal 重建，不丢。重启瞬间速率归零是可接受的保护语义。
6. **重试复活**：`failed` 视为终态不占 open；若随后被 `task_accepted`
   复活为 running，open 可能短暂超过上限（派发仍受 inflight 约束）。
7. **预热池/重派**：warmpool 任务默认 namespace，会计入 default 配额；
   重试/回收换机不重新走准入（同一任务行），但占用 inflight。
8. **429 无 `Retry-After` 头**：响应体给 `retry_after_seconds`，未加 HTTP
   头；客户端应按响应体退避。
9. **无按 agent / task_type 的细分配额**；namespace 是唯一维度。
