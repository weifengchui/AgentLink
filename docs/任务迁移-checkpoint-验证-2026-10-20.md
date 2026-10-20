# 任务迁移 + checkpoint 验证（2026-10-20，v0.7 §6.2）

路线图 v0.7：「任务迁移 + checkpoint（SIGTERM 保存、他机恢复，工件走现有文件通道）」。
本包交付**断点随任务迁移**：

1. 执行方在任意任务帧里带 `checkpoint`（不透明 JSON）→ 引擎存进任务记录；
2. 任务因节点失联被回收、重投到另一节点时，重投 `task_spec` 自动携带最新
   `checkpoint`，新执行方从 `spec["checkpoint"]` 续跑；
3. 节点优雅退出（SIGTERM/SIGINT）先广播 `node_drain`，对端**跳过 reclaim
   宽限期**立即换机；硬杀仍走原有宽限期；
4. Adapter SDK 提供 `BaseRunner.checkpoint()` + `TaskAdapter.checkpoint_all()`
   / `install_signal_handlers()`，宿主在信号收尾时把断点落盘/上报。

工件本身不进引擎：`checkpoint` 只带元数据（如 `artifact`/`offset`/`sha256`），
执行方用现有文件通道（`/v1/send-file` 等）传输，引擎不搬运字节。

缺省行为零变化：没有 checkpoint 的任务不写该字段；`node_drain` 只是新增控制面
动作；迁移仍由既有 `mesh.engine.reclaim` 开关决定。

## 1. 数据模型

`TaskRecord` 纯加性字段（老 journal/快照反序列化缺省 `None`）：

```jsonc
{
  "task_id": "ck-a",
  "state": "succeeded",
  "checkpoint": { "offset": 50, "node": "…", "artifact": "file:///tmp/ck-a.ckpt" },
  // 其余字段不变
}
```

- 任何**未被拦截**的任务帧（progress / failed / succeeded / accepted …）只要
  带非 null `checkpoint` 就覆盖旧值（last-write-wins）；
- 不带 `checkpoint` 的帧不擦除已有断点；
- 重投 spec 里带回的 checkpoint 与记录同值，幂等；
- `/v1/tasks/{id}` 直接序列化 `TaskRecord`，断点对编排方可读。

## 2. 重投与迁移链路

```
W1 执行器 ── task_progress{checkpoint:{offset:50}} ──► S 引擎（TaskRecord.checkpoint）
W1 失联（硬杀 / SIGTERM+drain）
        │
        ├─ reclaim：attempts+1，任务回 pending，assignee_node 清空
        ▼
S 引擎 ── task_spec{..., checkpoint:{offset:50}} ──► W2 执行器（从 50 续跑）
```

- `node_drain`：
  - 发送：`Router::broadcast_drain()`，在 `with_graceful_shutdown` 里先于
    `shutdown_tx` 发出，并留 300ms 刷出窗口；
  - 接收：`conn` 校验控制面签名（strict 必带 `sig`）→
    `Router::on_node_drain(node_id)`：记入 `draining`、标记离线、写审计
    `node.drain`、webhook `node_draining`；
  - 效果：`node_online()` 立即 false（不再派发新任务），reclaim 的宽限期按
    节点降为 0（下一 tick 换机）；重连注册清除 draining；
  - 多级拓扑：`relay.enabled` 时 `node_drain` 随既有控制面中继继续扩散。
- 硬杀：无 drain，沿用 `reclaim.grace_seconds`（本包冒烟用 10s）。

## 3. Adapter SDK

- `BaseRunner.checkpoint(spec) -> dict | None`：缺省返回 `None`；自定义 runner
  在子进程/会话侧保存进度后返回 `{"offset": N, "artifact": "...", ...}`。
- `TaskAdapter.checkpoint_all()`：遍历在途任务，调用 runner 钩子并上报
  `task_progress{stage:"checkpoint", checkpoint:{...}}`，返回上报条数。
- `TaskAdapter.install_signal_handlers()`：注册 `SIGTERM`/`SIGINT` →
  `checkpoint_all()` → `stop()` → `SystemExit(0)`；库不自动接管信号，
  由宿主在 `serve_forever()` 主线程调用。
- `report()` 的 seq 分配加锁：信号线程与 runner 线程并发上报不重号。

## 4. 改动面

- `src/taskengine.rs`：`TaskRecord.checkpoint` + 捕获逻辑 + 1 条单测。
- `src/reconcile.rs`：重投 spec 合并 checkpoint；按节点 drain 缩短 reclaim 宽限。
- `src/router.rs`：`draining` 集合、`on_node_drain`/`is_draining`/
  `broadcast_drain`、`node_online` 拦截、重连清除；1 条单测。
- `src/conn.rs`：`node_drain` 控制面分支（验签 + 中继）。
- `src/main.rs`：优雅退出先广播 drain 再停服务。
- `sdk/adapter.py`：runner checkpoint 钩子、信号收尾、并发 seq 锁。
- 单测 +2 → `cargo test 154/154`。

## 5. 验证

### 冒烟（5/5，`tests/checkpoint_smoke.py`，端口 18949/18950/18952）

拓扑：S 引擎（reclaim 开启、grace=10s）+ W1/W2 执行器；S 只向有 `cap:ck` 的
agent 派发。

| 场景 | 结论 |
|---|---|
| S0 拓扑就绪（W1 执行器在线） | PASS |
| S1 W1 上报 `checkpoint{offset:50}` 后硬杀：宽限后换机 W2，重投 spec 携带 checkpoint，任务 `succeeded`（attempts≥1） | PASS |
| S2 W1 重启、仅 W1 在线；上报断点后优雅 SIGTERM：drain 使换机耗时 **0.41s**（远小于 10s 宽限），W2 收到 offset=50 并续跑成功 | PASS |
| S3 S/W2 存活、日志无 panic | PASS |
| S4 Adapter SDK：在途任务经 `checkpoint_all()` 上报 `offset=42` | PASS |

证据：`tests/checkpoint_smoke.jsonl`。

### 零回归

`cargo fmt --check` 全绿 · `cargo clippy -- -D warnings`（default + mdns）零 ·
`cargo test 154/154`；acceptance 5/5（200/200）、chaos 6/6、slo 7/7、audit 7/7、
degraded 6/6、adapter-sdk 10/10、relay-cascade 10/10、engine 27/27、tasks 17/17、
events 20/20、streaming 5/5、gitops 10/10、webhook 7/7、warmpool 6/6、circuit 3/3。

## 6. 边界与残留（后续波次）

1. **只迁移执行方，不迁移编排方**：任务表仍在提交它的引擎节点上；若该引擎
   整体宕机，没有表复制/选主，checkpoint 无法被别的编排方接管。这属于
   Raft/HA 范畴（当前 p2p 架构不做全局队列）。
2. **checkpoint 不自动推进**：引擎只负责携带；执行方必须自己实现幂等与续跑，
   错误/伪造的 offset 引擎不校验（信任执行方，与任务状态权威口径一致）。
3. **工件不托管**：文件通道只负责传输，引擎不做 GC/大小/哈希校验；`artifact`
   地址失效由执行方/运维负责。
4. **drain 是尽力而为**：300ms 刷出窗口内未送达则退回宽限期；只影响直连/中继
   可达的对端，纯目录节点仍按离线探测。
5. **迁移依赖 reclaim 开关**：`mesh.engine.reclaim.enabled=false` 时 drain 仍会
   标记离线（不再派发），但在途任务不会自动回收；这是既有语义，未改动。
6. **节点退出不替执行器保存**：node 进程只广播 drain；同机执行器进程需要宿主
   （systemd 等）也发 SIGTERM，并调用 SDK 的 `install_signal_handlers()` 才会
   在退出前上报断点。不同进程的退出顺序由部署层保证。
7. **任务结果不入 TaskRecord**：本包验证“续跑成功”看新执行方收到的 checkpoint
   与终态；`result` 仍只在任务帧/历史里，未新增存储字段。
