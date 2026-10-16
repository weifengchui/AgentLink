# v0.5 内核 §1/§2 验证记录（2026-10-17）

范围：`/v1/events` 控制面事件流（§1）+ payload 脱敏管线（§2），分支 `feat/v0.5-events`（基于 `75b5ea5`）。
内核改动全部加性；main 工作区的 web console WIP 未触碰（本工作用独立 git worktree）。

## 单元与静态门

- `cargo test --bin agentlink`：redact 4 组单测全过（token 留键隐值、JSON 递归、PEM/hex、custom+禁用回退）。
- `cargo fmt` + `cargo clippy --all-targets`：0 warning。

## 端到端（scripts/events-smoke.py，隔离总线 18766，生产 8765 未触碰）

17/17 PASS（debug 版二进制 + websocket-client 实连）：

| 场景 | 结果 | 验收对应 |
|---|---|---|
| S1 `/v1/health` `features:["events","redact"]` | PASS | §5 能力探测 |
| S2a–c 事件视图 token/hex/PEM/自定义 IP 全脱敏，键名保留 | PASS | §2 内置默认集 |
| S2d WS 收件人收到**原文**（含 X-Auth-Token 秘密串） | PASS | 收件人拿原文、观察者拿脱敏件 |
| S2e `/v1/history` 同一管线脱敏（history 环与磁盘行在 `record_history` 单点净化） | PASS | §2 统一出口 |
| S3a 发送后事件可见延迟 | 3ms | 验收 P50 < 500ms |
| S3b/c/d `seq` 严格递增、`next_seq` 水位=尾+1、`since` 续传只回新事件不丢 | PASS | §1 增量语义 |
| S4 `agent=` / `types=` 过滤生效；视图不含 ctrl/heartbeat | PASS | §1 业务帧 |
| S5 环满（len=4）后 `since` 落后 → **409** + `oldest_seq` + 重建提示 | PASS | §1 降级路径 |
| S6 重启后 seq 从磁盘 history 恢复续传（水位不回退、环内 seq 单调） | PASS | §1 重启不丢序 |
| S7 `redact.enabled=false` 观察者拿原文（配置回退） | PASS | §2 可配置 |

复跑方法：`cargo build --bin agentlink && python3 scripts/events-smoke.py <target>/debug/agentlink`（自带隔离节点起停与清理，端口 18766）。

## 实现要点

- **seq**：`Router.next_seq` AtomicU64，`push_history` 单点盖戳；boot 时以恢复环最大 seq+1 播种；旧盘行 `serde default=0`。`evicted` 计数区分"环满丢事件"（409）与全新节点（200 空视图）。
- **脱敏**：新模块 `src/redact.rs`，规则编译一次（boot）；内置 pem → token → password → hex(32+) → custom 顺序应用；JSON payload 递归只净化字符串值、键名保留。净化点在 `record_history`/`record_file_event`（观察者副本），路由/投递路径零触碰——结构上保证收件人永远拿原文。
- **配置**：新增 `redact.enabled`（默认 true）+ `redact.patterns`（非法正则记 warn 跳过，不拖垮节点）。
- `/v1/events` 走本地 API token 鉴权，limit 默认 500 上限 5000。

## 待办 / 风险

- 与 main 工作区 web console WIP（api.rs/store.rs）有合并冲突面，收口会话合并时处理；web 页面对 `seq` 新字段为透明（serde 全序列化，前端忽略未知字段）。
- Windows 交叉构建留到发版时验证（regex 纯 Rust，无平台风险预期）。
- 参考实现 orchestrator 的 `wait()` 切到 `/v1/events` 属 scripts/task-proto 后续轮（协议不变，纯收益）。
