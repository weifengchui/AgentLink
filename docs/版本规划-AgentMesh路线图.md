# AgentMesh 设计案吸收 · 版本路线图

拍板（用户 2026-10-16）：《AgentMesh-局域网多机Agent资源调度编排层-产品优化设计方案》的特性**不砍，全部纳入版本规划**。产品阶梯：**个人（自用）→ OPC 一人公司 → 中小团队 → 可商用可售**。

## 0. 落地三原则（全案前提）

1. **单二进制零依赖形态保留到最后一站**。设计案的"微内核拆进程"一律实现为二进制内模块边界；不引入 etcd/NATS/Redis 等外部依赖——HA 用内置 Raft（tokio+badger-raft），事件总线用现有 WS 骨干 + 进程内队列。
2. **协议先行、内核后置**。业务层消息先在现网总线验证（v0.5，升级协议就是先例），验证成熟的引擎再上提进 Rust 内核（v0.6+）；agentlink.json 全程向后兼容，老节点混跑不破。
3. **拓扑随规模演化**：≤10 台全网格 WS + UDP 发现（现状）；10~30 台种子清单 + Lease 差异上报；30~100 台 spine/relay 中继节点破全网格 O(n²)。**产品天花板=中小团队（≤100 机）**。

## 1. v0.5.x — 个人 / OPC 版（1~10 台，现舰队自用）

目标：声明式任务化落地、编排零事故、别人第一次装就能跑通。

- 任务层协议 `task_spec / task_status / task_event`：泛化升级协议 5 topic，升级任务变为内置类型，编排器驻桥层【§2.1】
- 控制面事件流 `subscribe_events`（peer up/down、ACK、文件落盘、注册变更），编排端告别轮询哨兵【§5.1 Watch】
- 注册表扩展：agent capabilities 标签 + 节点资源/时窗标签（折扣窗、可重启性）【§2.3】
- history 落盘前 regex 脱敏（sk-…、BEGIN PRIVATE KEY → [REDACTED]）【§4.4】
- 结构化 JSON 日志 + trace_id（reply_to 链即天然 trace 骨架）【§7.1】
- /web 任务 timeline 视图（首个用例=滚动升级）+ 一键 lockdown【§3.2、§4.5】
- CLI 薄壳 `agentlink get nodes / apply / logs`（REST wrapper）【§3.3】
- 首次运行 wizard + hello-world 自检任务（自动遍历节点汇总）【§3.1、§3.4】✅ v0.5.6 完成（`agentlink setup`/裸启动向导 + `agentlink hello` + 内核 `__node__` 应答）

验收门：一次真实滚动升级全程走 task 协议、零手工对账；编排由轮询改事件驱动。

## 2. v0.6.x — OPC → 小团队版（10~30 台，单二进制不动）

目标：任务引擎进内核，**别人的舰队**能跑起来。

- **任务引擎内核化**：声明式 CRUD + reconcile loop + bin-packing 调度（优先级/重试/超时/断点续传）【§2.1、§2.2 Scheduler、§5.2 增量调度】
- Adapter SDK：Hermes / Claude Code / OpenCode → 标准 Task Stream 协议；现役 hermes_bridge 转为第一参考实现【§2.3、Phase2】——已交付（2026-10-19，`docs/adapter-sdk-设计-2026-10-19.md`：`sdk/` 参考实现正本（TaskAdapter 状态机 + requires/deadline 预检 + (task_id,seq) 幂等 + budget 重发 + cancel/rollback 终态）+ 三宿主参考实现（hermes 唤醒式承接现役桥数据流 / claude_code / opencode CLI 非交互）；桥本体不动、零内核改动；冒烟 10/10 隔离端口 18983）
- RBAC：人 token 与 agent token 分轨；Admin/Operator/Viewer/Executor 四角色【§4.2 前半】
- 可选 mTLS：内置 CA 一键签发+24h 轮转 + **CA 自动分发**（权威节点经已认证链路零拷贝下发，成员热启用；冲突保护）+ **强制 require 模式**（`mtls.require`，拒绝一切明文对等链路）（rustls+rcgen，LAN 默认关闭、一键开启）【§4.1】
- 新节点准入：bootstrap token + 硬件指纹双验证（已交付：对等/文件通道同一闸门、fail-closed、单因素显式裁剪；`agentlink admission` CLI + `join --admission`）【§4.1】
- Lease 心跳 + 阈值差异上报（CPU±5% 才发 Full Status）【§5.1】——已交付（2026-10-19，`docs/lease差异上报-验证-2026-10-19.md`：`ctrl/lease` 加性帧 + 首拍/阈值跨越发 Full + 裸心跳节流 + hello 补推；缺省关零流量）
- 出站 webhook（OnTaskFailed/OnNodeOffline/OnAuditAlert），wake-kit 收敛为标准实现【§8.2】——已交付全量（2026-10-19，`docs/webhook-wakekit收敛-验证-2026-10-19.md`：`251805a` 任务终态触发基础上补齐 ① `webhook.secret` HMAC-SHA256 签名（`X-Hub-Signature-256`，wake-hook 同口径，空=不签名零破坏）；② OnNodeOffline（`unregister_peer` 全下线路径 + discovery bye 补口）③ OnAuditAlert（lockdown 开关 / admission·token·协议版握手拒绝 / 入站签名时间戳拒绝）④ OnNodeTaint（熔断包 deferred 项闭环）⑤ `agent_message` 投递事实事件（替代 wake_hook.py 侧车：定向 text/file 帧被本节点接收即外呼，payload 带 to_agent/content/msg_id/reply_to/channel=inbox|live，HMAC 签名）；wake-hook/README 加官方退役指引，既有装好侧车继续兼容；冒烟 7/7（隔离端口 18995-18997））
- Prometheus `/metrics`（queue_depth、task_duration、node_utilization）【§7.1】
- GitOps：watch 仓库 tasks/ 目录自动 apply【§8.3】——已交付（2026-10-19，`docs/gitops目录驱动-验证-2026-10-19.md`：sha256 差分轮询 + 与 POST /v1/tasks 同一内核口径 `reconcile::apply_task_spec`；缺省关、mesh 缺席自拒）
- 预热池形式化：常驻桥 = agent warmpool，任务到达即注上下文，冷启动 <200ms【§5.3】——已交付（2026-10-19，`docs/warmpool-预热池-验证-2026-10-19.md`：`engine.warmpool` 加性缺省关（members 显式名单 / keep_inflight / interval_secs / warmup.ping）；引擎 tick 内 keep-warm sweep——在线+未熔断+未满位才注入预热任务（与 POST /v1/tasks 同口径入库 + 直投），常驻桥 no-op 秒回收即保会话热态；`/v1/mesh/status` 出池视图；SDK BaseRunner 补 no-op 承接契约 + stop() 硬关连接；冷启动路径分解=引擎 tick 200ms 上界派发 + 常驻桥收帧即回；冒烟 6/6 隔离端口 18985/18986，单测 +1=121）
- 熔断隔离：异常节点自动置 unschedulable 标签【§4.5 taint 等价物】——已交付（2026-10-19，`docs/熔断隔离-验证-2026-10-19.md`：`engine.circuit` 加性缺省关；首次转入 Failed 按执行节点归账、滑窗阈值触发、dispatch 选点硬过滤、冷却到期惰性恢复；`/v1/mesh/status` 出 taints 视图）
- CLI 完全体：--dry-run、补全、`top nodes` 色标【§3.3】——已交付（2026-10-19，`docs/CLI-完全体-验证-2026-10-19.md`：`completions <shell>`（clap_complete 纯 Rust）+ `top` ANSI 色标快照/`--watch`（在线/CPU 三段色/熔断红标，mesh 任务摘要）；--dry-run v0.5 既有补验证）

验收门：20 机并发 200 任务 0 丢失 0 崩溃；P99 调度延迟 <500ms；外部用户 5 分钟装完并跑通多机任务。——单机仿真已过（2026-10-19，`docs/v0.6验收门-单机仿真-2026-10-19.md`：10 节点进程/10 常驻执行器/200 agentless 任务并发 → 200/200 succeeded、0 崩溃、P50 176ms/P99 234ms（<500ms，内存队列口径）、quota 跨 10 节点；**同时修复 M3 遗留真 bug**——bin-pack 只选节点不选 agent，agentless 派发必失败（scheduler select_agent + 2 单测，cargo test 123/123）；持久化模式 P99 ~2s 系每条 fsync（v0.7 group commit 候选，需拍板掉电语义）；真 20 机与「5 分钟上手」仍待实机/外部验收

## 3. v0.7.x — 商用化版（30~100 台，可售卖预演）

目标：团队付费意愿的功能与合规。

- spine/relay 中继拓扑 + 静态种子清单（破全网格连接数与广播风暴）——第一波已交付（2026-10-19，`docs/spine-relay-验证-2026-10-19.md`：`relay{enabled,spine,max_hops=2}` 加性缺省关；叶子经 `discovery.static_peers` 连 spine、非直连目标改投 spine；spine 验签+去重+`hops` 加性字段（超限丢弃）+透明转发（保 `from`）+逐跳 ACK/逐跳重试 + 广播扩散 + `agents_update` 中继；未知目标丢弃不与离线队列混用；单测 5 + 冒烟 7/7（三节点 A-R-B）零回归）；第二波已交付（2026-10-20，`docs/spine-relay-第二波-验证-2026-10-20.md`：`relay.upstream` 多级级联（链式相邻 spine 互为上游、未知目标兜底且不回弹）+ `ctrl/node_dir` 节点目录同步（`/v1/peers` 带 `relayed/via`、绝对 `expires` 防环路续期、fleet `node_online` 目录兜底）+ 控制面帧验签（`security.control_auth`，`agents_update`/`node_dir` 带签名必验、无签名可强制拒绝并审计；单测 11 + 四节点冒烟 10/10 含 kill-node TTL 收敛，零回归）
- journal group commit：`delivery.journal_sync_ms`（缺省 0=逐条 fsync 掉电零丢；>0=批量窗口，掉电最多丢一窗，突发派发 P99 从 ~2s 回到 395ms）——已交付（2026-10-19，`docs/journal-groupcommit-验证-2026-10-19.md`：批量线程 + Drop 收尾强刷；单测 +3=126；验收仿真 persist=true+50ms 5/5，P99 395ms）
- ABAC 策略引擎（label × node 属性 allow/deny，如 finance-data 禁入低安全级节点）+ namespace 多租户【§4.2】
- 不可变审计日志（hash 链 append-only）+ 导出 ELK/文件【§4.4】——已交付（2026-10-20，`docs/审计-验证-2026-10-20.md`：`security.audit{enabled=false}` 加性缺省关；`<data_dir>/audit/audit.jsonl` 逐条 `{seq,ts,event,actor,node_id,detail,prev,hash}`、`hash=sha256(prev‖"\n"‖body)`、每条 fsync；启动校验、损坏 fail-closed（拒绝追加但服务照跑）；task.submit/cancel/delete、lockdown.set、config.update（仅键名）、`audit_alert`（握手/准入/中继拒绝等）统一入链；`GET /v1/audit/verify` 与 `GET /v1/audit/export?limit=N` 仅 Admin；单测 4 + 七场景冒烟 7/7（含停机篡改后 invalid、未启用零文件），零回归）
- 消息护栏中间件：PII / API Key / prompt-injection 特征过滤；topic 默认 Deny All 作用域隔离【§4.3】
- 资源配额（每 namespace token/时长/磁盘）；进程隔离 Linux=cgroups v2、Windows=Job Objects【§5.4】
- 任务迁移 + checkpoint（SIGTERM 保存、他机恢复，工件走现有文件通道）【§6.2】
- 降级：控制面故障 read-only 模式；工作节点本地队列自治 Standalone【§6.3】
- chaos 套件：kill-node / latency / disk-fill【§6.4】——kill-node 第一波已交付（2026-10-20，`docs/chaos-验证-2026-10-20.md`：`mesh.engine.reclaim{enabled,grace_seconds=10,max_attempts=3}` 加性缺省关；`Router::node_online`（直连/目录/最近断开记忆）+ 派发按节点在线过滤 + 负载计入「已派发未 accepted」；失联宽限后任务回 pending 换机重派（独立重试预算、超限 failed 终态），`TaskRecord.assignee_node` 加性绑定；单测 2 + 六进程冒烟 6/6（kill W1/W2/W3、6/6 succeeded、reclaim 日志 3 条、零崩溃），零回归；latency/disk-fill 与 20 机实机版后续）
- SLO 告警 + 历史基线异常检测（耗时 +30% 类趋势）【§7.2】——已交付（2026-10-20，`docs/slo-验证-2026-10-20.md`：`mesh.engine.slo{enabled=false, window_seconds=300, min_samples=20, p99_ms=5000, error_rate=0.05, queue_depth=500, baseline_ratio=1.3, cooldown_seconds=60}` 加性缺省关；终态样本（排除预热池）→ 窗口 P50/P95/P99/错误率 + 慢速 EWMA 历史基线（`<data_dir>/slo_baseline.json` 跨重启）→ 四类告警（p99/error_rate/baseline_drift/queue_depth）同类冷却 + webhook `slo_alert`；`GET /v1/mesh/slo` 只读视图；单测 7 + 七场景冒烟 7/7（含 webhook 四类、冷却、未启用对照），零回归）
- OIDC SSO（企业采购门槛，先做 SSO 不做 HSM/FIPS）
- 流式结果前端逐字渲染（WS 通道已有，补 /web 侧）【§5.3】——已交付（2026-10-19，`docs/流式前端逐字渲染-验证-2026-10-19.md`：`/web` 星图页新增「任务流式视图」浮层——复用无 client 观察者 WS，`alStreamReduce`（seq 收敛/乱序丢弃/幂等重放/终态判定）与 `alStreamStep`（逐字单调不越界）为可提取纯函数，24ms/拍打屏；冒烟 5/5：页面标记静态检查 + node --check 全页语法 + 14 项纯逻辑单测（node 跑页面真实函数）+ 真节点/适配器端到端帧喂入逐字打满）

验收门：50 机全链路；chaos 注入 kill-node×3 自动回收任务；审计导出包交得出客户合规审查。

## 4. v1.0+ — 商业形态（Open Core）

- **内置 Raft 3/5 master 选主 + quorum 脑裂保护**（tokio+badger-raft，无外部 etcd）【§6.1，工程最重，压轴】
- WASM 插件运行时（wasmtime）+ plugin SDK + 插件市场 `mesh plugin install`【§2.3、§8.1】
- 事件溯源 + time-travel 回放（/v1/history 是现成雏形）【§7.3】
- Kafka/RabbitMQ 导出、GitHub Actions / Jenkins 插件【§8.2、§8.3】
- GPU 共享（MPS/MIG）——仅当出现 GPU 节点客户再排期【§5.4】
- 版本矩阵与 license：Community（任务引擎全量、≤10 节点、免费）/ Team（按节点计费）/ Enterprise（HA、SSO、多租户、闭源插件）；license 模块做节点数校验 + 离线激活【第十章定价：Node-Based，控制节点免费】
- 仓库拆分：agentlink（community crate，Apache-2.0）+ agentlink-enterprise（闭源特性门控 feature flag）

## 5. 设计案条目 → 版本映射总表

| 设计案 | 实现形态（不破坏单二进制） | 版本 |
|---|---|---|
| 2.1 声明式 API/Reconcile | 桥层协议 → 内核引擎 | v0.5 → v0.6 |
| 2.2 微内核拆分 | 进程内模块边界；Scheduler/Controller=任务+事件订阅者 | v0.6（形态转换） |
| 2.2 Event Bus（NATS） | 现有 WS 骨干 + 进程内队列 | v0.6 |
| 2.3 插件（WASM） | v0.6 adapter 接口 → v1.0 wasmtime | v0.6/v1.0 |
| 3.1/3.4 旅程与首次运行 | wizard + hello-world 任务 | v0.5 |
| 3.2 Web 面板 | timeline v0.5；Bento 仪表盘 v0.6；资源热力图 v0.7 | v0.5~0.7 |
| 3.3 CLI | 薄壳 v0.5、完全体 v0.6 | v0.5/0.6 |
| 4.1 mTLS/准入 | 内置 CA 可选启用；bootstrap+指纹 | v0.6 |
| 4.2 RBAC/ABAC | RBAC v0.6；ABAC v0.7 | v0.6/0.7 |
| 4.3 消息沙箱护栏 | 总线 middleware + topic Deny All | v0.7 |
| 4.4 脱敏/审计 | 脱敏 v0.5；不可变审计+导出 v0.7 | v0.5/0.7 |
| 4.5 熔断/Kill Switch | lockdown v0.5；自动隔离 v0.6 | v0.5/0.6 |
| 5.1~5.3 性能 | Lease/差异/增量调度/预热/流式 | v0.6（流式前端 v0.7） |
| 5.4 cgroups/GPU | v0.7 cgroups+Job Objects；GPU v1.0+ | v0.7/v1.0 |
| 6.1 HA Raft | 内置 badger-raft | v1.0 |
| 6.2/6.3 自愈/降级 | checkpoint 迁移；read-only/standalone | v0.7 |
| 6.4 混沌 | mesh chaos 命令 | v0.7 |
| 7.1~7.3 可观测 | 日志 v0.5、metrics v0.6、traces/alerting v0.7、event-sourcing v1.0 | v0.5~1.0 |
| 8.x 生态 | webhook v0.6、GitOps v0.6、marketplace v1.0、Kafka/CI 插件 v1.0 | v0.6~1.0 |
| 9 MVP 三阶段 | 被本路线图 v0.5/0.6/0.7 阶梯取代 | — |
| 10 商业化 | Open Core + Node-Based license | v1.0 |

## 6. 市场边界（非特性裁剪，是客群选择）

多 region/geo-failover、千节点集群、HSM/FIPS、SAP/Salesforce 手写连接器——客群（个人/OPC/中小团队）不存在这些场景，不进路线图；企业数据平台对接以"导出接口"（Kafka/webhook）替代。

## 7. 商业模式（2026-10-16 拍板，正本）

**AgentLink 免费 / AgentMesh 收费**，一个代码库两种构建（feature gate，单二进制形态不变，license=本地文件）：

- **AgentLink（OSS，Apache-2.0，永远免费）**：总线/文件/发现/注册表/事件流/**任务协议 spec 公开**/基础 web/CLI 薄壳。免费层必须传输完整、单独可用——协议公开即生态护城河，他人在总线上写编排器=替我们做标准。
- **AgentMesh（收费）**：内置 reconcile 引擎+调度、断点续传/迁移、RBAC/ABAC、mTLS+内置 CA、不可变审计、消息护栏、GitOps、配额隔离、chaos、HA(Raft)、WASM 插件市场、OIDC。卖的是"敢放进团队生产"的治理与省心，不是第二条总线。
- **定价**：按*被 Mesh 调度的节点数*计费，控制节点一律免费。Starter ≤15 机（OPC）/ Pro ≤50 机（治理件）/ Enterprise（HA/SSO/多租户，年框）。自托管 + 离线 license、绝不 phone-home（与"数据不出 LAN"宣言一致，本地验签+本地点数）。
- **与版本对应**：v0.5 全进 OSS；v0.6 引擎内核化=Mesh 收费起点；v0.7=Pro；v1.0=Enterprise。
- **红线**：support/hosting 不做主营收（OPC 扛不动 SLA），营收只靠 license。

## 8. 版本增量惯例延续

每个 minor 升版照旧滚动升级协议先升自家舰队再放包（08:00–22:00 窗、先远端后本机）；v0.5 起 task 协议自身就是升级编排的新底座——吃自己的狗粮。
