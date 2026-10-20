## v0.6.1 发布物说明（2026-10-20，机器身份铁律 + Windows 目标修复）

**核心：node_id 一旦生成就永不变——重启、换发行目录、配置被删/重建/`init --force` 都不会换身份。**

- **身份锚点**：node_id 另存 `<data_dir>/node-identity.json`（原子写）。启动时以锚点为准，
  配置里的 node_id 若漂移会被就地修回并重新落盘；配置缺失/半截写入时从锚点恢复。
  锚点放在 data_dir（Windows 默认 `%LOCALAPPDATA%\AgentLink\data`），跨发行目录稳定。
- **新命令 `agentlink identity`**：
  - `agentlink --config agentlink.json identity` —— 查看当前身份/锚点/漂移状态；
  - `... identity --set <旧node_id>` —— 显式改回旧身份（同时改配置 + 锚点，改完需重启）；
  - `... identity --recover` —— 从 backups/、upgrade-ledger.jsonl、出站历史、agents.json
    自动找回上一个身份并恢复（列出全部候选）。
- **配置原子写**：`Config::save()` 改为临时文件 + fsync + rename，进程被杀不再留半截 JSON。
- **Windows 目标修复**：`src/load.rs` 的 wmic/PowerShell 探测把 `Option` 当 `Result` 用，
  导致 v0.6 起 Windows 交叉编译失败（此前 Windows 节点停留在 v0.5.10）。本版起可正常编译 Windows 版。
- 协议与消息格式不变，0.5.x/0.6.0 混布兼容；配置文件向后兼容（无 node-identity.json 的旧节点首次启动自动补写）。

发布包：`AgentLink-v0.6.1.zip`（9 文件，win/linux 双二进制）
sha256 = `4c68d2981e0db89e7343f1fb9742e0cb7b8712ada402d1130654be950af18d5a`（6,512,647 B）

## v0.5.6 发布物说明（2026-10-17，Web 控制台第十三/十四轮，协议兼容 0.5.x）

- 星图消息弹窗瘦身：删话题标题行与底部重复胶囊，时间+类型胶囊并入路由行居右——弹窗=顶行序列/关闭 → 路由行（左 A→B 胶囊、右时间+类型）→ 正文 → 快捷键提示。
- 星图磁避让：拖动 agent 胶囊时，被压到/贴近的其他胶囊沿射线瞬时让开（单枚上限 120px），松手平滑回位；
  连线/粒子/卡组同帧跟随，落位口径不变，不持久化、复位与重建即清空。
- 协议不变，0.5.x 混布兼容；仅 Web 观测页改动，桥接脚本无需更新。

## v0.5.5 发布物说明（2026-10-17，Web 控制台第十一/十二轮，协议兼容 0.5.x）

- 星图连线闪烁修复：飞行粒子不再被快轮询/整体重建绞杀（temp 线保留、飞行线重绑、悬停态跨重建恢复、fetch 8s 超时自愈）。
- 星图右下角控件简化：去掉「实时」按钮与时段弹出面板、去掉时间区间文本——星图只提供实时视图，
  时间窗恒为跟随现在的滑窗；「窗口长度」滑杆上限放宽到 1 小时（60s–3600s，步进 60s，默认 10min）。
- 协议不变，0.5.x 混布兼容；仅 Web 观测页改动，桥接脚本无需更新。

## v0.5.4 发布物说明（2026-10-17，纯 Web/观测轮次，协议兼容 0.5.x）

- 星图实时化：`/v1/ws` 观察者通道（观测页可见定向消息），事件驱动 400ms 合并刷新 + 断线指数退避，
  HUD 增「推送：实时/轮询5s」指示灯；高频突发防卡顿（poll 合流、粒子预算 8/轮、>60 条跳过粒子）。
- 星图布局自愈：尺寸一律以画布 rect 为准 + 每轮对表（修"刷新后整体偏左需二次刷新"）。
- 悬停胶囊"有卡无线"修复：话题聚焦校验悬停态，卡组重建跳过正在悬停的展开。
- 右下角按钮改版：删「应用时段」按钮与时间区间滑杆；「实时」按钮点击即在 实时↔指定时段 切换，
  开始/截止时间在按钮上方弹出面板选择，时段内改时间即时生效。
- 消息弹窗展示话题完整往来链（含当前实体未直接参与的中转消息），列表逐行标注发件人；
  卡片条数与弹窗序列同口径。
- 胶囊扁平化定版：全项目统一「无阴影＋cap-nudge 悬停微动效」，弹窗/画布/对话记录三处同款。
- 控制台：「刷新」按钮联动星图即时刷新；「查磁盘」按钮改名「查询」。

# AgentLink

局域网内多台机器上 **AI Agent 之间** 的通信总线。只为程序服务，不为人聊天。

一台机器跑一个常驻小进程（Node），本机 Agent 通过 `127.0.0.1` 的 HTTP/WS 接口收发消息与文件；
各机器的 Node 之间自动发现、建立 WebSocket 连接，把消息可靠地送到对端或广播给所有节点。

- 设计文档：[`docs/设计方案.md`](docs/设计方案.md)
- 协议规范：[`docs/protocol.md`](docs/protocol.md)
- 部署指南：[`docs/部署.md`](docs/部署.md)
- 定位：小、轻、稳、零配置
- 不做：任务板、角色体系、工作流编排、人类聊天 UI、公网穿透

## 状态

全部里程碑已完成：单文件 ~1.5 MB 的 Rust 常驻进程，包含发现、连接、可靠消息、文件传输、安全与部署脚本。

> v0.4.9：Windows 开机自启内置——`agentlink autostart` 在当前用户"启动"文件夹写入
> `AgentLink.vbs` 隐藏窗口启动脚本（无需管理员），`启动.bat` 双击时自动静默注册；
> `run --no-open` 后台拉起不弹浏览器。Linux 侧仍用 `deploy/install-systemd.sh`。
>
> v0.4.16：总览页重构——快捷发送面板移除，替换为「消息星图」全屏面板（`/v1/starmap` iframe）；
> 对话消息按话题链可视化，悬停/点选节点、agent、消息卡片只显示相关连线与目的地；
> 历史记录条目新增 `reply_to` 透传，`/v1/history/search` 返回回复链元数据。
>
> v0.4.19：Web 控制台第四轮修订（星图弹窗滚轮分区与换组按钮、节点圈收缩、侧栏拖拽/折叠、全局状态栏、全面局部刷新）。
>
> v0.5.0：AgentMesh v0.5 内核首版——新增 `/v1/events` 控制面事件流（append-only seq 续传、环满 409 降级、agent/types 过滤，编排方告别轮询）；payload 脱敏管线（history/events/web 观察者统一净化，收件人拿原文，`redact.patterns` 可配 + 内置默认集）；`/v1/fleet/agents?requires=` 能力查询（cap: 分层通配，仅在线）；`/v1/tasks` 任务资源视图（history 派生投影，correlation/state 过滤，任务协议 v1 见 docs/task-protocol.md）；`/v1/health` 增 `features` 数组供编排方探测降级；Web 控制台第五/六轮收口（history 节点+agent 四维过滤、status 暴露 listen_addr、星图端点下拉与自检面板）。全部加性接口，老节点混跑不破。
>
> v0.5.2：Web 控制台第七/八轮——顶栏在线节点口径修正为含本机；磁盘查询防抖自动搜索；agent 胶囊 hover 微动效；星图消息弹窗路由改双层胶囊（与对话记录同款，`@node` 端 `●节点名` 回退）。纯前端，协议兼容。
>
> v0.5.3：Web 控制台第九轮——发消息/文件区新增「发件身份」下拉（本机注册 agent，默认记住上次选择，可选「整机」）；出站文本与文件上传（`/v1/send`、`/v1/send-file-upload` 的 `from_agent`）历史记录带上发件 agent 身份，对话记录/台账发端双层胶囊不再缺失。入站文件通道为整机级，接收侧记录不带对端 agent 身份（协议限制，代码注释注明）。加性改动，协议兼容 0.5.x/0.4.x。
>
> v0.4.10：数据目录默认外置（Windows `%LOCALAPPDATA%\AgentLink\data`、Linux `~/.local/share/agentlink`），
> 不再放软件目录里以免升级覆盖；`agentlink.json` 的 `data_dir` 可按机器各自改为绝对路径，
> 相对路径以配置文件目录为基准。发布包新增 `SKILL-agent-registration.md`（Agent 上线注册技能，
> 每台机器的 Agent 都应先学习并执行它）。

| 阶段 | 内容 | 状态 |
|------|------|------|
| M0 | 骨架：配置 / 日志 / CLI / health | ✅ |
| M1 | UDP 发现 + WS 建连 + 心跳 + `/v1/peers`（含静态 peers） | ✅ |
| M2 | 消息：信封 / 点对点 / 广播 / `/v1/send` + WS 订阅 | ✅ |
| M3 | 可靠性：ACK / 重试 / 去重 / 离线暂存 | ✅ |
| M4 | 文件传输：分块 / 校验 / 续传位 / 大小与文件名安全 | ✅ |
| M5 | 安全（token/HMAC/防重放）+ 服务化打包脚本 | ✅ |
| M6 | mDNS（feature 开关）/ 静态 peers / 指标 / 文档 | ✅ |

## 构建

```bash
cargo build --release
# 产物：target/release/agentlink（Linux/macOS）或 agentlink.exe（Windows）
# release 体积约 1.5 MB（LTO + strip + opt-level=z）

# 可选：启用 mDNS 发现（需在配置 discovery.methods 加入 "mdns"）
cargo build --release --features mdns
```

## 使用

```bash
# 1) 主机：创建配置（自动生成随机 node_id 与 cluster_token）
agentlink init --name agent-PC-A
#    init 会直接提示：其他机器运行 agentlink join <token> 一键配对

# 2) 其他机器：自动配对（无需手改 agentlink.json）
agentlink join <token> --name agent-PC-B

# 免密快速测试（局域网互信，跳过 token 校验）
agentlink init --open        # 或 agentlink join <token> --open

# 查看本机 cluster_token
agentlink token

# 3) 运行
agentlink run

# 4) 查看状态与节点
curl http://127.0.0.1:8765/v1/health
curl -H 'X-Auth-Token: <local token>' http://127.0.0.1:8765/v1/status
curl -H 'X-Auth-Token: <local token>' http://127.0.0.1:8765/v1/peers
```

## 本地接口

| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| GET | `/v1/health` | 无 | 存活探针 |
| GET | `/v1/status` | `X-Auth-Token` | 节点状态（peers / pending_acks / offline_queue / local_pending / active_transfers） |
| GET | `/v1/peers` | `X-Auth-Token` | 已知节点及在线状态 |
| POST | `/v1/send` | `X-Auth-Token` | 发消息（点对点 / `"*"` 广播）；body 无需 Content-Type |
| POST | `/v1/send-file` | `X-Auth-Token` | 传本地文件（`to` + `file_path`） |
| POST | `/v1/send-file-upload` | `X-Auth-Token` | 上传字节流发文件（`?to=` + 头 `X-File-Name`），供 `/web` 页面使用；上限 = `files.max_file_size`（默认 100 MB）。**v0.4.13 前实际卡在 2 MiB**（axum 默认体上限），超 2 MiB 会报纯文本 413 |
| POST | `/v1/ack-inbox` | `X-Auth-Token` | 确认已读的本机消息 |
| GET/POST/DELETE | `/v1/agents` | `X-Auth-Token` | 注册/查看/注销本机 Agent（多 Agent，能力标签） |
| GET | `/v1/fleet/agents` | `X-Auth-Token` | 全网 Agent 目录（节点间自动同步） |
| GET/PUT | `/v1/config` | `X-Auth-Token` | 查看/修改配置（掩码显示；名称/标签即时生效） |
| GET | `/v1/history` | `X-Auth-Token` | 最近对话记录（环形缓冲） |
| GET | `/v1/starmap` | 无需令牌（HTML 页） | 消息星图全屏页：总览内嵌 iframe 同源展示，数据源 /v1/history/search + 拓扑接口（v0.4.16+） |
| GET | `/v1/history/stats` | `X-Auth-Token` | 对话记录体积统计（活动/归档/合计、滚动阈值，v0.4.15+） |
| GET | `/v1/history/export?format=json\|text&scope=all\|active\|archive:<名>` | `X-Auth-Token` | 导出对话记录（带下载名；只读，不动源文件，v0.4.15+） |
| GET | `/v1/history/search?from=&to=&who=&direction=&kind=&q=&since=&until=&limit=&max_mb=&scope=` | `X-Auth-Token` | 对话记录检索：按收发方/方向/类型/关键词/时间过滤（v0.4.15+） |
| GET/PUT | `/v1/colors` | `X-Auth-Token` | 节点与 Agent 的标识颜色：自动配色 + 手动覆盖（v0.4.15+） |
| GET | `/web` | 无（token 页内输入） | **人工可看页面**（V1 风格）：总览/节点/Agent/对话/发送/配置；token 只存会话不进 URL |
| WS | `/v1/ws?token=…&client=…` | query token | 订阅本机入站消息（`client` 即 agent_id，按 `to_agent` 定向投递）与 join/leave 事件 |
| WS | `/peer/ws` | cluster token（握手） | 对等节点消息连接 |
| WS | `/peer/file/ws` | cluster token（握手） | 对等节点文件传输连接 |

```bash
# 发消息（点对点 or 广播）
curl -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: $TOKEN" -d \
  '{"to":"agent-B","type":"text","action":"send","payload":{"content":"hello"}}'
curl -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: $TOKEN" -d \
  '{"to":"*","type":"text","action":"broadcast","payload":{"content":"notify all"}}'

# 发文件
curl -X POST http://127.0.0.1:8765/v1/send-file -H "X-Auth-Token: $TOKEN" -d \
  '{"to":"agent-B","file_path":"C:\\data\\report.pdf"}'
```

## 架构

- **发现**：UDP 广播（默认）→ mDNS（`--features mdns`）→ 静态 peers（兜底）
- **拓扑**：Mesh 全互联 WebSocket，`node_id` 字典序小的主动拨号，每对一条连接
- **可靠性**：点对点 at-least-once（ACK + 有界重试 + 离线暂存 TTL），广播 at-most-once
- **消息**：统一 JSON 信封（含 `from_agent`/`to_agent`），多 Agent 一机并存、Agent 间对话，`payload` 完全透传
- **Agent 目录**：注册能力标签，节点间 `ctrl/agents_update` 同步，`/v1/fleet/agents` 看全网
- **人工视图**：`/web` 页面 + `/v1/history` 看节点 / Agent / 双方聊了啥
- **机器标签**：`tags` 进 `/v1/peers`（A：能力标签）
- **文件**：独立 `/peer/file/ws` 连接，分块 + SHA-256 校验 + `recv_dir` 沙箱
- **安全**：`strict`（共享 `cluster_token` + HMAC 握手/帧签名 + 时间窗防重放）/ `open`

## 端口

- `8765`（`listen_port`）：本地 API + 对等节点 WebSocket
- `8766`（`broadcast_port`）：UDP 发现广播

首次运行请在防火墙放行这两个端口（见 [`docs/部署.md`](docs/部署.md)）。
## 版本提要
- **v0.4.12**：消息可靠性持久化——队列（inbox/offline/pending/local/seen）以
  `data_dir/journal.jsonl` 追加日志（每条 fsync，重启全量回放，过期离线件自动丢弃）；
  agent 注册表原子快照 `data_dir/agents.json`；`history.jsonl` 重启回放、超 16MB 轮转。
  开关：`delivery.persist=false` 关日志（临时测试节点）、`delivery.agent_stale_ttl_seconds`
  清理久未重注册的离线 agent（默认 3600，0 关闭）。`/v1/status` 新增 inbox/durable 字段。
  web 控制台：节点/Agent 表头排序、消息展开与复制、选中文本不再被自动刷新打断、toast 提示。
- **v0.4.11**：`init/join --force` 保留 node_id/token/data_dir（堵死身份漂移事故）；
  fleet agents 在线状态联动节点（幽灵在线消除）；web 节点列表加「本机」标记与离线节点「遗忘」
  按钮（`DELETE /v1/peers/<id>/forget`，持久化 data_dir/forgotten_peers.json，真实握手自动解禁）。

## v0.5.3 发布物说明（Web 控制台第九轮）

- 发件身份下拉：发消息/文件区新增「发件身份」选择器，列出本机已注册 agent，默认值记住上次
  选择（localStorage），可选「整机」（不携带 agent 身份）。
- 出站历史记录带上 `from_agent`：`/v1/send` 透传身份选择；`/v1/send-file-upload` 新增
  `from_agent` 查询参数，文件出站台账记录发件 agent——修复对话记录/台账发端双层胶囊缺失的根因
  （发送端此前不带 from_agent）。
- 已知边界：文件通道是整机级，入站记录无法携带对端 agent 身份（协议限制，代码注释注明）。
- 加性改动，协议兼容 v0.5.x/v0.4.x，各机可独立择时升级。

## v0.5.2 发布物说明（Web 控制台第七/八轮）

- 版本号说明：0.5.1 编号预留给待并入的合并线（CLI 薄壳 / JSON 日志 / 任务 Timeline 熔断），
  本发布为纯 Web 控制台改动，跳号 0.5.2。前端-only、协议兼容 v0.5.0/v0.4.x，各机可独立择时升级。
- 第七轮：总览顶栏「在线节点」口径修正为**含本机**（原来少算本机，如 3/3 实际 4 台显示 4/4）；
  磁盘查询条件 350ms 防抖自动搜索（保留手动按钮）；agent 双层胶囊 hover 时内胶囊左右缓晃微动效
  （`prefers-reduced-motion` 自动降级为不动）。
- 第八轮：星图消息弹窗的路由（头部 `v-hd` 与 meta 行 `v-route`）从纯文本
  `【a@node -> b@node】` 改为**双层胶囊**，CSS/配色/几何与对话记录 `chip()` 逐字同源
  （外层节点深档壳 + 内嵌 agent 派生色胶囊，左弧 ≈14px）；节点级端点（`@node`）与对话记录
  一致回退 `●+节点名`。
- 已知问题归档：对话记录部分定时消息行首无 agent 胶囊（只有 `●节点`），根因是**发送端脚本**
  未带 `from_agent` 字段，属数据侧，渲染层回退正确；发送端修复另行推进，不影响本机升级。

## v0.5.0 发布物说明（及时响应标准化）

- 新增**及时响应标准三件套**发布物：`AgentLink-v0.5.0-response-kit.zip`（顶层 `response-kit/`：
  install-all.sh、hermes_bridge.py、wake_hook.py、bridge.sh、README.md、SKILL.md、
  **SKILL-message-response.md**（标准正本）、**al_watch.py**（cron 看门狗通用版））。
- 散件同步入本目录：`SKILL-message-response.md`、`al_watch.py`；
  `AGENTS-使用说明.md` 新增 §11「及时响应标准（强烈建议默认配置）」；
  `SKILL.md` 上线自检新增第 7 步；`ONBOARDING-新机器接入.md` 刷新至 v0.5.0 口径。
- 清理过期发布件：`SKILL-agent-registration.md`（与 `SKILL.md` 逐字节重复）已删除——
  注册技能正本从此**只有** `SKILL.md` 一个文件。
- 产品立场：常驻桥+唤醒钩子+看门狗是使用 AgentLink 的**强烈建议默认配置**，
  各接入 agent 应按 `SKILL-message-response.md` 的自查清单完成标准化并定期复查。

## v0.4.15 发布物说明

- 对话记录不再丢：活动文件写满或超龄后**整份搬进** `data/history_archive/`（按滚动时刻命名），
  旧的单份 `.1` 备份（覆盖即删记录）已废弃；启动时自动收编遗留 `.1` 为 `history-legacy.jsonl`。
  `history.archive_keep=0`（默认）= 归档永久保留，不自动删。
- 界面导出：对话页与总览页显示「当前/归档/合计体积 + 滚动阈值」，只读导出 JSONL / 可读文本
  （可选含归档或仅当前）；**当前**活动文件达到阈值 80% 时标红提醒「接近阈值，建议导出备份」。
- 布局：版本号徽章挪到「AgentLink」标题之后；两个发送面板的「发送」按钮挪到文件/拖拽/目录控件下面。
- 默认阈值：活动文件 1 GB（`history.max_file_mb=1024`）或 7 天（`history.archive_days=7`）双触发滚动；
  导出为流式响应（多文件按行拼接、256 KiB 分块），大体积导出不吃服务端内存；
  后端另支持 `scope=archive:<文件名>` 单份归档导出。

## v0.4.14 发布物说明

- 界面：标题/顶栏带版本号；「发送」按钮合并文本+附件；支持 Ctrl+V 贴图、拖拽投递区、
  选择目录自动打包 `<目录名>.zip` 发送。
- 错误可见性：HTTP 非 2xx 不再被静默当作成功（v0.4.13 之前 >2 MiB 上传会显示 `{}` 且清空选择，
  看起来像「文件消失了」）。
- 上传体积上限跟随 `agentlink.json` 的 `files.max_file_size`（默认 100 MB）。
