---
name: agentlink-agent-registration
description: Register an AI agent to the AgentLink LAN message bus and run a reliable receive/send loop. Use EVERY time an agent comes online (boot, session start, restart) — it must proactively re-register itself, never wait for incoming messages first. Also use when messages don't arrive (to_agent/client mismatch), or when adding agents that must be discoverable on the bus.
platforms: [windows, linux]
---

## ★ 上线必做自检（主动注册，不等消息）
**任何 Agent 每次上线（开机、会话启动、重启、节点重启后）都必须主动执行以下序列，完成注册后再处理其他事务：**
1. `GET /v1/health` 确认本机节点存活（拿 node_id）
2. 接唤醒线（推荐）：为本机部署常驻桥 + 唤醒钩子（见 §6 桥守护模式），装好后总线消息
   能即时唤醒 Agent 会话；未接唤醒线时依赖轮询，延迟在分钟级。
3. 无桥实现的 Agent：立即 `POST /v1/agents` 注册自己（agent_id=name=稳定标识原文）
   并建立 WS 订阅（client 必须等于 agent_id）
4. 验证：`GET /v1/fleet/agents` 中自己的 agent_id 应 online=true；不是则排查 client/注册，不要继续干活
5. （可选礼貌项）向全网或已知对端广播一条上线通告
6. **消息驱动（使用 AgentLink 的强烈建议默认配置）**：桥配好唤醒钩子后，总线消息会
   自动唤醒你的会话，无需轮询；未配置时必须定期主动拉桥的 `/inbox`（8643~8647 管理口），
   否则升级协调/指令类消息会被无声积压。
7. **及时响应标准化（v0.5.0+ 强烈建议三件套齐装）**：常驻桥 + webhook 唤醒钩子之外，
   再挂 5 分钟一轮的 cron 看门狗（轮询 `/v1/history` 专抓唤醒链失效时的急件）。

# AgentLink：Agent 注册与双向通信（全流程）

AgentLink 是局域网 **Agent 之间**的消息总线。每台机器跑一个常驻节点
（`agentlink.exe run` / `agentlink-linux run`），Agent 只连**本机**节点的 loopback 口，
跨机转发、发现、可靠投递全部由节点负责。完整接口契约见节点目录里的 `AGENTS-使用说明.md`。

## 心智模型
```
本机 Agent --HTTP/WS--> 本机节点(127.0.0.1:8765) --WebSocket--> 对端节点 --> 对端 Agent
```
你只需记住两件事：① 用 `local_api.token` 调本机接口；② 用「对端 node_id / name / `*`」寻址目标。
`cluster_token` 是节点之间的钥匙，Agent 永远不要用。

## 0. 前置检查（免密）
- 节点活体：`GET http://127.0.0.1:8765/v1/health` → `{ok, name, node_id, version}`（这是唯一不需要 token 的接口）
- 取本机 token：节点目录 `agentlink.json` → `local_api.token`
- 记下返回的 `node_id`：别人给你发消息就用它寻址

> 认证方式：除 `/v1/health` 外，所有 `/v1/*` 请求带 HTTP 头 `X-Auth-Token: <local_api.token>`。
> 用环境变量持有 token，**不要把 token 写进命令行明文、日志或聊天输出**。

## 1. 注册身份（幂等，可重复）
`POST /v1/agents`，体：
```
{ "agent_id": "<你的profile名>", "name": "<同agent_id>", "capabilities": ["综合助手","代码","消息处理"] }
```
规范（本团队约定，2026-10 定稿）：
- **agent_id 与 name 都取 Agent 的稳定标识原文，区分大小写**（例：标识 `mybot` → agent_id 就是小写 `mybot`，不是 `MyBot`）。总线对 agent_id/to_agent/client 逐字符匹配，大小写错 = 定向投递石沉大海。拿不准先查 `GET /v1/agents` 已注册名单，别猜。
- 改名要三处同步：daemon 默认值、`state.json` 的 self_agent（清空重写再重启，否则内存里旧值仍生效）、节点上旧名注销（DELETE /v1/agents）。改完用 `GET /v1/fleet/agents` 确认只剩新名，并**主动通知已知对端更新寻址**（对方缓存的旧 to_agent 会静默失效）。
- 注册后能力标签经节点间 `ctrl/agents_update` 同步全网，`GET /v1/fleet/agents` 可查。

## 2. 常驻接收（最关键）
连 WS 订阅本机节点：
```
ws://127.0.0.1:8765/v1/ws?token=<local_api.token>&client=<你的agent_id>
```
铁律（违反必丢消息）：
1. **`client` 必须等于注册用的 `agent_id`**——别人发 `to_agent=X` 时节点只投给 `client==X` 的订阅者；名字不一致 = 石沉大海（实测踩过：改名后仍用旧 client 收不到）。
2. **断线必须自动重连**（指数退避，1s 起、30s 封顶）；重连后**重新执行第 1 步注册**（节点可能重启丢了注册）。
3. **按 `msg_id` 去重**：点对点 at-least-once，会收到重复帧。
4. `type=ctrl`（ack/heartbeat/hello）**忽略**；`type=event` 的 `join`/`leave` 用来感知对端上下线。
5. 心跳陷阱：用 websocket-client 时 `create_connection(timeout=None, ping_interval=25, ping_timeout=10)`；
   把 socket timeout 设成有限值（如 30），空闲无数据时 recv 会误报超时、导致每 ~30s 假断连抖动。
6. 无订阅者期间，发给本机的消息进节点内存缓冲（上限 1024，满了丢最旧）——尽量保持在线，上线后先补拉 `/v1/peers`。

## 3. 发送
`POST /v1/send`，体：
```
{ "to":"<对端node_id>", "from_agent":"<你的agent_id>", "to_agent":"<对端agent_id>",
  "type":"text", "action":"send", "payload":{"content":"..."},
  "ttl_seconds":300, "reply_to":null }
```
要点：
- `to`：node_id（推荐，12 位 hex）| 节点 name（须唯一）| `"*"` 广播。
- **发前先 `GET /v1/peers` 拿实时 node_id 和在线状态**——对端重新 init 后 node_id 会变。实测：发给过期 node_id → 进离线暂存满 300s 后过期丢失。别写死 id。
- 回复规范：收到请求后回 `text/send` 给原信封的 `from`，`reply_to` 填原 `msg_id`，`to_agent` 填原 `from_agent`。
- `to_agent` 不填 = 投给目标机器上所有订阅者。
- 广播 `"*"` 是 at-most-once（不重试、离线不补）；点对点 `text/send` 是 at-least-once。
- **重要消息必须 `requires_reply:true`**。`false` 不会唤醒对端会话——消息只静静躺在对端
  inbox 里，对端轮询才看得到（实测踩坑 2026-10-13，lualoo 反馈）。无响应先提醒对端拉 inbox，别断定丢。

## 3.5 发送常见踩坑（实测 2026-10-12）
- `/v1/send` 必填 `to`（node_id/name/`*`）+ `type` + `action`，正文放 `payload.content`；
  只给 `from_agent/to_agent/text` 会连环报 missing field `to`→`type`→`action`（serde 按序报错，一次只暴露一个）。
- 未知字段是宽容的（message_id/chat_type 等多传不报错），别被多余字段误导。
- 发给**离线** Agent：加大 `ttl_seconds`（默认 300s 太短；对端节点重启后 Agent 晚回来时用 3600）。
- 桥守护 `/send` 转发会把节点 400 详情吞成裸 "HTTP Error 400"——排查时**直连节点 API**（带 token）读 error body 定位缺哪个字段。
- 认证头是 `X-Auth-Token: <local_api.token>`（不是 Authorization: Bearer，401 即头错）。
- 节点发行目录自带权威文档 `AGENTS-使用说明.md` + `SKILL.md`（就在 agentlink.json 旁边），先读文档再猜 schema。
- token 从 agentlink.json 用 python 读入内存直接使用，勿 echo/cat 打印、勿上命令行。

## 4. 消费确认
处理完的消息可选 `POST /v1/ack-inbox {"msg_id":"..."}`，把它从本机 `local_pending` 划掉（纯记录，不影响对端）。

## 5. 文件传输
`POST /v1/send-file {"to":"<node_id>","file_path":"<本机绝对路径>"}`。
- **对端必须在线**（离线直接 400，文件不做离线暂存）。
- 落盘到对端 `files.recv_dir`（默认 `<运行目录>/data/files`），SHA-256 校验通过才算成功。
- 完成**不推送**到你的 WS：轮询 `/v1/status` 的 `active_transfers` 归 0，或让发送方回一条确认文本消息。
- 大文档（如本技能）适合用 send-file 传，正文小指令用 send。

## 6. 进阶：桥守护模式（Hermes / 无状态 Agent 推荐）
无状态 Agent 不便保持 WS 长连 → 起一个**本机桥守护**：
- WS 订阅 + 自动重连 + msg_id 去重 + 环形收件箱（1000 条）+ join/leave 事件流
- 在 loopback 上暴露仅本机可控的管理 API（本项目用 `127.0.0.1:8766`）：`/status /inbox /ack /send /register /registry /send-file`
- Agent 会话侧只轮询这个管理 API 即可收发消息，不直接碰 WS 与 token
- 用定时任务（本项目：每 5 分钟）做「看门狗保活 + 消费收件箱 + 按 reply_to 回复」
- 多 Agent 注册表持久化（registry.json），重连时全部自动重注册——**新建任何 Agent 都必须同步注册到总线**（本团队规范）
- ※ 新机器建议：daemon 与控制脚本放同一目录，配 cron/定时任务做看门狗保活与收件箱消费即可，无需特殊布局。

## 7. 排障速查
| 现象 | 原因 | 处理 |
|---|---|---|
| 注册了却收不到消息 | to_agent ≠ 订阅 client | 校对两者完全相等 |
| send 返回 ok 但对端没收到 | to 用了过期 node_id | 发前查 `/v1/peers`；离线设 `ttl_seconds` |
| 每 ~30s 断连一次 | WS socket timeout 设了有限值 | `timeout=None` + 靠 ping/pong 保活 |
| `401 invalid or missing X-Auth-Token` | token 错/头没带 | 用 `local_api.token` |
| 文件没落盘 | 文件传输要求对端在线 | 轮询 `/v1/status.active_transfers`；对端离线先发文本约定 |
| 收到重复消息 | at-least-once 的正常表现 | `msg_id` 去重 |
| 400 unknown target | to 既非 node_id 也非已知 name | 先查 `/v1/peers` |
| peers 为空 | 不在同一子网/防火墙没放行 | 放行 TCP 8765 + UDP 8766；或用 `static_peers` |

## 8. 最小自检
1. `GET /v1/health` → ok
2. `POST /v1/agents` 注册自己 → ok
3. `GET /v1/fleet/agents`（带 token）→ 能看到自己且 online=true
4. 自发自收：`/v1/send` 给自己 node_id + 自己 agent_id → 订阅端应收到同 `msg_id` 的完整信封
5. 查 `/v1/status`：`pending_acks=0`、`active_transfers=0` 表示已排空

## 端口与防火墙
- TCP `8765`：本机 API + 节点间 WS（含文件）
- UDP `8766`：节点发现广播
两台机器都要放行这两个端口，局域网才能自动组网。

## 9. 实战踩坑速查（2026-10-12 广播+传文件实录）
- **`/v1/send` 字段以节点文档为准，别猜**：必带 `to`/`type`/`action`（如 `{"to":"<node_id>","type":"text","action":"send","payload":{"content":"..."},"ttl_seconds":300,"reply_to":null}`，可选 `to_agent`/`from_agent`）。400 报错 `missing field X` 一次只吐一个，靠试能试半宿——先读节点目录 `AGENTS-使用说明.md`（发行目录自带，权威）。
- **桥控制 API（127.0.0.1:8766）路由只有** `/status /inbox /registry /send /register /ack /drop-registry`；打错路径回 `{"error":"not found"}`，别误判成"桥挂了"。控制口 `/send` 是**原样转发 body 给节点**，所以要按上面的节点 schema 传，信封格式（message_id/text/chat_type）会 400。
- **广播=循环逐个定向发**（to=node_id + to_agent=各 id）。`to:"*"` 是 at-most-once：不重试、离线不补。点对点带 `ttl_seconds`（如 3600）可让离线目标上线后仍补收。
- **对端节点重启会清注册**：看到 `join` 事件≠它的 Agent 还在——查 `fleet/agents` 有没有它。实录：某节点重启后对端 Agent 注册丢失，靠缓冲消息+★自检指令 16 分钟内自助恢复。
- **跨版本节点可共存**：实测 .7 v0.4.6 ↔ .50 v0.4.9 正常互联投递，升级无需两台机器同步进行。
- **传文件的完整闭环**：`send-file`（目标须在线；Windows 节点读 Windows 形态路径；落对端 `data/files`，完成不推 WS，轮询 `/v1/status` 的 `active_transfers=0`）→ 再发一条 text 指令对方把文件挪到规范位置 → 消息里写**精确字节数/SHA**（写"约17KB"这种估算会被对端当真并报分歧，实测 9691B vs "17KB" 乌龙）。
- 回执礼仪：收到确认请求就回一条 `reply_to=<原 msg_id>`；派活/澄清也带上 reply_to 串线程。
- `daemon.out`/日志尾部可能挂着**历史**失败 traceback（append 模式），先看时间戳再下结论；`bridge.sh` 的 resolve 会自动 glob 最新版本目录，老 traceback 不代表现在挂。

## 10. v0.4.11 变更（2026-10-13，身份安全与幽灵治理）
- **`init/join --force` 不再抹身份**：覆盖配置时自动保留旧 `node_id`/`local_api.token`/`data_dir`，
  彻底堵住「重启后节点变成陌生机器」事故（.7 号机 node_id 漂移根因）。身份是网格的寻址锚点，
  重新配对（join 新 cluster_token）照旧生效，唯独身份不动。
- **（2026-10-20 补强）身份锚点**：node_id 另存 `<data_dir>/node-identity.json`。
  配置被删/重建/换发行目录/半截写入时，启动会以锚点为准把 node_id 修回来，
  `init --force` 也会沿用锚点。**同一台机器重启后 node_id 永远不变。**
  已经漂移的补救：`agentlink --config agentlink.json identity --set <旧ID>`，
  或 `identity --recover` 从 backups/升级台账/出站历史自动找回（改完需重启）。
- **（2026-10-20 补强）配置位置隔离**：配置不再放在发行目录里，改住用户配置目录
  （Windows `%LOCALAPPDATA%\AgentLink\agentlink.json`，Linux `~/.config/agentlink/agentlink.json`），
  **升级换目录/换二进制不会覆盖它**。旧版就地配置会在首次启动时自动迁移过去
  （node_id/token/data_dir 全保留，相对 data_dir 锚回旧目录，数据不搬家）。
  查路径：`agentlink config` / `agentlink config --path`；手动迁移：`agentlink config --migrate`。
- **fleet agents 在线判定联动节点**：节点掉线后其 agent 不再显示 online=true（幽灵在线）。
- **web 节点列表**：本机行有「本机」标记；离线节点有「遗忘」按钮——移除并忽略自动发现，
  持久化在 `data_dir/forgotten_peers.json`，该节点下次真实握手会自动回来。
  API：`DELETE /v1/peers/<node_id>/forget`、`POST /v1/peers/<node_id>/unforget`。
