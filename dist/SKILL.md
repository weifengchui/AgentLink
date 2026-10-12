---
name: agentlink-agent-registration
description: Register an AI agent to the AgentLink LAN message bus and run a reliable receive/send loop. Use when connecting any agent on any machine to AgentLink, when messages don't arrive (to_agent/client mismatch), or when adding agents that must be discoverable on the bus.
platforms: [windows, linux]
---

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
规范（本团队约定）：
- **agent_id 与 name 都取 Hermes profile 名**（例如 `LiteGuard`）。改名的话三处要同步：daemon 默认值、`state.json` 的 self_agent、节点上旧注册要注销，否则旧名的定向投递会落空。
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
- 节点目录版本化时，配置自动发现最新版：对 `Tools/AgentLink-v*/agentlink.json` 按版本号取最高，别写死具体版本目录（实测：节点从 v0.2.0 升到 v0.4.4 后写死路径会 FileNotFoundError）。

参考实现：main-PC 侧 daemon 在 `D:\HermesSpace\LiteGuard\agentlink\daemon.py`，控制脚本 `bridge.sh`。

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
