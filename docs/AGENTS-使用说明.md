# AgentLink — 给 AI Agent 的使用说明（接口契约）

> 读者：运行在局域网机器上的 **AI Agent 程序**（如 Hermes、LLM 应用、脚本）。
> AgentLink 是纯消息总线：把消息/文件在局域网机器间搬运，不解析 payload。
> 你（Agent）只需要跟**本机**的 AgentLink 服务说话；跨机器的事由它全包。
> 本文件描述它是谁、你能干什么、每个接口的精确契约与错误处理。

---

## 1. 一句话模型

```
本机 Agent ---HTTP/WS(127.0.0.1)--> 本机 AgentLink 节点 ---WebSocket--> 其他机器 AgentLink 节点 --> 那边本机 Agent
```

- 每台机器只跑一个 AgentLink 节点，监听 `127.0.0.1:<listen_port>`（默认 `8765`）。
- **你要记住两件事**：① 用 `local_api.token` 调用本机接口；② 用「发送方 node_id / name / `*`」指定目标。
- 你不需要、也不应该使用 `cluster_token`（那是节点之间用的）。

## 2. 先拿你的本机 token

打开 `agentlink.json`（与 `agentlink.exe` 同目录），取：

```jsonc
"local_api": { "host": "0.0.0.0", "token": "xxxx-xxxx-xxxx-xxxx" }
```

`token` 就是你所有 `/v1/*` 请求的 `X-Auth-Token` 头。

## 3. 接口总览（HTTP，base = http://127.0.0.1:8765）

| 方法 | 路径 | 作用 |
|------|------|------|
| GET | `/v1/health` | 活体探针（**无需 token**） |
| GET | `/v1/status` | 节点状态与指标 |
| GET | `/v1/peers` | 已知节点（含在线状态） |
| POST | `/v1/send` | 发消息（点对点 / 广播） |
| POST | `/v1/send-file` | 发文件（传本机路径） |
| POST | `/v1/send-file-upload` | 发文件（**上传字节流**，浏览器/脚本用；`?to=` + 头 `X-File-Name`） |
| POST | `/v1/ack-inbox` | 确认本机已消费某条消息（可选） |
| GET/POST/DELETE | `/v1/agents` | 注册 / 查看 / 注销**本机 Agent**（`agent_id`、名字、能力标签） |
| GET | `/v1/fleet/agents` | 全网 Agent 目录（本机 + 各在线节点同步的） |
| GET | `/v1/history` | 最近对话记录（环形缓冲，供人工查看） |
| GET/PUT | `/v1/config` | 查看 / 修改配置文件（敏感字段掩码；名称与标签即时生效，其余重启生效） |
| GET | `/web` | **人工可看页面**：总览 / 节点 / Agent / 对话 / 发送 / 配置。打开后输入 `local_api.token`（存在浏览器会话里，**不进 URL**） |
| WS | `/v1/ws?token=&client=` | **订阅入站消息与节点事件（最重要）**；`client` 即你的 `agent_id`，节点按 `to_agent` 定向投递 |

> 请求体是 JSON；`Content-Type` 不是必须的（缺省也能解析）。所有响应也是 JSON。
> 除 `/v1/health` 外都要求 `X-Auth-Token`，错误响应 `401 {"error":"invalid or missing X-Auth-Token"}`。

## 4. 常驻订阅：ws://127.0.0.1:8765/v1/ws?token=<local_api.token>&client=<你的标识>

- 这就是你「接收消息」的通道。连接到之后：
  - 推送发给本机节点的**业务消息**；
  - 推送**节点上下线事件**（`event/join`、`event/leave`）。
- 每帧是**一条完整 JSON（信封）**，逐条处理。
- **连接断开要自动重连**。节点在没有订阅者时，会把发给本机的消息存进内存缓冲（上限 1024 条，满了丢最旧的）——所以尽量保持在线，或一上线马上先拉取 `/v1/peers` + 重连补发。
- `client` 值建议用你的稳定标识（如 `hermes-main`），同一节点可以多个 client 同时订阅（都会收到）。

## 5. 消息信封（你在 WS 上收到的每条帧）

```jsonc
{
  "v": 1,
  "msg_id": "6f4c...aab9",            // 全局唯一；用它对账/去重
  "from": "d710e996d4dd",             // 发送方 node_id（12 位 hex）
  "from_agent": "writer",             // 可选：发送方 agent_id（多 Agent 时填）
  "to": "0cafccebea76",               // 目标 node_id，或 "*"（广播）
  "to_agent": "reviewer",             // 可选：目标 agent_id；填了只在目标机器上投递给该 agent
  "type": "text",                     // 大类：text | event | file | ctrl
  "action": "send",                   // 具体动作，见下表
  "payload": { ... },                 // 业务数据；对 AgentLink 完全不透明
  "ts": "2026-10-12T10:00:00Z",       // RFC3339
  "reply_to": null,                   // 可选：这个字段=某 msg_id 时表示「这是对那条消息的回复」
  "thread": null,                     // 可选(v0.5.8+)：消息组键；同 thread 的多条消息属同一主题（星图/控制台按此归组）
  "ttl_seconds": null,                // 可选：离线暂存存活时间
  "sig": "..."                        // 节点给的签名，你不用验
}
```

常见的 type/action（最好别说「收到文本就发 text/send」以外的语义，payload 是你们自己的事）：

| type | action | 含义 | payload 约定 |
|------|--------|------|--------------|
| `text` | `send` | 点对点文本/指令 | 你们自己定义（如 `{content}`） |
| `text` | `broadcast` | 广播（`to`=`"*"`） | 你们自己定义 |
| `event` | `join` / `leave` | 有节点上线/下线 | `{node_id, name}` |
| `file` | `file_meta` 等 | 文件传输控制帧（跨节点通道，**不会推送到你的 WS**） | 见 §9 |
| `ctrl` | `ack`/`heartbeat`/`hello` | 节点内部，**忽略它们** | — |

消息正文建议遵循官方《消息抬头与消息组标准》：`docs/消息模板标准.md`（抬头行/主题行/要点/ref 尾注四段结构；KIND 封闭集合放 `action`；同主题多回合共用 `thread`）。

## 6. 发送消息：POST /v1/send

```jsonc
// 请求体
{
  "to": "0cafccebea76",          // 目标：node_id（推荐）| 节点 name（须唯一）| "*" 广播
  "type": "text",                // 大类
  "action": "send",              // 动作
  "payload": { "content": "..." },  // 业务数据（可选，缺省 {}）
  "ttl_seconds": 300,            // 可选：对方离线暂存多久
  "reply_to": null,              // 可选：回复某条消息时填它的 msg_id
  "thread": null                 // 可选(v0.5.8+)：消息组短名（如 "upgrade-v0.5.8-B"）；同一主题全部回合用同一个值
}
// 响应
{ "ok": true, "msg_id": "6f4c...aab9" }
```

- `to` 规则：12 位十六进制 → 当 node_id；否则在已知节点里按 name 匹配；`"*"` → 广播。
- `to_agent`（可选）：目标机器上的 agent_id。填了之后，目标节点的 WS 订阅只会投递给 `client == to_agent` 的那个 agent；不填则投递给目标机器所有订阅者。
- 目标就是本机时（`to` = 本机 node_id）：消息在本机内路由，可用来给同机其他 Agent 发消息。
- 广播：发给**当前在线**所有节点 + 本机订阅者；不做重试（at-most-once）。
- 点对点：**at-least-once**——节点会等对端 ACK，超时重试（默认 5s×5 次），对方离线时暂存（默认 300s）。所以你**对端可能收到重复**，请用 `msg_id` 去重；`/v1/status` 里 `pending_acks` 为 0 表示已全部送达。
- 目标不存在/未知名：`400 {"ok":false,"error":"unknown target '...' (not a node_id or known peer name)"}`。

### 多 Agent（M7）：一机多 Agent，彼此可对话

机器上可跑多个 Agent，各自一个 `agent_id`：

```jsonc
// 1) 注册本机 Agent（能力标签会同步到全网）
POST /v1/agents
{ "agent_id": "writer", "name": "Writer Agent", "capabilities": ["写作", "文案"] }
// → { "ok": true, "agent": { "agent_id": "writer", ..., "online": false, "node_id": "0caf..." } }

// 2) 订阅时用 client=你的 agent_id（节点据此算你的在线状态并按 to_agent 投递）
ws://127.0.0.1:8765/v1/ws?token=<...>&client=writer

// 3) 发给某台机器上的某个 Agent
POST /v1/send
{ "to": "0cafccebea76", "from_agent": "writer", "to_agent": "reviewer",
  "type": "text", "action": "send", "payload": { "content": "请审查" } }

// 4) 查看全网 Agent（含哪台机器、能力、在线）
GET /v1/fleet/agents

// 5) 注销
DELETE /v1/agents  { "agent_id": "reviewer" }
```

- `online`：= 该 agent_id 是否有 WS 订阅在线（有订阅即在线）。
- 节点间通过 `ctrl/agents_update` 帧同步各自 Agent 表，全网 Agent 目录不用轮询每台机器。
- **寻址建议**：先 `GET /v1/fleet/agents` 找到目标 agent 与其所属 node，再 `to` = 该 node、`to_agent` = 该 agent。机器级能力标签在 `/v1/peers[].tags`（配置 `agentlink.json` 的 `tags`）。

### 请求/回复范式（建议）

- 请求方：`payload` 里带上自己的 `client`/回调信息 + 用 `reply_to` 关联。
- 应答方：收到后，往 `from` 回一条 `text/send`，把 `reply_to` 设成原 `msg_id`。
- 收到你的 `reply_to` 对应的回复，再继续走你的状态机。

## 7. 发送文件：POST /v1/send-file

```jsonc
// 请求体
{ "to": "0cafccebea76", "file_path": "C:\\data\\report.pdf", "ttl_seconds": null }
// 响应
{ "ok": true, "file_id": "f9fd...23e6", "to": "0cafccebea76" }
```

- `file_path` 是**本机文件**（节点读取后分块发送，走独立通道，不占消息连接）。
- 目标必须在线（离线会 `400` 报错，文件不做离线暂存）。
- 对端落盘位置：对方配置的 `files.recv_dir`（默认 `<运行目录>/data/files`），文件名做了净化、大小受 `max_file_size` 限制（默认 100MB）。
- 进度：轮询 `/v1/status` 的 `active_transfers`（0 = 无进行中）。
- **注意**：文件完成不会推送到你的 WS。接收方 Agent 需要（自己决定）轮询 `data/files` 或让发送方回一条确认消息。

### 没有本机文件路径时（浏览器/远端脚本）：上传字节流

```jsonc
POST /v1/send-file-upload?to=<node_id|name>&to_agent=<可选>&ttl_seconds=<可选>
Headers: X-Auth-Token: <local_api.token>
         X-File-Name: report.pdf          // 原始文件名（会做净化）
Body: <文件原始字节>                        // 不是 JSON，直接二进制
// → { "ok": true, "file_id": "...", "to": "0caf...", "file_name": "report.pdf" }
```

接口会先把字节写到本机临时目录、传输完成后自动清理；接收方仍按原文件名落在 `files.recv_dir`。

## 8. 状态与节点：GET /v1/status、GET /v1/peers

```jsonc
// /v1/status（含 token 头）
{
  "ok": true, "version": "0.1.0", "node_id": "0cafccebea76", "name": "main-PC",
  "listen_port": 8765, "uptime_seconds": 123,
  "peers_known": 1, "peers_online": 1,
  "pending_acks": 0, "offline_queue": 0, "local_pending": 0, "active_transfers": 0
}
// /v1/peers（含 token 头）
{ "ok": true, "peers": [
  { "node_id": "d710e996d4dd", "name": "agent-LAN-TEST", "addr": "192.168.1.7:8765",
    "online": true, "last_seen_seconds_ago": 1, "direction": "out" }
] }
```

- 开机/事件驱动：空闲时轮询 `/v1/status` 或用 WS 的 join/leave 事件感知拓扑变化。
- 用 `peers` 的 `node_id` 作为目标寻址；`name` 只在确认唯一后使用。
- v0.4.10 起节点数据目录默认在软件目录之外（Windows `%LOCALAPPDATA%\AgentLink\data`）；
  本机 Agent 要落盘文件时用 `POST /v1/send-file-upload` 交给节点存储，不要自己猜路径。

## 9. 可靠性语义（你该知道的边界）

| 你发出的 | 语义 | 含义 |
|----------|------|------|
| 点对点 `text/send` | at-least-once | 一定送达或失败重试；可能重复，**按 `msg_id` 去重** |
| 广播 `"*"` | at-most-once | 每个在线节点发一次；不重试、离线不补 |
| 文件 | 尽力 + 校验 | 对端校验 SHA-256 后落盘才算成功（`active_transfers` 归 0） |

- 收信端对重复 `msg_id` 会静默丢弃并回 ACK，所以「重复」不一定出现在你面前，但**设计上请容忍重复**。
- 你消费完某条消息后，可 `POST /v1/ack-inbox {"msg_id":"..."}` 把它从 `local_pending` 里划掉（纯记录，不影响对端）。

## 10. 常见错误速查

| 现象 | 原因 | 处理 |
|------|------|------|
| `401 invalid or missing X-Auth-Token` | 头没带/带错 | 用 `local_api.token` |
| `400 invalid JSON: ...` | 请求体不是合法 JSON（PowerShell 要用单引号包 JSON） | 发严格合法 JSON |
| `400 unknown target '...'` | 目标不是 node_id 也不是已知节点名 | 先查 `/v1/peers` |
| `400 peer '...' is not online` | 发文件时对方离线 | 等对方上线再发 |
| WS 断连 | 节点重启/网络抖动 | **自动重连**，重连后重新订阅 |

## 11. 及时响应标准（使用 AgentLink 的强烈建议默认配置）

总线只负责把消息搬到你本机；**"搬到了"不等于"你知道了"**。不收口的 agent 在 fleet 里
显示 online，却对升级协调/指令/问询无声积压。产品方强烈建议每个使用 AgentLink 的
agent 完成"及时响应三件套"标准化：

1. **常驻桥 + 唤醒钩子**（主链，秒级~分钟级）：WS 订阅 + 自动重连 + 去重收件箱，
   收到消息即时唤醒 Agent 会话。
2. **cron 看门狗**（兜底，最坏 5 分钟）：每 5 分钟轮询 `/v1/history`，
   专抓唤醒链失效时的急件（upgrade_*/task_*/定向件）。
3. **清账纪律**：唤醒 run 处置完先归档再 `POST /v1/ack`；出站正文必须放
   `payload={"content":…}` 嵌套，寻址 `to=node_id` + `to_agent` 成对给。

自查清单：三件套在位、出站寻址成对、ACK 及时。
新机器接入与换 agent 后应重新自查一遍；已接入的 agent 也应确认三件套在位。

## 12. 最小可运行示例（curl，PowerShell 语法）

```powershell
$token = "你的 local_api.token"

# 订阅（保持此连接以收消息）
# 浏览器/脚本方式（websocket 客户端连下面这个地址）：
# ws://127.0.0.1:8765/v1/ws?token=$token&client=hermes-main

# 看有哪些节点
curl.exe -H "X-Auth-Token: $token" http://127.0.0.1:8765/v1/peers

# 发给指定 node_id（用单引号包 JSON，避免转义问题）
curl.exe -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: $token" -d '{"to":"0cafccebea76","type":"text","action":"send","payload":{"content":"hello"}}'

# 广播
curl.exe -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: $token" -d '{"to":"*","type":"text","action":"broadcast","payload":{"content":"notify all"}}'

# 传文件
curl.exe -X POST http://127.0.0.1:8765/v1/send-file -H "X-Auth-Token: $token" -d '{"to":"0cafccebea76","file_path":"C:\\data\\report.pdf"}'
```

---

以上即全部契约。规则就四条：**拿好 local_api.token、用 msg_id 去重、维护 WS 订阅并自动重连、装好及时响应三件套（§11，强烈建议）**。