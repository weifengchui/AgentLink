# AgentLink 协议规范 v1

> 对外稳定契约。`v` 字段标识协议版本；不兼容变更需递增版本号。

所有帧均为 **UTF-8 JSON 文本帧**（WebSocket text frame）。
除发现报文（UDP）外，字段命名统一使用 snake_case。

---

## 1. 发现（UDP 广播，端口默认 8766）

轻量、独立于消息协议，只用于让节点互相知道地址。

```jsonc
{
  "discovery": 1,                 // 发现协议版本
  "action": "announce",           // announce | bye
  "node_id": "a1b2c3d4e5f6",
  "name": "agent-PC-B",
  "port": 8765,                   // 对等 WS 监听端口
  "ts": "2026-10-12T10:00:00Z"
}
```

规则：

- 启动即发一次 `announce`，之后每 `announce_interval_seconds`（默认 5s）重发。
- 退出时发 `bye`；接收方立即标记离线。
- **接收方一律以 UDP 包的源 IP 为准**，`announce` 中的端口为准；报文内不含 IP。
- 同一 `node_id` 重复收到只刷新时间戳，不重复建连。

---

## 2. 连接与握手（WebSocket）

### 2.1 连接去重

对每个已知对端建立一条 WS。为避免双方同时拨号产生两条连接：

- 当 `本机 node_id < 对端 node_id`（按字节字典序）时，本机**主动拨号**；
- 否则本机只监听，等待对方连入。

### 2.2 握手

连接建立后，双方在 5s 内各发一帧 `hello`：

```jsonc
{
  "type": "ctrl",
  "action": "hello",
  "v": 1,
  "node_id": "a1b2c3d4e5f6",
  "name": "agent-PC-B",
  "nonce": "随机 16 字节 hex",
  "auth": "HMAC-SHA256(cluster_token, node_id | nonce | v) 的 hex"
}
```

校验失败 → 立即关闭连接，不发送任何业务帧。

### 2.3 心跳

- WS 层：每 `heartbeat_interval_seconds`（默认 10s）发 Ping；`offline_timeout_seconds`（默认 30s）未收到 Pong 判死。
- 应用层：额外发 `{"type":"ctrl","action":"heartbeat","ts":...}` 作为心跳；
  超过 `offline_timeout_seconds` 未收到任何帧 → 标记离线并触发重连。
- 重连：指数退避 1s → 2s → 4s → … → 封顶 30s。

---

## 3. 消息信封

```jsonc
{
  "v": 1,                          // 协议版本
  "msg_id": "uuid-v4",             // 全局唯一
  "from": "a1b2c3d4e5f6",          // 发送方 node_id
  "from_agent": "writer",          // 可选：发送方 agent_id（多 Agent）
  "to":   "f6e5d4c3b2a1",          // 目标 node_id，或 "*" 表示广播
  "to_agent": "reviewer",          // 可选：目标 agent_id（目标机器按此投递）
  "type": "text",                  // 大类
  "action": "send",                // 具体动作
  "payload": {},                   // 业务数据，中间层不解析
  "ts": "2026-10-12T10:00:00Z",    // RFC3339
  "reply_to": null,                // 可选：关联的 msg_id（请求/响应）
  "ttl_seconds": 300,              // 可选：离线暂存存活时间
  "sig": "HMAC-SHA256(cluster_token, node_id|msg_id|ts) 的 hex"  // 见 §6
}
```

### 3.1 type / action 对照

| type | action | 方向 | 说明 |
|------|--------|------|------|
| `text` | `send` | 点对点 | 业务文本消息 |
| `text` | `broadcast` | 广播 | `to` 必须为 `"*"` |
| `ctrl` | `hello` / `heartbeat` / `bye` | 双向 | 连接管理 |
| `ctrl` | `ack` / `nack` | 双向 | 送达确认 |
| `ctrl` | `agents_update` | 节点→节点 | Agent 目录同步（payload：`{agents:[{agent_id,name,capabilities,online,node_id}]}`） |
| `event` | `join` / `leave` / `peer_update` | 节点→本地订阅者 | 上下线通知 |
| `file` | `file_meta` / `file_accept` / `file_chunk` / `file_complete` / `file_done` / `file_error` | 双向 | 文件传输（§5） |

`payload` 的语义由上层 Agent 定义，AgentLink 只做透传。

投递规则：`to_agent` 非空时，目标节点只投递给 `client == to_agent` 的订阅者；
否则投递给目标节点所有订阅者。若 `to` 为本机 `node_id`，消息在本机内路由（同机 Agent 互发）。

---

## 4. 可靠性语义

| 场景 | 语义 | 机制 |
|------|------|------|
| 点对点 | at-least-once | 发送方记录待确认表 → 收到 `ack` 移除；`ack_timeout_seconds`（默认 5s）超时重发，最多 `max_retries`（默认 5）次；接收方按 `msg_id` 去重 |
| 广播 | at-most-once | 向每个在线 peer 各直发一份；不重试；mesh 不转发，无回环 |
| 对端离线 | 短期暂存 | 入内存队列，对端上线后投递；超过 `ttl_seconds` 丢弃 |
| 接收方 | 幂等 | LRU 记录最近 `dedup_cache_size` 个 `msg_id`，重复帧直接丢弃并回 `ack` |

ACK 帧：

```jsonc
{ "type": "ctrl", "action": "ack",
  "payload": { "ref": "<被确认的 msg_id>" } }
```

不承诺 exactly-once。

---

## 5. 文件传输

顺序（沿用设计文档，补齐校验与续传）：

```
file_meta → file_accept → file_chunk × N → file_complete → file_done
任意阶段出错 → file_error
```

`file_meta`：

```jsonc
{ "type": "file", "action": "file_meta",
  "payload": {
    "file_id": "uuid",
    "file_name": "report.pdf",
    "file_size": 10485760,
    "chunk_size": 65536,
    "total_chunks": 160,
    "checksum": "sha256:<hex>"     // 整文件
  } }
```

约束：

- 接收方只写入 `files.recv_dir`；拒绝绝对路径、`..`，并对文件名做净化。
- `file_size` 超过 `files.max_file_size` 时，接收方回 `file_error` 拒绝。
- 每块带 `seq`；`file_accept` 回报 `resume_from` 支持断点续传。
- 先写 `*.part`，整文件校验通过后 `rename` 为正式文件。
- 大文件使用**独立 WS 连接**，不占用消息通道。

---

## 6. 帧签名与防重放

开启 `security.mode = "strict"` 时：

1. 握手 `hello` 带 `nonce`，用 `cluster_token` 校验。
2. 业务帧带 `ts` 与 `sig`；`sig = HMAC-SHA256(cluster_token, node_id | msg_id | ts)`。
3. `ts` 超出本地时间 ±60s 的帧丢弃（防重放）。
4. `msg_id` 去重（防重复投递）。

`open` 模式跳过握手校验与帧签名（仅用于隔离/调试网络）。

---

## 7. 兼容性

- 收到 `v` 大于本机支持版本的帧 → 回 `nack`，不处理。
- 未知 `type`/`action` → 忽略并记录日志；未知 `payload` 字段原样透传。
- 新增可选字段不得改变既有字段语义。
