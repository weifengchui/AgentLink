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
| POST | `/v1/send-file-upload` | `X-Auth-Token` | 上传字节流发文件（`?to=` + 头 `X-File-Name`），供 `/web` 页面使用 |
| POST | `/v1/ack-inbox` | `X-Auth-Token` | 确认已读的本机消息 |
| GET/POST/DELETE | `/v1/agents` | `X-Auth-Token` | 注册/查看/注销本机 Agent（多 Agent，能力标签） |
| GET | `/v1/fleet/agents` | `X-Auth-Token` | 全网 Agent 目录（节点间自动同步） |
| GET/PUT | `/v1/config` | `X-Auth-Token` | 查看/修改配置（掩码显示；名称/标签即时生效） |
| GET | `/v1/history` | `X-Auth-Token` | 最近对话记录（环形缓冲） |
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