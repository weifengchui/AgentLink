# AgentLink

**给 AI Agent 用的局域网消息总线。只为程序服务，不为人聊天。**

[English](README.md) · [简体中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-Apache--2.0-blue) ![Rust](https://img.shields.io/badge/Rust-edition%202021-orange) ![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS%20%7C%20windows-lightgrey) ![Version](https://img.shields.io/badge/version-0.6.1-green)

每台机器跑一个常驻小进程（**节点**）。本机 Agent 通过 `127.0.0.1` 的 HTTP/WS 接口收发消息与文件；各机器的节点在局域网内自动互相发现，用 WebSocket 网状连接把消息可靠送到对端或广播给全网。单二进制、零运行时依赖——纯 Rust，无 C 依赖、无外部进程。

## 解决什么问题

- **跨机器的 Agent 没法对话。** SSH 和共享目录都是凑合；AgentLink 给出真正的寻址（`node_id` + `agent_id`）、点对点与广播投递、全网 Agent 目录。
- **一机一个守护进程，而不是每 Agent 一个。** 多个 Agent 共用一个节点，各自注册能力标签，只收自己名下的消息。
- **不搭基础设施也要可靠。** ACK + 有界重试 + 离线暂存（TTL）。没有 broker、没有数据库、没有 K8s。
- **程序优先。** 干净的 JSON 信封 API 和实时 WebSocket 订阅流。内置人看的网页控制台——但它是窗户，不是产品。

## 三分钟上手

```bash
# 1) 主机：创建配置（自动生成 node_id 与 cluster token）
agentlink init --name PC-A

# 2) 其他机器：一条命令配对，不用手改任何配置文件
agentlink join <cluster-token> --name PC-B

# 3) 运行（前台启动；deploy/ 目录有 systemd 与 Windows 安装脚本）
agentlink run
```

发一条消息，看它在对端落地：

```bash
curl -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: $TOKEN" -d \
  '{"to":"<PC-B 的 node-id>","to_agent":"worker","from_agent":"orch",
    "type":"text","action":"send","payload":{"content":"hello"}}'
```

打开 `http://127.0.0.1:8765/web` 看实时消息星图与控制台。
还没有配置、又有交互终端？直接裸跑 `agentlink`，首次运行向导会接管。

## 功能一览

| 领域 | 内容 |
|---|---|
| 发现 | UDP 广播（默认）· mDNS（feature 开关）· 静态 peers（兜底） |
| 传输 | 节点间全互联 WebSocket，按 `node_id` 字典序决定拨号方向，每对一条连接 |
| 投递 | 点对点 at-least-once（ACK、有界重试、离线暂存 TTL）；广播 at-most-once |
| 多 Agent | 本机 Agent 注册 + 能力标签，全网自动同步（`/v1/fleet/agents`） |
| 文件 | 独立对等连接分块传输，SHA-256 校验、断点续传位、沙箱到 `recv_dir` |
| 安全 | `strict`：共享 cluster token + HMAC 握手/帧签名 + 时间窗防重放；`open`：互信局域网免密 |
| 观测 | `/web` 控制台 + 实时星图、`/v1/history` 对话记录、`/v1/events` 脱敏事件流、`/v1/ws` 订阅 |
| 运维 | `agentlink hello` 集群自检 · `top` 实时总览 · 首次运行向导 · 开机自启（systemd 用户单元 / Windows 计划任务）· shell 补全 |
| 任务 | 任务协议（JSON 信封约定）+ Python 参考执行器；编排引擎本体属商业版 |

## 本地 API 速览

| 方法 | 路径 | 鉴权 | 用途 |
|---|---|---|---|
| GET | `/v1/health` | 无 | 存活探针 |
| POST | `/v1/send` | token | 发消息（点对点或 `"*"` 广播） |
| POST | `/v1/send-file` | token | 按路径发送本机文件 |
| GET/POST/DELETE | `/v1/agents` | token | 注册 / 查看 / 注销本机 Agent |
| GET | `/v1/fleet/agents` | token | 全网 Agent 目录 |
| GET | `/v1/history` | token | 最近消息记录（环形缓冲） |
| GET | `/v1/events` | token | 观察者事件流（脱敏） |
| WS | `/v1/ws?token=…&client=…` | query token | 订阅发给指定 Agent 的本机入站消息 |
| GET | `/web` | 页内输入 token | 人工控制台 + 实时消息星图 |

完整规范：[`docs/protocol.md`](docs/protocol.md) · [`docs/task-protocol.md`](docs/task-protocol.md) · [`docs/upgrade-protocol.md`](docs/upgrade-protocol.md)

## 架构

```
┌──────────── 机器 A ───────────────┐      ┌──────── 机器 B ──────────┐
│  agent-1 ─┐                       │      │  ┌─ agent "worker"       │
│  agent-2 ─┤ HTTP/WS → 节点        │ WS   │  节点 ← HTTP/WS         │
│  agent-3 ─┘   :8765               │◄────►│   :8765                 │
│             :8766 UDP 发现        │ mesh │   UDP 发现 / 静态 peers │
└───────────────────────────────────┘      └─────────────────────────┘
```

- **信封**：万物一个 JSON 结构；`payload` 完全透传。
- **寻址**：`to` = `node_id`（12 位 hex）+ `to_agent` = 已注册的 agent_id。跨机同名 Agent 是特性，靠 node_id 区分。
- **端口**：`8765` 本地 API + 对等 WS，`8766` UDP 发现。首次运行请在防火墙放行（见[部署指南](docs/部署.md)）。

## 从源码构建

```bash
cargo build --release                    # → target/release/agentlink
cargo build --release --features mdns    # + mDNS 发现
```

只需要 Rust 工具链（edition 2021）。支持 Linux、macOS、Windows；strip 后二进制约 6 MB。

## 开源版与商业版

本仓库是 **开源核心**：传输、投递、文件、安全、控制台、CLI。商业发行版在核心之上叠加任务编排、RBAC、审计与高可用。核心不依赖任何商业代码，可独立构建运行。

## 商标

"AgentLink" 是本项目的名称与商标。未经书面许可，不得用其命名或宣传衍生产品——详见 [`NOTICE`](NOTICE)。

## 许可

Copyright 2026 The AgentLink Authors

遵循 Apache License, Version 2.0（[LICENSE](LICENSE)）。
