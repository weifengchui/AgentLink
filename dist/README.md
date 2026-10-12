# AgentLink v0.4.1

局域网多台机器上 **AI Agent 之间**的通信总线。只为程序服务，不为人聊天。
一台机器跑一个常驻小进程；Agent 通过 `127.0.0.1` 的 HTTP/WS 接口收发消息与文件，
机器之间自动发现、组网、可靠互通。

## 文件

| 文件 | 说明 |
|------|------|
| `agentlink.exe` | Windows 10/11 x64 可执行文件（单文件，无依赖） |
| `agentlink-linux` | Linux x64 可执行文件 |
| `启动.bat` | Windows 一键启动 / 自动配对脚本 |
| `AGENTS-使用说明.md` | **给 AI Agent 的接口契约**（接入前必读） |
| `SKILL.md` | Agent 注册与双向通信的 **Skill 定义**（`agentlink-agent-registration`，给 AI Agent 消费） |
| `agentlink.json.example` | 配置示例（一般用 `init` / `join` 自动生成，无需手建） |

## 快速开始（Windows）

1. 把 `agentlink.exe` 与 `启动.bat` 放入同一文件夹。
2. 双击 `启动.bat`：
   - 无配置 → 自动生成（免密模式）并启动；
   - 局域网其他机器也双击本脚本，即自动互通。
3. 需要鉴权模式时，先在某台机器运行 `agentlink token` 拿到 `cluster_token`，
   其他机器执行 `启动.bat <token>` 自动配对。

## 验证

```powershell
# 节点是否活着
curl.exe http://127.0.0.1:8765/v1/health

# 在线节点（token 见 agentlink.json 的 local_api.token）
curl.exe -H "X-Auth-Token: <token>" http://127.0.0.1:8765/v1/peers

# 人工查看：节点 / Agent / 双方对话（浏览器打开）
#   http://127.0.0.1:8765/web   (打开后输入 local_api.token，不进URL)
#   http://127.0.0.1:8765/v1/web?token=<token>
```

## 常用命令速查

```bat
agentlink token                        :: 打印 cluster_token（配对其他机器）
agentlink join <token> --name xxx      :: 新机器一键配对
agentlink init --open                  :: 免密初始化（内网快速联调）
agentlink run                          :: 启动节点
```

## 端口

- TCP `8765`：本地 API + 对等节点 WebSocket（含文件）
- UDP `8766`：节点发现广播

请放行防火墙这两个端口。

## 常见问题

- **peers 为空**：确认同子网、防火墙放行；或配置 `static_peers` 写死对端地址。
- **token 不一致**：日志报 `handshake rejected`，节点保持离线（改回相同 token 即可）。
- **端口占用**：提示 bind 失败时，改 `agentlink.json` 的 `listen_port`。

详细接口契约见 `AGENTS-使用说明.md`；协议与部署文档见项目仓库 `docs/`。