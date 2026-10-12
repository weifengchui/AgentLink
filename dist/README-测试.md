# AgentLink 局域网测试包（v0.1.0）

> 用于在**另一台局域网电脑**上跑一个 AgentLink 节点，与本机节点互通。

## 文件

| 文件 | 说明 |
|------|------|
| `agentlink.exe` | Windows 10/11 x64 可执行文件（1.8 MB，无依赖，双击即可运行） |
| `agentlink-linux` | Linux x64 可执行文件（如另一台是 Linux） |
| `启动测试.bat` | Windows 一键运行脚本（首次会引导配置） |
| `agentlink.json.example` | 配置示例（一般不需要手动建，用 `init` 生成） |

## 快速开始（Windows）—— 现在不需要手动改配置

1. 把 `agentlink.exe` 和 `启动测试.bat` 放到同一个文件夹（比如 `D:\agentlink-test\`）。
2. 先拿到本机开发机的 **cluster_token**：在那台机上，要么看 `agentlink.json` 里
   `security.cluster_token`，要么运行 `agentlink token`（推荐）复制这串。
3. 在新机器上运行（**二选一**）：

   ```bat
   启动测试.bat <token>     :: 自动配对（推荐）
   启动测试.bat --open      :: 免密模式，不校验 token
   ```

   或者先双击 `启动测试.bat` 看提示，再按提示执行。

## 一键命令速查

```bat
:: 主机：查看并复制 cluster_token（自动配对用）
agentlink token

:: 新机器：一步配对（自动生成配置 + 写入 token，无需编辑 json）
agentlink join <token>

:: 免密互信（内网快速测试，所有节点跳过 token 校验）
agentlink init --open
agentlink join <token> --open
```

## 验证互通

两台机器都在同一局域网、防火墙放行 **TCP 8765 / UDP 8766** 后：

```powershell
# 查看本节点状态与在线节点
curl.exe http://127.0.0.1:8765/v1/health

# 用 PowerShell 拿本地 token（把 <token> 换成 agentlink.json 里 local_api.token 的值）
curl.exe -H "X-Auth-Token: <token>" http://127.0.0.1:8765/v1/peers
```

当你看到另一台机器出现在 `peers` 里且 `"online": true`，就说明通了。

## 发消息 / 传文件（对局域网测试）

```powershell
# 发给指定机器（to 可以是名字或 12 位 node_id）
curl.exe -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: <token>" -d "{\"to\":\"agent-主机名\",\"type\":\"text\",\"action\":\"send\",\"payload\":{\"content\":\"hello\"}}"

# 广播给所有机器
curl.exe -X POST http://127.0.0.1:8765/v1/send -H "X-Auth-Token: <token>" -d "{\"to\":\"*\",\"type\":\"text\",\"action\":\"broadcast\",\"payload\":{\"content\":\"hi all\"}}"

# 发文件（linux 上文件路径用 / 即可）
curl.exe -X POST http://127.0.0.1:8765/v1/send-file -H "X-Auth-Token: <token>" -d "{\"to\":\"agent-主机名\",\"file_path\":\"C:\\data\\report.pdf\"}"
```

收到的文件会保存在运行目录下 `data\files\` 中。

## 常见问题

- **peers 为空**：先确认两台机器在同一子网、防火墙放行 8765/8766；或在配置里加 `static_peers`：
  ```json
  "static_peers": ["192.168.1.11:8765"]
  ```
- **host 上能看到这台、这台看不到 host**：UDP 广播首包可能偶发丢失，等 5 秒自动重发即可；或改用 `static_peers` 互相写死。
- **token 不一致**：报 `handshake rejected (bad cluster token)`，节点会保持离线。
- **想查看日志**：运行窗口里直接看；`log.level` 调 `debug` 可看更细。

## Linux 那台机器

```bash
chmod +x agentlink-linux
./agentlink-linux join <主机token> --name agent-LAN-TEST   # 与主机自动配对
./agentlink-linux run
curl http://127.0.0.1:8765/v1/health
```