# AgentLink 新机器接入指南（一次到位：节点 + 自启 + 消息唤醒）

> 适用：局域网内新电脑加入现有网格。以 Windows + WSL 机器为例；包内文件通用。
> 当前网格：50=192.168.1.50（主节点）、7=192.168.1.7。

## 0. 拿配对钥匙（在老机器上做）

老机器任一节点目录开终端：
```
agentlink.exe token          # 或读 agentlink.json → security.cluster_token
```
把得到的 token 带到新机器（只走人工/安全渠道，**不要**写进任何包或聊天记录存档）。

## 1. 部署节点（新机器，Windows 侧）

1. 解压 bundle 到固定目录，如 `E:\Tools\AgentLink-v0.4.10\`
2. 首次入网（自动配对，生成配置+随机身份）：
   ```
   启动.bat <cluster_token>
   ```
   窗口显示"配对成功(strict 模式)"即入网。以后每次开机双击它即可；
   **登录自启已自动注册**（Startup 文件夹 AgentLink.vbs，无窗口无弹窗），
   想取消：`agentlink.exe autostart --remove`。
3. 验证：浏览器开 `http://127.0.0.1:8765/v1/health` → `{"ok":true, "version":"0.4.10"}`；
   在 50 号的 `/v1/peers` 或 web 页 `127.0.0.1:8765/web` 应能看到新节点上线。
4. 防火墙（首次运行弹窗点允许；或管理员执行）：
   ```
   netsh advfirewall firewall add rule name=AgentLink dir=in action=allow protocol=TCP localport=8765
   netsh advfirewall firewall add rule name=AgentLink-U dir=in action=allow protocol=UDP localport=8766
   ```
5. 数据目录：默认已在软件目录外（`%LOCALAPPDATA%\AgentLink\data`），升级换目录不丢数据；
   想放别处就改 `agentlink.json` 的 `data_dir`（绝对路径）。

## 2. 装 Agent 桥 + 消息唤醒钩子（WSL 侧，可选但强烈建议）

没有这一步，机器上的 hermes agent 不会自动收到总线消息（升级/指令会积压）。

```bash
cd <解压目录>/wake-kit
# ① 开 webhook 并订阅（P=要唤醒的 hermes profile 名）
P=study
hermes -p $P config set platforms.webhook.enabled true
hermes -p $P config set platforms.webhook.extra.host "127.0.0.1"
hermes -p $P config set platforms.webhook.extra.port "8644"
hermes -p $P config set platforms.webhook.extra.secret "$(openssl rand -hex 24)"
hermes -p $P gateway restart
curl -s http://127.0.0.1:8644/health     # 期望 status ok
hermes -p $P webhook subscribe agentlink-msg \
  --prompt "【AgentLink 总线新消息】来自 {from}（agent {from_agent}）：主题={topic}
内容：{content}
msg_id={msg_id}
如果这条消息需要你行动（指令/升级协调/问询），现在处理并按需回复；纯通告类记录即可。"
SECRET=*** -p $P webhook list 输出取>"

# ② 桥文件就位（目录不存在就先按 SKILL-agent-registration.md 的§6 建桥）
mkdir -p /mnt/d/HermesSpace/.fleet-B1/agentlink
cp hermes_bridge.py wake_hook.py /mnt/d/HermesSpace/.fleet-B1/agentlink/
# （liteguard 目录同理：cp daemon.py bridge.sh wake_hook.py → LiteGuard/agentlink/）

# ③ 一键安装+重启桥
AGENTLINK_BRIDGE_DIR=/mnt/d/HermesSpace/.fleet-B1/agentlink \
  AGENT_PROFILE=$P BRIDGE_AGENT_ID=study WEBHOOK_SECRET=*** \
  bash wake_install.sh
```

## 3. Agent 上线注册（每个 hermes profile 会话）

照包内 `SKILL-agent-registration.md` 执行"上线必做自检"：
health → 桥 start → 注册 agent_id（全小写）→ `GET /v1/fleet/agents` 确认 online。

## 4. 给网格登记新机器

在 50 号（编排方）说明新机器：节点 name（默认取本机 IP）+ 该机的 agent 名单，
编排方会把新节点纳入升级协调的广播范围（`/v1/fleet/agents` 自动同步，无需额外配置）。

## 日后升级（新机器作为目标机时）

执行侧照协议自动走：编排方 send-file zip + upgrade_notice → 该机执行者
（liteguard daemon 或任一具备系统能力的 agent，见 upgrade_runner.py）
备份→替换→重启→health 校验→回 upgrade_done。数据目录已外置，换目录升级零风险。
