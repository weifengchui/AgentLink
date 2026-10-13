# 7 号机唤醒钩子安装包（wake-kit）


> ⚠ 已过时提醒：本文的 wake_install.sh 已废弃（第 8 行 SECRET 变量名被脱敏腰斩，必炸）。现在一律 `bash install-all.sh <profile>` 一键安装。
**目的**：桥收到 AgentLink 总线消息后自动唤醒本机 agent 会话（hermes webhook），
不再依赖 agent 主动拉收件箱。附带修复：liteguard bridge.sh 的 ensure_node 改为
Windows exe 形态拉起（WSL 内跑 linux 节点会被防火墙挡，导致节点互不可见——50 已实证）。

## 文件
- `wake_hook.py` — 钩子模块（放桥目录）
- `wake_install.sh` — 一键安装（幂等）
- `hermes_bridge.py` — 新版 study 桥（内置钩子调用，client=study）
- `daemon.py` — 新版 liteguard 守护（内置钩子调用）
- `bridge.sh` — 新版启停脚本（ensure_node 用 agentlink.exe Windows 形态）

## 步骤（在 7 号机 WSL 执行；fanbot/study 任一会话均可）

1. 解包：`tar xzf wake-kit.tar.gz -C /tmp/wake-kit`

2. 给要唤醒的 profile 开 webhook（幂等；profile 名按 7 号机实际，如 study）：
   ```bash
   P=study
   hermes -p $P config set platforms.webhook.enabled true
   hermes -p $P config set platforms.webhook.extra.host "127.0.0.1"
   hermes -p $P config set platforms.webhook.extra.port "8644"
   hermes -p $P config set platforms.webhook.extra.secret "$(openssl rand -hex 24)"
   hermes -p $P gateway restart
   curl -s http://127.0.0.1:8644/health   # 期望 {"status": "ok", ...}
   ```

3. 建路由：
   ```bash
   hermes -p $P webhook subscribe agentlink-msg --prompt "【AgentLink 总线新消息】来自 {from}（agent {from_agent}）：主题={topic}
   内容：{content}
   msg_id={msg_id}
   如果这条消息需要你行动（指令/升级协调/问询），现在处理并按需回复；纯通告类记录即可。"
   ```
   记下输出的 Secret。

4. 替换桥文件并安装钩子（study 桥目录与 liteguard 目录都要，各自 agent 名对应）：
   ```bash
   cp /tmp/wake-kit/hermes_bridge.py /mnt/d/HermesSpace/.fleet-B1/agentlink/hermes_bridge.py
   cp /tmp/wake-kit/daemon.py /mnt/d/HermesSpace/LiteGuard/agentlink/daemon.py
   cp /tmp/wake-kit/bridge.sh /mnt/d/HermesSpace/LiteGuard/agentlink/bridge.sh
   cp /tmp/wake-kit/wake_hook.py /mnt/d/HermesSpace/.fleet-B1/agentlink/
   cp /tmp/wake-kit/wake_hook.py /mnt/d/HermesSpace/LiteGuard/agentlink/
   AGENTLINK_BRIDGE_DIR=/mnt/d/HermesSpace/.fleet-B1/agentlink AGENT_PROFILE=$P BRIDGE_AGENT_ID=study \
     WEBHOOK_SECRET=*** bash /tmp/wake-kit/wake_install.sh
   ```
   （若 liteguard 守护也要唤醒同一 profile，对 LiteGuard/agentlink 目录再跑一次，
   BRIDGE_AGENT_ID=liteguard；两桥各写各的 wake.json。）

5. 自检：
   ```bash
   curl -s --noproxy '*' http://127.0.0.1:8767/status | head -c 200  # ws_connected 应为 true
   ```
   然后在任意机器向 7 号 `to_agent=study` 发一条 text/send 测试消息，
   几秒内本机应弹出一个新的 hermes run（sessions 目录新会话处理该消息）。

6. 完成后回一条消息给编排方：`to="4f793d7cd6fc" to_agent="study"`，
   payload `{"topic":"wakekit_done","agent":"<你的名字>","health_ok":true}`。

注意：**0.4.10 升级指令仍在你们收件箱里排队**（9 步流程，zip 已在节点 recv_dir）。
建议顺序：先装 wake-kit（本说明），再执行升级——升级回报 upgrade_done 就靠钩子自动送达。
