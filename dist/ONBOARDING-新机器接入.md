# AgentLink 新机器接入指南（一次到位：节点 + 自启 + 及时响应三件套）

> 适用：局域网内新电脑加入现有网格。以 Windows + WSL 机器为例；发布目录文件通用。
> 当前网格（以 `/v1/peers` 现查为准）：B=192.168.1.50（主节点）、A=192.168.1.7、
> C=192.168.1.10、D=192.168.1.60。
> 及时响应标准（常驻桥+唤醒钩子+cron 看门狗）见 `SKILL-message-response.md`——
> **强烈建议每台接入机器一次装齐，不要只装节点。**

## 0. 拿配对钥匙（在老机器上做）

老机器任一节点目录开终端：
```
agentlink.exe token          # 或读 agentlink.json → security.cluster_token
```
把得到的 token 带到新机器（只走人工/安全渠道，**不要**写进任何包或聊天记录存档）。

## 1. 部署节点（新机器，Windows 侧）

1. 解压发布 zip（如 `AgentLink-v0.5.0.zip`）到固定目录，如 `E:\Tools\AgentLink-v0.5.0\`
2. 首次入网（自动配对，生成配置+随机身份）：
   ```
   启动.bat <cluster_token>
   ```
   窗口显示"配对成功(strict 模式)"即入网。以后每次开机双击它即可；
   **登录自启已自动注册**（Startup 文件夹 AgentLink.vbs，无窗口无弹窗），
   想取消：`agentlink.exe autostart --remove`。
3. 验证：浏览器开 `http://127.0.0.1:8765/v1/health` → `{"ok":true, "version":"0.5.0"}`；
   在 50 号的 `/v1/peers` 或 web 页 `127.0.0.1:8765/web` 应能看到新节点上线。
4. 防火墙（首次运行弹窗点允许；或管理员执行）：
   ```
   netsh advfirewall firewall add rule name=AgentLink dir=in action=allow protocol=TCP localport=8765
   netsh advfirewall firewall add rule name=AgentLink-U dir=in action=allow protocol=UDP localport=8766
   ```
5. 数据目录：默认已在软件目录外（`%LOCALAPPDATA%\AgentLink\data`），升级换目录不丢数据；
   想放别处就改 `agentlink.json` 的 `data_dir`（绝对路径）。

## 2. 装及时响应三件套（WSL 侧，强烈建议）

没有这一步，机器上的 hermes agent 不会自动收到总线消息（升级/指令会无声积压）。

**① 常驻桥 + 唤醒钩子**：解包 `AgentLink-v0.5.0-response-kit.zip`（仓库 `wake-hook/` 为同源正本）：
```bash
cd response-kit
bash install-all.sh study        # 例：profile 叫 study；换你自己的 profile 名。幂等，失败排障后可重跑
```
脚本自动完成：解释器探测 → webhook 平台与路由（secret 自动读写）→ wake.json →
起桥 → HMAC 自检。看到"完成 ✅"即成功。同机第二条 agent 链路用隔离参数，例如：
`BRIDGE_DIR=/mnt/d/HermesSpace/.fleet-<id>/agentlink BRIDGE_PORT=8647 PORT=8646 bash install-all.sh <profile>`
（旧脚本 wake_install.sh 已废弃删除——它带一个损坏的 SECRET 行，勿使用。）

**② cron 看门狗（兜底）**：把同包 `al_watch.py` 放进 `~/.hermes/profiles/<profile>/scripts/`，
建每 5 分钟 cron 任务（schedule 写 `every 5m`，不是 `5m`；deliver 显式指定；repeat 设 forever），
按哨兵（NO_NEW/URGENT/DOWN/DRIFT）行事的 prompt 契约见 `SKILL-message-response.md`。

**③ 端到端验证**：请老机器发一条定向 probe → ≤90s 内目标 profile 出新会话、桥
`logs/agentlink_handled.jsonl` 落行、ack 后 inbox 清空；再手跑 `python3 al_watch.py` 看哨兵输出。

## 3. Agent 上线注册（每个 hermes profile 会话）

照发布目录根 `SKILL.md`（agentlink-agent-registration 技能）执行"上线必做自检"：
health → 接唤醒线 → 注册 agent_id（全小写）→ `GET /v1/fleet/agents` 确认 online。

## 4. 给网格登记新机器

在 50 号（编排方）说明新机器：节点 name（默认取本机 IP）+ 该机的 agent 名单，
编排方会把新节点纳入升级协调的广播范围（`/v1/fleet/agents` 自动同步，无需额外配置）。

## 日后升级（新机器作为目标机时）

执行侧照协议自动走：编排方 send-file zip + upgrade_notice → 该机执行者
（liteguard daemon 或任一具备系统能力的 agent，见 upgrade_runner.py）
备份→替换→重启→health 校验→回 upgrade_done。数据目录已外置，换目录升级零风险。
