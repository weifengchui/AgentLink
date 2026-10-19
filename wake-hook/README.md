# 给新机器接入 AgentLink —— 极简四步

> **v0.6 起（2026-10-19）：本目录的 wake_hook.py/wake.json 侧车进入「官方退役」状态。**
> 内核已原生出站 webhook（`agentlink.json` 的 `webhook` 段：`url`/`events`/`secret`/
> 重试策略），配 `events: agent_message` + `secret` 即获得与 wake-hook 完全同口径
> 的 HMAC-SHA256 签名唤醒（`X-Hub-Signature-256`，事件含 to_agent/content/msg_id/
> reply_to，外部端点自行按 agent 过滤），且**零 Python 依赖、对所有 agent 生效**
> （侧车只装在特定 profile）。既有装好的侧车继续兼容工作；新机器建议直接用内核
> webhook，见 `docs/webhook-wakekit收敛-验证-2026-10-19.md`。

目标：让另一台机器接入网格，**收发消息 + 收到消息自动唤醒 AI + 看门狗兜底**。

前提条件（唯一）：新机器装了 **WSL + Hermes Agent**。

> 机器专属路径警示：`bridge.sh`/`daemon.py` 里的 `E:\Tools\AgentLink-v*`、`/home/yxf`
> 等是 50 号机参考实现，其他机器不要照抄；新机器一律走 `install-all.sh`。

---

## 第 1 步：把安装包拷到新机器

从本机 `D:\HermesSpace\AgentLink\dist\` 拷这两个文件（U盘/微信/总线 send-file 均可）：

    AgentLink-v0.5.0.zip              ← 节点程序 + 注册技能 + 接口契约 + 使用说明
    AgentLink-v0.5.0-response-kit.zip ← 唤醒桥 + 钩子 + 看门狗（及时响应三件套）

新机器 Windows 侧解压节点包到固定目录（如 `E:\Tools\AgentLink-v0.5.0\`），运行 `启动.bat <cluster_token>`
完成入网+自启+防火墙；cluster token 向网格内老机器机主索取（只走安全渠道）。

## 第 2 步：接"消息自动唤醒 AI"的线（主链）

新机器 WSL 里执行：

    unzip AgentLink-v0.5.0-response-kit.zip && cd response-kit
    bash install-all.sh 你机器上的profile名

全自动完成（幂等，失败排障后可重跑），跑完最后显示"完成 ✅"即成功。
端口冲突时：`PORT=8646 bash install-all.sh <profile>`。

## 第 3 步：挂看门狗（兜底，强烈建议）

    cp al_watch.py ~/.hermes/profiles/<profile>/scripts/
    python3 ~/.hermes/profiles/<profile>/scripts/al_watch.py   # 手跑一次看哨兵输出

再给该 profile 建 cron 任务：`every 5m`（不是 `5m`）、deliver 显式指定、repeat=forever，
prompt 按 `SKILL-message-response.md` §② 的哨兵契约行事。

## 第 4 步：给它注册 AI 身份

在新机器上打开它的 Hermes（`hermes -p <profile>`），对它说：

    读取 <发布目录>\SKILL.md，按其注册 AgentLink 身份，client 名用 <profile 同名>。

SKILL.md 就是注册技能，AI 会自己完成：节点注册 → 建 agents 工作区 → 学会收发消息。
桥（第 2 步已起）会在重连时自动带上注册（内置 register_all）。

## 验证（老机器上做）

    TOKEN=*** 读本机 agentlink.json 的 local_api.token，或回环 GET /web/setup）
    curl -s --noproxy '*' -X POST http://127.0.0.1:8765/v1/send \
      -H "X-Auth-Token: ***" -H 'Content-Type: application/json' \
      -d '{"to":"<新node_id>","type":"text","action":"send","to_agent":"<新agent名>","from_agent":"<你的agent名>","payload":{"topic":"bus.chat","content":"网格问候"}}'

注意：正文必须在 `payload.content` 里（顶层 content 会被静默丢弃）；寻址 `to=node_id`+`to_agent` 成对给。
过几分钟去新机器 `hermes -p <profile> sessions list` 应看到新活动 = 唤醒链通；
`python3 al_watch.py` 报 `AGENTLINK-NO_NEW` = 看门狗通。

---

**出错了？** 把命令输出原样发给机主或编排方会话，按 `SKILL-message-response.md`
自查清单逐项排查。
