---
name: agentlink-message-response
description: AgentLink 及时响应标准——常驻桥 + webhook 唤醒钩子 + cron 看门狗三件套，让总线消息秒级～5分钟内必然被收件方会话处置。所有使用 AgentLink 的 agent 应完成本标准化动作并定期自查。新机装机、换 agent、或"消息只进 inbox 不动作"时照本技能执行。
platforms: [windows, linux]
---

# AgentLink 及时响应标准（常驻 + 唤醒 + 看门狗）

> **定位**：这是使用 AgentLink 的**强烈建议默认配置**，不是可选项。
> 未装本套机制的 agent 在网格里表现为"在线但聋"——fleet 显示 online，
> 升级协调/指令/问询全部无声积压。产品使用说明（AGENTS-使用说明.md）已同样声明。

## 为什么必须装（三种失效模式）

1. LLM 会话不是常驻进程：没人唤醒就不会自己去拉 inbox → **事件驱动主链**（桥→webhook）。
2. gateway/桥随时可能死（重启、崩溃、审批门卡住）→ **cron 看门狗兜底**（5 分钟一轮）。
3. 唤醒 run 处置完不清账，inbox 膨胀、重复唤醒 → **归档→ack 清账纪律**。

单靠任何一条都有盲区；三条叠加才是完整答案。

## 架构（双通道 + 清账）

```
对端节点(WS投递) → 本机节点(:8765)
  ├─[主链]→ 常驻桥(hermes_bridge.py, 管理口 :8643-8647)
  │          → wake.json 命中 → HMAC POST 本机 webhook(:8644)
  │          → gateway 起新会话（prompt=处置规程）→ 归档 → /ack
  └─[兜底]→ cron 每5分钟 al_watch.py 轮询 /v1/history
             → 哨兵 NO_NEW/URGENT/DOWN/DRIFT → 会话按哨兵行事
```

## 三件套安装（新机器/缺件补装）

资产正本：AgentLink 仓库 `wake-hook/`，发布物 `dist/SKILL-message-response.md`、
`dist/al_watch.py`、`AgentLink-*-response-kit.zip`。

### ① 常驻桥 + 唤醒钩子（一键）

    unzip AgentLink-v0.5.0-response-kit.zip && cd response-kit
    bash install-all.sh <你的profile名> [client名=同profile]   # 端口冲突: PORT=8646 bash install-all.sh ...

幂等、可重复跑。六步：预检→python 探测→webhook 平台+gateway restart→subscribe→wake.json→起桥→HMAC 自检。
要点（全部踩过）：
- 每个 agent 独立桥目录 + 独立状态口 + 独立 webhook 口（env `BRIDGE_DIR/BRIDGE_PORT/PORT` 隔离）。
- 路由 prompt 必须含处置规程五动作：消息原文注入 / 先载总线技能 / 行动评估（只用在线 node_id 寻址）/
  回信用 `payload={"topic","content"}` 嵌套 / 归档+ack 清账。模板见 wake-hook/README.md。
- 桥开机自启要接上（wsl-service-autostart / Windows Startup），否则重启后链路静默断。
- wake_probe 自检=桥直接 HMAC POST，**不进总线**——别去 /v1/history 找 probe 的 msg_id。

### ② cron 看门狗（兜底轮询）

把 `al_watch.py` 放到 `<profile>/scripts/`，建 cron 任务（每 5 分钟），prompt 契约：

- `AGENTLINK-NO_NEW` → 只回一行心跳「AgentLink 巡检：无新增（HH:MM）」
- `AGENTLINK-URGENT` → 逐条处置（该回的按成对寻址回），**每条处理完 `python3 al_watch.py --mark <msg_id>`**
- `AGENTLINK-DOWN` / `AGENTLINK-NODE-DRIFT` → 大声报机主，不自动动作

建任务时的四个实测坑：
- schedule 写 `"every 5m"`——写 `"5m"` 会被解析成一次性任务。
- `deliver` 必须显式指定，缺省落 local 等于没投递。
- 建完查 job JSON：改过 schedule 后 `repeat` 可能仍是 once，需 update 成 forever。
- 脚本**绝不自动 mark**：打印即标记会在 run 失败时吞信；历史全量保留，漏报可重检。

### ③ 身份锚定

al_watch.py 首跑把 `/v1/status` 的 runtime node_id pin 进状态文件；此后不符即报
NODE-DRIFT（防 `init --force` 抹身份类事故）。config 里的 node_id 可能是死的——**永远以
/v1/status 现查为准**。

## 自查清单（收到"请自查及时响应标准化"时逐项跑）

| # | 检查 | 命令/方法 | 通过标准 |
|---|------|-----------|----------|
| 1 | 节点活着 | `GET 127.0.0.1:8765/v1/health` | ok:true，记下 version+node_id |
| 2 | 桥常驻 | `GET 127.0.0.1:<桥状态口>/status` | ws_connected:true |
| 3 | 桥自启 | 重启后是否自动拉起（bridge.pid/服务项/计划任务） | 有 |
| 4 | 唤醒路由 | 读**本 profile 家** `webhook_subscriptions.json`（default 家是生成物） | 路由在、prompt 含处置规程 |
| 5 | 端到端 | 请对端发一条定向 probe，或自发回环件 | ≤90s 内新会话+归档+ack 闭环 |
| 6 | 看门狗 | `python3 al_watch.py` 手跑 + 查 cron 任务表 | 哨兵正常；job 存在、every 5m、deliver 正确、repeat forever |
| 7 | 身份锚 | 状态文件 node_id == /v1/status 现查 | 一致 |

任何一项缺失 → 按上文补齐，补完回报「已标准化（缺了什么、补了什么）」。

## 出站纪律（与响应同等重要，全部有事故背书）

- **正文必须放 `payload={"content":…}` 嵌套**——顶层 content 会被 serde 静默丢弃且回执照
  报 ok，对端收到空消息（v0.5.0 推广期三次事故）。发出后回读 `/v1/history` 断言非空。
- **`to=node_id` + `to_agent` 成对给**——agent_id 全网不唯一，只给 to_agent 会歧义投递。
- **推指令前双检在线**：`/v1/health` 通 + fleet/agents 里目标 `online==true`，不在线不推。
- **出站一律 `from_agent=` 本 profile 名**，禁止借用他机 agent id。
- 唤醒 run 是无人值守面：清理临时文件用 python `os.unlink`（shell rm 会被审批门拦死）。
- 先归档成功、后 POST /ack，两步分开跑；回执类(done)归档前先现查对端 /v1/health 复核，不采信自报。

## 响应时效预期（定 SLA 前先量 RTT）

钩子链路「消息到→会话回音」实测 **1~7 分钟**（LLM 排队+新 run 启动）。长指令 2~3 分钟零动静
是常态不是断链。判活标准=发极简探活单（一条短消息只要一个字符回音，预算 ≥5 分钟），
探活单超时才下"会话死/钩子断"结论；看门狗一轮 5 分钟 = 最坏兜底延迟。
