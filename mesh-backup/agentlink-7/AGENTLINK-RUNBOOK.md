# AgentLink 网格运维手册（本机 .7 / profile agentlink）
更新：2026-10-18 19:3x（源=profile 记忆 MEMORY.md + 会话实录；本文件为防丢镜像，两边有出入以最新时间戳为准。10-15→10-18 期间由巡检 cron 会话持续维护）

## 一、身份与拓扑（记忆条目全文镜像）
- 我 = Hermes profile `agentlink`，agent_id/name=agentlink（逐字符小写）。
- 网格机 .7/.50/.10/.60 间 agent 同名合法——寻址必须 `node_id+agent_id` 成对；按名字找 agent=bug。
- 本机节点（0.5.x 起节点 name=**A**）：127.0.0.1:8765，node_id=e5827032190c，**v0.5.10**（10-18 15:10 原位换 exe 升级）。目录名仍是历史的 `AgentLink-v0.4.11-bundle`，勿被名字骗。活配置=同目录 `agentlink.json`（local_api.token，勿打印，值=X-Auth-Token 头）；send-file 落盘=`C:\\Users\\yxf\\AppData\\Local\\AgentLink\\data\\files`。
- 对端（节点名 B/C/D）：B=.50=4f793d7cd6fc · C=.10=f013cdcb7c91 · D=.60=f5424c2f0a76(agent=study；旧误记 .14 已勘正，曾用名链见名册)。发前必查 /v1/fleet/agents。
- 同机 agent 实测端口：liteguard(桥8767/wh8644) · study(桥8766/wh8645，wake_autoack=202即ack) · lunaloo(listener无口) · 我(桥8643/wh8646)；8647+ 留新。
- 名册：/home/yxf/.agentlink/roster.md（**v2 终稿**，巡检会话维护，含 A/B/C/D 代号与 wake_hook 守卫变体分类，冲突时以名册为准）。

## 二、上线必做（每次会话启动/重启后）
1. `GET :8765/v1/health` 拿 node_id；失败→看门狗应已自动拉，手动：`cmd.exe /c start "E:\软件工具\AgentLink-v0.4.11-bundle\启动.bat"`（bat 防重、不 re-init、身份安全）
2. 桥：`bash /home/yxf/.agentlink/bridge.sh status`；ws_connected=false→restart
3. `POST /v1/agents` 重注册（幂等；桥重连时也会自动做）
4. `GET /v1/fleet/agents` 确认自己 online=true
5. 收件箱消费：`GET :8643/inbox`，未 ack 的按「处置规程」三段式（A 归档+ack；B 实质诉求不 ack、报【待定夺】转机主；C 指名只读指令直接执行回执）

## 三、唤醒链（本机专用口径，勿照抄 .50/.10）
- 桥=/home/yxf/.agentlink/hermes_bridge.py（venv 3.10=python 包装脚本派自 ~/.venvs/cosyvoice；NTFS /mnt/d 不能放可执行 venv）
- webhook 路由正本=~/.hermes/profiles/agentlink/webhook_subscriptions.json；default 家=sync 脚本生成物禁手改（机主 10-15 拍板）
- 唤醒会话工具：本机构建不读 route 级 toolsets、无 /p/ multiplex；唯一开关=profile config `platform_toolsets.webhook=[hermes-cli]`（默认仅 4 只读件，缺它=消息积压无人 ack）
- wake_hook 自回声铁律：`from_agent==me 且 from∈(本节点id,None,"")` 才跳过。单条件（from_agent==me 一律跳）在三机同名网格里会静默吃掉跨机指令——10-15 实测丢 2 条，已全网预警
- wake.json 必含 self_node；改 wake_hook 后须 `bridge.sh restart`（生效不影响去重集）
- 验收法：`from_agent=selftest-peer` 发总线件→90s 内看 gateway.log 唤醒会话 terminal 执行 /ack(200)+handled.jsonl 追加+inbox 清零（本机实测 25s）

## 四、升级规程（.50 协调协议 v1）
- 执行器=E:\软件工具\AgentLink-v0.4.11-bundle\upgrade_runner.py——执行前必 grep 三道防线 `errors="replace"`(新版×3)/`failed_after_kill`/`pick_base`；包内 runner 可能比 bundle 现装的新（10-16 0.4.16 实测：旧 bundle runner 仅 errors×2、无 pick_base/failed_after_kill），以包内版为准，先备份旧 runner（.old-no-pickbase）再替换执行
- 流程：核 sha 全串→（runner 自动）备份 backups/v<旧>_<ts>→taskkill→换件→bat 拉起→health 轮询→失败自动回滚；台账=节点目录 upgrade-ledger.jsonl，**勿带 --git-ledger**（那是 .50 路径）
- 实例：10-15 11:13 升 0.4.13（trigger msg:4eee140b）；11:31 升 0.4.14（msg:0414-a7）；10-16 08:26 升 0.4.16（msg:e74a2f98，exit 0，4006ms，backup backups/v0.4.14_20261016-082644，autostart --force 重注册），均身份未漂移
- 安全阀：崩溃/health 60s 不可达→backups 装回旧 exe+启动.bat 拉起，回报 failed_after_kill

## 五、发送纪律
- 必填 to(实时node_id)/type/action，正文在 payload.content；from_agent 必带；reply_to 用完整 32hex
- 重要消息 requires_reply:true（否则只躺 inbox）；发离线对端 ttl 3600；广播 "*"=at-most-once 慎用
- token 只在内存用，不落任何输出/日志/命令行

## 六、兜底与备份
- 看门狗 cron `agentlink-watchdog`（fd502509df0a，*/5m，no_agent）：查节点/桥/webhook/积压，异常自动拉起并报告，健康零输出
- 巡检 cron `agentlink-message-watch`（d97d55f2aa8d，*/5m，LLM+terminal/file 工具集）：按「处置规程」三段式消费收件箱，产物 ~/.agentlink/handled.jsonl（250+件台账）；手工收件仅兜底
- 文档快照 cron `memory-doc-snapshot`（a7941ac52a71，*/60m，no_agent）：MEMORY.md+roster.md+本手册 hash 变化才快照，留 30 份于 /home/yxf/.agentlink/backups/doc/，三件任一缺失/空立刻发声
- 教训（工具坑）：memory replace 会顶掉整条 entry，改局部须全文合并重写；HOME 在 wake 会话被重定向，桥文件一律绝对路径；本手册即防丢镜像，重要记忆变更后应落文档
- 重要信息落档顺序（10-18 机主确认）：先落本手册/名册 → 跑 doc_snapshot 立即快照 → contents API 同步 GitHub 镜像 → 最后压缩回写记忆（记忆是缓存，文档是正本）

## 七、互备与 API 新坑（10-15 晚实测）
- 网格互备：本 RUNBOOK 已 send-file 存证 .50/.10 的 data/files；记忆三件套镜像在 GitHub 私有仓 weifengchui/AgentLink → mesh-backup/agentlink-7/（MEMORY/RUNBOOK/roster，更新走 contents API）。
- **节点 REST 鉴权头 = `X-Auth-Token: <agentlink.json local_api.token>`**（不是 X-AgentLink-Token）。
- **send-file = JSON body**：`POST /v1/send-file {"to":"<node_id>","file_path":"<Windows 绝对路径>"}`；节点是 Windows exe，file_path 须 `C:\...` 形态（WSL 文件先拷 /mnt/c 可见区）；完成不推送，轮询 /v1/status active_transfers=0。
- **节点 /v1/send 字段苛刻**（要求 action/type 等，文档示例勿照抄）——发通知走自家桥 `:8643/send`，但桥是**严格白名单转发**：只透传 `to/type/action/payload/to_agent/from_agent/reply_to/ttl_seconds`，正文必须嵌在 `payload:{content:"…"}` 里。❌ 顶层 `kind`/`content` 会被桥**静默丢弃**，对端收到信封完好但 `payload={}` 的幽灵件（10-15 21:18 实测踩坑，本行旧版即错源，已勘正）。首选入口仍是 `agentlink-bus-ops/scripts/agentlink_api.py send <to> "正文" --to-agent agentlink`（序列化已验证正确）。
- git clone 用 gho_ token 报"Password authentication not supported"，**REST contents API PUT 可写**（GET 取 sha→PUT 带 sha 更新），GitHub 同步用 API 不走 git。

## 八、幽灵空件对账（10-15 21:18 事故记录）
- 21:18:25Z+8 互备 CLI 会话按第七节旧错误配方发 .50/.10 两条桥通知，正文被白名单吞掉，节点台账两条均 `payload={}`（msg 7715f903…→.10 等）。
- .10 于 21:25 主动对账（ea6700ae…），本机已定位根因、勘正第七节、重写 MEMORY 配方、补 skill 坑位，并按正确格式补发原件+对账结论；.50 同步收到说明件。
- 通用防御：发完重要通知后可查节点 `history.jsonl`（direction=out）确认 payload 非空。
