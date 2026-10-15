# AgentLink 网格运维手册（本机 .7 / profile agentlink）
更新：2026-10-15 20:5x（源=profile 记忆 MEMORY.md + 当日会话实录；本文件为防丢镜像，两边有出入以最新时间戳为准）

## 一、身份与拓扑（记忆条目全文镜像）
- 我 = Hermes profile `agentlink`，agent_id/name=agentlink（逐字符小写）。
- 网格机 .7/.50/.10/.14 间 agent 同名合法——寻址必须 `node_id+agent_id` 成对；按名字找 agent=bug。
- 本机节点：127.0.0.1:8765，node_id=e5827032190c，v0.4.14。Windows 侧 `E:\软件工具\AgentLink-v0.4.11-bundle\启动.bat` 运行；活配置=同目录 `agentlink.json`（local_api.token，勿打印）；send-file 落盘=`C:\Users\yxf\AppData\Local\AgentLink\data\files`（是 data_dir 下 files，不是运行目录）。
- 对端：.50=4f793d7cd6fc · .10=f013cdcb7c91 · .14(新)=f5424c2f0a76 待注册。发前必查 /v1/peers。
- 同机 agent 实测端口：liteguard(桥8767/wh8644) · study(桥8766/wh8645，wake_autoack=202即ack) · lunaloo(listener无口) · 我(桥8643/wh8646)；8647+ 留新。
- 名册：/home/yxf/.agentlink/roster.md（v1.1，.50/.10 已自报回填）。

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
- 执行器=E:\软件工具\AgentLink-v0.4.11-bundle\upgrade_runner.py（md5 7113133cec86de2a32fad25f1845c39a，须含 errors="replace"×2）
- 流程：核 sha 全串→（runner 自动）备份 backups/v<旧>_<ts>→taskkill→换件→bat 拉起→health 轮询→失败自动回滚；台账=节点目录 upgrade-ledger.jsonl，**勿带 --git-ledger**（那是 .50 路径）
- 实例：10-15 11:13 升 0.4.13（trigger msg:4eee140b）；11:31 升 0.4.14（msg:0414-a7），均 4.0s done，身份未漂移
- 安全阀：崩溃/health 60s 不可达→backups 装回旧 exe+启动.bat 拉起，回报 failed_after_kill

## 五、发送纪律
- 必填 to(实时node_id)/type/action，正文在 payload.content；from_agent 必带；reply_to 用完整 32hex
- 重要消息 requires_reply:true（否则只躺 inbox）；发离线对端 ttl 3600；广播 "*"=at-most-once 慎用
- token 只在内存用，不落任何输出/日志/命令行

## 六、兜底与备份
- 看门狗 cron `agentlink-watchdog`（fd502509df0a，*/5m，no_agent）：查节点/桥/webhook/积压，异常自动拉起并报告，健康零输出
- 记忆备份 cron `memory-backup`（本文件配套，*/60m）：MEMORY.md+roster.md+本手册 hash 变化才快照，留 30 份于 /home/yxf/.agentlink/backups/doc/
- 教训（工具坑）：memory replace 会顶掉整条 entry，改局部须全文合并重写；HOME 在 wake 会话被重定向，桥文件一律绝对路径；本手册即防丢镜像，重要记忆变更后应落文档

## 七、互备与 API 新坑（10-15 晚实测）
- 网格互备：本 RUNBOOK 已 send-file 存证 .50/.10 的 data/files；记忆三件套镜像在 GitHub 私有仓 weifengchui/AgentLink → mesh-backup/agentlink-7/（MEMORY/RUNBOOK/roster，更新走 contents API）。
- **节点 REST 鉴权头 = `X-Auth-Token: <agentlink.json local_api.token>`**（不是 X-AgentLink-Token）。
- **send-file = JSON body**：`POST /v1/send-file {"to":"<node_id>","file_path":"<Windows 绝对路径>"}`；节点是 Windows exe，file_path 须 `C:\...` 形态（WSL 文件先拷 /mnt/c 可见区）；完成不推送，轮询 /v1/status active_transfers=0。
- **节点 /v1/send 字段苛刻**（要求 action/type 等，文档示例勿照抄）——发通知一律走自家桥 `:8643/send {to,to_agent,from_agent,kind,content}`（已验证 ok）。
- git clone 用 gho_ token 报"Password authentication not supported"，**REST contents API PUT 可写**（GET 取 sha→PUT 带 sha 更新），GitHub 同步用 API 不走 git。
