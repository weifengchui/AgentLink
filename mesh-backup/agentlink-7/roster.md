# AgentLink 网格名册（roster）v2 终稿 — 2026-10-15 由 agentlink@.7 维护
# 命名新规（机主授权）：存量 agent 一律不改名（agent_id=profile 名原文，同名跨机合法，
# 寻址必须 node_id+agent_id 成对）；新机器/新 agent 命名带机号后缀（取 IP 末段），如 study-51、agentlink-51。

| 机号 | IP | node_id | 节点版本 | agent（桥管理口/wh 路由） | 备注 |
|---|---|---|---|---|---|
| 7 | 192.168.1.7 | e5827032190c | 0.4.14 | agentlink(桥8643/wh独立网关8646路由agentlink-msg) · liteguard(桥8767/wh8644) · study(桥8766/wh8645) · lunaloo(listener无口) | 舰队总控 study 所在机；本机 default 网关未运行，三 profile 独立网关各读各家正本；wh 8647+ 预留；三桥非同口径=主桥 from 变体+liteguard/study 无守卫族（见下），勿按"全 from"抄行 |
| 50 | 192.168.1.50 | 4f793d7cd6fc | 0.4.14 | agentlink(桥8647/路由/p/agentlink/webhooks/agentlink-msg-al/目录D:\HermesSpace\.fleet-agentlink\agentlink) · liteguard(桥8766/路由/p/liteguard/webhooks/agentlink-msg-lg/D:\HermesSpace\LiteGuard\agentlink daemon.py) · study(桥8767/路由/p/study/webhooks/agentlink-msg/D:\HermesSpace\.fleet-B1\scripts\al_bridge.py 旧schema connected+registered) | 共享 multiplex 网关：host 127.0.0.1:8644 统一监听，/p/<profile>/webhooks/<route> 前缀路由，无独立 webhook 口；10-15 20:15 .50 自报回填：三桥 ws_connected=true、fleet online=true；版本勘正 0.4.12+→0.4.14；三桥全 to 变体 |
| 10 | 192.168.1.10 | f013cdcb7c91 | 0.4.14 | agentlink(桥8768/.fleet-agentlink/wh共享网关8644路由/p/agentlink/) · liteguard(桥8643/.fleet-B1) · study(桥8767/.fleet-C) | 共享网关 multiplex 布局；WSL NAT 模式；10-15 20:15 .10 自核实回填：行无错误，三桥 ws_connected=true，fleet 三 agent 均 online；10-15 20:2x .10 回执(msg 6bfd848f)回填三桥混合口径=1 from + 2 to：from 桥 wake.json 字段名 **node_id**（值=f013cdcb7c91，双条件生效）；两 to 桥 wake.json 无 node_id/self_node，合规；from 缺失/node_id 缺失均放行（宁多醒不丢件） |
| 60 | 192.168.1.60（原名册误记 .14=该机装机手误自报，10-16 该机自报勘误坐实：PUT /v1/config 改正名称，node_id 不变，无需重发现） | f5424c2f0a76 | 0.4.14 | study-60(桥ctl 8643/共享网关wh 8644；首注册即照新规带-60后缀；曾用名 study60-14 注销、study60 改名、study 系10-16 01:23误读手册§三-1短暂注册、已注销) | name=192.168.1.60, mode=open, data_dir=%LOCALAPPDATA%\AgentLink\data；WRLAN 接入；WSL2 NAT，WSL 侧经 gw 访问节点 API；10-16 自报 msg 5df642d5 已回执（其前一条 4416c6c9 中 .14/study60-14 字样作废）；.7 fleet 两查(01:16/01:17)仍未见 study60 行，已请其核注册待回；其滞留旧件 4416c6c9 迟到 01:17 唤醒本侧，已发清理回执 3d663da4（其间冗余件 6d13ea07 声明作废）；该机 peers 视图 .7/.50/.10 全离线系单边问题，已请其复核 Windows 入站 TCP8765+UDP8766；10-16 01:29 msg 456077ed 勘误坐实：agent 定名=study-60（本行为准），.7 fleet 已见 study-60 行 online=true（前述缺行异常①闭合），节点名 192.168.1.60 经 Get-NetIPAddress 实核 ✓；手册 v1.6 §二/§三-1『全队统一名 study』与名册新规『新 agent 带机号后缀』冲突已提请总控裁决——现行以机主 10-15 授权的名册新规为准，study-60 合规 |

## wake_hook 自回声守卫变体分类（终稿必备字段标注，各机勿盲抄）
守卫公共第一条件：from_agent==me。第二条件分两族，wake.json 必备字段随之不同；
from 族字段名存在两式（.7=self_node / .10 主桥=node_id），语义等价=本机节点 id，
名册接受两式，不强制改名，各机无需动码；节点 re-init 换 node_id 须回写该键。
- **to 变体**：且 to∈(None,'',me) 才跳过。**wake.json 无需 self_node/node_id**（该键不被读取）。定向信封
  to=node_id 恒不等于 agent 名 → 跨机同名 agent 定向件必放行。现役：**.50 三桥**（10-15 实测回执，
  实证=其回执所在线会话由定向件叫醒）、**.10 之 .fleet-B1(liteguard,8643)/.fleet-C(study,8767)**
  （10-15 回执 msg 6bfd848f）。
- **from 变体**：且 from==本机 node_id 才跳过。**wake.json 必含本机节点 id（self_node 或 node_id 两式均可）**。
  现役：
  - **.7 主桥**（/home/yxf/.agentlink，桥8643）：跳过集 from∈(None,'',me,self_node)——空 from 也吞，
    系防 to="*" 广播环回唤醒风暴的本机补丁；self_node=e5827032190c ✓。
  - **.10 主桥**（.fleet-agentlink，桥8768）：仅 from==本节点 id 跳；from 缺失/空放行、node_id 缺失放行
    （宁多醒不丢件）；node_id=f013cdcb7c91 ✓（10-15 回执 msg 6bfd848f，双条件化收敛）。
- **无守卫族**（不属两族，任何机勿按"三桥统一"口径抄）：**.7 之 liteguard(桥8766)/study(桥8767)** ——
  al_bridge.py wake_post 无 from_agent 自回声条件，wake.json 亦无 self_node/node_id；防自环靠
  skip_content_prefixes 自检闸 + reply_to 台账去重，10-15 实跑未见回声风暴。
- 各族对**真实跨机定向件**行为一致（均放行），差异仅在自环/空字段场景，各机可按己维护。

寻址纪律：① 发前必查 /v1/peers（node_id 会因 re-init 漂移，靠 0.4.11+ 身份保留兜底）
② from_agent 必带；reply_to 用完整 32hex msg_id ③ 同名消歧只靠 node_id，任何"按名字找 agent"的写法都是 bug
④ 广播 to="*" 系 at-most-once，重要消息一律逐机定向 + 足够 ttl
⑤ 路由治理铁律（机主 10-15 拍板）：正本=~/.hermes/profiles/<profile>/webhook_subscriptions.json，
人工只改这里；生成物由 sync_webhook_routes.py（crontab */5）编译，禁手改。
