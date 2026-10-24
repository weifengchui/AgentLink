# AgentLink 升级协调协议 v1

状态：已定稿（2026-10-12，时序 5min/120s/30s，尽力原则，本机最后升）
定位：纯业务层消息协议，不改 AgentLink 内核。任何 agent 按本文档即可充当执行者/准备方。

## 角色
- 发起方（orchestrator）：发布新版本者（通常由主节点上的编排 agent 担任）
- 执行者（executor）：目标机上能执行系统命令的常驻 agent（部署形态不限，收消息触发即可）
- 准备方：目标机上其余所有在线 agent

## 时序参数（默认，可在 notice 里覆盖）
- 提前量 lead：300s（notice.eta = 发出时刻 + lead）
- 报备超时 ready_deadline：120s（超时未报 = 视为就绪，尽力原则）
- 健康校验窗口 health_timeout：30s（超时 = 自动回滚）
- 备份保留：最近 2 版

## 消息（均为 text/send，payload.topic 路由，点对点 at-least-once）

### 1. upgrade_notice（执行者广播给目标机全体；不带 to_agent）
```json
{"topic":"upgrade_notice","old":"0.4.7","new":"0.4.8",
 "eta":"2026-10-12T22:30:00+08:00","ready_deadline":120,
 "file_name":"AgentLink-v0.4.8.zip","sha256":"…","executor":"ops"}
```
执行者收到 notice 后自己先做：把 zip 下载/落位到 recv_dir、校验 sha256、备好备份目录。

### 2. upgrade_ready / upgrade_declined（各 agent → 发起方）
```json
{"topic":"upgrade_ready","agent":"worker-a"}
{"topic":"upgrade_declined","agent":"worker-a","reason":"正在传大文件","need_extra_seconds":180}
```
准备动作 = 暂停发大件/批量消息、消费完自己收件箱、持久化自有状态。
执行者（兼任准备方）自动报 ready。

### 3. upgrade_go / upgrade_abort（发起方 → 执行者，定向 to_agent）
```json
{"topic":"upgrade_go","new":"0.4.8","file_name":"AgentLink-v0.4.8.zip","sha256":"…"}
{"topic":"upgrade_abort","reason":"…"}
```
go 前：declined 且追加时间可接受 → 顺延一次；其余按尽力原则放行。

### 4. upgrade_done / upgrade_rolled_back（执行者 → 发起方，并广播全体）
```json
{"topic":"upgrade_done","old":"0.4.7","new":"0.4.8","health_ok":true,"elapsed_ms":4200,"backup":"backups\\v0.4.7_20261012-2230"}
{"topic":"upgrade_rolled_back","reason":"health timeout","restored":"0.4.7"}
```

## 执行者流程（upgrade_runner.py 实现）
1. 备份：节点目录 exe + agentlink.json + 启动.bat → `<nodeDir>\backups\v<old>_<ts>\`
2. 解压 zip → 校验 sha256（不符即失败，不碰现网）
3. `taskkill /F /IM agentlink.exe` → 覆盖二进制 → `启动.bat` 拉起（日志 run.log）
4. 轮询 `/v1/health`：version==new 且 ok → done；30s 不达 → 还原备份 exe → 重启 → 校验旧版 → rolled_back
5. 台账：`<nodeDir>\upgrade-ledger.jsonl` 追加条目；发起方 git 侧留副本
6. agentlink.json 全程不动（node_id/token 不变，全网连接自动重连回来）

## 发起方编排流程
1. 前置：git 已 commit+tag、dist zip 存在、**已推远端**（版本管理前置，防丢）
2. send-file 发 zip 给目标机执行者（to_agent 定向）
3. 发 notice → 收报备 → 决策 go/abort
4. go → 盯 done/rolled_back → 核对全网 /v1/health 版本表 → 记台账 git 副本
5. 多机：逐台串行，先远端后本机；本机自升时发起方先把 in_progress 状态落盘

## 执行者部署要求（新机器接入）
- 能读写节点目录、能跑 cmd（taskkill/启动.bat）、能访问 127.0.0.1 节点 API
- 拿到 `upgrade_runner.py` + daemon 升级分支源码（随 dist 分发）
- 在节点 API 注册 capabilities 含「升级执行者」，发起方以此寻机
