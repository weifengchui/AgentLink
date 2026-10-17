# Web 控制台需求台账 — 2026-10-17 第七轮

来源：用户 2026-10-17 反馈（4 项，其中第 3、4 项为方案讨论，不产生本轮代码）。

## R1 【bug】顶栏「在线节点」少算本机（显示 3/3，实际 4 台在线）
- 现象：全网 A(7)/B(50 本机)/C(10)/D(60) 四台均在线，顶栏统计胶囊显示 `3/3`。
- 根因：`/v1/status` 的 `peers_online`/`peers_known` 只统计**远端 peer**（`state.router.peers_online()` 与 peers 表长度），不含本机；而「节点」页表格是 `本机 + PEERS` 合并渲染（4 行），两处口径不一致。
- 修法：前端统计口径与节点表对齐——在线 = `peers_online + 1`（本机恒在线）、已知 = `peers_known + 1`。后端 `peers_*` 字段语义保持不变（watchdog/其他消费方仍按远端口径）。
- 验收：隔离节点冒烟——本机+1 peer 在线时顶栏显示 `2/2`；断掉 peer 变 `1/2`。
- 状态：☑ 已修（18781/18782 双测点节点实机：顶栏 `2/2`、节点表 2 行全「在线」，口径一致）。

## R2 【优化】对话记录（磁盘查询）条件改变时应即时搜索
- 现象：发送方/接收方下拉、起止时间、条数、含归档勾选改完**不会自动查磁盘**，必须再点一次「查磁盘」；关键词框倒是即时的（内存过滤），两类交互不一致。
- 修法（保留手动按钮 + 加自动触发）：
  - 六个磁盘查询控件统一 `onchange="schedSearch()"`，`schedSearch()` 做 350ms 防抖后调 `histSearch()`——datetime-local 逐段输入不会连环打服务器；
  - 查询期间 `ch-qmeta` 显示「查询中…」，完成/失败覆写结果摘要（复用现有 qmeta 通道）；
  - 「查磁盘」按钮保留作手动立即触发（跳过防抖），「重置」行为不变。
- 验收：改 ch-from 选项后 ≤0.5s 列表自动刷新且 `ch-qmeta` 出现查询摘要；连改时间框只发 1 次请求（网络计数断言）。
- 状态：☑ 已做（fetch 计数断言：单改条件 1 请求；since+until+含归档 连改 3 次合并为 1 请求；qmeta 先「磁盘查询中…」后落「服务器命中 1 条·扫描 1 KB」，#history 渲染正确行）。

## R3 【优化】agent 双层胶囊 hover 动效（内胶囊左右缓晃复位）
- 现象/诉求：鼠标悬停 agent 胶囊缺一个「响应了」的微反馈。
- 修法：CSS-only——`.idc:hover .idci` 播放一次性 keyframes：translateX 0 → -5px → +4px → -2.5px → +1.5px → 0，0.7s 缓出曲线；提供 `prefers-reduced-motion` 降级为不动。对话行、配色区预览等全部 `.idc` 实例统一生效。星图胶囊不改（其 hover 已有一整套卡组展开反馈，再加晃动属于过度）。
- 验收：hover 后 `getComputedStyle(.idci).animationName === 'cap-nudge'`，位移峰值 ±5px；移出再移入可重播。
- 状态：☑ 已做（CDP 真实 mouseMoved：悬停中 animationName=cap-nudge/0.7s/playState=running；移出→none；再移入→currentTime 归 0 重播，确认非单次状态）。

## R4 【讨论】看门狗间隔是否进 /web 设置页 — 结论：不做（本轮无代码）
- 理由：看门狗（al_watch.py cron）是**消费端 agent 运行时**的组件，不是 AgentLink 节点二进制的一部分；/web 设置页读写的是本节点 agentlink.json。把 interval 做进 /web 要么跨进程边界（web 去改别人机器上的 cron——需要新协议面与鉴权），要么逼 AgentLink 内嵌调度器（微内核化，与单二进制定位冲突）。对商用形态同理：客户机上的 agent 编排归客户侧运行器管，总线不该越界。
- 替代：间隔调整走 cronjob 工具直接改（一条命令的事）；若将来要做「响应度可见性」，正确方向是 agent 注册元数据带 last_poll/心跳上报，/web 只**展示**不**配置**——归 AgentMesh 路线图，不在控制台轮次里做。
- 状态：☑（决议，无代码）

## R5 【机制】模型切换：08:00–22:00（非百炼折扣窗）编码工作委托 pi
- pi CLI 已装（@earendil-works/pi-coding-agent@1.2.0，~/.local/node24/bin/pi），但无凭据（auth.json 空、环境无 key）→ 需用户拍板接入哪个 provider 后才能生效。
- 规则固化进 profile 记忆：折扣窗 22:00–08:00 内继续用自有模型/子 agent 做编码；窗外用 pi 承接繁重编码，主会话只做总控/验证/合并。
- 状态：☐（待凭据）
