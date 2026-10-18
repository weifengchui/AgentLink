# /web 控制台需求台账 — 2026-10-18 第十五轮

背景：用户提出三件事——①消息抬头模板标准化（参考 A 机 study 团队现行格式【study@B → study@A｜ESC｜10-18 08:30】）；②消息组（同一主题如升级的 预告→指令→go→done 多回合往来应作为一组，星图弹窗完整展示）；③星图连线偶发不出现的老 bug 必须彻底修复。

现状代码基线：v0.5.7（tag 80d37a4，fleet 全机已升）。星图页 `web/starmap.html`，Rust 侧 `src/proto.rs`（Envelope 顶层 `reply_to`）、`src/router.rs`（SendRequest ~L640-680、HistoryEntry L561-585）、`src/cli.rs`（send 读 reply_to ~L730）、`src/api.rs`（控制台发送表单）。

---

## R1 消息抬头官方标准格式（文档轮，待 A 机 study 回信后定稿）

- 现象/诉求：各团队自发用【a@机 → b@机｜标记｜MM-DD HH:MM】抬头，可读性好但纯手打：时间会写错/漂移、标记位无统一集合、机器无法据此归组。
- 根因：总线已有权威元数据（from_agent/from node/to_agent/to node/ts/action），但没有沉淀「正文抬头」规范，也没有把抬头与结构化字段打通。
- 修法：
  1. 咨询 study@A（已发 msg b67774e046f74e9bbceebd075d70aa0c）取团队实战规范；
  2. 结合总线能力给出官方标准 `docs/消息模板标准.md`：抬头语法、标记位集合（对齐任务协议 type/action：NOTICE/ASK/GO/DONE/ACK/ESC/VERDICT/REPLY）、时间与线程标识规则；
  3. 结构化优先：抬头里「谁能机器算的就让总线算」（路由与时间戳由 HistoryEntry 派生，星图/控制台自动渲染），正文手打头只留「标记位+主题意图」；R2 的 `thread` 字段即消息组的机器标识。
- 验收：标准文档合入 main 并在 AGENTS 使用说明/发布 README 摘要指引；控制台与星图弹窗能按标准渲染抬头。
- 状态：✅ 定稿（2026-10-18）。study@A 回信（09:52，全量 v2/v2r.1-3 规范+五条硬教训）已吸收进 `docs/消息模板标准.md` v1.0；AGENTS 使用说明 §5/§6 已挂 thread 字段与标准指引。UI 自动渲染胶囊为后续项（§4 已定义口径）。

## R2 消息组（thread 线程标识，端到端）

- 现象：滚动升级一个主题的 升级预告/升级指令/upgrade_go/upgrade_done 在星图里被拆成 3~4 个独立话题卡（升级消息只有 done 带 reply_to，预告与指令全部 reply_to=None）；弹窗看不到完整主题往来。
- 根因：`web/starmap.html` buildTopics（L728-735）只按 reply_to 链根归组；协议层没有「线程」概念。
- 修法：
  1. 协议：`proto.rs` Envelope 顶层新增 `thread: Option<String>`（serde default，混版本转发兼容，与 reply_to 同法）；`router.rs` SendRequest 增 thread 透传入账；`HistoryEntry`（router.rs L561-585）增 `thread`；`/v1/history/search` 原样返回；出站/入站落账两处（router.rs L1181、L2078 邻域 reply_to 同位置）。
  2. CLI：`cli.rs` send 增 `--thread`（L730 reply_to 同型读法）。
  3. 控制台：`api.rs` 发送表单增「消息组」输入（可选，placeholder=主题短名），doSendAll 带上；上传通道可不带。
  4. 星图：buildTopics 归组键改为 `e.thread || 链根`——显式 thread 优先，无 thread 回退现有 reply_to 链根（向后兼容）；话题名 thread 存在时取 thread 值；卡片「N 条」、弹窗 chainFor、卡组 groupsFor 自动继承新分组，无需改逻辑。
  5. 规程：升级编排脚本（rolling-upgrade 技能话术）今后 notice/instructions/go/done/ack 全部带同一 `thread=upgrade-vX.Y.Z-<机名>`。
- 验收：隔离双节点冒烟——同 thread 的 4 条消息（含跨节点转发）在 /v1/history/search 返回中 thread 字段齐全；星图悬停发起实体仅见 1 张话题卡，点开弹窗按时间序完整显示 4 条往来；无 thread 的旧消息仍按 reply_to 链成组（回归不变）。
- 状态：✅ 已合并（feat/v058-thread @ f91a1c4）。复验：合并产物 thread-smoke 11/11 PASS（须显式传真旧二进制作 argv[2]，默认 old 路径=新构建会假 FAIL）、fmt/test(21)/clippy/build 全绿、服务端页含 thread 归组代码、浏览器验证弹窗 4 条按时间完整显示。

## R3 星图连线偶发不出现 — 彻底修复

- 现象：连线（新消息闪光链路）有时候不出现，鼠标悬停/点卡都不亮，刷新页面恢复正常。第十一轮（10-17）修过四处根因（cancelLiveAnims 绞杀、innerHTML 抹 temp 线、重建后悬停态丢失、J() 无超时）仍复发。
- 剩余嫌疑（按优先级排查，逐项给复现证据）：
  1. **突发降级规则**：poll 单轮新增 >60 条只 ping 徽章不飞粒子（L755）——后台标签页节流/断线重连后积压一次性入 KNOWN，这些消息的连线永久不可见；刷新清 KNOWN 后 90s 内重新飞一遍才「恢复正常」。与症状完全吻合。
  2. **rAF 暂停**：标签页隐藏时 flyIn 的 requestAnimationFrame 挂起，4500ms setTimeout 兜底 arrive 时页面不可见，粒子/线白飞；消息已入 KNOWN 不再重飞。
  3. **WS poke 竞态残留**：pokePoll 400ms 与 5s 轮询叠加下 syncWorld 重建窗口内 fresh 计算与 renderLinks 顺序仍有缝隙（重建清 map 后、renderLinks 前到达的 fresh）。
  4. 幽灵过滤时序：growLive/mergeAgents 在 buildTopics 之后，首轮新 agent 的 fresh 判定 ghost() 未及更新被跳过。
- 修法（设计原则：**连线可见性不能依赖一次性的粒子动画**）：
  1. 新增「持久亮线」机制：窗口内消息对应的链路挂 `.live` 类持续显示（如最后一条消息到达起 60s，或该消息仍在窗口内且所属话题被本实体收过——具体口径实现时定，须保证 HUD=Σ徽章=卡组同一口径）；粒子飞行动画与亮线解耦——粒子错过（突发/隐藏页/重建）时线仍然亮过。
  5. 突发/积压时对超预算消息不再静默吞掉：改为逐条静默补亮线（lightLink 队列合并，一帧内完成，不拖主线程）。
  7. 页面从隐藏恢复（visibilitychange）时：把「恢复前 90s 内、本会话还没亮过线」的消息补放一轮亮线（不飞粒子也可）。
  8. 以上全部先建复现矩阵（隔离双节点 + 浏览器自动化：后台标签页模拟、突发灌 61+ 条、poke 风暴、新 agent 注册首消息），每个嫌疑给出「修复前必现 / 修复后必绿」断言。
- 验收：四条复现场景在修复后星图页全部可见连线（`.lk.live` 或 `.lit` 断言 + 截图存证 docs/mockups/）；十一轮既有回归（飞行中插 poll 线不被绞杀、悬停跨感应区卡组不消失）不回退。
- 状态：✅ 已合并（fix/v058-starlink @ ed02931）。真根因在四个嫌疑之外：buildWorld 的 `stageEl.innerHTML=''` 把连线宿主 svg#map 抹掉、后续连线全写进游离树（名单重建时触发→「偶发不出现、刷新恢复」）；叠加嫌疑1/2 以 `.live` 持久亮线机制根治（连线可见性与一次性粒子动画解耦+visibilitychange 补放+突发静默补线）。四场景修复前必现/修复后必绿断言齐全（docs/星图连线-修复验证-2026-10-18.md，14 张存证）；十一轮回归全重验；合并产物服务端页含补丁、node --check 过。

## 发版口径

本轮全部合入 main 并通过六套冒烟后，按惯例 bump v0.5.8 再走发布流程（发版需用户确认，保护窗外执行）。
