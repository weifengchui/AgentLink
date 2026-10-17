# Web 控制台 第十轮需求台账（2026-10-17）

用户报告 3 项 + 改名 1 项 + 追加 R6 扁平化/R7 按钮改版/R8 完整往来。目标版本 v0.5.4（纯 Web/观测轮次，协议兼容 0.5.x，0.5.1 编号仍预留 feat/v0.5.1-merge）。

## R1 星图实时刷新与高频防卡顿
- 现象/自查：星图无推送，固定 `setInterval(poll,5000)` 全量拉拓扑+history(1500) 重建；消息到达后 0~5s 才上屏，用户直观感受"不实时"。突发高频消息时：每个 poll 全量 innerHTML 重建 + 每条新消息一个粒子（独立 rAF + 临时链路），且 flyIn 在 renderLinks 之前执行——临时链路刚建就被 renderLinks 清掉，飞行中无发光轨道；粒子无上限，高频时主线程被拖住。
- 根因：①无事件触发通道；②poll 无并发合流（触发风暴时多个 poll 叠行）；③粒子数无上限；④poll 顺序 flyIn→renderLinks 互相打架。
- 修法：
  1. 服务端 `/v1/ws` 观察者语义：`client` 为空 = 观察者，收本机全部事件（现状把带 to_agent 的消息过滤掉了，观测页看不到定向消息）。仅影响空 client 连接，agent 桥（恒带 client）行为不变。
  2. 星图接 `/v1/ws?token=`（不带 client）：任一事件 → 400ms 合并 → 立即 poll；断线指数退避重连（2s→30s 封顶）；5s 兜底轮询保留。HUD 增「推送」指示灯（实时/轮询5s）。
  3. poll 加运行锁 + 尾随合流（polling 中再来触发只排一次）。
  4. flyIn 预算：每轮最多 8 个粒子，超预算只做徽章 ping；单轮新消息 >60 条视为突发，全部跳过粒子（徽章仍对），防高频卡顿。
  5. poll 顺序改为 ingest→renderLinks→renderAllDecks→flyIn（临时链路存活，且优先点亮已有永久链路）。
  6. 主控制台发信成功/点「刷新」→ postMessage {al:'poke'} 给星图 iframe，出站消息也秒级上屏（服务端出站单播不进广播，不动 router 以免混合版本舰队收重复）。
- 验收：浏览器里 WS 连接状态=实时；投一条消息 ≤1s 粒子出现；连发 80 条无卡死（poll 合流、粒子 ≤8）；关掉 WS（模拟）退回轮询 5s 仍能出图。
- 状态：☑（验收见文末）

## R2 星图刷新后整体偏左、需二次刷新才恢复
- 现象：偶尔刷新页面星图挤向左边，再刷新一次才好。
- 根因：W/H 在脚本解析时取 innerWidth/innerHeight，iframe 首帧尺寸未定（默认 300×150 / 隐藏态 0×0）时全量按错尺寸落位；sizeUp 靠 resize 事件修正，但 iframe 初始尺寸就位不一定触发 resize，且 `rx=W/W0` 在 W0=0 时得 Infinity/NaN 把 STA 全部打坏，只能整页重载。
- 修法：①W/H 一律以 `stageEl.getBoundingClientRect()` 为准，boot 先量一次；②每次 poll 开头 syncDims() 对表：尺寸变了就按新尺寸重标（5s 内自愈，poke 触发时更快）；③prev 尺寸为 0/NaN 时不再缩放，改从 MACH 分数坐标 reAnchor 重建；④AGTP 半径重算同样防 NaN。
- 验收：把 iframe 先隐藏后显示、以及首帧小尺寸两种时序下，一次 poll 内布局居中；连续 3 次硬刷新截图对比无左偏。
- 状态：☑

## R3 悬停 agent 胶囊：有消息卡片但没有连线
- 根因：`gTopic`（卡片悬停的话题聚焦）只在卡片 pointerleave 时清除；5s 轮询重建卡组把被悬停的卡片节点移除后 pointerleave 永不触发，gTopic 残留 → 之后 applyLanes 恒走 `gTopic` 分支（只显示旧话题链路），新悬停实体的链路被压暗，即"有卡无线"。
- 修法：①卡片 pointerenter 同时记录 gOwnerK；applyLanes 开头校验 `dwraps[gOwnerK].matches(':hover')`，不在悬停态即清空 gTopic；②renderAllDecks 跳过正在悬停展开的卡组（不重建、不闪、保留 .hot 态，离开后下轮自然更新）。
- 验收：JS 触发 卡片hover→卡组重建→再悬停另一胶囊，链路必须亮；真鼠标路径复测一次。
- 状态：☑

## R4 右上角「自动刷新」开关与「刷新」按钮有效性核查
- 现状：`setInterval(()=>{if($('auto-refresh').checked)refresh();},2000)`，按钮 onclick=refresh()；逻辑有效，但「刷新」只刷控制台面板，星图 iframe 不受手动刷新影响（体验割裂）。
- 修法：「刷新」按钮 = refresh() + pokeStar()（postMessage 给 iframe）；回归测试验证开关 off 时 2s 轮询确实停、on 时恢复、按钮立即刷。
- 状态：☑

## R5 「查磁盘」按钮改名「查询」
- 对话记录卡片的检索按钮文案 查磁盘 → 查询（功能不动）。
- 状态：☑

## R6 星图大屏胶囊按图扁平化（去阴影）+ 弹窗 hover 微动效统一（10-17 用户追加，附截图）
- 诉求：胶囊样式统一为截图（消息弹窗）同款「深壳＋纯色亮芯、无描边无高光无阴影」；所有胶囊悬停动效与对话记录一致（内胶囊 cap-nudge 左右晃一次）；钉选态按建议=1px 同色细描边＋轻微提亮。
- 现状差异：控制台 `.idc` 与弹窗 `.idc` 本就扁平（即基准）；星图画布 `.agt .dot` 带 1px 白描边、内芯径向高光渐变、静止/高亮/钉选三处 box-shadow 发光、hover 用 scale(1.1)；弹窗 `.idc` 无 hover 动效。
- 修法（仅 web/starmap.html）：①`.dot` 去白描边改 transparent 占位（保几何）、圆角 14→13；②`.in` 纯色平面、字重 600、文字色改 JS onCol 随手选色自动深浅；③hover 由 scale 改 cap-nudge（与 api.rs 逐字同源），`.idc:hover .idci` 同步补上；④`.lit`/`glow` 去 box-shadow，改 filter 提亮（glow 另加同色细描边）；⑤`.pinned` 发光圈→`border-color:var(--ac)`＋brightness(1.15)；reduced-motion 降级两处都带。
- 验收：隔离节点实机——计算样式断言 `.dot`/`.in` boxShadow==='none'、`.dot` border 透明；CDP 真鼠标悬停 `.in` getAnimations 见 cap-nudge running，弹窗卡片与对话记录同效；钉选/悬停截图 vision 复核无发光残留。
- 状态：☑ 完成（2026-10-17）。验收记录：隔离节点 18793（debug 二进制）实机——计算样式：`.dot`/`.in` boxShadow 全态 none、border 静止透明(rgba(0,0,0,0))/钉选与 glow 态=var(--ac)、`.in` backgroundImage:none（渐变已除）、字重 600、文字色 inline onCol、outer−inner=14px、justify=flex-end、圆角 13px；CDP 真鼠标悬停星图胶囊 `cap-nudge:running`、离开后归零；弹窗 `.idc` 悬停同样 `cap-nudge:running`；lit/pinned/链路点亮四态逐项读定（probe 内关 transition 防过渡插值假阴性）；vision 复核画布截图无描边/高光/发光残留。

## R7 右下角按钮改版（10-17 用户追加，原话："去掉应用时段按钮，实时按钮点击切换指定时段，在按钮上方弹窗选择开始截止时间；把时间区间滑块去掉"）
- 修法（仅 web/starmap.html）：①删「应用时段」独立按钮（`#bMan`）与时间区间滑杆（`#back`/backS，含 CSS 与 change 接线）；②「实时」按钮变双态切换：实时态=青色高亮(on)；点击→「时段」态（暗色，弹出选择面板，暂停 LIVE 自动窗口，手工 mFrom/mTo 生效并立即刷新）；再点→回实时（面板收起、win 窗口恢复）；③开始/截止时间两个 datetime-local 输入移入按钮上方浮动面板 `#mPop`（absolute，bottom 定位于 bLive 上方，右缘对齐）；④时段态下改时间即时生效（无需确认键），非法区间（起≥止）拒绝并保持上一有效区间；⑤RNG 后缀三态：实时/时段/回溯。
- 验收：DOM 断言 bMan/#back 不存在；双态切换文案/高亮/面板显隐/LIVE 标志逐项；面板 rect 在按钮上方且右缘交叠；时段态改 mFrom→RNG 即时更新；非法区间保持；回实时面板收起。
- 状态：☑（验收见文末）

## R8 消息弹窗展示完整往来（10-17 用户追加，原话："消息弹窗里，往来消息都要展示出来，而不仅仅是当前的收件"）
- 根因（口径旧）：卡片/弹窗按"实体参与的链"计数（topic 内 f 或 to/k 命中才计），点开 study 只见 study 收发的消息，同话题里 liteguard→boss 的中转段不可见。
- 修法（仅 web/starmap.html）：①`chainFor` 去实体过滤=话题完整往来链（reply_to 并查集分组下全量消息）；卡片"N 条"、卡组"共 N 条往来"、弹窗左列表、◀▶ 组翻页同口径；②弹窗左列表每行标注发件人（新增 `.rf`，`@node`=整机）；③弹窗初始选中=链内最后一条"涉及该实体"的消息（有收件看最新收件，有发件无收件看最新发件，纯旁观进第一条），◀▶ 仍可翻全链。
- 验收：三方链 study→lg→boss→study 从 study 点开：列表 3 行含 study 不在场的 liteguard→boss 段、逐行发件人正确、选中首屏=boss→study（3/3）、卡片"3 条"=seq 分母。
- 状态：☑（验收见文末）

## 本轮总验收记录（10-17 16:30–16:55，隔离节点 r78-test@18795，debug 二进制 + 注入 study/liteguard/boss 三 agent 与三方话题链）
- R1：HUD 推送=实时（WS 观察者态收全事件）；poke/postMessage、poll 合流、粒子预算在位。
- R2：iframe 内 canvas 尺寸与视口一致；隐藏→显示时序一次 poll 内 syncDims 重标。
- R3：卡片 hover 重建卡组后链路仍亮（`:hover` 校验 + 跳过悬停卡组）。
- R4：下发页 `function pokeStar` 存在、「查询」按钮在位、refresh 手动按钮带星图 poke。
- R7：bMan/#back 已除；「实时」↔「时段」双态切换与上方弹出面板几何（pop.bottom≤btn.top）；时段即时生效（窗口 16:40→16:42）；非法区间拒绝保持；回实时收起面板；`window.__al` 双态标志实机读取。
- R8：弹窗左列表=话题完整 3 条（含 study 不在场的 liteguard→boss），逐行发件人 study/liteguard/boss，序列 3/3、首屏选中涉及消息；vision 复核截图 `cache/scratch/r78_viewer.png`/`r78_manual_pop.png`。
- 服务端下发页脚本 node --check 全绿（served_star/served_web，各 1 块）。

## 发布
- v0.5.4：Cargo.toml bump → fmt/clippy → 双平台 release → dist zip → commit + tag → push → 本地上线（本机生产节点换新 exe，用户在线检查）。按分发拍板：只交版本号+更新要点，不做全网通知。
- 遗留（不入本轮）：工作树中 57 个 dist 旧版 zip 的删除记录待用户拍板（历史发布产物，台账引用较多，建议恢复）；对话记录卡片标题文案「可按条件查磁盘全量」未改（R5 只改按钮）。
