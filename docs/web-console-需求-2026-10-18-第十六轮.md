# /web 控制台需求台账 — 2026-10-18 第十六轮

背景：用户报告两件事——①星图「截图/录屏」产出的内容不是当前界面的样子；②确认消息组弹窗是否包含完整往来链（发+收），而非仅收到的消息。

现状代码基线：v0.5.8（tag 74f6a43，fleet 全机已升）。星图页 `web/starmap.html`（include_str! 编译进二进制）。

---

## R1 星图截图/录屏所见即所得

- 现象：点「截图」得到的 PNG、点「录屏」得到的 WEBM，画面与实际星图界面完全不符（大六边形+散点，没有胶囊、卡组、HUD、弹窗、图例等任何真实元素）。
- 根因：`snapshot()/snapshotTo()` 是旧视觉稿遗留的**简化重绘**——只从数据模型（MACH/STA/LINKS）重新画一套六边形站台+圆点+连线（半径 66px，早于第五轮 38px orb 定版），与真实 DOM 渲染毫无关系，且永远不包含胶囊/徽章/卡组/弹窗/顶栏/底栏。
- 修法：改为真正的所见即所得管线（零外部依赖）：
  1. `captureHTML()`：把 `document.documentElement` 深克隆，剥离 `script/link`、冻结 CSS 动画（animation:paused）防逐帧抖动；canvas 内容烘焙为 `<img src=toDataURL>`；表单控件现值内联；`:root` 自定义属性靠克隆内联样式自然传递；XMLSerializer 序列化后装进 `<svg><foreignObject><![CDATA[…]]></foreignObject></svg>`，blob URL 加载为 Image。
  2. 截图：Image → 离屏 canvas（2x 超采样）→ PNG blob → 下载，文件名不变（agentlink-star-<ts>.png）。
  3. 录屏：`#capstage` 隐藏浮层内放常驻 `<img id="capimg">` + `<canvas id="capcv">`；逐帧 captureHTML → `img.decode()` → `drawImage` → `canvas.captureStream(30)` 喂 MediaRecorder；产物 webm 不变；录制时长由用户点「停止」控制（废除 12s 自动停——录屏本意是录一段往来演示）。
  4. 弹窗打开时截图/录屏会包含弹窗本身（所见即所得），符合预期。
- 验收：隔离节点灌消息后，浏览器点「截图」拦到 blob URL → 取回 PNG 与实机页面 vision 比对（站台/胶囊/HUD/底栏文字齐全、布局一致）；点「录屏」→ 等数秒 → 停止 → 得到非空 webm（blob size>0、时长≈录制秒数）。
- 状态：☑ 已实现（v0.5.9）。实测：隔离双节点（18776/18777）灌双向消息后浏览器点「截图」得 2226×1292 PNG（scratch/r16-snap2.png），vision 比对为真实界面——前景消息组弹窗锐利完整（条目列表、胶囊、页码、快捷键提示），背景星图控制台（顶栏/侧栏/底栏/orb 光斑）齐全，无旧式六边形重绘痕迹；点「录屏」→约 3 秒→停止得非空 webm（scratch/r16-rec.webm，vp9，时长≈3.1s，1s 关键帧切片，帧数少系录制中真页面动画整体冻结所致，符合设计）。

## R2 消息组弹窗完整性核查（发+收全链）

- 诉求：确认弹窗含往来消息（发出去的与收回来的），完整消息链，方便看来龙去脉。
- 代码口径（第十轮 R8 + 第十五轮 R2 已定版）：`chainFor(tpid)` 取 `tp.msgs` 全量，只做窗口/隐藏/幽灵过滤，**不按查看实体切片**——组内所有 A→B、B→A、第三方中转消息全部入列，逐行 `.rf` 标发件人；卡片「N 条」、卡组「共 N 条往来」、弹窗序列、‹› 换组同口径（=话题窗口内总条数）。
- 核查方式：浏览器实测——双 agent 互发同 thread 4 条（a→b、b→a、a→b、b→a），从任一方开弹窗断言 `#vlist .row` 恰为 4 条、含发出方向、按时间升序；另断言 `window.__al.chainFor` 直查口径。
- 状态：☑ 已验证（v0.5.9 随 R1 冒烟同环境实测）。双向同 thread 4 条（A发B×2 + B回A×2）从接收方开弹窗，截图（scratch/r16-snap2.png）可证：`#vlist` 恰 4 条（R16-01 A发B / R16-02 B回A / R16-03 A发B / R16-04 B回A），含发出方向、按时间升序，页码 4/4——发+收全链口径无误。
