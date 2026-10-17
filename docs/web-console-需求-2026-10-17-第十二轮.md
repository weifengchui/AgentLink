# Web 控制台需求台账 — 2026-10-17 第十二轮

随本批发布的还有第十一轮「星图连线闪烁修复」（改动在工作树，无独立台账，四根因与回归口径见技能 agentlink-web-dev「连线闪烁修复（10-17 第十一轮）」节）。

## R1 星图右下角控件简化为纯实时视图（10-17 用户原话："去掉实时按钮和时间段文本，只提供实时视图，窗口长度允许最大1小时。做完就发布"）
- 现状：底栏有「窗口长度」滑杆（max 1800s）+ 时间区间文本 `#rng`（第十轮 R7 的三态后缀 实时/时段/回溯）+ 「实时」双态按钮（点击切时段态并弹出 `#mPop` 开始/结束 datetime-local 面板）。
- 修法（仅 web/starmap.html）：
  1. 删 `#bLive` 按钮与 `#mPop` 面板（含 `.bwrap`/`#mPop`/`#mFrom`/`#mTo` CSS 与 `setLive`/`liveUI`/`applyManual`/`renderManual`/`pad2`/`dtv`/pointerdown 外点收面板等全部接线）；
  2. 删 `#rng` 时间区间文本与 `updateRange`/`fmtT`；
  3. 时间窗恒为跟随现在的滑窗：`wBounds()=[now-winS*1000, now]`，`LIVE`/`mFrom`/`mTo` 变量废除（`edge=LIVE&&!paused`→`!paused`；`window.__al.live` 保留但恒 true，兼容外部断言脚本）；
  4. 窗口长度滑杆 `min=60 max=3600 step=60`（上限 1 小时），默认 600（10min）不变。
- 验收：隔离节点实机——DOM 断言 bLive/mPop/mFrom/mTo/rng 全部不存在；底栏按钮=复位/流入/截图/录屏/全屏；滑杆拖到 max 时 `winv`=60min 且窗口滤数随 `winS` 生效；`window.__al.live===true`；boot/poll 无 JS 错误；源码与服务端下发页脚本 node --check 全绿。
- 状态：☑ 完成（2026-10-17）。验收记录：隔离节点 al055-test@18797（release 二进制）实机——DOM 断言 bLive/mPop/mFrom/mTo/rng 全部不存在；底栏按钮恰为 复位/流入/截图/录屏/全屏；滑杆 min60/max3600/step60，拖到 max 显示 60min、回 60 显示 1min（oninput 生效）；`window.__al.live===true`；注入三方话题后 HUD 窗口内 3、悬停胶囊出卡组 2 卡；vision 复核底栏截图无实时按钮/无区间文本。源码与服务端下发 `/v1/starmap` 脚本 node --check 全绿；双端二进制 strings 标记 max="3600"×1、bLive/mPop/updateRange×0。

## 发布
- v0.5.5：☑ 已发布（commit+tag；zip `AgentLink-v0.5.5.zip` 2,992,689 B，sha256 6abb5c15…cc676；随本批含第十一轮连线闪烁修复）。按分发拍板：只交版本号+更新要点，各机交付用户自理，不做全网通知。
