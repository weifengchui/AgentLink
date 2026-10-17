# Web 控制台需求台账 — 2026-10-17 第十三轮

## R1 星图消息弹窗瘦身：删标题行、底部元信息并入路由行居右、删底部重复胶囊（10-17 用户原话："星图大屏里的消息弹窗里，最上面一行主题标题去掉，即我标注的1处；最下面的红框里的内容移到最上面的胶囊同一行的后面，居右排列，或者你有更好的设计也行；去掉最后一行胶囊"）
- 现状：弹窗右栏自上而下=话题标题 `#v-tp`（【a -> b】大字）→ 序列 `#v-seq`（2 / 2）→ 路由胶囊行 `#v-hd` → 正文 → 底部元信息行 `#v-meta`（`T-…s · 时间` + 「消息」类型胶囊 + **重复的** A→B 路由胶囊 `#v-route`）。路由与主题信息呈现两遍，标题与胶囊行内容也高度重合。
- 根因：历轮逐个加料（第八轮胶囊化、元信息行）无人统一收排布。
- 修法（仅 web/starmap.html）：
  1. 删 `#v-tp`（DOM + CSS + `drawViewer` 内赋值三处全清）；
  2. `#v-hd` 改 flex 行：左 `#v-caps`（=routeHtml 胶囊，原 `#v-hd` 内联内容改投此 span）+ 右 `#v-hmeta`（`margin-left:auto` 包 `#v-time`+`#v-kind`，等宽字体/11.5px 样式随迁）；
  3. 删 `#v-meta`/`#v-route` 整行（时间、类型已并入路由行，路由胶囊不再重复）。
- 验收：隔离节点实机——DOM 断言 `v-tp`/`v-meta`/`v-route` 均不存在；开弹窗后 `v-hd` 文本序=胶囊→时间→「消息」；几何断言 `#v-hmeta` 右缘距容器内边 12px、`#v-caps` 不与元信息重叠、元信息在「关闭」按钮下方不碰撞；vision 复核顶行只剩 2/2+关闭、正文下方无重复行、无重叠裁剪错位；服务端下发页脚本 node --check 全绿。
- 状态：☑ 完成（2026-10-17）。验收记录：隔离节点 r13test@18791（debug 二进制，node_id 7a4d6700ba41）alice/bob 双 agent 灌 2 条 reply_to 链消息，悬停 bob 胶囊→点卡组开弹窗——`tp:false, metaLeft:false, routeLeft:false`；`hd` 文本=`bob r13test→alice r13test T-27s · 19:25:10 消息`；几何 `{gap:12, capsOverlapsHm:false, hmBelowClose:true}`；vision 复核四项全过（无标题行/右侧时间+消息胶囊/正文下无重复/无重叠错位），截图存档 docs/mockups/starmap-viewer-r13.png；脚本 node --check 绿。

## 发布
- 待用户确认版本号（拟 v0.5.6，纯 Web 轮次）。
