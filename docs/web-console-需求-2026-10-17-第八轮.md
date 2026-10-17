# Web 控制台需求台账 — 2026-10-17 第八轮

来源：用户 2026-10-17 反馈（2 项：星图弹窗胶囊统一 + 对话记录胶囊缺失 bug）。

## R1 【优化】星图消息弹窗的路由信息改为双层胶囊（与对话记录同款）
- 现象：消息弹窗（viewer）头部显示纯文本 `【agentlink@A -> agentlink@B】`、meta 行显示纯文本 `agentlink@A → agentlink@B`，与对话记录里 `.idc/.idci` 双层胶囊风格不一致。
- 修法：`web/starmap.html` 新增 `.idc/.idci/.idn` CSS 与 `onCol()/capHtml()`（配色函数与 `src/api.rs` 逐字同源，星图已有 nodeCol/agCol 直接复用）；`v-hd`、`v-route` 从 `textContent` 改 `innerHTML=capHtml(f) → capHtml(to)`。节点级端点（`@node`）与对话记录一致走 `●+节点名` 回退。顺带清死代码 `hdOf()` 与 `M()` 的 `hd:` 字段（从未被读取）。
- 验收：弹窗内两端各渲染 1 个 `.idc`；外层背景=节点深档 `hsl(H,46%,30%)`、内胶囊=agent 派生色、`justify-content:flex-end`（左弧 ≈14px）；几何断言 outer = inner + 14 + padding；截图目检与对话记录同款。
- 状态：☑ 已做（18791 隔离节点实机：弹窗 `v-hd`/`v-route` 两端渲染 `.idc/.idci`，几何断言 outer70=inner54+左弧14+右衬2、`justify-content:flex-end`、外壳=节点深档 hsl；第三跳 `@node` 端走 `●+节点名` 回退、`.idc` 计数=1；vision 目检与对话记录同款、无截断错位；`node --check` 过，死代码 hdOf/aKey2disp/hd 字段已清）。

## R2 【bug】对话记录部分行缺发送方 agent 胶囊（如 `⬆ 发出 · ●B → studyA`）
- 现象：10-17 10:40:02「领取结果/low_stock 预警」类定时消息，行首只有 `●B`，没有 agent 胶囊。
- 根因（已定位，属**发送端**数据缺失，非渲染层）：生产 `/v1/history/search` 实查——所有缺失行 `from_agent:null`，全部是 `:XX:01` 定时发出的「领取结果」「low_stock ESC 预警」类消息；发送链是 B1 的 `al_send.py`（直连 POST `/v1/send`，信封不带 `from_agent`）与 `b1_watchdog.py`，而经桥 `hermes_bridge.py`/`al_bridge.py`（有 `setdefault("from_agent", AGENT_ID)`）或手工发出的同类消息胶囊齐全。前端 `chip(n,a)` 在 `a` 为空时按数据渲染 `●节点名` 回退——行为正确，无数据可渲染。
- 修法：应改发送脚本补 `from_agent`（study 舰队侧）。**用户拍板：本项搁置，归另一会话处理；本会话只动 AgentLink /web。**
- 验收（移交口径）：修复后新发出的定时消息在对话记录行首出现 study 胶囊；历史旧行保持 `●B` 回退属正常。
- 状态：⏸ 已定位、搁置（发送端，study 舰队会话负责）
