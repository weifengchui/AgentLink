# Web 控制台优化 · 第六轮（2026-10-16）

测试节点：/tmp/alweb1866（webtest, node_id=6739e5ffb995, 端口 18768, discovery 关闭）
测试地址：http://127.0.0.1:18768/web （本机打开 token 自动带入；局域网 /web 手动输 token）

## R6 对话记录搜索：发送方/接收方下拉与发送目标同口径
- 现象：原为手输框 + datalist（"发送方（agent/节点/IP）"），不好用，且无法精确到"某节点上的某 agent"。
- 方案：两个输入框改为 `<select>`，选项与发送消息目标下拉（to2）完全同源：
  - 第一组「节点（整机）」：本机 + 各 peers（离线标注）；
  - 第二组「Agent（节点 · agent）」：fleet 注册的全网 agent；
  - 前置「全部（不过滤）」空选项；2s 轮询下签名比对防重写、保留选中值（同 to2 机制）。
- 后端：`/v1/history/search` 新增 `from_node/from_agent/to_node/to_agent` 精确 pair 参数（store::HistSearch 扩展，大小写不敏感相等匹配）；旧 `from/to` 子串参数保留兼容。
- 验收 ☑：curl 与页面实测一致——发送方=本机 32 条、接收方=webtest·alpha-agent 精确 9 条；下拉选项与 to2 逐项对齐；重置正常。

## R7 节点页地址列一律 IP:端口
- 现象：本机行地址列写死"本机"，与其他行口径不一致。
- 方案：`/v1/status` 新增 `listen_addr`（local_ip_address::local_ip() + listen_port）；前端本机行 addr 改用 ST.listen_addr。名称列保留"本机"chip 标识不动。
- 验收 ☑：节点表本机行地址列显示 `192.168.1.50:18768`。

## R8 星图去掉字体大小滑动块
- 现象：底部控制栏"字体"滑条（fsr，0.85–1.6 缩放 --fs）用户不需要。
- 方案：删除滑条 DOM、`fsEl/applyFs` JS 与 `#fsr/#fsv` 死 CSS；启动时清除 localStorage "starfs" 残留并 removeProperty 内联 `--fs`，恒用 CSS 默认 `--fs:1`。
- 验收 ☑：JS 语法 node --check 通过；截图目测控制栏仅剩 窗口长度/时间区间 两滑块 + 5 按钮，胶囊/节点/HUD 渲染正常。

## 构建
- cargo fmt / clippy（release）无 error；Linux release 重建通过。
- 双平台构建、dist、bump、tag、push：待用户验收后执行。
