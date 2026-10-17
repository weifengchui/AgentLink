# v0.5.6 滚动升级轮（2026-10-17 晚）

- 内容：Web 控制台第十三/十四轮——星图消息弹窗瘦身 + 星图磁避让；协议兼容 0.5.x，纯观测页改动，桥接脚本无需更新。另：星图 3D 可行性调研报告交付（条件推荐：先改良 2D，≤3 人日原型过可读性门槛再谈产品化）。
- 发布：代码 7c6e784 + dist e11d8d8 + tag v0.5.6；zip sha256 bc1e970ce7bbe9f0be3541a8d7320d518ecd99beac5cc70d0bdfe1e9402e92db（2,995,049 B，平铺九件套，与 0.5.5 同构）。
- 验收：用户 20:37 预览热换件实机拖拽、20:42 确认放行；node --check 绿（源与 served /v1/starmap 双验）；构建产物 strings 标记齐全（AVOT/avTick/AGSZ/v-hmeta）；clippy -D warnings 全绿。
- 冒烟：Linux 件隔离端口 18795，health version=0.5.6，minted node_id，served 星图页标记命中，验后杀进程。
- 发令：20:51 一轮批量 send-file+notice+instructions+go 至 A(e5827032190c)/C(f013cdcb7c91)/D(f5424c2f0a76)，executor=agentlink 会话执行体，免 ready 直执；12 帧回读 content 全非空。
- done：D 20:53:17（msg 899bb8768ea7…，runner elapsed 2054ms，备份 v0.5.5_20261017-205227）、C 20:53:23（msg 180f1863394b…，elapsed 2031ms，备份 v0.5.5_20261017-205235）。A 未回 done（执行体回报缺失），以编排侧终态为准：health=0.5.6、node_id=e5827032190c 不变、join 闪烁 20:56:37 捕获、fleet/agents 在线。
- 补发 done：三机各一条 upgrade_done_ack（reply_to 各自 done；A 无 done 可挂，正文自带复核依据）——A 6e908b1923f54304b0b970c5f32554d1 / C 8866de04a26f4d47854fa4835ca803d3 / D 489f8851d4c942069361e00ab6215e18，回读非空。
- 本机 B（4f793d7cd6fc）：in-progress.json 落盘 → runner 直执 0.5.5→0.5.6，elapsed 2004ms，备份 backups/v0.5.5_20261017-205652，health 终核 version=0.5.6 + node_id 一致 + name=B；autostart 用盘上新 exe --force 重注册 rc=0；桥 8647 ws_connected=true（20:56:56 自动重连注册 resp_ok）；in-progress 已删。
- 重连闪烁：A/C/D 均捕获 leave→join 配对，join 带新名（node_id 匹配成功，无 <node_id> 回落）。
- 终态：fleet 全机 0.5.6。台账四机 done 行 + upgrade-ledger.jsonl 落账（ledger 00aa6d8）。
