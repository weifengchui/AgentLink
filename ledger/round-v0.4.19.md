# v0.4.19 滚动升级轮（2026-10-16）

- 发布：代码 f446468 + tag v0.4.19，dist 0ab0c0c；zip sha256 50f3b4da707bf38aa6b182556af0f5b426e39d4ecd8b5ee60fdd8b8bfead6f4f（2,334,876 B，平铺九件套）
- 冒烟：Linux 二进制隔离端口 18775 起动，health=0.4.19，控制台/星图 R2–R7 标记命中
- 发令：08:51:59Z send-file+notice+instructions 至 .7(e5827032190c, 0.4.12→跳升)/.10(f013cdcb7c91, 0.4.18)/.60(f5424c2f0a76, 0.4.14)，executor=agentlink 会话执行
- ready：.10（08:52:28Z，sha 逐字核）、.60（09:00:52Z，sha 逐字核）；.7 未报，用户令"不等回执直接推进"→ 09:02:11Z 对其发 go（含"免 ready 直执"注记）
- go：.10 08:56Z（msg f9bc3b97）、.60 08:56Z（a6ed1299）、.7 09:02Z（22ff1672）；另发三条去重护栏（同 sha 多 go 只跑一次 runner）
- 本机 50：runner 直执完成 0.4.18→0.4.19，elapsed 2004ms，node_id 4f793d7cd6fc 不变，备份 backups/v0.4.18_20261016-170240，桥 8647 ws_connected=true
- done：.7 09:16:46Z（msg 2cf45661，reply_to=go 22ff1672）：0.4.12→0.4.19，elapsed 4008ms，sha 全串匹配，node_id 不变，备份 v0.4.12_20261016-171520；编排侧 health 复核 0.4.19✓，已回 done_ack（9b12d9eb）。.7 自述 dedup 依据（7dc8321e 护栏）认可。
- 回落核查（.7 请求，health 现查）：50=0.4.19 无回落；.60=0.4.19（uptime≈65s）无回落；.10=0.4.18 未重启=done 未报非回落。.7 的 0.4.16→0.4.12 回落与其 10:18 重启吻合，疑自启 vbs 旧路径，已建议其人工 `autostart --force` 重注册。
- 遗留：仅剩 .10 done 未回报（go f9bc3b97 已送达，节点未重启，疑其 agentlink 会话未醒或未跑 runner，待观察/人工催）。
