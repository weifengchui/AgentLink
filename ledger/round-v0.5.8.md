# 滚动升级 round v0.5.8（第十五轮收官：thread 消息组 + 星图连线修复）

- 日期：2026-10-18 10:57–11:01 CST（窗口内）
- 版本：0.5.7 -> 0.5.8；包 AgentLink-v0.5.8.zip（4,591,581B）sha256=128077c6c1dda181ef6254b4d5ae1e3b4120f746a84aa36dcf2e5c985061ee4c
- 发版 commit：8838144（bump）+ 74f6a43（dist），tag v0.5.8（ls-remote 实证）
- 升前快照：A/C/D/B 全部 0.5.7，三机 agentlink.online=true（在线门通过）
- 执行体：A=liteguard daemon 在线（会话亦可）；C/D=agentlink 会话（不等 ready 直接 go）
- 全程帧带 thread=upgrade-v0.5.8-<机名>（thread 字段首战实跑）；换装前本机预览实证 thread 持久化（smoke msg c287de4ab3194dc9953ddc17fcfbe914）

## 分发明细（msg_id）
| 机 | node_id | notice | instr | go | 结果 |
|---|---|---|---|---|---|
| A | e5827032190c | 65804f1368bb440485df8c65591b7e5e | 3e85328e7b8741b89c9555ca046344e5 | e5ec1f25c4ca41aa96e51da881d26c68 | health 核销 0.5.8 @11:01:26（done 未报，编排实证收口）|
| C | f013cdcb7c91 | a92f3f0542694cf597e72600f7516d3c | dcd8d21fbe7f441ea9a1c8d9afe8f665 | 777ddc5bdf084f4b8f8931b775819aa8 | done 22539229d82646afa09bc32920f664e8，ack acbd6e7bfb45448cbd32e6a3f6081bf3 终结 |
| D | f5424c2f0a76 | 794ba2331320454cb4a950dc9f724ad7 | a6dedc2b31df4943bc6bc6473120eadb | cac228da1e8a4df294302fdb36fba052 | done 092ee2127a6d4f7587b6fb2d3659ac01，ack 12b91c5b50ea4761a2f6fbcf9b510329 终结 |

- C 记账说明：本轮三件由并行唤醒会话抢跑处置，runner 单次执行（ledger done trigger=msg，10:59:54，elapsed_ms=2047），信封合并记账——同 sha 重复 go 幂等约定生效，无双重执行。
- 12 帧回读 history 断言 payload.content 全非空。send-file 后 active_transfers 归零门全过。
- 本机 B：runner 直跑（§6），2004ms，备份 backups/v0.5.7_20261018-105706；autostart --force 重注册；桥 8647 ws_connected=true。
- 上一会话交接勘误：所谓"v0.5.8 tag=6c36b58 已推送"不存在（本地/远端/tag 均无），本轮为真实首次发版。

## 终态
四机全部 0.5.8，node_id 全部不变，fleet 10/10 agent 在线；三机换装全部在 3 分钟内完成（10:59:18 D / 10:59:54 C / 11:01:26 A），全程处于 08:00-22:00 窗口内。
