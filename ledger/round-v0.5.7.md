# 滚动升级 round v0.5.7（收官特性发版）

- 日期：2026-10-18 08:29–08:45 CST（窗口内）
- 版本：0.5.6 -> 0.5.7；包 AgentLink-v0.5.7.zip（4,585,814B）sha256=03eb01377ba11b2c3cfdbc9ab87e9c765ea736b0cc72942675f9979dc7311ac9
- 发版 commit：49e8a7c（bump）+ 80d37a4（dist），tag v0.5.7
- 升前快照：A/C/D/B 全部 0.5.6，三机 agentlink.online=true（在线门通过）
- 执行体：各机 agentlink 会话（executor=agentlink，不等 ready 直接 go）

## 分发明细（msg_id）
| 机 | node_id | notice | instr | go | 结果 |
|---|---|---|---|---|---|
| A | e5827032190c | ccea11b4283446d29d871e0ce3a7e42d | c6f9ca562fda4f72ac2a75825afc970a | 3a149f322d054a0eb3c59e9ecb1d00d2 | health 核销 0.5.7（done 未报）|
| C | f013cdcb7c91 | 532ae4a7afdf47b2a331e327ac7a7471 | d67820f9b21344a0bbf49948ff0290d5 | b56b72addf964cb5821d977e8299e423 | done×2，ack 终结 |
| D | f5424c2f0a76 | 3accc145662547daa906dbf88bdf4071 | b72c4bff885945db93b09d7dbc11052e | 2df3b53a3e8f431a9a0b5742d46b7e92 | done×3，ack 终结 |

- A 的 send-file 首轮断言解析 bug（状态行 HTTP/1.1 200）未重发文件帧，file_id=f713dab10c6d44049078ce9631cc8423 实际已送达。
- 12 帧回读 history 断言 payload.content 全非空。
- 本机 B：runner 直跑（§6），2004ms，备份 backups/v0.5.6_20261018-084130；autostart --force 重注册；桥 8647 重启后 ws_connected=true、agentlink 注册 ok。

## 终态
四机全部 0.5.7，node_id 全部不变，features 含 hello（收官特性上线）。
