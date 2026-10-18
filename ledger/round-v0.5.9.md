# 滚动升级 round v0.5.9（星图截图/录屏所见即所得）

- 日期：2026-10-18 13:13–13:18 CST（窗口内）
- 版本：0.5.8 -> 0.5.9；包 AgentLink-v0.5.9.zip（4,595,548B）sha256=274a5550f1d34a1eec8cd5497f6498f780eff62e66af0b44ab48540c879921fd
- 发版 commit：582786b（功能+台账+R16 冒烟）+ 1837a13（dist），tag v0.5.9=1837a13（ls-remote 实证）
- push 备注：GitHub 443 直连超时，走 Shadowsocks SOCKS5 127.0.0.1:1080（`no_proxy= git -c http.proxy=socks5h://127.0.0.1:1080 push`）成功；推送前远端多一条他机 MEMORY.md mesh-sync 提交（28b7dcb），本地两笔 rebase 其上，tag 随之 -f 移位强推
- 升前快照：A/C/D 全部 0.5.8、node_id 匹配、agentlink.online=true 全过在线门
- 执行体：A/C/D 均为 agentlink 会话（不等 ready 直接 go）

## 分发明细（msg_id）
| 机 | node_id | file_id | notice | instr | go | 结果 |
|---|---|---|---|---|---|---|
| A | e5827032190c | 7aa607886ca2466fa24608f69ed66da0 | 0eade8cb4aca4404942c4565d56b78bf | ace50036f0cc4b41a5dae9812b3f309c | a01ec290c18f4ae489d4acbfd15f5aff | done df68aa9164484da786534ef5587c364a（elapsed 5018ms），ack 1aba898c579449d5b515da2d10a8888f |
| C | f013cdcb7c91 | b4b32a017d0e42e5a1d4f9e23523dde5 | 3ad21a3f64434e84a4a9f062bcda5919 | 408d44edbb0f4ec9b2b718f5afe57a39 | e490ed090cf14d8bb16f2290e7a8c0f8 | done 66dd97d081504de3bb089d097850c895（elapsed 2050ms），ack f11d263f7bac410abad6ede104283354 |
| D | f5424c2f0a76 | 14c026e6b4f84e2f93d3c8e605ece934 | f9283c3fdb8649b6bacf47ffa994de85 | 9db1a8c72079425283f552cca05ab72e | 19871e566cb24ae29340f2c2915e32b2 | done 08e033b7bab5474f939a3b5660b5767e（elapsed 4014ms，备份 v0.5.8_20261018-131419），ack be20fad4ca5445829e05f13a39333ada |

- 12 帧单核连发；send-file 后 active_transfers 归零门全过；回读断言 content 非空（首轮 False 系编排侧断言 bug：history ts 为 UTC 串、误用 CST 门槛比较——正文实非空，已按 UTC 门槛复核）。
- 全程帧带 thread=upgrade-v0.5.9-<机名>；A/C 的 done 迟到约 1-2 分钟到达，均已补 ack 终结。

## 本机 B
runner 直跑（§6），2004ms，备份 backups/v0.5.8_20261018-131737；autostart --force 重注册；health node_id=4f793d7cd6fc 不变；桥 8647 ws_connected=true。

## 终态
四机全部 0.5.9，node_id 全部不变，fleet agentlink 全在线；三机换装 13:14:28（D）/13:15:33（C）/13:16:21（A）全部在 3 分钟内完成，全程 08:00-22:00 窗口内。done 回执 3/3 齐全（本fleet 历史首次全收）。
