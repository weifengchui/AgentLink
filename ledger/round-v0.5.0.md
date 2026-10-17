# round-v0.5.0 发版编排记录（2026-10-17）

- 包：AgentLink-v0.5.0.zip sha256=344a726b876346992f82cad80cfefa830e94f93ce8bdb0a57cb22473c4387b41（全程未重打）
- git：main=e72ae37，tag v0.5.0 已 push

## 事故与更正（重要）
08:10:12–08:15:36 第一轮 notice/instr/go 全部为**空消息**：编排方把正文放 /v1/send 顶层 content 字段（SendRequest 无此字段，serde 静默丢弃，回执仍 ok）。
08:18:00 重发有效 notice（作废旧 msg：57492ac6/c9554cd2/4d1303b2、aa89385f/a2f4f919/16a2b336、go 8d82176a/36cc2282/21f63e7b，均空帧未触发任何执行，.7 会话亦独立证实 payload={}）。
08:23:52 有效 go 发出。教训已入 skill agentlink-rolling-upgrade（发送后必须回读 history 断言 payload.content 非空）。

## 各机状态
| 节点 | old→new | notice(有效) | go(有效) | done 回执 | health 现值 |
|---|---|---|---|---|---|
| e5827032190c .7 (A) | 0.4.12→**0.5.0** ✓ | 78584361460242f1a96ee85c23dae86f | f80c0692a87642ce9b46fe59286be25e | 00:28:14/00:30:37Z 双份，node_id 不变✓，sha 实算全串✓；收尾问答(39f34a98)已答：运行 exe=E:\软件工具\AgentLink-v0.4.11-bundle\agentlink.exe（runner 升后正本），自启 AgentLink.vbs mtime 08:29 指向 bundle exe+cfg 带 --config，无旧路径抢占 | 0.5.0 ✓ |
| f5424c2f0a76 .60 (D) | 0.4.19→**0.5.0** ✓ | fe2e3ee1c9f64ad689ff5291accb370e | 7181c1be8e9b4f94b8eaf19bd12fa750 | ready 00:46:43（迟到 23min，唤醒完成协议慢）+ done 00:47:31：node_id 不变✓ sha 全串✓ 2975492B 备份 backups/v0.4.19_20261017-08464 | 0.5.0 ✓ |
| f013cdcb7c91 .10 (C) | 0.4.18→**0.5.0** ✓ | a03e7d33e1af43ecbe1b2833edd03571 | d433a4536298458da67376a52399dea4 | ready 01:06:49（机主现场明示授权，会话系被人工捅醒——桥驻留问题另账）；health 09:07 实证翻转，node_id 不变 | 0.5.0 ✓ |
| 4f793d7cd6fc 本机 B | 0.4.19→**0.5.0** ✓ | — | — | runner 直跑 2017ms，备份 backups/v0.4.19_20261017-090801，health 0.5.0/node_id 不变/features 三件✓ | 0.5.0 ✓ |

## 收口（09:08）
全网四机 0.5.0，node_id 全部不变，fleet agentlink 四桥 online=True。台账四行+本机行已写 ledger/*.jsonl。
遗留：.10 桥非常驻（本次靠机主人工唤醒）；.60 会话消费机制已自整改（wake 会话 toolsets 无 terminal 根因，NOTE 01:05:56 收讫）；feat/v0.5.1-merge 全绿待并回 main 发 0.5.1。

## 各机状态
