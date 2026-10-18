# round v0.5.10（2026-10-18 15:17 编排：B/agentlink）

- 包：dist/AgentLink-v0.5.10.zip（4,741,445B）sha256=9828abccabfe9fe4a6f86e7149c76d0df4924c31cb31b4c33cbc26c6f5d883ab
- 内容：M1 任务引擎休眠骨架（taskstore/taskengine/license/config、/v1/tasks 写面 REST+license 门控；mesh.enabled=false 默认态行为=0.5.9 逐字符钉死）
- 发布：tag v0.5.10 = d418b40（GitHub ls-remote 已核 main=tag=d418b40）

## 分发明细
- A e5827032190c（升前 0.5.9）：file_id 9466eab846834068b5d59c144aa0d993，notice 3f24a61b32f64496ab15b9f3f3a6fe7d，instr e42b545d586f4cfa8413fd46166b5fa0，go 4350f84a08474cc0b430dcef31133a2a
- C f013cdcb7c91（升前 0.5.9）：file_id fb5680406eb64f45b91dd9fd42619d93，notice ecd1781bcf3f4e80a91266474d738638，instr 8a5b6cf9954c4f39aa2948ac46e5ca3c，go 0660db5736f043488fe142c03213119a
- D f5424c2f0a76（升前 0.5.9）：**skipped_offline**——health 可达但 /v1/fleet/agents 该机无任何 agentlink 注册（升前门 + go 后复查两次均空）。按身份铁律不顶替身份推送；待该机 agentlink 会话/桥上线后补推。

## 终态（编排侧 health 实证 2026-10-18 15:17）
- A version=0.5.10 node_id=e5827032190c ok ✓
- C version=0.5.10 node_id=f013cdcb7c91 ok ✓
- B 本机自升 runner done，elapsed 3006ms，备份 backups/v0.5.9_20261018-151641，health 0.5.10/4f793d7cd6fc，桥 8647 ws_connected=true ✓
- D 仍 0.5.9（本轮跳过，未动现网）

## done 回执
A/C 为会话执行体，截至收口未见 upgrade_done 回报（编排侧 health 三断言全过，按手册 §5.2b 收口；迟到 done 由在场会话补 ack 即终结）。
