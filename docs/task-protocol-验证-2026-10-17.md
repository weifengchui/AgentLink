# 任务协议 v1 验证记录（2026-10-17）

环境：隔离测试总线 TESTBUS（端口 18765，与生产 8765 物理隔离，discovery 关闭），节点内核 agentlink 0.4.19（Linux build），参考实现 `scripts/task-proto/`（Python，websocket via LiteGuard venv）。生产总线全程未触碰。

执行时刻：2026-10-17 00:3x–00:5x CST。命令：`venv-python smoke.py evidence.json`。

## 结论：9/9 场景通过

| # | 场景 | 断言 | 结果 |
|---|---|---|---|
| 1 | happy_path | spec→accepted→progress×3→succeeded 收敛 | PASS（task 5c8f63ba…） |
| 2 | capability_decline | requires=cap:node:admin，执行者缺能力→declined | PASS |
| 3 | sha_decline | expect≠actual→declined(sha_mismatch)（升级协议 SHA 硬条件泛化） | PASS |
| 4 | idempotent_double_send | 同 task_id+seq 双发：accepted 恰 1 条 + task_ack(dup-dropped) 1 条，仍 succeeded | PASS |
| 5 | cancel | 运行中 task_cancel→task_cancelled(带 stage) | PASS |
| 6 | maintain_drift | 心跳缺勤>3×interval，编排方判 drift（不伪造终态） | PASS |
| 7 | canary_gate | 批内首任务 declined→后续 blocked-canary | PASS |
| 8 | canary_release | 补发成功→闸口放行 | PASS |
| 9 | legacy_upgrade_compat | 旧 upgrade_notice→upgrade_ready+task_accepted 双轨回报 | PASS（trail 完整） |

## 实现期发现（已回写 README/坑节）

- drift 检测计时必须锚定"新心跳出现"，按轮询墙钟刷新会导致永远检不出（首轮 smoke 实错于此，修复后过）。
- history `ts` 为 ISO 字符串；帧级 msg_id 去重（LRU）之上仍需业务级 (task_id,seq) 幂等——两次 REST 发送产生不同 msg_id，总线不去重，执行者 SEEN 集合兜住（场景 4 验证双层设计必要）。
- 同节点 agent→agent 投递可直接用 `/v1/send`（to=本机 node_id），编排/执行可共总线共节点跑闭环——升级协议"编排会话↔远端 agentlink"形态的最小等价。

## 意义

v0.5 的协议契约（task_spec/task_status 状态机、幂等、reconcile、护栏、升级映射）在无内核改动前提下端到端成立；后续 `docs/v0.5-内核待发清单.md` 各项均为性能/治理增强，不改变本文验证过的业务语义。
