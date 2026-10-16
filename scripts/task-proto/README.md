# task-proto — 任务协议 v1 参考实现

对 `docs/task-protocol.md` 的可执行参考：编排方、执行方、冒烟场景。纯 payload 层约定，**任何 0.4.x+ 总线均可跑**，无需内核改动。

## 快速开始（仅限隔离测试总线）

```bash
# 1. 测试总线（端口必须与生产 8765 隔离，防串入生产 mesh——铁律）
agentlink init --config test.json   # 或手写：listen_port=18765, node_id 必填, local_api.token 自定
agentlink --config test.json run --no-open

# 2. 执行者（agent=worker，capabilities 见 executor.py）
venv-python executor.py --base http://127.0.0.1:18765 --token <T>

# 3. 冒烟全场景（happy/decline/幂等/cancel/drift/金丝雀/兼容层）
venv-python smoke.py evidence.json

# 手工单发
venv-python taskctl.py submit --type rollout.node_version --params '{"target_version":"0.5.0"}'
```

## 文件

| 文件 | 角色 |
|---|---|
| tasklib.py | 总线客户端封装：REST(send/register/history)、WS URL、spec/msg 构造、回报哨兵（blob 全文匹配） |
| executor.py | 参考执行者：WS 收 spec→状态机回报；能力门/SHA 门/幂等/取消/maintain 心跳/旧 upgrade_notice 兼容 |
| taskctl.py | 参考编排者：submit+wait 收敛；drift 检测基于"新心跳出现时刻"（勿每次轮询刷新——必测坑）；金丝雀闸口 |
| smoke.py | 9 场景端到端验证，退出码即结论 |

## 已知坑（WSL 实测）

- 客户端接入是 `GET /v1/ws?token=&client=`（节点间 WS hello 握手是另一条通道，勿混）。
- `POST /v1/agents` 注册 agent（agent_id/name/capabilities），REST send 带 `from_agent` 才符合身份铁律。
- history 帧 `ts` 是 ISO 字符串（非 epoch）；解析时间直接切字符串，别拿 float 比大小。
- `agentlink --config <file> run`（不是 `-c`）；config 必填 `node_id`。
- 桥/脚本一律用带 websocket-client 的 venv python（如 LiteGuard venv），系统 python3 缺依赖。
