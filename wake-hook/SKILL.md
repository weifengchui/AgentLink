# AgentLink 消息唤醒钩子 · 安装技能（AI 可执行）

人类读者请先看同目录 README.md。本文件是给 AI 的操作手册，含决策树与故障码。

## 组件与机制

- `hermes_bridge.py` — 桥：WS 订阅本地 AgentLink 节点收信 → POST hermes webhook 触发唤醒；`BRIDGE_AGENT` 指定订阅 client；每 agent 单实例（pid 文件）。状态口 127.0.0.1:8643/status。配置 `wake.json`：`{"url","secret","agent","skip_topics"}`。
- `wake_hook.py` — 钩子（`--check <topic> <from> <content_json>`），桥收到消息后调用；退出码：0 放行 / 2 非目标 / 3 skip_topic / 4 探测·无路由。钩子缺失=放行（只落 inbox）。
- `install-all.sh [profile] [client]` — 一键装：探测可用 python（自带 venv→常见 venv→python3+websocket，都没有才 pip --user/venv 兜底）→ webhook 平台+gateway 重启 → webhook subscribe → wake.json → 起桥 → 自检 POST。**幂等，可重复跑。**
- `bridge.sh` [start|stop|status|logs|agent] — 手动控制（脚本失败时才用）。
- 依赖链：桥→webhook(HMAC)→profile gateway 唤醒会话→`hermes tools` 里的 AgentLink 技能→会话真实收信。缺一环表现为"消息只进 inbox、AI 不动"。

## 决策树

1. 机器有 hermes + AgentLink 节点(8765)？无 → 先按 bundle 部署节点（见 bundle 根 ONBOARDING）。
2. `bash install-all.sh <profile> [agent名]` → "完成 ✅" → 跳 5；中途失败 → 按输出排障后**重跑（安全）**。
3. 常见失败：
   - `webhook :8644 没起来` → 端口被占 `PORT=8645 bash install-all.sh <profile>`
   - `找不到带 websocket-client 的 python` → 装 `python3-websocket`（或 `sudo apt install -y python3-venv` 后重跑）
   - 自检 401 → 路由 secret 被重建，重跑脚本
   - profile 名不存在 → 先 `hermes profiles list`
4. 已有桥要改目标/换 agent 名：`bridge.sh agent <新client>`。
5. 注册 agent：让目标 profile 的 Hermes 会话读 bundle 根 `SKILL.md`（agentlink-agent-registration 技能）自助注册；桥重连时内置 register_all 自动补登记。client 名须与 wake.json 的 agent 一致。
6. 验证端到端：从一台已接入机器发 probe（见下）→ 目标机 `ls ~/.hermes/profiles/<profile>/sessions/ -lt` 几分钟内出活跃会话；同时目标机 inbox 落文件。
7. 仍不通：`tail -30 logs/bridge.out`；日志里 `wake -> 401` 走 3-401；`ws_connected:false` 查节点 8765；`status=started` 但无会话 → 查 profile gateway 与 `hermes -p <p> logs`。

probe 命令模板（发方机器）：

    curl -s --noproxy '*' -X POST http://127.0.0.1:8765/v1/send \
      -H "Authorization: Bearer <本方agent token>" -H 'Content-Type: application/json' \
      -d '{"topic":"bus.chat","to":"<目标agent_id>","to_agent":"<目标client名>","content":{"type":"text","text":"probe"},"from":"<发方agent_id>","from_agent":"<发方client>"}'

## 安全边界

- webhook 只绑 127.0.0.1（桥在本机回环触发）；**别手动改 host 成 0.0.0.0**。
- 触发 prompt 只含元数据+文本预览，不含文件路径等敏感注入面。
- 唤醒会话有真实工具权限——profile 权限配置按需收紧。

## 部署实例（网格现状）

| 机器 | profile | 端口 | 状态 |
|---|---|---|---|
| 50 (本机) | study | 8644 | ✅ 端到端通 |
| 50 (本机) | agentlink | 8646 | ✅ 专属链路（桥状态口 8647，.fleet-agentlink） |
| 7  (192.168.1.7) | fanbot | 8644 | 桥收信 inbox 通；唤醒钩子未装（待跑 install-all.sh fanbot） |
| 10 (192.168.1.10) | study | 8644 | ✅ 2026-10-13 经 install-all.sh 装好，agent study 在线 |
