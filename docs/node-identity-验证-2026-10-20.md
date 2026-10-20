# 机器身份（node_id）稳定性 —— 修复与验证 2026-10-20

## 问题

重启 AgentLink 后本机 node_id 变了（同一台机器在 Mesh 里被当成新节点）。
历史根因有两类：

1. 从新发行目录/新 `agentlink.json` 启动，配置不存在 → 重新 `init` 生成随机 node_id；
2. 配置文件被 `init --force` 覆盖、或半截写入/丢失 → 旧的身份随配置一起没了
   （2026-10-13 `.7` 号机事故同源）。

## 修复（身份铁律：node_id 一旦生成，永不变）
新增 **机器身份锚点**：`<data_dir>/node-identity.json`

```json
{ "v": 1, "node_id": "4f793d7cd6fc" }
```

- `Config::pin_identity()`：首次启动/首次 init 时把 node_id 写入锚点；
- `Config::pinned_node_id()`：读取锚点；
- 启动时 `reconcile_identity()`：锚点存在就以锚点为准。配置里的 node_id
  若漂移（重建配置、换目录、`init --force`、损坏），就地修回并重新落盘；
- `generate_preserving_identity()`：即使配置文件完全不存在，只要锚点还在，
  新配置也沿用原 node_id（换发行目录重新 init 不会换身份）；
- `Config::load()`：配置缺 `node_id` 字段/为空时先从锚点恢复，再校验；
- `Config::save()` 改为**原子写**（唯一下载临时文件 + fsync + rename），
  避免进程被杀时留下半截 JSON、下次启动被迫换身份。

锚点放在 `data_dir`（Windows 默认 `%LOCALAPPDATA%\AgentLink\data`，
Linux 默认 `~/.local/share/agentlink`）：跨发行目录稳定，且与
journal / agents.json / mtls 证书同属机器数据，升级不迁移。

## 已经漂移了怎么补救

如果升级前已经发生过重启换身份（配置和锚点都变成了新 id），用新命令改回：

```bash
# 查看当前身份/锚点状态
agentlink --config agentlink.json identity

# 直接指定旧 node_id（同时修配置 + 锚点，之后重启生效）
agentlink --config agentlink.json identity --set 4f793d7cd6fc

# 不确定旧 id：从 backups/、upgrade-ledger.jsonl、出站历史、agents.json
# 自动找候选并恢复（优先最新备份；会列出所有候选）
agentlink --config agentlink.json identity --recover
```

`--set` / `--recover` 都会**覆盖**身份锚点——这是唯一允许覆盖锚点的
入口，否则下次启动会又被错误锚点搬回去。改完必须重启节点进程。

## 验证

```bash
cargo test --bin agentlink identity_tests   # 5/5 PASS
AGENTLINK_BIN=target/debug/agentlink python3 tests/node_identity_smoke.py  # 12/12 PASS
```

冒烟覆盖：

1. 首次启动 node_id 正确，锚点落盘；
2. 把配置里的 node_id 改成随机新值后重启 → health 仍是原 node_id，
   配置文件也被修回；
3. 配置和锚点都已是错误身份时，`identity --set <旧ID>` 能改回；
4. `identity --recover` 能从 `backups/` 自动找到旧身份并改回；
5. 换两个不同目录分别 `init --force`（共享 `XDG_DATA_HOME`）→ 两次
   生成的 node_id 完全一致，共享锚点记录同一身份。

`cargo test --bin agentlink` 全量 195 项通过。
