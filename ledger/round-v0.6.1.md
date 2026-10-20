# round v0.6.1（2026-10-20 编排：B/agentlink，Windows 环境）

## 事由

用户报：**重启 AgentLink 后 node_id 变了**——同一台机器在 Mesh 里被当成新节点。
排查定位到两类根因：

1. 从新发行目录 / 新 `agentlink.json` 启动 → 配置不存在 → 重新 `init` 生成随机 node_id；
2. 配置被 `init --force` 覆盖、被删、或半截写入（旧 `Config::save()` 非原子写）→
   旧身份随配置一起丢失（同 2026-10-13 `.7` 号机事故同源）。

## 修复（身份铁律：node_id 一旦生成，永不变）

新增**机器身份锚点** `<data_dir>/node-identity.json`（原子写）：

- 启动时 `reconcile_identity()`：锚点存在即以锚点为准，配置里 node_id 漂移就地修回并落盘；
- `generate_preserving_identity()`：配置文件完全不存在时（换目录 / 重建）也沿用锚点；
- `Config::load()`：node_id 缺失/为空 → 先从锚点恢复再校验；
- `Config::save()`：改为 temp + fsync + rename（原子写），进程被杀不留半截 JSON。

**补救入口** `agentlink identity`（新增子命令）：

- `identity` —— 查看当前 node_id / 锚点路径 / DRIFT 状态；
- `identity --set <旧ID>` —— 显式改回旧身份（同时覆盖配置 + 锚点，改完需重启）；
- `identity --recover` —— 从 backups/、upgrade-ledger.jsonl、出站历史、agents.json
  自动找回上一个身份（列候选，采用第一个）。

## 附带修复：Windows 目标编译失败

`src/load.rs` 的 wmic/PowerShell 探测把 `probe_shell()` 的 `Option<u32>` 当 `Result` 用
（`if let Ok(pct) = ...`），导致 v0.6 起 **Windows 交叉编译直接失败**——
这正是 Windows 节点一直停留在 v0.5.10 的原因。已修（`if let Some(pct)`），
并给 Linux 专用函数加 `#[cfg]` 门消除 dead_code 警告。

## 验证

- `cargo test --bin agentlink`：195/195 PASS
- `cargo fmt --check`：OK
- `tests/node_identity_smoke.py`：12/12 PASS（配置漂移自修 / `--set` 改回 / `--recover` 从备份找回 /
  换目录重新 init 不换身份）
- **生产节点（192.168.1.50 / `4f793d7cd6fc`）实跑演练**（Windows，v0.6.1 新二进制）：
  1. `identity` → `status: unpinned`（首次）→ `--set` 写锚点 → `status: pinned`；
  2. 配置改成 `deadbeef1234` → `identity` 正确报 `status: DRIFT`；
  3. `identity --recover` → 自动找回 `4f793d7cd6fc`（来源：身份锚点），配置与锚点同步修回；
  4. 模拟「配置被重建」重启 → health 仍报 `4f793d7cd6fc`，配置文件被自动修回；
  5. 模拟「配置被删除」`run --create-if-missing` → 重建的配置 node_id 仍是 `4f793d7cd6fc`；
  6. 恢复原配置重启 → `name=B / tags=[main] / node_id=4f793d7cd6fc / version=0.6.1` 正常。

## 产物

- `dist/AgentLink-v0.6.1.zip`（9 文件，win/linux 双二进制）
  sha256 = `4c68d2981e0db89e7343f1fb9742e0cb7b8712ada402d1130654be950af18d5a`（6,512,647 B）
- `dist/pkg0621/`（平铺九件套）、`dist/agentlink.exe`、`dist/agentlink-linux`
- 版本号 0.6.0 → 0.6.1

## 现场状态

生产节点已就地升级为 v0.6.1 并在役：`node_id=4f793d7cd6fc`、name=B、tags=[main]、
锚点 `C:\Users\yxf\AppData\Local\AgentLink\data\node-identity.json` 已写。
旧二进制备份 `agentlink.exe.bak-20261020`。
对端 .7/.10/.60 本轮 8765 端口均不可达（节点未运行），peers 为空属正常，非本次改动所致。
