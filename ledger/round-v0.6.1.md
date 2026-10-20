# round v0.6.1（2026-10-20 编排：B/agentlink，Windows 环境）

## 事由

用户连续报两件事：

1. **重启 AgentLink 后 node_id 变了** —— 同一台机器在 Mesh 里被当成新节点；
2. **`agentlink.json` 放在应用目录里**，升级换目录/换二进制会被覆盖 ——
   「接收到的文件升级不动，配置也不应该动」。

## 修复一：身份铁律（node_id 永不变）

新增**机器身份锚点** `<data_dir>/node-identity.json`（原子写）：

- 启动时 `reconcile_identity()`：锚点存在即以锚点为准，配置里 node_id 漂移就地修回并落盘；
- `generate_preserving_identity()`：配置文件完全不存在时也沿用锚点（换目录不换身份）；
- `Config::load()`：node_id 缺失/为空 → 先从锚点恢复再校验；
- `Config::save()`：改 temp+fsync+rename 原子写，进程被杀不留半截 JSON。

**补救入口** `agentlink identity`：

- `identity` —— 查看当前 node_id / 锚点路径 / DRIFT 状态；
- `identity --set <旧ID>` —— 显式改回旧身份（同时覆盖配置 + 锚点，改完需重启）；
- `identity --recover` —— 从 backups/、upgrade-ledger.jsonl、出站历史、agents.json
  自动找回上一个身份（列候选，采用第一个）。

## 修复二：配置位置隔离（升级不覆盖配置）

配置搬到**用户配置目录**，不再跟着发行目录走：

| 平台 | 稳定位置 |
|------|----------|
| Windows | `%LOCALAPPDATA%\AgentLink\agentlink.json` |
| Linux | `$XDG_CONFIG_HOME/agentlink/agentlink.json`（或 `~/.config/agentlink/agentlink.json`） |

`resolve_config_path()` 解析顺序：

1. 显式 `--config <path>` → 完全按给的走，**不迁移、不解释**；
2. `AGENTLINK_CONFIG` 环境变量 → 同上；
3. 稳定位置已有配置 → 用它；
4. 就地旧文件（当前目录 / exe 同目录的 `agentlink.json`）存在 →
   **自动复制**到稳定位置，之后只用稳定位置那份；
5. 都没有 → 稳定位置（新配置写这里）。

迁移细节（`migrate_legacy_config`）：node_id / local_api.token / cluster_token /
端口 / 模式全部原样保留；**相对 `data_dir` / `files.recv_dir` 锚回旧目录**
（否则配置换位置会让数据"搬家"）；旧文件保留在原地。

**新命令** `agentlink config`：`config`（看路径）/ `config --migrate`（手动迁移，幂等）/
`config --path`（只输出一行路径，机器可读）。

**配套改动**：
- `启动.bat`：改问二进制要配置路径（`for /f` 读 `config --path`），不再依赖本目录的
  agentlink.json；`run` 不带 `--config`，走内置解析。
- 自启 VBS：exe 失效时的回落逻辑修正 —— 原来从 **cfg 目录**找 exe（现在 exe 与 cfg
  已不同目录），改为从 **exe 自己的目录**找。
- systemd 单元：仍用 `/etc/agentlink/agentlink.json`（发行目录外的固定位置，符合同一原则）。
- wake-hook `daemon.py` / `hermes_bridge.py` / `bridge.sh`：配置发现改为优先稳定位置。
- `dist/upgrade_runner.py`：备份/回滚改用真实的 `--config` 路径（`config.json`），
  旧布局的 `agentlink.json` 仍兼容。

## 修复三：Windows 目标编译失败（附带）

`src/load.rs` 的 wmic/PowerShell 探测把 `probe_shell()` 的 `Option<u32>` 当 `Result` 用
（`if let Ok(pct) = ...`），导致 v0.6 起 **Windows 交叉编译直接失败**——
这正是 Windows 节点一直停留在 v0.5.10 的原因。已修（`if let Some(pct)`），
并给 Linux 专用函数加 `#[cfg]` 门消除 dead_code 警告。

## 验证

- `cargo test --bin agentlink`：199/199 PASS
- `cargo fmt --check`：OK
- `tests/node_identity_smoke.py`：12/12 PASS
- `tests/config_location_smoke.py`：10/10 PASS
- **生产节点实跑演练**（Windows，v0.6.1，192.168.1.50 / `4f793d7cd6fc`）：

| 场景 | 结果 |
|------|------|
| 首次运行 | 自动把 `E:\Tools\AgentLink-v0.4.4\agentlink.json` 迁到 `C:\Users\yxf\AppData\Local\AgentLink\agentlink.json`，node_id/name/tags/data_dir 全保留 |
| 配置漂移成 `deadbeef1234` | `identity` 正确报 `status: DRIFT`；`--recover` 自动找回 `4f793d7cd6fc` |
| 配置被重建 | 重启后 health 仍 `4f793d7cd6fc`，配置自动修回 |
| 配置被删除 | `run --create-if-missing` 重建的配置 node_id 仍 `4f793d7cd6fc` |
| **发行目录配置被覆盖成 `999999999999`/NEW-RELEASE** | **重启后仍 `node_id=4f793d7cd6fc` name=B**（用稳定位置那份） |
| **发行目录配置文件整个移走** | **重启后仍 `4f793d7cd6fc` / name=B / tags=[main]** |
| 新的 `启动.bat` | 正常启动；自启 VBS 的 `cfg` 已指向 `%LOCALAPPDATA%\AgentLink\agentlink.json` |

## 产物

- `dist/AgentLink-v0.6.1.zip`（9 文件，win/linux 双二进制）
  sha256 = `733d5bbe0427fa8bf350c4f74c535d480cd373bd277f2c03995824582db122db`（6,520,000 B）
- `dist/pkg0621/`（平铺九件套）、`dist/agentlink.exe`、`dist/agentlink-linux`
- 版本号 0.6.0 → 0.6.1

## 现场状态

生产节点就地升级为 v0.6.1 并在役：
`node_id=4f793d7cd6fc` / name=B / tags=[main] / version=0.6.1；
配置已迁到 `C:\Users\yxf\AppData\Local\AgentLink\agentlink.json`；
身份锚点 `...\data\node-identity.json` 已写；
自启 VBS 指向稳定配置路径。旧二进制备份 `agentlink.exe.bak-20261020`。
发行目录里的 `agentlink.json` 保留作备份，但**不再被使用**（可自行删除）。

对端 .7/.10/.60 本轮 8765 端口均不可达（节点未运行），peers 为空属正常，非本次改动所致。
