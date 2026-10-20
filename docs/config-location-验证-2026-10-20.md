# 配置位置隔离 —— 修复与验证 2026-10-20

## 问题

`agentlink.json` 放在**应用目录**（`E:\Tools\AgentLink-v0.4.4\`）里。发行目录是
「随版本换」的地方——升级换目录、换二进制、解压新包都可能覆盖它。

接收到的文件（`data/files/`）早就放在 `%LOCALAPPDATA%\AgentLink\data` 里，
升级不动；但配置还留在应用目录，两者标准不一致。配置本质也是"机器设定"，
同样不该被升级覆盖。

## 修复

### 1. 配置搬到用户配置目录

| 平台 | 稳定位置 |
|------|----------|
| Windows | `%LOCALAPPDATA%\AgentLink\agentlink.json` |
| Linux | `$XDG_CONFIG_HOME/agentlink/agentlink.json`（或 `~/.config/agentlink/agentlink.json`） |

`Config::stable_config_path()`（src/config.rs）给出该路径。

### 2. 一次性自动迁移

`resolve_config_path()`（src/main.rs）按此顺序解析：

1. 显式 `--config <path>` → 完全按给的走，**不迁移、不解释**；
2. `AGENTLINK_CONFIG` 环境变量 → 同上；
3. 稳定位置已有配置 → 用它；
4. 就地旧文件（当前目录 / exe 同目录的 `agentlink.json`）存在 →
   **自动复制**到稳定位置，之后只用稳定位置那份；
5. 都没有 → 稳定位置（新配置写这里）。

迁移细节（`migrate_legacy_config`）：
- node_id / local_api.token / cluster_token / 端口 / 模式等**全部原样保留**；
- **相对 `data_dir` / `files.recv_dir` 锚回旧目录** —— 否则配置换了位置，
  数据会"搬家"到新位置（等于变相丢数据）；
- 旧文件保留在原地（升级脚本会备份它），不删除。

### 3. 新命令 `agentlink config`

```bash
agentlink config              # 打印：生效路径 / 稳定位置 / 数据目录
agentlink config --migrate    # 手动触发迁移（幂等）
agentlink config --path       # 只输出一行路径（机器可读，自启脚本用）
```

### 4. 配套改动

- **启动.bat**：改问二进制要配置路径（`for /f` 读 `config --path`），
  不再依赖本目录的 `agentlink.json`；`run` 不带 `--config`，走内置解析。
- **自启 VBS**：`exe` 失效时的回落逻辑修正 —— 原来从 **cfg 目录**找 exe
  （现在 exe 与 cfg 已不在同一文件夹），改为从 **exe 自己的目录**找。
- **systemd 单元**：仍用 `/etc/agentlink/agentlink.json`（发行目录外的固定位置，
  符合同一原则）。
- **wake-hook**：`daemon.py` / `hermes_bridge.py` / `bridge.sh` 的配置发现改为
  优先稳定位置（Windows LOCALAPPDATA / Linux ~/.config/agentlink），
  再回落到发行目录旧配置。

## 验证

```bash
cargo test --bin agentlink                       # 199/199 PASS
AGENTLINK_BIN=... python3 tests/config_location_smoke.py   # 10/10 PASS
```

冒烟覆盖（隔离端口 18962 + 独立 XDG_CONFIG_HOME）：

1. `config --path` 输出稳定位置；
2. 发行目录旧配置自动迁移到稳定位置，node_id 不变；
3. 相对 `data_dir` 迁移后锚回旧目录（数据不搬家）；
4. 发行目录配置被改成别的 node_id 后，节点仍读稳定位置那份；
5. 显式 `--config` 原样生效，不触发迁移、不影响稳定位置；
6. 稳定位置已存在时不碰发行目录旧文件；
7. `--migrate` 幂等（已在稳定位置则报"无需迁移"）。

**生产节点实跑演练**（Windows，v0.6.1，192.168.1.50 / `4f793d7cd6fc`）：

| 场景 | 结果 |
|------|------|
| 首次运行 `config --path` | 自动把 `E:\Tools\AgentLink-v0.4.4\agentlink.json` 迁到 `C:\Users\yxf\AppData\Local\AgentLink\agentlink.json`，node_id/name/tags/data_dir 全保留 |
| 把发行目录配置改成 `deadbeef9999`，不带 `--config` 启动 | health 仍 `4f793d7cd6fc`（用稳定位置那份） |
| 发行目录配置文件整个移走，不带 `--config` 启动 | health 仍 `4f793d7cd6fc` / name=B / tags=[main] |
| 跑新的 `启动.bat` | 正常启动，`for /f` 探测到稳定路径；自启 VBS 的 `cfg` 已指向 `C:\Users\yxf\AppData\Local\AgentLink\agentlink.json` |
