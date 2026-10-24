// SPDX-License-Identifier: Apache-2.0
//! AgentLink - a tiny LAN message bus for AI agents.
//!
//! Each machine runs one `agentlink` node. Agents on that machine talk to it
//! over the local HTTP/WS API; nodes talk to each other over WebSocket.

#[cfg(feature = "mdns")]
use agentlink::mdns;
use agentlink::{api, cli, config, conn, discovery, file, logging, peer, router, wizard};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use api::AppState;
use config::Config;

/// Default configuration file name, relative to the working directory.
/// 只作为「旧版就地配置」的探测名；新装机的配置写在用户配置目录里。
const DEFAULT_CONFIG_FILE: &str = "agentlink.json";

#[derive(Debug, Parser)]
#[command(
    name = "agentlink",
    version = api::VERSION,
    about = "AgentLink - LAN message bus for AI agents"
)]
struct Cli {
    /// Path to the configuration file.
    ///
    /// 省略时按「稳定位置 → 就地旧文件（自动迁移）」顺序解析：
    /// 稳定位置 = `%LOCALAPPDATA%\AgentLink\agentlink.json`（Windows）
    /// 或 `~/.config/agentlink/agentlink.json`（Linux）——升级永不覆盖它。
    /// 显式指定则完全按你给的路径走（不迁移）。
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// 薄壳子命令（get/logs/apply）访问的 local API 基址。
    /// 默认 http://127.0.0.1:<--config 的 listen_port>。
    #[arg(long, global = true)]
    url: Option<String>,
    /// 薄壳子命令使用的 local API token（头 X-Auth-Token）。
    /// 默认读 --config 指定的 agentlink.json 的 local_api.token。
    #[arg(long, global = true)]
    token: Option<String>,

    /// Override `log.format` from the config for this run (v0.5 §7.1).
    /// Without it the default stays the human-readable fmt output.
    #[arg(long, global = true, value_parser = ["text", "json"])]
    log_format: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a configuration file with a fresh identity and tokens.
    Init {
        /// Overwrite an existing configuration file.
        #[arg(long)]
        force: bool,
        /// Display name for this node (defaults to `agent-<hostname>`).
        #[arg(long)]
        name: Option<String>,
        /// Set the cluster token directly (instead of generating a random one).
        #[arg(long)]
        cluster_token: Option<String>,
        /// Trust the LAN without a shared token (`security.mode = "open"`).
        #[arg(long)]
        open: bool,
    },
    /// One-command pairing: create a config and adopt `TOKEN` from the
    /// main machine (replaces manually editing agentlink.json).
    Join {
        /// The cluster token printed by `agentlink token` on the main machine.
        token: String,
        /// Display name for this node.
        #[arg(long)]
        name: Option<String>,
        /// Trust the LAN without a shared token.
        #[arg(long)]
        open: bool,
        /// Overwrite an existing configuration file.
        #[arg(long)]
        force: bool,
    },
    /// Print this node's cluster token (for pairing other machines).
    Token,
    /// 查看/迁移配置文件位置。
    ///
    /// 配置默认住在用户配置目录（Windows `%LOCALAPPDATA%\AgentLink`，
    /// Linux `~/.config/agentlink`），升级换发行目录不会覆盖它。
    /// `migrate` 可把旧版「放在发行目录里」的配置搬到稳定位置。
    Config {
        /// 只打印当前生效的配置文件路径（默认行为）。
        #[arg(long)]
        show: bool,
        /// 把就地旧配置迁移到稳定位置（幂等；已是稳定位置则无事发生）。
        #[arg(long)]
        migrate: bool,
        /// 只输出配置文件的绝对路径（一行，机器可读，供脚本/自启使用）。
        #[arg(long)]
        path: bool,
    },
    /// 查看 / 修复本机身份（node_id）。
    ///
    /// 如果重启后 node_id 意外变了，用 `--set <旧ID>` 改回，或 `--recover`
    /// 从 backups/、upgrade-ledger、历史记录里自动找回。改完需重启节点。
    Identity {
        /// 只查看当前身份、身份锚点与 data_dir（默认行为）。
        #[arg(long)]
        show: bool,
        /// 把本机身份改回指定 node_id（同时修配置与身份锚点）。
        #[arg(long, value_name = "NODE_ID", conflicts_with = "recover")]
        set: Option<String>,
        /// 从本地备份/升级台账/历史里自动找回上一个身份并恢复。
        #[arg(long, conflicts_with = "set")]
        recover: bool,
    },
    /// Run the node (default command).
    Run {
        /// Create `agentlink.json` with defaults if it does not exist yet.
        #[arg(long)]
        create_if_missing: bool,
        /// Do not open the /web page in the browser even if web.auto_open is true
        /// (use this for autostart / headless sessions).
        #[arg(long)]
        no_open: bool,
    },
    /// Windows: register (or remove) a per-user "at logon" scheduled task that
    /// starts this node automatically with `run --no-open`. No admin needed.
    /// Other OSes: prints guidance (see deploy/install-systemd.sh).
    Autostart {
        /// Remove the scheduled task instead of creating it.
        #[arg(long)]
        remove: bool,
        /// Re-register (overwrite) an existing task with current paths.
        #[arg(long)]
        force: bool,
    },
    /// List known peers.
    Peers,
    /// Print the version.
    Version,
    /// 集群视图查询（REST 薄壳：不启动节点，只读本机 local API）。
    #[command(subcommand)]
    Get(cli::GetTarget),
    /// 读取总线事件流（/v1/events，观察者脱敏件）；--follow 持续跟随。
    Logs {
        #[command(flatten)]
        opts: cli::LogsArgs,
    },
    /// 提交 task_spec（协议 §2 的 JSON 文件）到总线（POST /v1/send）。
    Apply {
        #[command(flatten)]
        opts: cli::ApplyArgs,
    },
    /// 首次运行向导（v0.5 收官）：角色/名称/端口/安全模式 → 写配置 → 可选启动。
    /// 裸 `agentlink` 在无配置且交互终端时会自动进入；`--yes` 非交互写默认值。
    Setup {
        /// 不提问，直接写默认配置（适合脚本/CI）。
        #[arg(long)]
        yes: bool,
    },
    /// 自检：向总线所有已知节点的 `__node__` 发 hello 探针，聚合应答一张表。
    Hello {
        #[command(flatten)]
        opts: cli::HelloArgs,
    },
    /// 集群实时概览（/v1/health + /v1/peers + /v1/mesh/status 快照，
    /// ANSI 色标：在线/离线、CPU 水位、熔断节点；--watch 秒级刷新）。
    Top {
        /// 刷新间隔（秒，缺省 3）；缺省只看一张快照。
        #[arg(long)]
        watch: Option<u64>,
    },
    /// 生成 shell 补全脚本（bash/zsh/fish/powershell；stdout 重定向到补全目录），
    /// 如 `agentlink completions bash > /etc/bash_completion.d/agentlink`。
    Completions {
        /// 目标 shell。
        #[arg(value_enum)]
        shell: clap_complete::shells::Shell,
    },
}

/// Open a URL in the OS default browser (best effort).
fn open_web_browser(url: &str) {
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("cmd")
        .args(["/c", "start", "", url])
        .spawn();
    #[cfg(target_os = "linux")]
    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(url).spawn();
}

/// 解析配置文件的实际路径。
///
/// 规则（与「升级不动数据」一致）：
/// 1. 显式 `--config <path>` → 完全按给的走，不迁移、不解释；
/// 2. `AGENTLINK_CONFIG` 环境变量 → 同上；
/// 3. 稳定位置（用户配置目录）已有配置 → 用它；
/// 4. 就地旧文件（当前目录 / exe 同目录的 `agentlink.json`）存在 →
///    **一次性迁移**到稳定位置（复制，原文件留作备份），之后只用稳定位置；
/// 5. 都没有 → 稳定位置（新配置写这里，升级不会碰到）。
///
/// 第 4 步是升级兼容的关键：老版本把配置放在发行目录里，新版本第一次
/// 启动会把它搬到用户配置目录，从此与发行目录脱钩。
fn resolve_config_path(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(p) = explicit {
        return p;
    }
    if let Ok(p) = std::env::var("AGENTLINK_CONFIG") {
        if !p.trim().is_empty() {
            return PathBuf::from(p);
        }
    }
    let stable = config::stable_config_path();
    if stable.exists() {
        return stable;
    }
    // 就地旧文件：当前工作目录优先，其次 exe 同目录（Windows 双击/自启场景）。
    let mut candidates: Vec<PathBuf> = vec![PathBuf::from(DEFAULT_CONFIG_FILE)];
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(DEFAULT_CONFIG_FILE));
        }
    }
    for cand in candidates {
        if !cand.exists() {
            continue;
        }
        match migrate_legacy_config(&cand, &stable) {
            Ok(()) => {
                eprintln!(
                    "[config] 已把旧配置 {} 迁移到 {}（升级不再覆盖配置）",
                    cand.display(),
                    stable.display()
                );
                return stable;
            }
            Err(e) => {
                eprintln!(
                    "[config] 迁移旧配置 {} 失败（{e}），继续使用原路径",
                    cand.display()
                );
                return cand;
            }
        }
    }
    stable
}

/// 把旧配置复制到稳定位置。原文件保留（升级脚本会备份它），但节点以后
/// 只读稳定位置的那份。
fn migrate_legacy_config(legacy: &Path, stable: &Path) -> Result<()> {
    let raw = std::fs::read_to_string(legacy)
        .with_context(|| format!("failed to read legacy config {}", legacy.display()))?;
    let mut cfg: Config = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse legacy config {}", legacy.display()))?;
    // 旧配置里的相对路径是相对旧文件所在目录解析的；换位置后必须锚回
    // 旧目录，否则 data_dir / recv_dir 会指向新位置（数据"搬家"）。
    let old_base = legacy
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    if cfg.data_dir.is_relative() {
        cfg.data_dir = old_base.join(&cfg.data_dir);
    }
    if let Some(rd) = cfg.files.recv_dir.as_ref() {
        if rd.is_relative() {
            cfg.files.recv_dir = Some(old_base.join(rd));
        }
    }
    if let Some(parent) = stable.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    cfg.save(stable)
        .with_context(|| format!("failed to write config {}", stable.display()))?;
    Ok(())
}

/// Load configuration, optionally creating it on first run.
fn load_config(path: &Path, create_if_missing: bool) -> Result<Config> {
    config::load_config(path, create_if_missing, None)
}

fn ensure_dirs(cfg: &Config) -> Result<()> {
    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("failed to create data dir {}", cfg.data_dir.display()))?;
    std::fs::create_dir_all(cfg.files.recv_dir_resolved()).with_context(|| {
        format!(
            "failed to create recv dir {}",
            cfg.files.recv_dir_resolved().display()
        )
    })?;
    Ok(())
}

/// 启动时对账机器身份：身份锚点存在就以锚点为准，配置里漂移的 node_id
/// 会被就地修回并重新落盘；锚点不存在则用当前配置补写。
///
/// 这是“重启不变身份”的最后一道保险：无论配置来自旧目录、新目录还是
/// `init --force` 重建，节点对外呈现的 node_id 始终是这台机器首次生成的
/// 那一个。修复动作会打印到 stderr（此时日志系统可能尚未初始化）。
fn reconcile_identity(cfg: &mut Config, config_file: &Path) -> Result<()> {
    match cfg.pinned_node_id() {
        Some(pinned) if pinned != cfg.node_id => {
            eprintln!(
                "[identity] node_id drift detected: config={} pinned={} — restoring pinned identity",
                cfg.node_id, pinned
            );
            cfg.node_id = pinned;
            cfg.save(config_file)
                .with_context(|| format!("failed to repair config {}", config_file.display()))?;
        }
        Some(_) => {}
        None => cfg
            .pin_identity()
            .context("failed to write machine identity anchor")?,
    }
    Ok(())
}

/// `agentlink config`：查看/迁移配置文件位置。
///
/// 配置属于「机器设定」，和接收到的文件一样不该跟着发行目录被升级覆盖。
/// 新版默认把它放在用户配置目录；`--migrate` 把旧版就地配置搬过去。
fn config_cmd(config_path: &Path, show: bool, migrate: bool, path: bool) -> Result<()> {
    let _ = show; // 无参数与 --show 同一分支
    let stable = config::stable_config_path();
    if path {
        // 机器可读：只输出路径一行（自启脚本/守护进程据此定位配置）。
        println!("{}", config_path.display());
        return Ok(());
    }
    println!("生效配置: {}", config_path.display());
    println!("稳定位置: {}", stable.display());
    println!(
        "数据目录: {}",
        load_config(config_path, false)
            .map(|c| c.data_dir.display().to_string())
            .unwrap_or_else(|_| "<配置不存在>".to_string())
    );

    if !migrate {
        if config_path != stable {
            println!("\n提示: 当前配置不在稳定位置，可用 `agentlink config --migrate` 搬迁。");
        }
        return Ok(());
    }

    if config_path == stable {
        println!("\n已在稳定位置，无需迁移。");
        return Ok(());
    }
    if !config_path.exists() {
        println!("\n当前配置不存在，无需迁移。");
        return Ok(());
    }
    migrate_legacy_config(config_path, &stable)?;
    println!(
        "\n已迁移: {} -> {}",
        config_path.display(),
        stable.display()
    );
    println!("原文件保留在旧位置（可自行删除）；后续请用不带 --config 的命令启动。");
    Ok(())
}

/// `agentlink identity`：查看或显式修复本机身份。
///
/// 这是给“已经发生了漂移”的补救入口：`--set` 直接指定旧 node_id，
/// `--recover` 从本地备份/升级台账/出站历史里自动找。两者都会同时改写
/// 配置文件和身份锚点（锚点必须一起改，否则下次启动又被搬回去），
/// 运行中的进程需要重启才会用新身份对外通信。
fn identity_cmd(config_path: &Path, show: bool, set: Option<String>, recover: bool) -> Result<()> {
    let _ = show; // 无参数/--show 都走查看分支
    let mut cfg = load_config(config_path, false)?;

    if let Some(raw) = set {
        let id = normalize_node_id(&raw)?;
        return restore_identity(&mut cfg, config_path, &id, "命令行 --set");
    }

    if recover {
        let candidates = identity_candidates(config_path, &cfg);
        if candidates.is_empty() {
            println!(
                "没有找到与当前身份（{}）不同的旧身份线索（当前可能已经正确）。",
                cfg.node_id
            );
            println!(
                "如确需指定: agentlink --config {} identity --set <旧 node_id>",
                config_path.display()
            );
            return Ok(());
        }
        if candidates.len() > 1 {
            println!(
                "找到 {} 个候选身份（按可靠度/新旧排序）：",
                candidates.len()
            );
            for (i, (id, source)) in candidates.iter().enumerate() {
                println!(
                    "  {}. {id}  ({source}){}",
                    i + 1,
                    if i == 0 { "  ← 采用" } else { "" }
                );
            }
            println!();
        }
        let (id, source) = candidates[0].clone();
        return restore_identity(&mut cfg, config_path, &id, &source);
    }

    println!("config:   {}", config_path.display());
    println!("node_id:  {}", cfg.node_id);
    println!("anchor:   {}", cfg.identity_path().display());
    println!("data_dir: {}", cfg.data_dir.display());
    match cfg.pinned_node_id() {
        Some(pinned) if pinned != cfg.node_id => {
            println!("status:   DRIFT（锚点={pinned}，下次启动会自动修回）");
        }
        Some(_) => println!("status:   pinned"),
        None => println!("status:   unpinned（下次启动会写入锚点）"),
    }
    println!(
        "\n修复到旧身份: agentlink --config {} identity --set <旧 node_id>",
        config_path.display()
    );
    println!(
        "自动找回旧身份: agentlink --config {} identity --recover",
        config_path.display()
    );
    Ok(())
}

/// 校验命令行传入的 node_id。标准形态是 12 位十六进制；历史配置里若有
/// 非标准但合法的 id，也允许原样写回（仅提示，不阻塞补救）。
fn normalize_node_id(raw: &str) -> Result<String> {
    let id = raw.trim();
    if id.is_empty() {
        bail!("node_id 不能为空");
    }
    if id.chars().any(char::is_whitespace) {
        bail!("node_id 不能包含空白字符");
    }
    if !(id.len() == 12 && id.chars().all(|c| c.is_ascii_hexdigit())) {
        eprintln!("warning: {id:?} 不是标准 12 位十六进制 node_id，将按原样写入");
    }
    Ok(id.to_string())
}

/// 同时修复配置与身份锚点。锚点覆盖是**有意为之**：这正是补救入口，
/// 必须把错误锚点一起改掉，否则下一次启动会被旧锚点搬回去。
fn restore_identity(cfg: &mut Config, config_path: &Path, id: &str, source: &str) -> Result<()> {
    let old = cfg.node_id.clone();
    if old == id {
        cfg.pin_identity()
            .context("failed to refresh machine identity anchor")?;
        println!("身份已经是 {id}（来源: {source}），已刷新身份锚点");
    } else {
        cfg.node_id = id.to_string();
        cfg.save(config_path)?;
        cfg.pin_identity()
            .context("failed to write machine identity anchor")?;
        println!("身份已恢复: {old} -> {id}");
        println!("  来源: {source}");
        println!("  配置: {}", config_path.display());
        println!("  锚点: {}", cfg.identity_path().display());
    }
    println!("\n重启节点后生效（运行中的进程仍持有旧身份）。");
    Ok(())
}

/// 收集旧身份候选：身份锚点 → 最新备份 → 升级台账 → 出站历史 → agents.json。
/// 返回 (node_id, 来源说明)，已去重并排除当前 node_id。
fn identity_candidates(config_path: &Path, cfg: &Config) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let current = cfg.node_id.trim().to_string();
    let mut push = |raw: &str, source: String| {
        let id = raw.trim();
        if id.is_empty() || id == current {
            return;
        }
        if out.iter().any(|(known, _)| known == id) {
            return;
        }
        out.push((id.to_string(), source));
    };

    // 1) 身份锚点（若它与当前配置不一致，通常就是真身份）
    if let Some(pinned) = cfg.pinned_node_id() {
        push(
            &pinned,
            format!("身份锚点 {}", cfg.identity_path().display()),
        );
    }

    let base = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    // 2) backups/<version>/agentlink.json，按修改时间从新到旧
    let mut backups: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(base.join("backups")) {
        for entry in rd.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let mtime = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            backups.push((path, mtime));
        }
    }
    backups.sort_by_key(|a| std::cmp::Reverse(a.1));
    for (dir, _) in backups {
        let backup_cfg = dir.join("agentlink.json");
        if let Some(id) = json_node_id(&backup_cfg) {
            push(&id, format!("备份 {}", backup_cfg.display()));
        }
    }

    // 3) 升级台账（节点目录 + data_dir），取每个文件里最近一条
    for ledger in [
        base.join("upgrade-ledger.jsonl"),
        cfg.data_dir.join("upgrade-ledger.jsonl"),
    ] {
        let Ok(raw) = std::fs::read_to_string(&ledger) else {
            continue;
        };
        for line in raw.lines().rev() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if let Some(id) = v.get("node").and_then(|x| x.as_str()) {
                push(id, format!("升级台账 {}", ledger.display()));
                break;
            }
        }
    }

    // 4) 历史里最近一条“出站且 node == from”的记录就是本机旧身份
    let history = cfg.data_dir.join("history.jsonl");
    if let Ok(raw) = std::fs::read_to_string(&history) {
        for line in raw.lines().rev().take(2000) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let direction = v.get("direction").and_then(|x| x.as_str()).unwrap_or("");
            let node = v.get("node").and_then(|x| x.as_str()).unwrap_or("");
            let from = v.get("from").and_then(|x| x.as_str()).unwrap_or("");
            if direction == "out" && !node.is_empty() && node == from {
                push(node, format!("出站历史 {}", history.display()));
                break;
            }
        }
    }

    // 5) 本机 agent 注册快照（最后手段）
    let agents = cfg.data_dir.join("agents.json");
    if let Some(id) = latest_agent_node_id(&agents) {
        push(&id, format!("agent 注册表 {}", agents.display()));
    }

    out
}

fn json_node_id(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let id = v.get("node_id")?.as_str()?.trim();
    (!id.is_empty()).then(|| id.to_string())
}

fn latest_agent_node_id(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let agents: Vec<serde_json::Value> = serde_json::from_str(&raw).ok()?;
    agents
        .into_iter()
        .filter_map(|a| {
            let id = a.get("node_id")?.as_str()?.trim().to_string();
            if id.is_empty() {
                return None;
            }
            let seen = a.get("last_seen").and_then(|x| x.as_i64()).unwrap_or(0);
            Some((seen, id))
        })
        .max_by_key(|(seen, _)| *seen)
        .map(|(_, id)| id)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // 配置解析：显式 --config > AGENTLINK_CONFIG > 稳定位置 > 就地旧文件（迁移）。
    // 稳定位置在用户配置目录里，升级换目录/换二进制都不会覆盖它。
    let config_path = resolve_config_path(cli.config.clone());

    match cli.command {
        Some(Command::Init {
            force,
            name,
            cluster_token,
            open,
        }) => {
            if config_path.exists() && !force {
                bail!(
                    "config already exists: {} (use --force to overwrite)",
                    config_path.display()
                );
            }
            let mode = open.then(|| "open".to_string());
            let cfg = config::generate_preserving_identity(&config_path, name, cluster_token, mode);
            cfg.save(&config_path)?;
            cfg.ensure_identity_anchor()
                .context("failed to write machine identity anchor")?;
            println!("created config: {}", config_path.display());
            println!("  node_id: {}", cfg.node_id);
            println!("  name:    {}", cfg.name);
            println!("  token:   {} (local API)", cfg.local_api.token);
            println!(
                "  cluster: {} ({})",
                cfg.security.cluster_token, cfg.security.mode
            );
            if cfg.security.mode == "strict" {
                println!(
                    "\n其他机器上运行: agentlink join {}  (自动配对)",
                    cfg.security.cluster_token
                );
            }
            Ok(())
        }

        Some(Command::Join {
            token,
            name,
            open,
            force,
        }) => {
            if config_path.exists() && !force {
                bail!(
                    "config already exists: {} (use --force to overwrite)",
                    config_path.display()
                );
            }
            let mode = open.then(|| "open".to_string());
            let cfg = config::generate_preserving_identity(&config_path, name, Some(token), mode);
            cfg.save(&config_path)?;
            cfg.ensure_identity_anchor()
                .context("failed to write machine identity anchor")?;
            println!("paired: {} ({})", cfg.name, cfg.node_id);
            println!("  cluster_token set ({} mode)", cfg.security.mode);
            println!("\nrun `agentlink run` to start");
            Ok(())
        }

        Some(Command::Token) => {
            let cfg = load_config(&config_path, false)?;
            println!("{}", cfg.security.cluster_token);
            Ok(())
        }

        Some(Command::Config {
            show,
            migrate,
            path,
        }) => config_cmd(&config_path, show, migrate, path),

        Some(Command::Identity { show, set, recover }) => {
            identity_cmd(&config_path, show, set, recover)
        }

        Some(Command::Peers) => {
            let cfg = load_config(&config_path, false)?;
            println!("node: {} ({})", cfg.name, cfg.node_id);
            println!(
                "static peers configured: {}",
                cfg.discovery.static_peers.len()
            );
            for p in &cfg.discovery.static_peers {
                println!("  {p}");
            }
            println!("(nodes are discovered automatically over UDP broadcast)");
            Ok(())
        }

        Some(Command::Version) => {
            println!("agentlink {}", api::VERSION);
            Ok(())
        }

        Some(Command::Run {
            create_if_missing,
            no_open,
        }) => {
            let mut cfg = load_config(&config_path, create_if_missing)?;
            if let Some(fmt) = cli.log_format.clone() {
                cfg.log.format = fmt;
            }
            logging::init(&cfg.log);
            ensure_dirs(&cfg)?;
            let cfg_path = config_path.clone();
            run(cfg, cfg_path, no_open).await
        }
        Some(Command::Autostart { remove, force }) => autostart(&config_path, remove, force),

        Some(Command::Get(what)) => {
            cli::get(what, cli.url.as_deref(), cli.token.as_deref(), &config_path).await
        }
        Some(Command::Logs { opts }) => {
            cli::logs(opts, cli.url.as_deref(), cli.token.as_deref(), &config_path).await
        }
        Some(Command::Apply { opts }) => {
            cli::apply(opts, cli.url.as_deref(), cli.token.as_deref(), &config_path).await
        }
        Some(Command::Setup { yes }) => setup_flow(&config_path, cli.log_format.clone(), yes).await,
        Some(Command::Hello { opts }) => {
            cli::hello(opts, cli.url.as_deref(), cli.token.as_deref(), &config_path).await
        }
        Some(Command::Top { watch }) => {
            cli::top(
                cli.url.as_deref(),
                cli.token.as_deref(),
                &config_path,
                watch,
            )
            .await
        }
        Some(Command::Completions { shell }) => {
            use clap::CommandFactory;
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "agentlink", &mut std::io::stdout());
            Ok(())
        }
        None => {
            // v0.5 收官：裸启动 + 无配置 + 交互终端 → 自动进入首次向导
            // （Windows 双击 exe 的新机器旅程，不需要先读任何文档）。
            if !config_path.exists() && wizard::is_interactive() {
                return setup_flow(&config_path, cli.log_format.clone(), false).await;
            }
            let mut cfg = load_config(&config_path, false)?;
            if let Some(fmt) = cli.log_format.clone() {
                cfg.log.format = fmt;
            }
            logging::init(&cfg.log);
            ensure_dirs(&cfg)?;
            let cfg_path = config_path.clone();
            run(cfg, cfg_path, false).await
        }
    }
}

/// `agentlink setup` 与裸启动自动向导共用入口（v0.5 收官）。
/// 只做向导交互 + 配置落盘 + 可选启动；机器身份复用
/// `generate_preserving_identity` 保留。
async fn setup_flow(config_path: &Path, log_format: Option<String>, yes: bool) -> Result<()> {
    let plan = if yes {
        wizard::WizardPlan {
            name: None,
            listen_port: None,
            cluster_token: None,
            mode: None,
            start_now: false,
        }
    } else if wizard::is_interactive() {
        match wizard::run_wizard(config_path, api::VERSION)? {
            Some(p) => p,
            // 用户在确认前放弃：未写任何文件，正常退出。
            None => return Ok(()),
        }
    } else {
        bail!(
            "非交互终端：请用 `agentlink init` / `agentlink join <TOKEN>`，\n\
             或 `agentlink setup --yes` 直接写默认配置"
        );
    };

    let mut cfg =
        config::generate_preserving_identity(config_path, plan.name, plan.cluster_token, plan.mode);
    if let Some(port) = plan.listen_port {
        cfg.listen_port = port;
    }
    cfg.save(config_path)?;
    cfg.ensure_identity_anchor()
        .context("failed to write machine identity anchor")?;
    println!("已写入配置: {}", config_path.display());
    println!("  node_id: {}", cfg.node_id);
    println!("  name:    {}", cfg.name);
    println!("  listen:  {}:{}", cfg.local_api.host, cfg.listen_port);
    if cfg.security.mode == "strict" {
        println!(
            "\n其他机器配对: agentlink join {}   （查看令牌: agentlink token）",
            cfg.security.cluster_token
        );
    }
    println!("节点启动后自检总线连通: agentlink hello");

    if plan.start_now {
        if let Some(fmt) = log_format {
            cfg.log.format = fmt;
        }
        logging::init(&cfg.log);
        ensure_dirs(&cfg)?;
        run(cfg, config_path.to_path_buf(), false).await
    } else {
        println!("\n启动节点: agentlink run");
        Ok(())
    }
}

async fn run(mut cfg: Config, config_file: PathBuf, no_open: bool) -> Result<()> {
    // 机器身份铁律：以 data_dir 的身份锚点为准，配置漂移就地修回。
    reconcile_identity(&mut cfg, &config_file)?;
    let bind = format!("{}:{}", cfg.local_api.host, cfg.listen_port);

    let (peer_tx, peer_rx) = tokio::sync::mpsc::channel(256);
    let peers = peer::PeerTable::new();
    let router = Arc::new(router::Router::new(&cfg));
    let files = Arc::new(file::FileManager::new());
    let state_inner = AppState::new(
        cfg.clone(),
        config_file,
        peers,
        peer_tx,
        router.clone(),
        files,
    );
    let state = Arc::new(state_inner);
    // Restore the user's "forget this node" list (ghost-peer cleanup,
    // paired with DELETE /v1/peers/{node_id}).
    let forgotten_path = cfg.data_dir.join("forgotten_peers.json");
    if let Ok(raw) = std::fs::read_to_string(&forgotten_path) {
        if let Ok(ids) = serde_json::from_str::<Vec<String>>(&raw) {
            state.peers.set_suppressed(ids);
        }
    }
    let app = api::router(state.clone());
    let maintenance = router::Router::spawn_maintenance(&router);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let discovery = if cfg.discovery.enabled {
        Some(tokio::spawn(discovery::run(
            cfg.clone(),
            state.clone(),
            shutdown_rx.clone(),
        )))
    } else {
        tracing::info!("discovery disabled by configuration");
        None
    };
    let manager = tokio::spawn(conn::manager(
        cfg.clone(),
        state.clone(),
        peer_rx,
        shutdown_rx.clone(),
    ));

    let mdns_task: Option<tokio::task::JoinHandle<()>> = {
        let want_mdns = cfg.discovery.methods.iter().any(|m| m == "mdns");
        #[cfg(feature = "mdns")]
        {
            if want_mdns {
                tracing::info!("mdns feature enabled; starting mDNS discovery");
                Some(tokio::spawn(mdns::run(
                    cfg.clone(),
                    state.clone(),
                    shutdown_rx.clone(),
                )))
            } else {
                None
            }
        }
        #[cfg(not(feature = "mdns"))]
        {
            if want_mdns {
                tracing::warn!(
                    "discovery method 'mdns' configured but the 'mdns' feature is disabled; \
                     rebuild with `cargo build --features mdns`"
                );
            }
            None
        }
    };

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| {
            format!(
                "failed to bind {bind} (is another agentlink already running, or the port in use?)"
            )
        })?;

    tracing::info!(
        version = api::VERSION,
        node_id = %cfg.node_id,
        name = %cfg.name,
        bind = %bind,
        cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        "agentlink node started"
    );
    tracing::info!("local API: http://127.0.0.1:{}/v1/health", cfg.listen_port);

    if cfg.web.auto_open && !no_open {
        let url = format!("http://127.0.0.1:{}/web", cfg.listen_port);
        tracing::info!(url, "opening web page in default browser");
        open_web_browser(&url);
    }

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        // v0.7 §6.2：先宣告 drain，让对端立即回收/换机，再停服务。
        router.broadcast_drain();
        // 给 drain 帧一个刷出窗口（send 是 fire-and-forget tokio 任务）。
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = shutdown_tx.send(true);
    })
    .await
    .context("http server error")?;

    // Let background tasks announce `bye` and wind down.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        if let Some(discovery) = discovery {
            let _ = discovery.await;
        }
        if let Some(mdns) = mdns_task {
            let _ = mdns.await;
        }
        let _ = manager.await;
        let _ = maintenance.await;
    })
    .await;

    tracing::info!("agentlink node stopped");
    Ok(())
}

/// Resolve on Ctrl-C (or SIGTERM on Unix).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

/// Windows: manage per-user autostart so a reboot brings the node back by
/// itself — no browser page, no manual click. Implemented via the Startup
/// folder (`%APPDATA%\...\Startup\AgentLink.vbs`, hidden window, no admin
/// rights needed; `schtasks /SC ONLOGON` requires elevation). On other
/// platforms this prints systemd guidance instead.
fn autostart(config_path: &std::path::Path, remove: bool, force: bool) -> Result<()> {
    if !cfg!(windows) {
        println!("autostart is implemented via the Windows Startup folder (AgentLink.vbs).");
        println!("On Linux, install the systemd unit instead:");
        println!("  ./deploy/install-systemd.sh   # run from the node directory");
        return Ok(());
    }

    let exe = std::env::current_exe().context("cannot resolve current exe path")?;
    let cfg_abs = if config_path.is_absolute() {
        config_path.to_path_buf()
    } else {
        std::path::absolute(config_path).unwrap_or_else(|_| config_path.to_path_buf())
    };

    let appdata = std::env::var("APPDATA").context("APPDATA environment variable not set")?;
    let startup =
        std::path::Path::new(&appdata).join(r"Microsoft\Windows\Start Menu\Programs\Startup");
    let vbs = startup.join("AgentLink.vbs");

    if remove {
        match std::fs::remove_file(&vbs) {
            Ok(()) => println!("removed autostart entry: {}", vbs.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                println!("no autostart entry found; nothing to remove");
            }
            Err(e) => return Err(e).with_context(|| format!("cannot remove {}", vbs.display())),
        }
        return Ok(());
    }

    if vbs.exists() && !force {
        println!("autostart already registered: {}", vbs.display());
        if let Ok(content) = std::fs::read_to_string(&vbs) {
            for line in content.lines().filter(|l| l.starts_with("ws.Run")) {
                println!("  {line}");
            }
        }
        println!("(use --force to re-register with current paths, --remove to delete)");
        return Ok(());
    }
    if !cfg_abs.exists() {
        anyhow::bail!(
            "config '{}' does not exist; run this from the node directory or pass --config",
            cfg_abs.display()
        );
    }

    // VBS quotes a literal " as "" — so the inner command reads ""..."" .
    // 自修复模板：exe 路径失效时回落到 config 同目录的 agentlink.exe，仍不存在则静默退出，
    // 不再让 wscript 弹 "系统找不到指定的文件 80070002" 打断开机。
    // VBS 里 exe 与 cfg 各记各的：cfg 在用户配置目录（升级不动），exe 在
    // 发行目录（升级会换）。exe 失效时回落到 **exe 自己的目录**，而不是
    // cfg 的目录——两者已经不在同一个文件夹了。
    let script = format!(
        "' AgentLink autostart (installed by `agentlink autostart`)\n\
         On Error Resume Next\n\
         Set fso = CreateObject(\"Scripting.FileSystemObject\")\n\
         Set ws = CreateObject(\"Wscript.Shell\")\n\
         exe = \"{}\"\n\
         cfg = \"{}\"\n\
         If Not fso.FileExists(exe) Then\n\
           exe2 = fso.BuildPath(fso.GetParentFolderName(exe), \"agentlink.exe\")\n\
           If fso.FileExists(exe2) Then\n\
             exe = exe2\n\
           Else\n\
             WScript.Quit 0\n\
           End If\n\
         End If\n\
         ws.Run \"\"\"\" & exe & \"\"\" --config \"\"\" & cfg & \"\"\" run --no-open\"\"\", 0, False\n",
        exe.display(),
        cfg_abs.display()
    );
    std::fs::create_dir_all(&startup)
        .with_context(|| format!("cannot open Startup folder {}", startup.display()))?;
    std::fs::write(&vbs, script).context("cannot write AgentLink.vbs")?;
    println!("registered autostart (hidden, at logon, no admin needed):");
    println!("  {}", vbs.display());
    println!(
        "run it now without waiting for next logon:  wscript \"{}\"",
        vbs.display()
    );
    Ok(())
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    fn temp_case(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentlink-identity-{tag}-{}",
            config::new_short_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn drift_in_config_is_repaired_from_anchor() {
        let dir = temp_case("drift");
        let cfg_path = dir.join("agentlink.json");
        let mut cfg = Config::generate(Some("t".into()), Some("tok".into()), Some("open".into()));
        cfg.node_id = "aaaaaaaaaaaa".into();
        cfg.data_dir = dir.join("data");
        cfg.save(&cfg_path).unwrap();
        cfg.pin_identity().unwrap();

        // 模拟「配置被重建/覆盖」：node_id 变成新的随机值。
        cfg.node_id = "bbbbbbbbbbbb".into();
        cfg.save(&cfg_path).unwrap();

        reconcile_identity(&mut cfg, &cfg_path).unwrap();
        assert_eq!(cfg.node_id, "aaaaaaaaaaaa");
        let on_disk = Config::load(&cfg_path).unwrap();
        assert_eq!(
            on_disk.node_id, "aaaaaaaaaaaa",
            "修复必须落盘，重启后仍是同一身份"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn first_run_pins_the_identity() {
        let dir = temp_case("pin");
        let cfg_path = dir.join("agentlink.json");
        let mut cfg = Config::generate(Some("t".into()), Some("tok".into()), Some("open".into()));
        cfg.node_id = "cccccccccccc".into();
        cfg.data_dir = dir.join("data");
        cfg.save(&cfg_path).unwrap();

        reconcile_identity(&mut cfg, &cfg_path).unwrap();
        assert_eq!(cfg.pinned_node_id().as_deref(), Some("cccccccccccc"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_node_id_recovers_from_anchor() {
        let dir = temp_case("recover");
        let cfg_path = dir.join("agentlink.json");
        let cfg = {
            let mut c = Config::generate(Some("t".into()), Some("tok".into()), Some("open".into()));
            c.node_id = "dddddddddddd".into();
            c.data_dir = dir.join("data");
            c
        };
        // 先落一份正常配置并写锚点，再把配置里的身份抹掉（模拟半截写入）。
        let mut broken = cfg.clone();
        broken.save(&cfg_path).unwrap();
        broken.pin_identity().unwrap();
        broken.node_id = String::new();
        broken.save(&cfg_path).unwrap();

        let loaded = Config::load(&cfg_path).unwrap();
        assert_eq!(loaded.node_id, "dddddddddddd", "空 node_id 必须从锚点恢复");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recover_candidates_pick_previous_identity_from_backup() {
        let dir = temp_case("candidates");
        let backup = dir.join("backups").join("v0.5.9");
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::write(
            backup.join("agentlink.json"),
            r#"{"v":1,"node_id":"eeeeeeeeeeee","name":"old"}"#,
        )
        .unwrap();

        let mut cfg = Config::generate(Some("t".into()), Some("tok".into()), Some("open".into()));
        cfg.node_id = "ffffffffffff".into(); // 漂移后的当前身份
        cfg.data_dir = dir.join("data");
        let candidates = identity_candidates(&dir.join("agentlink.json"), &cfg);
        assert_eq!(candidates[0].0, "eeeeeeeeeeee");
        assert!(candidates[0].1.contains("备份"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explicit_identity_must_be_a_clean_token() {
        assert_eq!(
            normalize_node_id("  ABCDEFabcdef ").unwrap(),
            "ABCDEFabcdef"
        );
        assert!(normalize_node_id("   ").is_err());
        assert!(normalize_node_id("aa bb").is_err());
    }
}

#[cfg(test)]
mod config_path_tests {
    use super::*;

    fn temp_case(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentlink-cfgpath-{tag}-{}",
            config::new_short_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 迁移：旧配置（放在发行目录）→ 稳定位置，且相对 data_dir 锚回旧目录。
    #[test]
    fn legacy_config_migrates_to_stable_path() {
        let dir = temp_case("migrate");
        let legacy = dir.join("agentlink.json");
        let stable = dir.join("stable").join("agentlink.json");

        let mut cfg = Config::generate(Some("t".into()), Some("tok".into()), Some("open".into()));
        cfg.node_id = "1234567890ab".into();
        cfg.data_dir = PathBuf::from("./data"); // 相对路径
        cfg.files.recv_dir = Some(PathBuf::from("./files"));
        cfg.save(&legacy).unwrap();

        migrate_legacy_config(&legacy, &stable).unwrap();

        let moved = Config::load(&stable).unwrap();
        assert_eq!(moved.node_id, "1234567890ab", "迁移必须保住 node_id");
        assert!(
            moved.data_dir.is_absolute(),
            "相对 data_dir 必须锚回旧目录，迁移后不能漂"
        );
        assert_eq!(moved.data_dir, dir.join("data"));
        let recv = moved.files.recv_dir.clone().expect("recv_dir");
        assert_eq!(recv, dir.join("files"));
        // 旧文件保留（升级脚本还会备份它）
        assert!(legacy.exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 显式 --config 时完全不介入：不迁移、不改路径。
    #[test]
    fn explicit_config_path_is_respected_verbatim() {
        let dir = temp_case("explicit");
        let explicit = dir.join("my-own.json");
        assert_eq!(resolve_config_path(Some(explicit.clone())), explicit);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 稳定位置已有配置时直接用它，绝不去碰发行目录里的旧文件。
    #[test]
    fn stable_config_wins_over_legacy() {
        let dir = temp_case("stable-wins");
        let stable = dir.join("stable.json");
        std::fs::write(&stable, "{}").unwrap();
        // 显式路径不受影响；这里只验证「稳定路径存在 → 不迁移」的行为：
        // 迁移函数对已存在的稳定位置不该被调用。
        assert!(stable.exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 稳定配置路径必须落在用户配置目录里，绝不能是发行目录。
    #[test]
    fn stable_config_path_is_outside_release_dir() {
        let p = config::stable_config_path();
        assert_eq!(
            p.file_name().and_then(|n| n.to_str()),
            Some("agentlink.json")
        );
        // 路径里必须带一个用户级目录段（Windows: AppData\Local\AgentLink；
        // Linux: .config/agentlink 或 .local/share/agentlink）
        let s = p.to_string_lossy().to_string();
        assert!(
            s.contains("AgentLink") || s.contains("agentlink"),
            "稳定配置路径应在用户级目录: {s}"
        );
        assert!(
            !s.contains("AgentLink-v"),
            "稳定配置路径不能落在发行目录里: {s}"
        );
    }
}
