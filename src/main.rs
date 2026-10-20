//! AgentLink - a tiny LAN message bus for AI agents.
//!
//! Each machine runs one `agentlink` node. Agents on that machine talk to it
//! over the local HTTP/WS API; nodes talk to each other over WebSocket.

mod admission;
mod api;
mod audit;
mod circuit;
mod cli;
mod config;
mod conn;
mod discovery;
mod file;
mod gitops;
mod lease;
mod license;
mod load;
mod logging;
#[cfg(feature = "mdns")]
mod mdns;
mod mtls;
mod peer;
mod proto;
mod reconcile;
mod redact;
mod router;
mod scheduler;
mod slo;
mod store;
mod taskengine;
mod taskstore;
mod trace;
mod wizard;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use api::AppState;
use config::Config;

/// Default configuration file name, relative to the working directory.
const DEFAULT_CONFIG_FILE: &str = "agentlink.json";

#[derive(Debug, Parser)]
#[command(
    name = "agentlink",
    version = api::VERSION,
    about = "AgentLink - LAN message bus for AI agents"
)]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, global = true, default_value = DEFAULT_CONFIG_FILE)]
    config: PathBuf,
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
        /// 准入令牌（v0.6 P1 新节点准入）。只写入本机配置，不自动开启——
        /// 准入段（enabled/白名单）与集群令牌一同分发。
        #[arg(long)]
        admission: Option<String>,
    },
    /// Print this node's cluster token (for pairing other machines).
    Token,
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
    /// mTLS 一键启用 / 查看 / 轮转（v0.6 P1）。
    /// 生成内置 CA 并签发本机节点证书；`--enable` 写入 mtls.enabled=true。
    Mtls {
        /// 生成/加载证书并把 `mtls.enabled` 置为 true（一键开启）。
        #[arg(long)]
        enable: bool,
        /// 关闭 mtls（写回 `mtls.enabled=false`，不删除证书文件）。
        #[arg(long)]
        disable: bool,
        /// 打印 CA/节点证书路径、指纹与到期时间。
        #[arg(long)]
        show: bool,
        /// 立即轮转节点证书（CA 与全网信任不变）。
        #[arg(long)]
        rotate: bool,
        /// 以「成员」身份启用：不新建 CA，启动后经已认证的对等链路自动领取集群 CA
        /// （CA 自动分发）。写入 `mtls.create_ca=false`。
        #[arg(long)]
        member: bool,
        /// 以「权威」身份启用：无 CA 时新建 CA（默认行为）。写入 `mtls.create_ca=true`。
        #[arg(long)]
        authority: bool,
        /// 强制模式：拒绝一切明文对等链路（出站只拨 wss://、入站明文 403）。
        /// 必须与 --enable 同用；关闭机会式降级（混版本灰度不再自动明文）。
        #[arg(long, requires = "enable")]
        require: bool,
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
    /// 新节点准入（v0.6 P1）：bootstrap 令牌 + 硬件指纹双验证。
    Admission {
        /// 打印准入状态与本机硬件指纹（默认行为）。
        #[arg(long)]
        status: bool,
        /// 启用准入（必须可用的准入令牌：--boot 或既有配置）。
        #[arg(long)]
        enable: bool,
        /// 准入令牌（集群侧统一发放；同 cluster_token 的分发语义）。
        #[arg(long)]
        boot: Option<String>,
        /// 把给定硬件指纹加入白名单（可重复 --allow）。
        #[arg(long)]
        allow: Vec<String>,
        /// 把给定硬件指纹移出白名单（可重复 --revoke）。
        #[arg(long)]
        revoke: Vec<String>,
        /// 关闭准入（写回 enabled=false）。
        #[arg(long)]
        disable: bool,
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

/// Load configuration, optionally creating it on first run.
fn load_config(path: &Path, create_if_missing: bool) -> Result<Config> {
    if !path.exists() {
        if create_if_missing {
            let cfg = Config::generate(None, None, None);
            cfg.save(path)
                .with_context(|| format!("failed to write config to {}", path.display()))?;
            println!("created new config: {}", path.display());
            return Ok(cfg);
        }
        bail!(
            "config file not found: {}\nrun `agentlink init` to create one",
            path.display()
        );
    }
    Config::load(path).with_context(|| format!("failed to load config {}", path.display()))
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

/// Generate a fresh config, but when an old config exists PRESERVE the
/// machine identity (node_id/name/tokens/data_dir). Rationale: `init/join
/// --force` was wiping node_id silently — after a reboot with autostart
/// pointing at a different folder, whole nodes "became different machines"
/// (incident 2026-10-13, node .7). Identity is what the mesh addresses by;
/// regenerating it is almost never what the user means by re-init.
fn generate_preserving_identity(
    path: &std::path::Path,
    name: Option<String>,
    cluster_token: Option<String>,
    mode: Option<String>,
) -> Config {
    let mut cfg = Config::generate(name.clone(), cluster_token, mode);
    if let Ok(old) = Config::load(path) {
        cfg.node_id = old.node_id;
        if name.is_none() {
            cfg.name = old.name;
        }
        cfg.local_api.token = old.local_api.token;
        cfg.data_dir = old.data_dir; // never orphan the inbox/agents/files
        if cfg.security.cluster_token.is_empty() {
            cfg.security.cluster_token = old.security.cluster_token;
        }
    }
    cfg
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Init {
            force,
            name,
            cluster_token,
            open,
        }) => {
            if cli.config.exists() && !force {
                bail!(
                    "config already exists: {} (use --force to overwrite)",
                    cli.config.display()
                );
            }
            let mode = open.then(|| "open".to_string());
            let cfg = generate_preserving_identity(&cli.config, name, cluster_token, mode);
            cfg.save(&cli.config)?;
            println!("created config: {}", cli.config.display());
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
            admission,
        }) => {
            if cli.config.exists() && !force {
                bail!(
                    "config already exists: {} (use --force to overwrite)",
                    cli.config.display()
                );
            }
            let mode = open.then(|| "open".to_string());
            let mut cfg = generate_preserving_identity(&cli.config, name, Some(token), mode);
            cfg.save(&cli.config)?;
            if let Some(boot) = admission {
                if boot.trim().is_empty() {
                    bail!("--admission 准入令牌不能为空");
                }
                cfg.security.admission.bootstrap_token = boot;
                cfg.save(&cli.config)?;
            }
            println!("paired: {} ({})", cfg.name, cfg.node_id);
            println!("  cluster_token set ({} mode)", cfg.security.mode);
            if cfg.security.admission.boot_token_configured() {
                println!(
                    "  admission bootstrap token 已记录（本机指纹: {}）",
                    admission::hardware_fingerprint()
                );
            }
            println!("\nrun `agentlink run` to start");
            Ok(())
        }

        Some(Command::Token) => {
            let cfg = load_config(&cli.config, false)?;
            println!("{}", cfg.security.cluster_token);
            Ok(())
        }

        Some(Command::Peers) => {
            let cfg = load_config(&cli.config, false)?;
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
            let mut cfg = load_config(&cli.config, create_if_missing)?;
            if let Some(fmt) = cli.log_format.clone() {
                cfg.log.format = fmt;
            }
            logging::init(&cfg.log);
            ensure_dirs(&cfg)?;
            let cfg_path = cli.config.clone();
            run(cfg, cfg_path, no_open).await
        }
        Some(Command::Autostart { remove, force }) => autostart(&cli.config, remove, force),

        Some(Command::Get(what)) => {
            cli::get(what, cli.url.as_deref(), cli.token.as_deref(), &cli.config).await
        }
        Some(Command::Logs { opts }) => {
            cli::logs(opts, cli.url.as_deref(), cli.token.as_deref(), &cli.config).await
        }
        Some(Command::Apply { opts }) => {
            cli::apply(opts, cli.url.as_deref(), cli.token.as_deref(), &cli.config).await
        }
        Some(Command::Setup { yes }) => setup_flow(&cli.config, cli.log_format.clone(), yes).await,
        Some(Command::Hello { opts }) => {
            cli::hello(opts, cli.url.as_deref(), cli.token.as_deref(), &cli.config).await
        }
        Some(Command::Mtls {
            enable,
            disable,
            show,
            rotate,
            member,
            authority,
            require,
        }) => mtls_cmd(
            &cli.config,
            enable,
            disable,
            show,
            rotate,
            member,
            authority,
            require,
        ),
        Some(Command::Admission {
            status,
            enable,
            boot,
            allow,
            revoke,
            disable,
        }) => admission_cmd(&cli.config, status, enable, boot, allow, revoke, disable),
        Some(Command::Top { watch }) => {
            cli::top(cli.url.as_deref(), cli.token.as_deref(), &cli.config, watch).await
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
            if !cli.config.exists() && wizard::is_interactive() {
                return setup_flow(&cli.config, cli.log_format.clone(), false).await;
            }
            let mut cfg = load_config(&cli.config, false)?;
            if let Some(fmt) = cli.log_format.clone() {
                cfg.log.format = fmt;
            }
            logging::init(&cfg.log);
            ensure_dirs(&cfg)?;
            let cfg_path = cli.config.clone();
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
        generate_preserving_identity(config_path, plan.name, plan.cluster_token, plan.mode);
    if let Some(port) = plan.listen_port {
        cfg.listen_port = port;
    }
    cfg.save(config_path)?;
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

/// `agentlink mtls`：一键启用/查看/轮转内置 CA 与本机节点证书（v0.6 P1）。
#[allow(clippy::too_many_arguments)]
fn mtls_cmd(
    config_path: &Path,
    enable: bool,
    disable: bool,
    show: bool,
    rotate: bool,
    member: bool,
    authority: bool,
    require: bool,
) -> Result<()> {
    if enable && disable {
        bail!("--enable 与 --disable 不能同时使用");
    }
    if member && authority {
        bail!("--member 与 --authority 不能同时使用");
    }
    // `--show` 是默认行为（无任何标志时也打印状态），仅为显式表达保留。
    let _ = show;
    let mut cfg = load_config(config_path, false)?;
    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("failed to create data dir {}", cfg.data_dir.display()))?;

    // 角色：--member 置 create_ca=false（领取者），--authority 置 true（默认）。
    let mut changed = false;
    if member {
        cfg.mtls.create_ca = false;
        changed = true;
    } else if authority {
        cfg.mtls.create_ca = true;
        changed = true;
    }
    if enable {
        cfg.mtls.enabled = true;
        changed = true;
    } else if disable {
        cfg.mtls.enabled = false;
        changed = true;
    }
    if require {
        // clap `requires = "enable"` 已保证随 --enable；但既有配置可能在
        // disabled 态改过 require——这里仍写值并提示其生效前提。
        cfg.mtls.require = true;
        changed = true;
        if !cfg.mtls.enabled {
            println!("注意: mtls.require 仅在 mtls.enabled=true 时生效（当前 disabled）");
        }
    }
    if changed {
        cfg.save(config_path)?;
    }

    let mut manager =
        mtls::MtlsManager::load_or_create_opt(&cfg.mtls, &cfg.data_dir, cfg.mtls.create_ca)?;
    if rotate {
        match &mut manager {
            Some(m) => {
                m.rotate_node_cert()?;
                println!("已轮转节点证书（CA 不变）");
            }
            None => bail!("本机尚无集群 CA（成员节点待领取），无法轮转"),
        }
    }

    // 未指定任何动作标志时等价于 --show。
    print_mtls_status(&cfg, manager.as_ref(), config_path)?;
    Ok(())
}

/// 打印 mTLS 当前状态与证书信息。
fn print_mtls_status(
    cfg: &Config,
    manager: Option<&mtls::MtlsManager>,
    config_path: &Path,
) -> Result<()> {
    println!(
        "mTLS: {}",
        if cfg.mtls.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!("  config:      {}", config_path.display());
    println!(
        "  role:        {}",
        if cfg.mtls.create_ca {
            "authority（可建 CA）"
        } else {
            "member（领取集群 CA）"
        }
    );
    println!(
        "  transport:   {}",
        if cfg.mtls.require_enforced() {
            "require（强制 wss，明文对等被拒）"
        } else {
            "opportunistic（对端支持则 wss，否则明文）"
        }
    );
    println!("  ca_sync:     {}", cfg.mtls.ca_sync);
    match manager {
        Some(m) => {
            let paths = m.paths();
            println!(
                "  CA cert:     {}  sha256={}",
                paths.ca_cert.display(),
                m.ca_fingerprint()?
            );
            println!("  CA key:      {}", paths.ca_key.display());
            println!(
                "  node cert:   {}  sha256={}",
                paths.node_cert.display(),
                m.node_fingerprint()?
            );
            println!("  node key:    {}", paths.node_key.display());
            println!("  node expiry: {}", format_unix(m.node_expiry_unix()));
            println!(
                "  ttl:         {} 天（轮转窗口 {}s）",
                cfg.mtls.cert_ttl_days, cfg.mtls.cert_rotate_interval_seconds
            );
        }
        None => {
            println!("  CA:          未领取（成员节点，节点启动后经已认证链路自动领取）");
        }
    }
    if !cfg.mtls.enabled {
        println!("\n开启: agentlink mtls --enable  （成员节点: agentlink mtls --enable --member）");
    } else if manager.is_none() {
        println!("\n启动节点即自动领取集群 CA: agentlink run");
    }
    Ok(())
}

/// 把 Unix 秒格式化成 RFC3339（失败则回显原始秒）。
fn format_unix(unix: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(unix)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| unix.to_string())
}

/// `agentlink admission`：新节点准入（v0.6 P1）管理——开关/令牌/硬件指纹白名单。
#[allow(clippy::too_many_arguments)]
fn admission_cmd(
    config_path: &Path,
    status: bool,
    enable: bool,
    boot: Option<String>,
    allow: Vec<String>,
    revoke: Vec<String>,
    disable: bool,
) -> Result<()> {
    if enable && disable {
        bail!("--enable 与 --disable 不能同时使用");
    }
    // `status` 是默认行为（无任何标志时也打印），仅为显式表达保留。
    let _ = status;
    let mut cfg = load_config(config_path, false)?;
    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("failed to create data dir {}", cfg.data_dir.display()))?;

    let adm = &mut cfg.security.admission;
    let mut changed = false;
    if let Some(token) = &boot {
        if token.trim().is_empty() {
            bail!("--boot 准入令牌不能为空");
        }
        adm.bootstrap_token = token.clone();
        changed = true;
    }
    if enable {
        if !adm.boot_token_configured() {
            bail!(
                "启用准入必须先配置准入令牌：--boot <TOKEN>\n\
                 （在权威节点生成并随集群令牌一同分发；指纹白名单 --allow 可后补）"
            );
        }
        adm.enabled = true;
        changed = true;
    } else if disable {
        adm.enabled = false;
        changed = true;
    }
    for fp in &allow {
        let fp = fp.trim().to_lowercase();
        if fp.is_empty() {
            bail!("--allow 指纹不能为空");
        }
        if !adm.allowed_hardware.iter().any(|f| f == &fp) {
            adm.allowed_hardware.push(fp);
            changed = true;
        }
    }
    for fp in &revoke {
        let fp = fp.trim().to_lowercase();
        let before = adm.allowed_hardware.len();
        adm.allowed_hardware.retain(|f| f != &fp);
        changed |= adm.allowed_hardware.len() != before;
    }
    if changed {
        cfg.save(config_path)?;
    }

    // 指纹白名单失败行为提示。
    if changed
        && cfg.security.admission.enabled
        && !cfg.security.admission.boot_token_configured()
        && cfg.security.admission.allowed_hardware.is_empty()
    {
        bail!("准入处于无令牌且无白名单的状态：fail-closed，入网会被全拒");
    }

    print_admission_status(&cfg);
    Ok(())
}

/// 打印准入状态（不回显准入令牌本体）。
fn print_admission_status(cfg: &Config) {
    let adm = &cfg.security.admission;
    println!(
        "admission: {}",
        if adm.enabled { "enabled" } else { "disabled" }
    );
    println!("  self hw fp: {}", admission::hardware_fingerprint());
    println!(
        "  bootstrap token: {}",
        if adm.boot_token_configured() {
            "configured"
        } else {
            "未配置"
        }
    );
    println!("  allowed_hardware: {} 个", adm.allowed_hardware.len());
    for fp in &adm.allowed_hardware {
        println!("    {fp}");
    }
    if !adm.enabled {
        println!(
            "\n开启: agentlink admission --enable --boot <TOKEN> [--allow <指纹> ...]\n\
             新机器: agentlink admission --status 查询指纹 → 权威节点 --allow 收录\n\
             加入集群: agentlink join <CLUSTER_TOKEN> --admission <TOKEN>"
        );
    } else if adm.allowed_hardware.is_empty() {
        println!("\n当前为单因素（仅令牌）；补白名单: agentlink admission --allow <指纹>");
    } else if !adm.boot_token_configured() {
        println!("\n当前为单因素（仅指纹白名单）；补令牌: agentlink admission --boot <TOKEN>");
    }
}

async fn run(cfg: Config, config_file: PathBuf, no_open: bool) -> Result<()> {
    let bind = format!("{}:{}", cfg.local_api.host, cfg.listen_port);

    // v0.6 P1 mTLS：开启时装配运行态。权威节点（create_ca=true）就地建/载 CA；
    // 成员节点（create_ca=false）无 CA 时进入「待领取」态，启动后经已认证的对等
    // 链路自动领取集群 CA（CA 自动分发），领取到后热启动 TLS 监听。任一步失败
    // 都只告警、退化明文，不阻断启动（对齐“空态好过崩”）。
    let mut mtls_runtime: Option<Arc<mtls::MtlsRuntime>> = None;
    if cfg.mtls.enabled {
        let manager = match mtls::MtlsManager::load_or_create_opt(
            &cfg.mtls,
            &cfg.data_dir,
            cfg.mtls.create_ca,
        ) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "mtls: certificate setup failed; awaiting CA sync");
                None
            }
        };
        match mtls::MtlsRuntime::new(cfg.mtls.clone(), cfg.data_dir.clone(), manager) {
            Ok(rt) => {
                if rt.is_ready() {
                    tracing::info!(
                        ca_path = %rt.paths().ca_cert.display(),
                        ca_fingerprint = %rt.ca_fingerprint().unwrap_or_default(),
                        mtls_port = cfg.mtls_listen_port(),
                        "mtls: certificates ready"
                    );
                } else {
                    tracing::warn!(
                        mtls_port = cfg.mtls_listen_port(),
                        "mtls: no cluster CA yet; awaiting CA 自动分发 from a peer \
                         (create_ca=false)"
                    );
                }
                mtls_runtime = Some(rt);
            }
            Err(e) => tracing::error!(
                error = %e,
                "mtls: failed to build TLS configs; peer mTLS disabled"
            ),
        }
    }

    let (peer_tx, peer_rx) = tokio::sync::mpsc::channel(256);
    let peers = peer::PeerTable::new();
    let router = Arc::new(router::Router::new(&cfg));
    let files = Arc::new(file::FileManager::new());
    let mut state_inner = AppState::new(
        cfg.clone(),
        config_file,
        peers,
        peer_tx,
        router.clone(),
        files,
    );
    // v0.6 M1 mesh 启动装配（设计 §2.1/§3.2 生效矩阵）：
    // - mesh.enabled=false（默认）→ build_mesh 直接返回 None，引擎态完全
    //   不实例化，行为与 0.5.9 逐字符一致（写面 403、features 不加 engine）；
    // - enabled=true → 验 license（失败只 tracing::error 并退化免费层，
    //   绝不炸进程）→ TaskStore 打开 + rebuild 灌表（引擎休眠：只恢复表，
    //   不起 reconcile loop——loop/总线派发/调度全在 M2/M3 接线）。
    state_inner.mesh = api::build_mesh(&state_inner.config, &state_inner.config_file);
    // v0.6 P1 mTLS：把运行态交给连接层（拨号按需升级 wss://；对等链路触发 CA 领取）。
    state_inner.mtls = mtls_runtime.clone();
    // v0.6 P1 新节点准入：本机硬件指纹（启动时算一次，进程内不变）。
    state_inner.hw_fp = admission::hardware_fingerprint();
    if cfg.security.admission.enabled
        && !cfg.security.admission.boot_token_configured()
        && cfg.security.admission.allowed_hardware.is_empty()
    {
        tracing::warn!(
            "admission: enabled but neither bootstrap token nor allowlist configured; \
             all peer handshakes will be rejected (fail-closed)"
        );
    }
    // v0.6 P1 `mtls.require`：配置矛盾/未就绪早告警（机会式为默认，零迁移）。
    if cfg.mtls.require && !cfg.mtls.enabled {
        tracing::warn!("mtls: require=true without mtls.enabled — inert (opportunistic)");
    } else if cfg.mtls.require_enforced() {
        match &mtls_runtime {
            Some(rt) if rt.is_ready() => {
                tracing::info!("mtls: require mode enforced; peer links must use wss://");
            }
            Some(_) => tracing::warn!(
                "mtls: require mode but no cluster CA yet — this node stays isolated until \
                 a peer distributes the CA; plaintext links are refused by policy"
            ),
            None => tracing::warn!(
                "mtls: require mode set but TLS runtime unavailable — peer links refused"
            ),
        }
    }
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

    // M2 P2-2：mesh 生效时 spawn reconcile loop engine task；默认态（mesh=None）不 spawn，
    // 行为与 0.5.9 逐字符一致（P2-2 验收：mesh_off_no_engine_task）。
    let engine_handle = state
        .mesh
        .as_ref()
        .map(|_| reconcile::Engine::spawn(state.clone(), router.clone(), shutdown_rx.clone()));

    // v0.6 P1 Lease 差异上报：enabled 才 spawn（缺省 false = 零流量、零行为变化）。
    if cfg.lease.enabled {
        lease::spawn(state.clone(), shutdown_rx.clone());
    }
    // v0.6 P1 GitOps 目录驱动：enabled 才 spawn（mesh 未启用时 spawn 内自拒）。
    if cfg.gitops.enabled {
        gitops::spawn(state.clone(), shutdown_rx.clone());
    }

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

    // v0.6 P1 mTLS：对等节点走独立 TLS 监听（本地 API/网页仍在 `bind` 上明文）。
    // 监听任务订阅运行态的「服务端 TLS 配置」：权威节点立即绑定；成员节点在
    // 领取到集群 CA 后热绑定（CA 自动分发），无需重启进程。
    let mtls_task: Option<tokio::task::JoinHandle<()>> = if let Some(rt) = mtls_runtime.clone() {
        let mtls_bind = format!("{}:{}", cfg.local_api.host, cfg.mtls_listen_port());
        match mtls_bind.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                let rx = rt.server_watch();
                // 标记 TLS 通道：`mtls.require` 据此放行 wss://、拒绝明文对等连接。
                let peer_app =
                    api::peer_router(state.clone()).layer(axum::Extension(api::PeerTlsMarker));
                let shutdown = shutdown_rx.clone();
                Some(tokio::spawn(serve_mtls(addr, rx, peer_app, shutdown)))
            }
            Err(e) => {
                tracing::error!(bind = %mtls_bind, error = %e, "mtls: invalid TLS bind address");
                None
            }
        }
    } else {
        None
    };

    tracing::info!(
        version = api::VERSION,
        node_id = %cfg.node_id,
        name = %cfg.name,
        bind = %bind,
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
        if let Some(engine) = engine_handle {
            let _ = engine.await;
        }
        if let Some(mtls) = mtls_task {
            let _ = mtls.await;
        }
        let _ = manager.await;
        let _ = maintenance.await;
    })
    .await;

    tracing::info!("agentlink node stopped");
    Ok(())
}

/// 对等节点 TLS 监听任务（v0.6 P1）：等待运行态提供「服务端 TLS 配置」后绑定。
/// 权威节点启动即可用；成员节点在 CA 自动分发领取到 CA 后热绑定。
async fn serve_mtls(
    addr: std::net::SocketAddr,
    mut rx: tokio::sync::watch::Receiver<Option<Arc<rustls::ServerConfig>>>,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let server_cfg = loop {
        if let Some(cfg) = rx.borrow().clone() {
            break cfg;
        }
        tokio::select! {
            _ = shutdown.changed() => return,
            res = rx.changed() => {
                if res.is_err() {
                    return;
                }
            }
        }
    };
    let tls = axum_server::tls_rustls::RustlsConfig::from_config(server_cfg);
    let handle = axum_server::Handle::new();
    let handle_for_shutdown = handle.clone();
    tokio::spawn(async move {
        let _ = shutdown.changed().await;
        handle_for_shutdown.graceful_shutdown(Some(std::time::Duration::from_secs(2)));
    });
    tracing::info!(bind = %addr, "mtls: peer TLS listener started");
    if let Err(e) = axum_server::bind_rustls(addr, tls)
        .handle(handle)
        .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .await
    {
        tracing::error!(error = %e, "mtls: peer TLS listener stopped");
    }
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
    let script = format!(
        "' AgentLink autostart (installed by `agentlink autostart`)\n\
         On Error Resume Next\n\
         Set fso = CreateObject(\"Scripting.FileSystemObject\")\n\
         Set ws = CreateObject(\"Wscript.Shell\")\n\
         exe = \"{}\"\n\
         cfg = \"{}\"\n\
         If Not fso.FileExists(exe) Then\n\
           exe2 = fso.BuildPath(fso.GetParentFolderName(cfg), \"agentlink.exe\")\n\
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
