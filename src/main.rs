//! AgentLink - a tiny LAN message bus for AI agents.
//!
//! Each machine runs one `agentlink` node. Agents on that machine talk to it
//! over the local HTTP/WS API; nodes talk to each other over WebSocket.

mod api;
mod cli;
mod config;
mod conn;
mod discovery;
mod file;
mod license;
mod logging;
#[cfg(feature = "mdns")]
mod mdns;
mod peer;
mod proto;
mod redact;
mod router;
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
        }) => {
            if cli.config.exists() && !force {
                bail!(
                    "config already exists: {} (use --force to overwrite)",
                    cli.config.display()
                );
            }
            let mode = open.then(|| "open".to_string());
            let cfg = generate_preserving_identity(&cli.config, name, Some(token), mode);
            cfg.save(&cli.config)?;
            println!("paired: {} ({})", cfg.name, cfg.node_id);
            println!("  cluster_token set ({} mode)", cfg.security.mode);
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

async fn run(cfg: Config, config_file: PathBuf, no_open: bool) -> Result<()> {
    let bind = format!("{}:{}", cfg.local_api.host, cfg.listen_port);

    let (peer_tx, peer_rx) = tokio::sync::mpsc::channel(256);
    let peers = peer::PeerTable::new();
    let router = Arc::new(router::Router::new(&cfg));
    let files = Arc::new(file::FileManager::new());
    let state = Arc::new(AppState::new(
        cfg.clone(),
        config_file,
        peers,
        peer_tx,
        router.clone(),
        files,
    ));
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
