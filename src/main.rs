//! AgentLink - a tiny LAN message bus for AI agents.
//!
//! Each machine runs one `agentlink` node. Agents on that machine talk to it
//! over the local HTTP/WS API; nodes talk to each other over WebSocket.

mod api;
mod config;
mod conn;
mod discovery;
mod file;
mod logging;
#[cfg(feature = "mdns")]
mod mdns;
mod peer;
mod proto;
mod router;

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
    },
    /// List known peers.
    Peers,
    /// Print the version.
    Version,
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
    std::fs::create_dir_all(&cfg.files.recv_dir)
        .with_context(|| format!("failed to create recv dir {}", cfg.files.recv_dir.display()))?;
    Ok(())
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
            let cfg = Config::generate(name, cluster_token, mode);
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
            let cfg = Config::generate(name, Some(token), mode);
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

        Some(Command::Run { create_if_missing }) => {
            let cfg = load_config(&cli.config, create_if_missing)?;
            logging::init(&cfg.log);
            ensure_dirs(&cfg)?;
            run(cfg).await
        }

        None => {
            let cfg = load_config(&cli.config, false)?;
            logging::init(&cfg.log);
            ensure_dirs(&cfg)?;
            run(cfg).await
        }
    }
}

async fn run(cfg: Config) -> Result<()> {
    let bind = format!("{}:{}", cfg.local_api.host, cfg.listen_port);

    let (peer_tx, peer_rx) = tokio::sync::mpsc::channel(256);
    let peers = peer::PeerTable::new();
    let router = Arc::new(router::Router::new(&cfg));
    let files = Arc::new(file::FileManager::new());
    let state = Arc::new(AppState::new(
        cfg.clone(),
        peers,
        peer_tx,
        router.clone(),
        files,
    ));
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
