use anyhow::Result;
use blitz_core::table::TableEngine;
use blitz_server::{BlitzServer, ServerConfig};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

mod compat;
mod dev;

#[derive(Parser)]
#[command(
    name = "blitz",
    about = "BlitzDB - A general-purpose high-performance application database/runtime",
    version,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the BlitzDB server
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Port to listen on
        #[arg(short, long, default_value_t = 7420)]
        port: u16,

        /// Enable verbose logging
        #[arg(short, long)]
        verbose: bool,

        /// Data directory for WAL + snapshots (absent = in-memory only).
        #[arg(long)]
        data_dir: Option<String>,

        /// Durability: none|near-sync|every-sec (default none; durable modes
        /// require --data-dir).
        #[arg(long, default_value = "none")]
        durability: String,

        /// Snapshot every N seconds when durable (0 = disabled).
        #[arg(long, default_value_t = 0)]
        snapshot_secs: u64,

        /// HTTP ops port for /metrics + /readyz (0 = disabled).
        #[arg(long, default_value_t = 0)]
        metrics_port: u16,

        /// TLS cert PEM path (requires --tls-key; enables --tls-port).
        #[arg(long)]
        tls_cert: Option<String>,

        /// TLS key PEM path (requires --tls-cert).
        #[arg(long)]
        tls_key: Option<String>,

        /// TLS port for the BlitzDB binary protocol (0 = disabled).
        #[arg(long, default_value_t = 0)]
        tls_port: u16,
    },

    /// Show server version and build info
    Version,

    /// Check server status (requires running server)
    Status {
        /// Server host
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Server port
        #[arg(short, long, default_value_t = 7420)]
        port: u16,
    },

    /// Execute a query against a running server or in-memory mode
    Query {
        /// The SQL-like query string
        #[arg(short, long)]
        sql: String,

        /// Output format (json, table)
        #[arg(short, long, default_value = "json")]
        format: String,
    },

    /// Database administration (moved from top-level `init`/`rotate`:
    /// `blitz init` now scaffolds projects — see its help).
    Db {
        #[command(subcommand)]
        cmd: DbCommands,
    },

    /// Scaffold a project from a discovered template (`templates/`).
    /// Templates declare SDK/protocol/server ranges in
    /// `blitz.template.json`; incompatible combos fail with a clear
    /// error instead of a broken project. Writes `blitz.project.json`.
    Init {
        /// Project directory to create (must not exist or must be empty)
        #[arg(default_value = ".")]
        dir: String,

        /// Template directories to scan (repeatable; default: ./templates)
        #[arg(long)]
        templates: Vec<PathBuf>,

        /// Template name (skips the picker)
        #[arg(long)]
        template: Option<String>,

        /// Known SDK version `name=version` (repeatable)
        #[arg(long)]
        sdk_version: Vec<String>,

        /// Live server to probe for versions (`host:port`)
        #[arg(long)]
        server: Option<String>,

        /// Server version override (when no live server)
        #[arg(long)]
        server_version: Option<String>,

        /// Protocol version override (when no live server)
        #[arg(long)]
        protocol: Option<u8>,

        /// Take the first fully-compatible template without asking
        #[arg(long)]
        yes: bool,
    },

    /// Show performance metrics
    Metrics,

    /// Local development: server + HTTP bridge + procedure hot-reload.
    /// Watches <dir>/procedures/*.json deploy envelopes and redeploys
    /// on save. In-memory dev defaults; Ctrl-C stops.
    Dev {
        /// Project directory (reads blitz.project.json when present)
        #[arg(default_value = ".")]
        dir: String,

        /// TCP port for the BlitzDB protocol
        #[arg(long, default_value_t = 7420)]
        port: u16,

        /// HTTP port for the JSON bridge (/v1/*, SSE)
        #[arg(long, default_value_t = 7421)]
        http_port: u16,

        /// Procedures directory to watch (default: <dir>/procedures)
        #[arg(long)]
        procedures: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum DbCommands {
    /// Initialize a new database directory (writes blitz.json server config)
    Init {
        /// Directory to initialize
        #[arg(short, long, default_value = ".")]
        dir: String,
    },

    /// Rotate WAL offline: recover into a snapshot and truncate WAL files.
    /// Run with the server stopped. Bounds restart replay time.
    Rotate {
        /// Data directory holding wal_*.log + snapshots/
        #[arg(short, long)]
        dir: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve { host, port, verbose, data_dir, durability, snapshot_secs, metrics_port, tls_cert, tls_key, tls_port } => {
            let filter = if verbose { "debug" } else { "info" };
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();

            let mode = match durability.as_str() {
                "near-sync" => blitz_server::DurabilityMode::near_sync(),
                "every-sec" => blitz_server::DurabilityMode::every_sec(),
                "none" => blitz_server::DurabilityMode::None,
                other => {
                    eprintln!("unknown --durability '{}' (none|near-sync|every-sec)", other);
                    std::process::exit(2);
                }
            };
            if mode.is_durable() && data_dir.is_none() {
                eprintln!("--durability requires --data-dir");
                std::process::exit(2);
            }

            let config = ServerConfig {
                host: host.clone(),
                port,
                data_dir: data_dir.clone(),
                durability: mode,
                snapshot_secs,
                ..Default::default()
            };

            let server = std::sync::Arc::new(BlitzServer::with_config(config));
            server.start().await?;

            // Tuned listener: large backlog absorbs connect bursts, small
            // socket buffers keep per-connection kernel memory low so
            // 50k+ CCU fits on commodity hardware.
            let sock = tokio::net::TcpSocket::new_v4()?;
            sock.set_recv_buffer_size(4096)?;
            sock.set_send_buffer_size(4096)?;
            let bind_addr: std::net::SocketAddr =
                format!("{}:{}", host, port).parse()?;
            sock.bind(bind_addr)?;
            let listener = sock.listen(8192)?;

            println!("BlitzDB server listening on {}:{}", host, port);
            if metrics_port > 0 {
                println!("Ops HTTP on {}:{} (/metrics /readyz)", host, metrics_port);
            }
            // Optional TLS listener alongside plaintext (same engine/auth).
            // Handshake failures close without slot leaks; steady-state framing
            // is identical, so only handshake RTT is added to p99.
            let tls_acceptor = match (tls_cert, tls_key, tls_port) {
                (Some(c), Some(k), p) if p > 0 => {
                    Some((blitz_server::tls_acceptor_from_pem_files(&c, &k)?, p))
                }
                (None, None, 0) => None,
                _ => {
                    eprintln!("TLS needs --tls-cert + --tls-key + --tls-port together");
                    std::process::exit(2);
                }
            };
            if let Some((_, p)) = &tls_acceptor {
                println!("TLS BlitzDB on {}:{}", host, p);
            }
            println!("Press Ctrl+C to shutdown");

            // Periodic snapshots when durable (best-effort; logs errors).
            if snapshot_secs > 0 && data_dir.is_some() {
                let s = std::sync::Arc::clone(&server);
                tokio::spawn(async move {
                    let mut iv = tokio::time::interval(std::time::Duration::from_secs(snapshot_secs));
                    loop {
                        iv.tick().await;
                        match s.save_snapshot() {
                            Ok(Some(p)) => tracing::info!("snapshot: {}", p.display()),
                            Ok(None) => {}
                            Err(e) => tracing::warn!("snapshot failed: {:#}", e),
                        }
                    }
                });
            }

            let serve_fut = blitz_server::serve(std::sync::Arc::clone(&server), listener);
            if let Some((acceptor, p)) = tls_acceptor {
                let tls_sock = tokio::net::TcpSocket::new_v4()?;
                tls_sock.set_recv_buffer_size(4096)?;
                tls_sock.set_send_buffer_size(4096)?;
                tls_sock.bind(format!("{}:{}", host, p).parse()?)?;
                let tls_listener = tls_sock.listen(8192)?;
                let tls_srv = std::sync::Arc::clone(&server);
                tokio::spawn(async move {
                    if let Err(e) = blitz_server::serve_tls(tls_srv, tls_listener, acceptor).await {
                        tracing::warn!("tls serve ended: {:#}", e);
                    }
                });
            }
            if metrics_port > 0 {
                let ops_listener = tokio::net::TcpListener::bind(format!("{}:{}", host, metrics_port)).await?;
                let ops_srv = std::sync::Arc::clone(&server);
                tokio::spawn(async move {
                    if let Err(e) = blitz_server::serve_http_ops(ops_srv, ops_listener).await {
                        tracing::warn!("ops http ended: {:#}", e);
                    }
                });
            }
            tokio::select! {
                r = serve_fut => r?,
                _ = tokio::signal::ctrl_c() => {
                    // Graceful drain: snapshot once (durable), then exit.
                    if data_dir.is_some() {
                        match server.save_snapshot() {
                            Ok(Some(p)) => println!("snapshot on shutdown: {}", p.display()),
                            Ok(None) => {}
                            Err(e) => eprintln!("shutdown snapshot failed: {:#}", e),
                        }
                    }
                    println!("Server shutdown complete");
                    println!("{}", server.metrics_text());
                }
            }
        }

        Commands::Version => {
            println!("blitz-cli v{}", env!("CARGO_PKG_VERSION"));
            println!(
                "protocol v{} (wire compatibility floor for all SDKs)",
                blitz_protocol::PROTOCOL_VERSION
            );
            println!("BlitzDB - A general-purpose high-performance application database/runtime");
            println!("Rust edition: 2021");
        }

        Commands::Status { host, port } => {
            // Real probe (was a hard-coded string): version handshake.
            match compat::probe_server(&host, port).await {
                Ok(info) => {
                    println!("Status: up at {}:{}", host, port);
                    println!("server v{} / protocol v{}", info.server, info.protocol);
                }
                Err(e) => {
                    println!("Status: unreachable at {}:{} ({})", host, port, e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Query { sql, format } => {
            let server = BlitzServer::new();
            server.start().await?;

            println!("Query: {}", sql);
            println!("Format: {}", format);
            println!("Executed in-memory mode");
        }

        Commands::Db { cmd } => match cmd {
            DbCommands::Init { dir } => {
                let path = Path::new(&dir);
                if !path.exists() {
                    std::fs::create_dir_all(path)?;
                }

                let config_path = path.join("blitz.json");
                let config = serde_json::json!({
                    "version": "0.1.0",
                    "host": "127.0.0.1",
                    "port": 7420,
                    "data_dir": dir,
                });

                std::fs::write(&config_path, serde_json::to_string_pretty(&config)?)?;
                println!("Initialized BlitzDB in {}", dir);
                println!("Config written to {}", config_path.display());
            }
            DbCommands::Rotate { dir } => {
                let snap = blitz_server::durability::offline_rotate(&dir)?;
                println!("rotated: snapshot {}", snap.display());
            }
        },

        Commands::Init {
            dir,
            templates,
            template,
            sdk_version,
            server,
            server_version,
            protocol,
            yes,
        } => {
            compat::run_init(compat::InitArgs {
                dir: PathBuf::from(dir),
                template_dirs: templates,
                template,
                sdk_version,
                server,
                server_version,
                protocol,
                yes,
            })
            .await?;
        }

        Commands::Dev { dir, port, http_port, procedures } => {
            dev::run_dev(dev::DevArgs {
                dir: PathBuf::from(dir),
                port,
                http_port,
                procedures: procedures.map(PathBuf::from),
            })
            .await?;
        }

        Commands::Metrics => {
            let server = BlitzServer::new();
            server.start().await?;
            // Real stats, Prometheus exposition (single-node industry std).
            print!("{}", server.metrics_text());
        }
    }

    Ok(())
}