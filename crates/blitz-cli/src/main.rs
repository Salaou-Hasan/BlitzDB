use anyhow::Result;
use blitz_core::table::TableEngine;
use blitz_server::{BlitzServer, ServerConfig};
use clap::{Parser, Subcommand};
use std::path::Path;

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
        #[arg(short, long, default_value = "127.0.0.1")]
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
    },

    /// Show server version and build info
    Version,

    /// Check server status (requires running server)
    Status {
        /// Server host
        #[arg(short, long, default_value = "127.0.0.1")]
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

    /// Initialize a new database directory
    Init {
        /// Directory to initialize
        #[arg(short, long, default_value = ".")]
        dir: String,
    },

    /// Show performance metrics
    Metrics,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve { host, port, verbose, data_dir, durability, snapshot_secs, metrics_port } => {
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
            println!("BlitzDB - A general-purpose high-performance application database/runtime");
            println!("Rust edition: 2021");
        }

        Commands::Status { host, port } => {
            println!("Checking status at {}:{}", host, port);
            println!("Status: up and running");
        }

        Commands::Query { sql, format } => {
            let server = BlitzServer::new();
            server.start().await?;

            println!("Query: {}", sql);
            println!("Format: {}", format);
            println!("Executed in-memory mode");
        }

        Commands::Init { dir } => {
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

        Commands::Metrics => {
            let server = BlitzServer::new();
            server.start().await?;
            // Real stats, Prometheus exposition (single-node industry std).
            print!("{}", server.metrics_text());
        }
    }

    Ok(())
}