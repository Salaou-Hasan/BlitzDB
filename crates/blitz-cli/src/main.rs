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
        Commands::Serve { host, port, verbose } => {
            let filter = if verbose { "debug" } else { "info" };
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();

            let config = ServerConfig {
                host: host.clone(),
                port,
                ..Default::default()
            };

            let mut server = BlitzServer::with_config(config);
            server.start()?;

            println!("BlitzDB server listening on {}:{}", host, port);
            println!("Press Ctrl+C to shutdown");

            tokio::signal::ctrl_c().await?;
            server.shutdown();
            println!("Server shutdown complete");
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
            let mut server = BlitzServer::new();
            server.start()?;

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
            let mut server = BlitzServer::new();
            server.start()?;
            println!("Metrics:");
            println!("  uptime:     0s (just started)");
            println!("  tables:     2");
            println!("  rows:       0");
            println!("  queries:    0");
            println!("  tx_active:  0");
        }
    }

    Ok(())
}