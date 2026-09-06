//! `blitz dev`: local development environment in one command.
//!
//! Starts an in-process server (TCP + HTTP bridge), loads
//! `<project>/blitz/functions/*.json` deploy envelopes, and redeploys on save
//! (500ms mtime poll — zero new dependencies, robust over network mounts).
//! Prints addresses, project pins, and per-procedure results; Ctrl-C stops.
//!
//! This prefigures schema-first (`blitz/schema`, `blitz/functions`): the
//! watched directory convention is intentionally boring files on disk.

use anyhow::Result;
use blitz_server::{BlitzServer, ServerConfig};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub struct DevArgs {
    pub dir: PathBuf,
    pub port: u16,
    pub http_port: u16,
    pub procedures: Option<PathBuf>,
}

/// Known file states for change detection.
type Watched = HashMap<PathBuf, Option<SystemTime>>;

pub async fn run_dev(args: DevArgs) -> Result<()> {
    // Project context (optional but informative).
    let project_file = args.dir.join(blitz_project_file());
    if let Ok(text) = std::fs::read_to_string(&project_file) {
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(v) => println!(
                "project: {} (template {} v{}, sdk {} {})",
                args.dir.display(),
                v.get("template").and_then(|t| t.as_str()).unwrap_or("?"),
                v.get("template_version").and_then(|t| t.as_str()).unwrap_or("?"),
                v.get("sdk").and_then(|t| t.as_str()).unwrap_or("?"),
                v.get("sdk_version").and_then(|t| t.as_str()).unwrap_or("?"),
            ),
            Err(e) => eprintln!("warning: {} unreadable ({})", project_file.display(), e),
        }
    } else {
        println!("project: {} (no blitz.project.json — plain directory)", args.dir.display());
    }

    let mut cfg = ServerConfig::default();
    cfg.port = args.port;
    let server = Arc::new(BlitzServer::with_config(cfg));
    server.start().await?;

    // TCP listener (tuned backlog like production serve).
    let sock = tokio::net::TcpSocket::new_v4()?;
    sock.bind(format!("127.0.0.1:{}", args.port).parse()?)?;
    let listener = sock.listen(8192)?;
    let tcp_srv = Arc::clone(&server);
    tokio::spawn(async move {
        if let Err(e) = blitz_server::serve(tcp_srv, listener).await {
            eprintln!("[server] tcp ended: {:#}", e);
        }
    });
    // HTTP bridge (data plane for browsers/curl).
    let http = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", args.http_port)).await?;
    let http_srv = Arc::clone(&server);
    tokio::spawn(async move {
        if let Err(e) = blitz_server::serve_http_ops(http_srv, http).await {
            eprintln!("[server] http ended: {:#}", e);
        }
    });

    println!("BlitzDB dev server on 127.0.0.1:{} (TCP)", args.port);
    println!("HTTP bridge on http://127.0.0.1:{}/v1/op", args.http_port);
    println!("database: in-memory (dev defaults; use --help for data-dir plans)");

    // Procedures directory: explicit flag or <project>/blitz/functions
    // (the same tree `blitz generate` reads — one source of truth).
    let proc_dir = args
        .procedures
        .clone()
        .unwrap_or_else(|| args.dir.join("blitz").join("functions"));
    let mut watched: Watched = HashMap::new();
    if proc_dir.is_dir() {
        println!("watching procedures in {}", proc_dir.display());
        initial_load(&server, &proc_dir, &mut watched);
    } else {
        println!(
            "no functions directory ({} missing) — drop *.json deploy envelopes under blitz/functions/ to auto-deploy",
            proc_dir.display()
        );
    }

    println!("logs follow; Ctrl-C to stop.");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\ndev server stopped.");
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                poll_procedures(&server, &proc_dir, &mut watched);
            }
        }
    }
}

fn blitz_project_file() -> &'static str {
    crate::compat::PROJECT_FILE
}

/// Load every envelope once at startup (errors logged, never fatal).
fn initial_load(server: &Arc<BlitzServer>, dir: &Path, watched: &mut Watched) {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("json"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    for path in files {
        watched.insert(path.clone(), mtime(&path));
        deploy_file(server, &path);
    }
}

/// Redeploy changed files (mtime granularity is enough for saves).
fn poll_procedures(server: &Arc<BlitzServer>, dir: &Path, watched: &mut Watched) {
    if !dir.is_dir() {
        return;
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("json"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    for path in &files {
        let current = mtime(path);
        if watched.get(path) != Some(&current) {
            watched.insert(path.clone(), current);
            deploy_file(server, path);
        }
    }
    // Deletions are noticed but not undeployed (explicit `ProcDrop` stays
    // the removal path — deleting a file must not silently drop live code).
    watched.retain(|p, _| files.contains(p));
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Parse + validate + deploy one envelope file, logging the outcome.
fn deploy_file(server: &Arc<BlitzServer>, path: &Path) {
    let name = path.display().to_string();
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[watch] {}: unreadable ({})", name, e);
            return;
        }
    };
    let functions = match server.functions().read() {
        Ok(g) => g,
        Err(e) => {
            eprintln!("[watch] {}: registry locked ({})", name, e);
            return;
        }
    };
    let proc = match blitz_runtime::deploy_from_json(&text, &functions) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[watch] {}: invalid ({})", name, e);
            return;
        }
    };
    drop(functions);
    let proc_name = proc.name.clone();
    match server.deploy_procedure(proc) {
        Ok(v) => println!("[watch] {}: proc '{}' v{} deployed", name, proc_name, v),
        Err(e) => eprintln!("[watch] {}: rejected ({})", name, e),
    }
}
