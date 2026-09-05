//! Single-node social primitives (NOT multi-node fanout).
//!
//! - Async best-effort fanout: post inserts enqueue a job; a worker
//!   materializes `timeline` rows for followers. Bounded queue (drop+count),
//!   capped 200 followers/post. Beyond that (celebrity scale) the reader
//!   falls back to pull (`Get`/cursor-`Scan`); true fanout-out is Stage 9+.
//! - Exact-term bounded search index over post bodies (8 terms/post,
//!   128/posting). No TF-IDF/ranking — Stage 9+.
//! - Derived `timeline` rows are WAL-logged like any write (replay restores
//!   them); snapshots capture them too. No silent second state.

use std::sync::Arc;

use blitz_core::TableEngine;
use blitz_types::column::{ColumnDef, ColumnType};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::schema::TableSchema;
use blitz_types::value::Value;

use crate::server::BlitzServer;

/// One post awaiting fanout.
#[derive(Debug, Clone)]
pub struct FanoutJob {
    pub table: String,
    pub row_id: u64,
    pub author: String,
}

/// Cloneable fanout ingress (Send+Sync). Bounded; full → drop+count.
#[derive(Clone)]
pub struct FanoutSender {
    pub tx: std::sync::mpsc::SyncSender<FanoutJob>,
}

pub const FANOUT_QUEUE: usize = 4096;
pub const FANOUT_PER_POST_CAP: usize = 200;

/// Lowercase alphanumeric terms, max 8 per post.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            cur.push(ch.to_ascii_lowercase());
            if cur.len() >= 24 {
                terms.push(std::mem::take(&mut cur));
                if terms.len() >= 8 {
                    return terms;
                }
            }
        } else if !cur.is_empty() {
            terms.push(std::mem::take(&mut cur));
            if terms.len() >= 8 {
                return terms;
            }
        }
    }
    if !cur.is_empty() && terms.len() < 8 {
        terms.push(cur);
    }
    terms
}

fn timeline_schema() -> TableSchema {
    TableSchema::new("timeline")
        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
        .with_column(ColumnDef::new("owner", ColumnType::String).nullable())
        .with_column(ColumnDef::new("post", ColumnType::String).nullable())
        .with_column(ColumnDef::new("author", ColumnType::String).nullable())
        .with_column(ColumnDef::new("ts", ColumnType::String).nullable())
}

/// Install the fanout channel on the server and return the worker receiver.
/// Call once; spawn `run_fanout_loop(server, rx)` via `spawn_blocking`.
pub fn install_fanout(server: &Arc<BlitzServer>) -> std::sync::mpsc::Receiver<FanoutJob> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<FanoutJob>(FANOUT_QUEUE);
    server.install_fanout_channel(FanoutSender { tx });
    rx
}

/// Worker body (blocking): materialize timelines. Best-effort; lag drops.
pub fn run_fanout_loop(server: Arc<BlitzServer>, rx: std::sync::mpsc::Receiver<FanoutJob>) {
    // Follows tables known by prefix (sharded benches use follows_XX).
    let follow_tables = || -> Vec<String> {
        let mut v: Vec<String> = server
            .engine()
            .table_names()
            .into_iter()
            .filter(|t| t == "follows" || t.starts_with("follows_"))
            .collect();
        v.sort();
        v
    };
    let _ = server.engine().create_table(timeline_schema());
    while let Ok(job) = rx.recv() {
        // Collect follower owner-ids with "to" == author, capped.
        let mut owners: Vec<String> = Vec::new();
        for ft in follow_tables() {
            if owners.len() >= FANOUT_PER_POST_CAP {
                break;
            }
            let rows = server.engine().scan_arcs(&ft).unwrap_or_default();
            for r in rows {
                if owners.len() >= FANOUT_PER_POST_CAP {
                    break;
                }
                let to = r.values.get("to");
                let from = r.values.get("from");
                if to == Some(&Value::String(job.author.clone())) {
                    if let Some(Value::String(f)) = from {
                        owners.push(f.clone());
                    }
                }
            }
        }
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros().to_string())
            .unwrap_or_default();
        let mut done = 0u64;
        for owner in owners {
            let mut values = std::collections::HashMap::new();
            values.insert("owner".to_string(), Value::String(owner));
            values.insert("post".to_string(), Value::String(format!("{}:{}", job.table, job.row_id)));
            values.insert("author".to_string(), Value::String(job.author.clone()));
            values.insert("ts".to_string(), Value::String(ts.clone()));
            let data = crate::durability::values_to_json_bytes(&values);
            let mut row = Row::new(RowId::new(0));
            for (k, v) in &values {
                row.set(k.clone(), v.clone());
            }
            // Derived-but-durable: feed rows are WAL-logged so a crash
            // can't silently diverge timelines from posts. Replay restores
            // them (unknown tables get inferred schemas); postings stay
            // bounded because fanout itself is capped per post.
            match server.engine().insert("timeline", row) {
                Ok(assigned) => {
                    // Best-effort post-commit (worker context can't roll
                    // back): failure bumps wal_dropped, row stays until
                    // the next snapshot — same contract as atomic writes.
                    let _ = server.wal_log(
                        blitz_wal::EntryType::Insert,
                        "timeline",
                        assigned.as_u64(),
                        data,
                    );
                    done += 1;
                }
                Err(_) => {}
            }
        }
        server.fanout_record_done(done);
    }
}
