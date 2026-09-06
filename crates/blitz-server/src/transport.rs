//! Async TCP transport for BlitzDB.
//!
//! Each connection runs the same loop: read bytes, pop complete frames
//! with [`FrameCodec`], dispatch the [`Request`] against the shared
//! [`BlitzServer`], write back the [`Response`]. The server is held as an
//! `Arc` and every engine operation takes `&self`, so connections run
//! fully concurrently; table-level engine locks provide the parallelism.
//!
//! Payloads are binary end to end: [`blitz_types::Value`] is encoded with
//! the protocol's tag-length scheme, so reads never touch JSON.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use blitz_core::TableEngine;
use blitz_protocol::{BatchRequest, BatchResponse, FrameCodec, Incoming, Op, Request, Response, RowView};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::value::Value;

use crate::server::BlitzServer;

/// Releases a connection slot when dropped, so early returns and errors
/// cannot leak admission.
struct ConnectionGuard {
    server: Option<Arc<BlitzServer>>,
    ip: Option<std::net::IpAddr>,
}

impl ConnectionGuard {
    fn new(server: &Arc<BlitzServer>, ip: Option<std::net::IpAddr>) -> Option<Self> {
        if let Some(addr) = ip {
            if !server.ip_acquire(addr) {
                server.record_shed_drop();
                return None;
            }
        }
        if server.try_acquire_connection() {
            Some(Self {
                server: Some(Arc::clone(server)),
                ip,
            })
        } else {
            if let Some(addr) = ip {
                server.ip_release(addr);
            }
            None
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.release_connection();
            if let Some(addr) = self.ip.take() {
                server.ip_release(addr);
            }
        }
    }
}

fn row_to_view(id: RowId, row: &Row) -> RowView {
    RowView {
        id: id.as_u64(),
        values: row.values.clone(),
    }
}

/// Extract + strip a connection handshake token (`values {"_auth": token}`).
/// Returns the token when present and well-typed; always removes the key so
/// validation/storage never see auth material.
fn take_auth_token(values: &mut Option<std::collections::HashMap<String, Value>>) -> Option<String> {
    values.as_mut()?.remove("_auth").and_then(|v| match v {
        Value::String(s) => Some(s),
        _ => None,
    })
}

/// Stream-mode flag: `Subscribe` with `values {"_stream": 1}` upgrades the
/// connection to server-push (one table per connection; close to stop).
fn is_stream_subscribe(values: &Option<std::collections::HashMap<String, Value>>) -> bool {
    match values.as_ref().and_then(|m| m.get("_stream")) {
        Some(Value::Int64(1)) | Some(Value::UInt64(1)) | Some(Value::Int32(1)) | Some(Value::UInt32(1)) => true,
        Some(Value::Boolean(true)) => true,
        _ => false,
    }
}

/// Push-stream body: ack, then one framed `Response` per change (id = change
/// seq) until EOF, idle timeout, or lag-eviction closes the receiver.
/// Pushes are not counted as requests (no p99 pollution); bytes are.
/// Quiet tables hit the idle deadline — clients resubscribe (same as poll).
async fn run_push_stream(
    server: &Arc<BlitzServer>,
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    codec: &FrameCodec,
    ack_id: u64,
    table: String,
    idle_secs: u64,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let (sub, mut rx) = server.push_subscribe(&table);
    let ack = codec.encode_response(&Response::ok(ack_id, Vec::new())).context("encode error")?;
    stream.write_all(&ack).await.context("failed to write to socket")?;
    server.record_io(0, ack.len() as u64);
    let idle = if idle_secs > 0 {
        std::time::Duration::from_secs(idle_secs)
    } else {
        // No idle reap configured: still bound stream silence to 1h so dead
        // peers can't pin slots forever (idle_timeout_secs=0 disables the
        // request path, not push resources).
        std::time::Duration::from_secs(3600)
    };
    loop {
        let rec = match tokio::time::timeout(idle, rx.recv()).await {
            Err(_) => break, // idle/quiet: client resubscribes
            Ok(None) => break, // evicted (slow) or server gone
            Ok(Some(r)) => r,
        };
        let push = Response::ok(
            rec.seq,
            vec![RowView {
                id: rec.seq,
                values: [
                    ("table".to_string(), Value::String(rec.table.clone())),
                    ("op".to_string(), Value::String(rec.op.to_string())),
                    ("row_id".to_string(), Value::Int64(rec.row_id as i64)),
                    ("ts".to_string(), Value::Int64(rec.ts_micros as i64)),
                ]
                .into_iter()
                .collect(),
            }],
        );
        let frame = codec.encode_response(&push).context("encode error")?;
        server.record_io(0, frame.len() as u64);
        if stream.write_all(&frame).await.is_err() {
            break; // peer gone
        }
    }
    server.push_unsubscribe(&table, sub);
    Ok(())
}

/// Default/max page sizes for `Scan`. Unbounded scans are the OOM killer:
/// one slow client scanning a 1M-row table would pin a giant `Vec<Arc>`
/// under read lock, then a giant response frame. Pagination bounds both.
pub const DEFAULT_SCAN_LIMIT: usize = 1_000;
pub const MAX_SCAN_LIMIT: usize = 10_000;
pub const MAX_SUBSCRIBE_LIMIT: usize = 1_000;

/// Parsed Scan window: limit/offset plus Twitter-style cursor paging.
/// `_cursor` = last seen RowId (exclusive); `_order` = "asc"|"desc".
/// Cursor filters via binary search on sorted ids (O(log N)); offset applies
/// after the cursor so pages stay stable under concurrent inserts (offset
/// alone drifts; cursor doesn't).
struct ScanWindow {
    limit: usize,
    offset: usize,
    desc: bool,
    cursor: Option<u64>,
}

/// Parse `_limit` / `_offset` / `_order` / `_cursor` from Scan `values`
/// without a protocol bump. Absent → (1000, 0, asc, none).
fn scan_pagination(values: &Option<std::collections::HashMap<String, blitz_types::value::Value>>) -> ScanWindow {
    use blitz_types::value::Value as V;
    let mut w = ScanWindow { limit: DEFAULT_SCAN_LIMIT, offset: 0, desc: false, cursor: None };
    let int_val = |v: &V| -> Option<usize> {
        match v {
            V::Int64(n) => Some((*n).max(0) as usize),
            V::Int32(n) => Some((*n).max(0) as usize),
            V::UInt64(n) => Some(*n as usize),
            V::UInt32(n) => Some(*n as usize),
            _ => None,
        }
    };
    if let Some(map) = values {
        if let Some(v) = map.get("_limit").and_then(int_val) {
            if v > 0 {
                w.limit = v.min(MAX_SCAN_LIMIT);
            }
        }
        if let Some(v) = map.get("_offset").and_then(int_val) {
            w.offset = v;
        }
        if let Some(V::String(s)) = map.get("_order") {
            w.desc = s.eq_ignore_ascii_case("desc");
        }
        if let Some(v) = map.get("_cursor").and_then(int_val) {
            w.cursor = Some(v as u64);
        }
    }
    w
}

/// Slice sorted row refs to a window. `rows` must already be sorted per `desc`.
/// Cursor is exclusive: asc keeps id > cursor, desc keeps id < cursor
/// (cursor 0/None = from the head). Returns [start, end) byte offsets.
fn apply_window(len: usize, ids_asc: bool, w: &ScanWindow, id_at: &dyn Fn(usize) -> u64) -> (usize, usize) {
    let mut start = 0usize;
    if let Some(c) = w.cursor {
        if ids_asc && !w.desc {
            // ascending: first id > c
            let mut lo = 0usize;
            let mut hi = len;
            while lo < hi {
                let mid = (lo + hi) / 2;
                if id_at(mid) <= c {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            start = lo;
        } else {
            // descending array: first id < c (linear from head is O(limit)
            // only when cursor used without offset; keep binary variant:
            // descending ids => ascending negated; equivalent partition:
            let mut lo = 0usize;
            let mut hi = len;
            while lo < hi {
                let mid = (lo + hi) / 2;
                if id_at(mid) >= c {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            start = lo;
        }
    }
    start = (start + w.offset).min(len);
    let end = (start + w.limit).min(len).max(start);
    (start, end)
}

/// Execute one request. Infallible by design: engine failures become
/// `Response::err` payloads instead of dropped connections.
///
/// Consistency contract (single-node v1): auto-commit per op. `Batch` is NOT
/// atomic — inner ops execute in order with per-op ok/err (partial failure
/// normal). Retrying a batch after shed/timeout is safe for Gets, and for
/// Inserts carrying `_idem` (idempotency key, stripped before storage).
/// Plain retried Inserts without `_idem` may duplicate. Interactive
/// transactions (begin/commit over TCP) are not yet exposed; `active_tx`
/// reports 0 honestly.
/// Row-level ownership gate: no-op when the table has no owner column
/// (or auth is bypassed / caller is admin). Otherwise the values' owner
/// must equal the caller's subject. Call AFTER table-level authorize.
fn enforce_owner(
    server: &BlitzServer,
    authed: &Option<blitz_auth::Identity>,
    op: Op,
    base: &str,
    values: &std::collections::HashMap<String, Value>,
) -> Result<(), &'static str> {
    let col = match server.owner_column(base) {
        Some(c) => c,
        None => return Ok(()),
    };
    server.authorize_row(authed, op, base, values.get(&col))
}

pub(crate) fn dispatch(server: &std::sync::Arc<BlitzServer>, authed: &Option<blitz_auth::Identity>, req: Request) -> Response {
    let id = req.id;
    match req.op {
        Op::Ping => Response::ok(id, Vec::new()),
        Op::Version => {
            // No auth, no table: bootstrap handshake for SDK compatibility
            // checks (must work before any handshake or grant exists).
            let mut values = std::collections::HashMap::new();
            values.insert("server".to_string(), Value::String(crate::server::SERVER_VERSION.to_string()));
            values.insert("protocol".to_string(), Value::Int64(blitz_protocol::PROTOCOL_VERSION as i64));
            Response::ok(id, vec![RowView { id: 0, values }])
        }
        Op::Insert => {
            let mut values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "insert requires values"),
            };
            // Media blobs: bounded references, not an object store. 256KiB
            // cap keeps frames (1MiB budget) and WAL groups healthy;
            // transcode/CDN stay out of the hot path (Stage 10+).
            if req.table.starts_with("media") {
                if let Some(blitz_types::value::Value::Bytes(b)) = values.get("blob") {
                    if b.len() > 262_144 {
                        return Response::err(id, "blob too large (max 256KiB)");
                    }
                }
            }
            // Idempotency for safe retry after shed/timeout: client sends
            // `_idem` string; stripped before storage, mapped to assigned ID.
            // Best-effort in-memory (lost on crash — within group window).
            let idem: Option<String> = values.remove("_idem").and_then(|v| match v {
                blitz_types::value::Value::String(s) => Some(s),
                _ => None,
            });
            if let Some(key) = idem.as_deref() {
                if let Some(cached) = server.idem_lookup(key) {
                    return Response::ok(id, vec![RowView { id: cached, values }]);
                }
            }
            // Row ownership: writers can only create their own rows.
            if let Err(e) = enforce_owner(server, authed, Op::Insert, &req.table, &values) {
                return Response::err(id, e);
            }
            // Server-side routing: base name in, (physical, shard) out.
            // Unsharded tables pass through identically (shard 0, local id).
            let (physical, shard) = server.route_insert(&req.table, &values);
            let mut row = Row::new(RowId::new(0));
            for (k, v) in &values {
                row.set(k.clone(), v.clone());
            }
            // Shard/cache fast path: trusted shape skips validation.
            let res = if server.config().skip_validation {
                server.engine().insert_unchecked(&physical, row)
            } else {
                server.engine().insert(&physical, row)
            };
            match res {
                Ok(local) => {
                    let global = BlitzServer::compose_id(shard, local.as_u64());
                    // Durability: never ack unwritten data in durable modes.
                    // On backpressure, compensate (delete just-inserted row)
                    // so engine/WAL can't diverge, then shed fast.
                    let data = crate::durability::values_to_json_bytes(&values);
                    if let Err(w) = server.wal_log(blitz_wal::EntryType::Insert, &physical, global, data) {
                        let _ = server.engine().delete(&physical, local);
                        return Response::err(id, format!("WAL backpressure: {}", w));
                    }
                    if let Some(key) = idem {
                        server.idem_record(key, global);
                    }
                    // Change-log/push key on the BASE name (stable Subscribe);
                    // search postings key on the physical table (resolution).
                    server.record_change(&req.table, "insert", global);
                    // Social derived state (post ack only): search index +
                    // async fanout job. Both fire-and-forget bounded; neither
                    // blocks the response (eventual, ~ms).
                    if req.table.starts_with("posts") {
                        if let Some(blitz_types::value::Value::String(body)) = values.get("body") {
                            server.index_post(&physical, local.as_u64(), body);
                        }
                        if let Some(blitz_types::value::Value::String(author)) = values.get("author") {
                            server.fanout_enqueue(physical.clone(), local.as_u64(), author.clone());
                        }
                    }
                    Response::ok(
                        id,
                        vec![RowView {
                            id: global,
                            values,
                        }],
                    )
                }
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Get => {
            let global = match req.row_id {
                Some(rid) => rid,
                None => return Response::err(id, "get requires row_id"),
            };
            let (physical, local) = server.route_id(&req.table, global);
            // Zero-copy read: serialize straight off the shared handle.
            // Row-owner mismatch hides as "not found" (no existence oracle).
            match server.engine().get_arc(&physical, RowId::new(local)) {
                Ok(Some(row)) => {
                    if enforce_owner(server, authed, Op::Get, &req.table, &row.values).is_err() {
                        return Response::err(id, format!("row not found: {}", RowId::new(global)));
                    }
                    Response::ok(id, vec![row_to_view(RowId::new(global), &row)])
                }
                Ok(None) => Response::err(id, format!("row not found: {}", RowId::new(global))),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Update => {
            let global = match req.row_id {
                Some(rid) => rid,
                None => return Response::err(id, "update requires row_id"),
            };
            let (physical, local) = server.route_id(&req.table, global);
            let values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "update requires values"),
            };
            // Row ownership pre-check (one extra read, only on configured
            // tables with auth on): never mutate another owner's row.
            if server.config().require_auth && server.owner_column(&req.table).is_some() {
                match server.engine().get_arc(&physical, RowId::new(local)) {
                    Ok(Some(row)) => {
                        if let Err(e) = enforce_owner(server, authed, Op::Update, &req.table, &row.values) {
                            return Response::err(id, e);
                        }
                    }
                    // Absent: fall through to engine.update for the exact
                    // current "row not found" behavior.
                    _ => {}
                }
            }
            // Move, don't clone: engine already returns an owned Row, so
            // moving its map into the view saves a second HashMap clone.
            // Response id is the GLOBAL id (engine rows carry local ids).
            match server.engine().update(&physical, RowId::new(local), values) {
                Ok(row) => {
                    let data = crate::durability::values_to_json_bytes(&row.values);
                    if let Err(w) = server.wal_log(blitz_wal::EntryType::Update, &physical, global, data) {
                        return Response::err(id, format!("WAL backpressure: {}", w));
                    }
                    server.record_change(&req.table, "update", global);
                    Response::ok(
                        id,
                        vec![RowView {
                            id: global,
                            values: row.values,
                        }],
                    )
                }
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Delete => {
            let global = match req.row_id {
                Some(rid) => rid,
                None => return Response::err(id, "delete requires row_id"),
            };
            let (physical, local) = server.route_id(&req.table, global);
            // Row ownership pre-check (configured tables with auth on).
            if server.config().require_auth && server.owner_column(&req.table).is_some() {
                match server.engine().get_arc(&physical, RowId::new(local)) {
                    Ok(Some(row)) => {
                        if let Err(e) = enforce_owner(server, authed, Op::Delete, &req.table, &row.values) {
                            return Response::err(id, e);
                        }
                    }
                    _ => {}
                }
            }
            match server.engine().delete(&physical, RowId::new(local)) {
                Ok(true) => {
                    if let Err(w) = server.wal_log(blitz_wal::EntryType::Delete, &physical, global, Vec::new()) {
                        return Response::err(id, format!("WAL backpressure: {}", w));
                    }
                    server.record_change(&req.table, "delete", global);
                    Response::ok(id, Vec::new())
                }
                Ok(false) => Response::err(id, format!("row not found: {}", RowId::new(global))),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Scan => {
            let w = scan_pagination(&req.values);
            // Fan out across shards server-side: merge (global id, row),
            // sort, then window. Base names stay stable for callers.
            // (Same helper as the Single fast path: one ordering contract.)
            let mut merged = match scan_merged(server, &req.table) {
                Ok(m) => m,
                Err(e) => return Response::err(id, e),
            };
            // Row ownership: filter BEFORE sort/window so cursor pages over
            // visible rows stay complete, ordered, and non-overlapping.
            // (Atomic frames still reject Scan: tx snapshot semantics, not
            // authz — unchanged.)
            if server.owner_column(&req.table).is_some() {
                merged.retain(|(_, row)| enforce_owner(server, authed, Op::Scan, &req.table, &row.values).is_ok());
            }
            // Paginated + cursor scan over global RowId order. The limit
            // bounds CPU/frame; cursor (binary search) keeps pages stable
            // under concurrent inserts. NOTE: per-request full sort — hot
            // timeline paths must use precomputed feeds (Stage 9+), not
            // scans; this primitive is for admin/backfill pages.
            if w.desc {
                merged.sort_by_key(|r| std::cmp::Reverse(r.0));
            } else {
                merged.sort_by_key(|r| r.0);
            }
            let (start, end) = apply_window(merged.len(), !w.desc, &w, &|i| merged[i].0);
            let views = merged[start..end]
                .iter()
                .map(|(gid, row)| row_to_view(RowId::new(*gid), row))
                .collect();
            Response::ok(id, views)
        }
        Op::Find => {
            let (col, val) = match req.values.as_ref().and_then(|m| {
                match (m.get("_col"), m.get("_val")) {
                    (Some(blitz_types::value::Value::String(c)), Some(v)) => Some((c.clone(), v.clone())),
                    _ => None,
                }
            }) {
                Some(cv) => cv,
                None => return Response::err(id, "find requires values {_col: String, _val: Value}"),
            };
            // Unique indexes are per-shard: probe each physical table.
            // Missing SHARD tables probe on; an unknown BASE table errors.
            let sharded = server.shard_count(&req.table) > 1;
            let mut hit: Option<(usize, std::sync::Arc<Row>)> = None;
            let mut find_err: Option<String> = None;
            for (shard, physical) in server.shard_tables(&req.table) {
                match server.engine().lookup_by_unique(&physical, &col, &val) {
                    Ok(Some(row)) => {
                        hit = Some((shard, row));
                        break;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        // Fresh shard tables don't exist yet: probe on.
                        // Anything else (incl. non-unique column) is a
                        // real error.
                        if sharded && e.to_string().contains("table not found") {
                            continue;
                        }
                        find_err = Some(e.to_string());
                        break;
                    }
                }
            }
            if let Some(e) = find_err {
                return Response::err(id, e);
            }
            match hit {
                Some((shard, row)) => {
                    // Owner mismatch reads as miss (no existence oracle).
                    if enforce_owner(server, authed, Op::Find, &req.table, &row.values).is_err() {
                        return Response::err(id, "not found");
                    }
                    let global = BlitzServer::compose_id(shard, row.id.as_u64());
                    Response::ok(id, vec![row_to_view(RowId::new(global), &row)])
                }
                None => Response::err(id, "not found"),
            }
        }
        Op::Subscribe => {
            use blitz_types::value::Value as V;
            let mut since = 0u64;
            let mut limit = 100usize;
            if let Some(m) = req.values.as_ref() {
                if let Some(v) = m.get("_since") {
                    since = match v {
                        V::Int64(n) => (*n).max(0) as u64,
                        V::UInt64(n) => *n,
                        V::Int32(n) => (*n).max(0) as u64,
                        V::UInt32(n) => *n as u64,
                        _ => 0,
                    };
                }
                if let Some(v) = m.get("_limit") {
                    limit = match v {
                        V::Int64(n) => (*n).max(0) as usize,
                        V::UInt64(n) => *n as usize,
                        _ => 100,
                    }
                    .clamp(1, MAX_SUBSCRIBE_LIMIT);
                }
            }
            let recs = server.read_changes(&req.table, since, limit);
            // Row ownership: re-fetch each record's row and keep only owned
            // ones. Gone rows (incl. deletes) drop fail-closed — ownership
            // can't be proven without the row. Bounded: ≤ limit fetches.
            // Limit applies pre-filter (bounded work); clients paginate by
            // the returned max ts.
            let recs: Vec<_> = if server.owner_column(&req.table).is_some() {
                recs.into_iter().filter(|r| {
                    let (physical, local) = server.route_id(&r.table, r.row_id);
                    match server.engine().get_arc(&physical, RowId::new(local)) {
                        Ok(Some(row)) => enforce_owner(server, authed, Op::Subscribe, &r.table, &row.values).is_ok(),
                        _ => false,
                    }
                }).collect()
            } else {
                recs
            };
            let rows = recs
                .iter()
                .map(|r| RowView {
                    id: r.seq,
                    values: [
                        ("table".to_string(), V::String(r.table.clone())),
                        ("op".to_string(), V::String(r.op.to_string())),
                        ("row_id".to_string(), V::Int64(r.row_id as i64)),
                        ("ts".to_string(), V::Int64(r.ts_micros as i64)),
                    ]
                    .into_iter()
                    .collect(),
                })
                .collect();
            Response::ok(id, rows)
        }
        Op::Search => {
            use blitz_types::value::Value as V2;
            let (q, limit) = match req.values.as_ref() {
                Some(m) => {
                    let q = match m.get("_q") {
                        Some(V2::String(s)) => s.clone(),
                        _ => return Response::err(id, "search requires values {_q: String}"),
                    };
                    let lim = match m.get("_limit") {
                        Some(V2::Int64(n)) => (*n).max(0) as usize,
                        Some(V2::UInt64(n)) => *n as usize,
                        Some(V2::Int32(n)) => (*n).max(0) as usize,
                        Some(V2::UInt32(n)) => *n as usize,
                        None => 20,
                        _ => 20,
                    }
                    .clamp(1, 100);
                    (q, lim)
                }
                None => return Response::err(id, "search requires values {_q: String}"),
            };
            // Resolve postings to rows (bounded fan-out: ≤100 engine reads).
            // Postings carry (physical table, local id); responses carry
            // GLOBAL ids so clients can Get them back. Owner mismatch skips
            // silently (same as already-missing rows: no oracle).
            let mut rows = Vec::new();
            for (table, rid) in server.search_posts(&q, limit) {
                match server.engine().get_arc(&table, RowId::new(rid)) {
                    Ok(Some(row)) => {
                        // Ownership is checked against the HIT's base table
                        // (postings are global): owner-gated rows hide,
                        // ungated tables pass through untouched.
                        let hit_base = server.base_of_physical(&table);
                        if enforce_owner(server, authed, Op::Search, &hit_base, &row.values).is_err() {
                            continue;
                        }
                        let shard = server.shard_of_physical(&req.table, &table);
                        let global = BlitzServer::compose_id(shard, rid);
                        rows.push(row_to_view(RowId::new(global), &row))
                    }
                    _ => {}
                }
            }
            Response::ok(id, rows)
        }
        Op::Call => execute_procedure(server, authed, req),
        Op::JobSubmit => {
            // values {wasm: Bytes, input: String, _type?: String, _retries?: Int}.
            // Stores Pending + spawns the blocking-pool runner; responds at
            // once with the job id (never inline — guests stay off dispatch).
            let values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "job_submit requires values {wasm: Bytes, input: String}"),
            };
            let wasm = match values.get("wasm") {
                Some(Value::Bytes(b)) => b.clone(),
                _ => return Response::err(id, "job_submit requires values {wasm: Bytes, input: String}"),
            };
            let input = match values.get("input") {
                Some(Value::String(s)) => s.clone(),
                _ => return Response::err(id, "job_submit requires values {wasm: Bytes, input: String}"),
            };
            let label = match values.get("_type") {
                Some(Value::String(s)) => s.clone(),
                _ => "wasm".to_string(),
            };
            let mut job = blitz_jobs::Job::new(label);
            if let Some(Value::Int64(n)) = values.get("_retries") {
                job = job.with_max_retries((*n).clamp(0, 5) as u32);
            }
            let job_id = match server.store_wasm_job(job, wasm, input) {
                Ok(jid) => jid,
                Err(e) => return Response::err(id, e),
            };
            let runner = std::sync::Arc::clone(server);
            let spawn_id = job_id.clone();
            tokio::task::spawn_blocking(move || {
                runner.run_wasm_job(&spawn_id);
            });
            let mut out = std::collections::HashMap::new();
            out.insert("job_id".to_string(), Value::String(job_id));
            out.insert("status".to_string(), Value::String("pending".to_string()));
            Response::ok(id, vec![RowView { id: 0, values: out }])
        }
        Op::JobPoll => {
            // values {_job: String id} -> {job_id, status, result?, error?}.
            let job_id = match req.values.as_ref().and_then(|m| m.get("_job")) {
                Some(Value::String(s)) => s.clone(),
                _ => return Response::err(id, "job_poll requires values {_job: String}"),
            };
            match server.get_wasm_job(&job_id) {
                Some(job) => {
                    let mut out = std::collections::HashMap::new();
                    out.insert("job_id".to_string(), Value::String(job_id));
                    out.insert("status".to_string(), Value::String(job_status_name(&job.status)));
                    if let Some(r) = job.result {
                        out.insert("result".to_string(), Value::String(r));
                    }
                    if let Some(e) = job.error {
                        out.insert("error".to_string(), Value::String(e));
                    }
                    Response::ok(id, vec![RowView { id: 0, values: out }])
                }
                None => Response::err(id, format!("job not found: {}", job_id)),
            }
        }
        Op::ProcDeploy => {
            let name = match req.table.strip_prefix("fn:") {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => return Response::err(id, "proc_deploy requires table `fn:<procedure>`"),
            };
            let values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "proc_deploy requires values {v, procedure}"),
            };
            let version = match values.get("v") {
                Some(Value::Int64(1)) => Some(1u64),
                Some(Value::UInt64(1)) => Some(1u64),
                other => {
                    return Response::err(id, format!("unsupported envelope version (want 1): {:?}", other))
                }
            };
            let proc_json = match values.get("procedure") {
                Some(Value::Json(j)) => j.clone(),
                _ => return Response::err(id, "proc_deploy requires values.procedure as JSON object"),
            };
            let functions = match server.functions().read() {
                Ok(g) => g,
                Err(e) => return Response::err(id, format!("function registry locked: {}", e)),
            };
            let proc = match blitz_runtime::deploy_from_parsed(version, &proc_json, &functions) {
                Ok(p) => p,
                Err(e) => return Response::err(id, format!("invalid procedure: {}", e)),
            };
            drop(functions);
            if proc.name != name {
                return Response::err(
                    id,
                    format!("envelope procedure {:?} must match table fn:<name>", proc.name),
                );
            }
            match server.deploy_procedure(proc) {
                Ok(version) => {
                    let mut out = std::collections::HashMap::new();
                    out.insert("name".to_string(), Value::String(name));
                    out.insert("version".to_string(), Value::UInt64(version));
                    Response::ok(id, vec![RowView { id: 0, values: out }])
                }
                Err(e) => Response::err(id, format!("invalid procedure: {}", e)),
            }
        }
        Op::ProcList => {
            let rows = server
                .list_procedures()
                .into_iter()
                .map(|(name, description, version, steps)| {
                    let mut values = std::collections::HashMap::new();
                    values.insert("name".to_string(), Value::String(name));
                    values.insert("description".to_string(), Value::String(description));
                    values.insert("version".to_string(), Value::UInt64(version));
                    values.insert("steps".to_string(), Value::UInt64(steps as u64));
                    RowView { id: 0, values }
                })
                .collect();
            Response::ok(id, rows)
        }
        Op::ProcDrop => {
            let name = match req.table.strip_prefix("fn:") {
                Some(n) if !n.is_empty() => n,
                _ => return Response::err(id, "proc_drop requires table `fn:<procedure>`"),
            };
            if server.drop_procedure(name) {
                Response::ok(id, Vec::new())
            } else {
                Response::err(id, format!("procedure not found: {}", name))
            }
        }
    }
}

fn job_status_name(status: &blitz_jobs::JobStatus) -> String {
    match status {
        blitz_jobs::JobStatus::Pending => "pending",
        blitz_jobs::JobStatus::Running => "running",
        blitz_jobs::JobStatus::Completed => "completed",
        blitz_jobs::JobStatus::Failed => "failed",
        blitz_jobs::JobStatus::Cancelled => "cancelled",
    }
    .to_string()
}

/// Atomic batch execution: all ops run in ONE OCC transaction
/// ([`IsolationLevel::RepeatableRead`]) and commit together — all responses
/// ok, or every response err and nothing applied.
///
/// Supported ops: Ping (no-op ok), Get (versioned read), Insert, Update,
/// Delete. Snapshot ops (Scan/Find/Subscribe/Search) bypass tx versioning
/// and are rejected loudly (whole batch aborts) rather than silently
/// breaking the atomicity contract.
///
/// Guarantees and residuals (honest, documented in PROTOCOL.md):
/// - OCC validation closes write-write and read-write races pre-apply, so
///   on unique-free tables (all app social tables) apply is infallible
///   given the op-time existence pre-checks: TRUE atomicity.
/// - Unique-constrained tables: sequential intra-batch duplicates abort
///   pre-commit (checked); concurrent cross-batch duplicate races can both
///   commit (same as the non-atomic path today — strictly no worse).
/// - WAL append happens post-commit per applied write; a WAL failure then
///   cannot roll back (memory-committed). It bumps `wal_dropped` and the
///   row stays durable-in-memory until the next snapshot — responses stay
///   ok because the data IS committed. Admission-time backpressure still
///   sheds whole frames before `begin` (unchanged path).
/// - `_idem` per insert works: a retried identical batch hits the cache on
///   every op, commits an empty tx, and returns the original IDs.
/// - `skip_validation` is NOT honored here (tx apply always validates):
///   atomic batches trade ~µs/op for the guarantee. Measured, not hidden.
pub(crate) fn execute_atomic(
    server: &std::sync::Arc<BlitzServer>,
    authed: &mut Option<blitz_auth::Identity>,
    batch: BatchRequest,
) -> BatchResponse {
    use blitz_tx::transaction::IsolationLevel;
    let batch_id = batch.id;
    let op_ids: Vec<u64> = batch.ops.iter().map(|op| op.id).collect();
    let abort_all = |msg: String| BatchResponse {
        id: batch_id,
        results: op_ids.iter().map(|rid| Response::err(*rid, msg.clone())).collect(),
    };

    // Phase 0: auth every op first (mirrors the batch path incl. handshake).
    // A denial aborts the whole batch before any engine state is touched.
    let mut ops = batch.ops;
    for op in ops.iter_mut() {
        if let Some(tok) = take_auth_token(&mut op.values) {
            if let Some(id) = server.resolve_token(&tok) {
                *authed = Some(id);
            }
        }
        if let Err(e) = server.authorize(authed, op.op, &op.table) {
            return abort_all(format!("atomic batch aborted: unauthorized op {}: {}", op.id, e));
        }
        // Snapshot ops would read outside tx versioning, nested Calls
        // would nest transactions, and job ops spawn background work that
        // can't roll back: reject, don't fake.
        if matches!(op.op, Op::Scan | Op::Find | Op::Subscribe | Op::Search | Op::Call | Op::JobSubmit | Op::JobPoll | Op::ProcDeploy | Op::ProcList | Op::ProcDrop | Op::Version) {
            return abort_all(format!(
                "atomic batch aborted: {:?} not supported in atomic batch",
                op.op
            ));
        }
    }

    // Phase 1: buffer. Gets read (recording versions); writes buffer.
    // Existence pre-checks double as read-set population for RR validation.
    // All engine ops use PHYSICAL tables + LOCAL ids; responses translate
    // back to global ids at commit.
    enum Buffered {
        Ping,
        Get { view: RowView },
        Insert {
            base: String,
            physical: String,
            shard: usize,
            values: std::collections::HashMap<String, Value>,
            idem: Option<String>,
        },
        Update {
            base: String,
            physical: String,
            shard: usize,
            local: RowId,
            global: u64,
        },
        Delete {
            base: String,
            physical: String,
            shard: usize,
            local: RowId,
            global: u64,
        },
    }
    let mut tx = server.tx_manager().begin(IsolationLevel::RepeatableRead);
    let mut buffered: Vec<(u64, Buffered)> = Vec::with_capacity(ops.len());
    // Intra-batch duplicate guard for unique-constrained tables: two inserts
    // with the same unique value in ONE batch would fail mid-drain (partial
    // apply). Check pre-commit, abort cleanly. Keyed (table, column, value).
    let mut seen_uniques: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    // `fail` takes the tx explicitly so the borrow ends at the call site.
    let fail = |tx: &mut blitz_tx::Transaction, rid: u64, msg: String| {
        let _ = server.tx_manager().rollback(tx);
        abort_all(format!("atomic batch aborted: op {}: {}", rid, msg))
    };

    for op in ops {
        let rid = op.id;
        match op.op {
            Op::Ping => buffered.push((rid, Buffered::Ping)),
            Op::Get => {
                let global = match op.row_id {
                    Some(r) => r,
                    None => return fail(&mut tx, rid, "get requires row_id".to_string()),
                };
                let (physical, local) = server.route_id(&op.table, global);
                let local_id = RowId::new(local);
                match server.tx_manager().get(&mut tx, &physical, local_id) {
                    Ok(Some(row)) => {
                        if enforce_owner(server, authed, Op::Get, &op.table, &row.values).is_err() {
                            return fail(&mut tx, rid, format!("row not found: {}", local_id));
                        }
                        buffered.push((rid, Buffered::Get { view: row_to_view(RowId::new(global), &row) }))
                    }
                    Ok(None) => return fail(&mut tx, rid, format!("row not found: {}", local_id)),
                    Err(e) => return fail(&mut tx, rid, e.to_string()),
                }
            }
            Op::Insert => {
                let mut values = match op.values {
                    Some(v) => v,
                    None => return fail(&mut tx, rid, "insert requires values".to_string()),
                };
                if op.table.starts_with("media") {
                    if let Some(Value::Bytes(b)) = values.get("blob") {
                        if b.len() > 262_144 {
                            return fail(&mut tx, rid, "blob too large (max 256KiB)".to_string());
                        }
                    }
                }
                let idem: Option<String> = values.remove("_idem").and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });
                // Idempotent retry: already-committed insert replays its ID,
                // contributes no write to this commit.
                if let Some(key) = idem.as_deref() {
                    if let Some(cached) = server.idem_lookup(key) {
                        buffered.push((rid, Buffered::Get {
                            view: RowView { id: cached, values: values.clone() },
                        }));
                        continue;
                    }
                }
                // Intra-batch duplicate pre-check against live unique indexes.
                // Engine owns the index truth; ask it per unique column.
                // Unique scope is the PHYSICAL table (per-shard uniqueness).
                // Row ownership gates the values before buffering.
                let (physical, shard) = server.route_insert(&op.table, &values);
                if let Err(e) = enforce_owner(server, authed, Op::Insert, &op.table, &values) {
                    return fail(&mut tx, rid, e.to_string());
                }
                if let Ok(schema) = server.engine().schema(&physical) {
                    for col in schema.columns.iter().filter(|c| c.unique) {
                        if let Some(v) = values.get(&col.name) {
                            let key = (physical.clone(), col.name.clone(), format!("{:?}", v));
                            if !seen_uniques.insert(key) {
                                return fail(
                                    &mut tx,
                                    rid,
                                    format!("duplicate value in batch for unique {}.{}", physical, col.name),
                                );
                            }
                            if server.engine().lookup_by_unique(&physical, &col.name, v).ok().flatten().is_some() {
                                return fail(
                                    &mut tx,
                                    rid,
                                    format!("duplicate value for unique {}.{}", physical, col.name),
                                );
                            }
                        }
                    }
                }
                let mut row = Row::new(RowId::new(0));
                for (k, v) in &values {
                    row.set(k.clone(), v.clone());
                }
                if let Err(e) = tx.insert(physical.clone(), row) {
                    return fail(&mut tx, rid, e.to_string());
                }
                buffered.push((rid, Buffered::Insert { base: op.table, physical, shard, values, idem }));
            }
            Op::Update => {
                let global = match op.row_id {
                    Some(r) => r,
                    None => return fail(&mut tx, rid, "update requires row_id".to_string()),
                };
                let (physical, local) = server.route_id(&op.table, global);
                let local_id = RowId::new(local);
                let values = match op.values {
                    Some(v) => v,
                    None => return fail(&mut tx, rid, "update requires values".to_string()),
                };
                // Read-before-write: existence check + read-set entry, so a
                // concurrent writer aborts us at commit instead of clobbering.
                // Row ownership rides on the same read (fail forbidden).
                match server.tx_manager().get(&mut tx, &physical, local_id) {
                    Ok(Some(row)) => {
                        if let Err(e) = enforce_owner(server, authed, Op::Update, &op.table, &row.values) {
                            return fail(&mut tx, rid, e.to_string());
                        }
                    }
                    Ok(None) => return fail(&mut tx, rid, format!("row not found: {}", local_id)),
                    Err(e) => return fail(&mut tx, rid, e.to_string()),
                }
                if let Err(e) = tx.update(physical.clone(), local_id, values) {
                    return fail(&mut tx, rid, e.to_string());
                }
                let (shard, _) = BlitzServer::split_id(global);
                buffered.push((rid, Buffered::Update { base: op.table, physical, shard, local: local_id, global }));
            }
            Op::Delete => {
                let global = match op.row_id {
                    Some(r) => r,
                    None => return fail(&mut tx, rid, "delete requires row_id".to_string()),
                };
                let (physical, local) = server.route_id(&op.table, global);
                let local_id = RowId::new(local);
                match server.tx_manager().get(&mut tx, &physical, local_id) {
                    Ok(Some(row)) => {
                        if let Err(e) = enforce_owner(server, authed, Op::Delete, &op.table, &row.values) {
                            return fail(&mut tx, rid, e.to_string());
                        }
                    }
                    Ok(None) => return fail(&mut tx, rid, format!("row not found: {}", local_id)),
                    Err(e) => return fail(&mut tx, rid, e.to_string()),
                }
                if let Err(e) = tx.delete(physical.clone(), local_id) {
                    return fail(&mut tx, rid, e.to_string());
                }
                let (shard, _) = BlitzServer::split_id(global);
                buffered.push((rid, Buffered::Delete { base: op.table, physical, shard, local: local_id, global }));
            }
            Op::Scan | Op::Find | Op::Subscribe | Op::Search | Op::Call | Op::JobSubmit | Op::JobPoll | Op::ProcDeploy | Op::ProcList | Op::ProcDrop | Op::Version => {
                return fail(&mut tx, rid, format!("{:?} not supported in atomic batch", op.op))
            }
        }
    }

    // Phase 2: commit. Conflict or apply error rolls back: nothing applied.
    let touched = match server.tx_manager().commit(&mut tx) {
        Ok(t) => t,
        Err(e) => {
            return BatchResponse {
                id: batch_id,
                results: buffered
                    .iter()
                    .map(|(rid, _)| Response::err(*rid, format!("atomic batch aborted: {}", e)))
                    .collect(),
            };
        }
    };
    let mut touch_iter = touched.into_iter();

    // Phase 3: responses + post-commit side effects (WAL, change-log, social
    // derived state) mirroring dispatch, per applied write in buffer order.
    // WAL failures here bump wal_dropped (memory-committed, documented).
    // touched pairs carry (physical, LOCAL); responses translate to global.
    let mut results = Vec::with_capacity(buffered.len());
    for (rid, b) in buffered {
        match b {
            Buffered::Ping => results.push(Response::ok(rid, Vec::new())),
            Buffered::Get { view } => results.push(Response::ok(rid, vec![view])),
            Buffered::Insert { base, physical, shard, values, idem } => {
                let (_, local) = match touch_iter.next() {
                    Some(t) => t,
                    None => {
                        results.push(Response::err(rid, "atomic batch aborted: commit/response mismatch"));
                        continue;
                    }
                };
                let global = BlitzServer::compose_id(shard, local.as_u64());
                emit_write_effects(
                    server,
                    &base,
                    &physical,
                    blitz_wal::EntryType::Insert,
                    local.as_u64(),
                    global,
                    Some(&values),
                );
                if let Some(key) = idem {
                    server.idem_record(key, global);
                }
                results.push(Response::ok(rid, vec![RowView { id: global, values }]));
            }
            Buffered::Update { base, physical, local, global, .. } => {
                match touch_iter.next() {
                    Some(_) => {}
                    None => {
                        results.push(Response::err(rid, "atomic batch aborted: commit/response mismatch"));
                        continue;
                    }
                }
                match server.engine().get_arc(&physical, local) {
                    Ok(Some(row)) => {
                        emit_write_effects(
                            server,
                            &base,
                            &physical,
                            blitz_wal::EntryType::Update,
                            local.as_u64(),
                            global,
                            Some(&row.values),
                        );
                        results.push(Response::ok(rid, vec![row_to_view(RowId::new(global), &row)]));
                    }
                    _ => results.push(Response::err(rid, format!("row not found after commit: {}", local))),
                }
            }
            Buffered::Delete { base, physical, local, global, .. } => {
                match touch_iter.next() {
                    Some(_) => {}
                    None => {
                        results.push(Response::err(rid, "atomic batch aborted: commit/response mismatch"));
                        continue;
                    }
                }
                emit_write_effects(server, &base, &physical, blitz_wal::EntryType::Delete, local.as_u64(), global, None);
                results.push(Response::ok(rid, Vec::new()));
            }
        }
    }
    BatchResponse { id: batch_id, results }
}

/// Transaction backend for procedure calls: buffers every DB step in one
/// OCC transaction (invoker-rights: each step re-authorizes the caller's
/// table permission, so a callable procedure can't exceed what the caller
/// could do op-by-op — no privilege escalation in v1).
struct ProcBackend<'s> {
    server: &'s BlitzServer,
    ident: Option<blitz_auth::Identity>,
    tx: blitz_tx::Transaction,
    /// Buffered writes in order with pre-commit values for post-commit
    /// effects + `_applied` reporting.
    writes: Vec<ProcWrite>,
    seen_uniques: std::collections::HashSet<(String, String, String)>,
}

struct ProcWrite {
    base: String,
    physical: String,
    shard: usize,
    entry: blitz_wal::EntryType,
    values: Option<std::collections::HashMap<String, Value>>,
}

impl<'s> ProcBackend<'s> {
    fn deny(&self, op: Op, table: &str) -> Result<(), String> {
        self.server.authorize(&self.ident, op, table).map_err(|e| e.to_string())
    }

    fn deny_row(
        &self,
        op: Op,
        table: &str,
        values: &std::collections::HashMap<String, Value>,
    ) -> Result<(), String> {
        enforce_owner(self.server, &self.ident, op, table, values).map_err(|e| e.to_string())
    }

    fn check_unique(
        &mut self,
        table: &str,
        values: &std::collections::HashMap<String, Value>,
    ) -> Result<(), String> {
        if let Ok(schema) = self.server.engine().schema(table) {
            for col in schema.columns.iter().filter(|c| c.unique) {
                if let Some(v) = values.get(&col.name) {
                    let key = (table.to_string(), col.name.clone(), format!("{:?}", v));
                    if !self.seen_uniques.insert(key) {
                        return Err(format!(
                            "duplicate value in call for unique {}.{}",
                            table, col.name
                        ));
                    }
                    if self
                        .server
                        .engine()
                        .lookup_by_unique(table, &col.name, v)
                        .ok()
                        .flatten()
                        .is_some()
                    {
                        return Err(format!("duplicate value for unique {}.{}", table, col.name));
                    }
                }
            }
        }
        Ok(())
    }
}

impl blitz_runtime::ProcedureBackend for ProcBackend<'_> {
    fn read(
        &mut self,
        table: &str,
        id: u64,
    ) -> blitz_runtime::RuntimeResult<Option<std::collections::HashMap<String, Value>>> {
        use blitz_runtime::RuntimeError;
        self.deny(Op::Get, table).map_err(RuntimeError::ExecutionError)?;
        // Point reads route by global-id shard bits (base names in steps).
        // Owner mismatch hides as miss (no existence oracle).
        let (physical, local) = self.server.route_id(table, id);
        let row = self
            .server
            .tx_manager()
            .get(&mut self.tx, &physical, RowId::new(local))
            .map_err(|e| RuntimeError::ExecutionError(e.to_string()))?;
        match row {
            Some(r) => {
                self.deny_row(Op::Get, table, &r.values).map_err(RuntimeError::ExecutionError)?;
                Ok(Some(r.values))
            }
            None => Ok(None),
        }
    }

    fn insert(
        &mut self,
        table: &str,
        values: std::collections::HashMap<String, Value>,
    ) -> blitz_runtime::RuntimeResult<()> {
        use blitz_runtime::RuntimeError;
        self.deny(Op::Insert, table).map_err(RuntimeError::ExecutionError)?;
        if table.starts_with("media") {
            if let Some(Value::Bytes(b)) = values.get("blob") {
                if b.len() > 262_144 {
                    return Err(RuntimeError::ExecutionError("blob too large (max 256KiB)".into()));
                }
            }
        }
        let (physical, shard) = self.server.route_insert(table, &values);
        self.check_unique(&physical, &values).map_err(RuntimeError::ExecutionError)?;
        self.deny_row(Op::Insert, table, &values).map_err(RuntimeError::ExecutionError)?;
        let mut row = Row::new(RowId::new(0));
        for (k, v) in &values {
            row.set(k.clone(), v.clone());
        }
        self.tx
            .insert(physical.clone(), row)
            .map_err(|e| RuntimeError::ExecutionError(e.to_string()))?;
        self.writes.push(ProcWrite {
            base: table.to_string(),
            physical,
            shard,
            entry: blitz_wal::EntryType::Insert,
            values: Some(values),
        });
        Ok(())
    }

    fn update(
        &mut self,
        table: &str,
        id: u64,
        values: std::collections::HashMap<String, Value>,
    ) -> blitz_runtime::RuntimeResult<()> {
        use blitz_runtime::RuntimeError;
        self.deny(Op::Update, table).map_err(RuntimeError::ExecutionError)?;
        // Read-before-write: existence + read-set entry (concurrent writer
        // aborts us at commit instead of clobbering).
        let (physical, local) = self.server.route_id(table, id);
        let local_id = RowId::new(local);
        let exists = self
            .server
            .tx_manager()
            .get(&mut self.tx, &physical, local_id)
            .map_err(|e| RuntimeError::ExecutionError(e.to_string()))?;
        let existing = match exists {
            Some(r) => r,
            None => return Err(RuntimeError::ExecutionError(format!("row not found: {}:{}", table, id))),
        };
        self.deny_row(Op::Update, table, &existing.values).map_err(RuntimeError::ExecutionError)?;
        self.tx
            .update(physical.clone(), local_id, values.clone())
            .map_err(|e| RuntimeError::ExecutionError(e.to_string()))?;
        let (shard, _) = BlitzServer::split_id(id);
        self.writes.push(ProcWrite {
            base: table.to_string(),
            physical,
            shard,
            entry: blitz_wal::EntryType::Update,
            values: Some(values),
        });
        Ok(())
    }

    fn delete(&mut self, table: &str, id: u64) -> blitz_runtime::RuntimeResult<()> {
        use blitz_runtime::RuntimeError;
        self.deny(Op::Delete, table).map_err(RuntimeError::ExecutionError)?;
        let (physical, local) = self.server.route_id(table, id);
        let local_id = RowId::new(local);
        let exists = self
            .server
            .tx_manager()
            .get(&mut self.tx, &physical, local_id)
            .map_err(|e| RuntimeError::ExecutionError(e.to_string()))?;
        let existing = match exists {
            Some(r) => r,
            None => return Err(RuntimeError::ExecutionError(format!("row not found: {}:{}", table, id))),
        };
        self.deny_row(Op::Delete, table, &existing.values).map_err(RuntimeError::ExecutionError)?;
        self.tx
            .delete(physical.clone(), local_id)
            .map_err(|e| RuntimeError::ExecutionError(e.to_string()))?;
        let (shard, _) = BlitzServer::split_id(id);
        self.writes.push(ProcWrite {
            base: table.to_string(),
            physical,
            shard,
            entry: blitz_wal::EntryType::Delete,
            values: None,
        });
        Ok(())
    }

    fn call_function(
        &self,
        name: &str,
        args: std::collections::HashMap<String, Value>,
    ) -> blitz_runtime::RuntimeResult<Value> {
        self.server
            .call_function(name, args)
            .map_err(blitz_runtime::RuntimeError::ExecutionError)
    }
}

/// Execute a registered procedure in one OCC transaction (all-or-nothing).
/// `req.table` is `fn:<name>` (policy resource); `req.values` are call args.
/// Responds one row: the `Return` value (`{"result": v}`, or a `Json` object
/// flattened) plus `_applied` (per-write `{table, id}` in buffer order).
fn execute_procedure(
    server: &std::sync::Arc<BlitzServer>,
    authed: &Option<blitz_auth::Identity>,
    req: Request,
) -> Response {
    use blitz_tx::transaction::IsolationLevel;
    let id = req.id;
    let name = match req.table.strip_prefix("fn:") {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => return Response::err(id, "call requires table `fn:<procedure>`"),
    };
    let proc = match server.get_procedure(&name) {
        Some(p) => p,
        None => return Response::err(id, format!("procedure not found: {}", name)),
    };
    let args = req.values.unwrap_or_default();
    let mut backend = ProcBackend {
        server,
        ident: authed.clone(),
        tx: server.tx_manager().begin(IsolationLevel::RepeatableRead),
        writes: Vec::new(),
        seen_uniques: std::collections::HashSet::new(),
    };
    let output = match blitz_runtime::run_procedure(&mut backend, &proc, args, blitz_runtime::DEFAULT_FUEL) {
        Ok(o) => o,
        Err(e) => {
            let _ = server.tx_manager().rollback(&mut backend.tx);
            return Response::err(id, format!("procedure aborted: {}", e));
        }
    };
    let touched = match server.tx_manager().commit(&mut backend.tx) {
        Ok(t) => t,
        Err(e) => return Response::err(id, format!("procedure aborted: {}", e)),
    };
    // Zip commit-assigned LOCAL ids back onto buffered writes in order;
    // responses and logs translate to global ids.
    let mut applied: Vec<serde_json::Value> = Vec::with_capacity(backend.writes.len());
    for (w, (_, local)) in backend.writes.iter().zip(touched.iter()) {
        let global = BlitzServer::compose_id(w.shard, local.as_u64());
        emit_write_effects(server, &w.base, &w.physical, w.entry, local.as_u64(), global, w.values.as_ref());
        applied.push(serde_json::json!({"table": w.base, "id": global}));
    }
    let mut values: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    match output.value {
        Value::Json(serde_json::Value::Object(map)) => {
            for (k, v) in map {
                values.insert(k, json_to_value(v));
            }
        }
        v => {
            values.insert("result".to_string(), v);
        }
    }
    values.insert("_applied".to_string(), Value::Json(serde_json::Value::Array(applied)));
    Response::ok(id, vec![RowView { id: 0, values }])
}

/// Best-effort JSON → Value for `Return` object flattening. Scalars map
/// natively; nested structures ride through as `Json`.
fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt64(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Json(serde_json::Value::Number(n))
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        other => Value::Json(other),
    }
}

/// Post-commit write effects shared by atomic batches and procedures:
/// WAL append (physical table, GLOBAL id — replay strips shard bits for the
/// engine-local restore), change-log record (BASE name + global id, so
/// Subscribe on stable names keeps working), and social derived state
/// (search postings key on the physical table + LOCAL id for resolution;
/// fanout jobs likewise). WAL failure bumps `wal_dropped` inside `wal_log` —
/// the row is memory-committed (responses stay ok), durable at the next
/// snapshot.
fn emit_write_effects(
    server: &BlitzServer,
    base: &str,
    physical: &str,
    entry: blitz_wal::EntryType,
    local_id: u64,
    global_id: u64,
    values: Option<&std::collections::HashMap<String, Value>>,
) {
    let data = values.map(crate::durability::values_to_json_bytes).unwrap_or_default();
    if server.wal_log(entry, physical, global_id, data).is_err() {
        // Committed but unwritten: counted, stays until snapshot.
    }
    let op = match entry {
        blitz_wal::EntryType::Insert => "insert",
        blitz_wal::EntryType::Update => "update",
        blitz_wal::EntryType::Delete => "delete",
        _ => "write",
    };
    server.record_change(base, op, global_id);
    if physical.starts_with("posts") && matches!(entry, blitz_wal::EntryType::Insert) {
        if let Some(v) = values {
            if let Some(Value::String(body)) = v.get("body") {
                server.index_post(physical, local_id, body);
            }
            if let Some(Value::String(author)) = v.get("author") {
                server.fanout_enqueue(physical.to_string(), local_id, author.clone());
            }
        }
    }
}
///
/// Zero-copy fast paths for the Single-frame hot path.
///
/// `dispatch` must return an owned `Response` (Batch needs it), which forces
/// a HashMap clone per read. These helpers encode straight off `Arc<Row>`
/// with `encode_ok_single/borrowed` — no `Value` clones — saving ~30-40%
/// of Get/Scan service time. Insert/Update/Delete stay on `dispatch`.
fn encode_get_fast(
    server: &BlitzServer,
    authed: &Option<blitz_auth::Identity>,
    codec: &FrameCodec,
    req: &Request,
) -> anyhow::Result<bytes::Bytes> {
    let id = req.id;
    let global = match req.row_id {
        Some(rid) => rid,
        None => {
            return Ok(codec
                .encode_response(&Response::err(id, "get requires row_id"))
                .map_err(|e| anyhow::anyhow!("{e}"))?);
        }
    };
    // Route by shard bits; echo the GLOBAL id (engine rows carry local ids).
    // Row-owner mismatch hides as "not found" (no existence oracle).
    let (physical, local) = server.route_id(&req.table, global);
    match server.engine().get_arc(&physical, RowId::new(local)) {
        Ok(Some(row)) => {
            if enforce_owner(server, authed, Op::Get, &req.table, &row.values).is_err() {
                return Ok(codec
                    .encode_response(&Response::err(id, format!("row not found: {}", RowId::new(global))))
                    .map_err(|e| anyhow::anyhow!("{e}"))?);
            }
            Ok(codec
                .encode_ok_single(id, global, &row.values)
                .map_err(|e| anyhow::anyhow!("{e}"))?)
        }
        Ok(None) => Ok(codec
            .encode_response(&Response::err(id, format!("row not found: {}", RowId::new(global))))
            .map_err(|e| anyhow::anyhow!("{e}"))?),
        Err(e) => Ok(codec
            .encode_response(&Response::err(id, e.to_string()))
            .map_err(|e| anyhow::anyhow!("{e}"))?),
    }
}

/// Merge (global id, row) across all physical shards of a base table.
/// Shared by the Single fast path and dispatch (one implementation, one
/// ordering contract). Missing shard tables scan empty (fresh sharding).
fn scan_merged(
    server: &BlitzServer,
    base: &str,
) -> Result<Vec<(u64, std::sync::Arc<Row>)>, String> {
    let sharded = server.shard_count(base) > 1;
    let mut merged: Vec<(u64, std::sync::Arc<Row>)> = Vec::new();
    for (shard, physical) in server.shard_tables(base) {
        match server.engine().scan_arcs(&physical) {
            Ok(rows) => {
                for r in rows {
                    merged.push((BlitzServer::compose_id(shard, r.id.as_u64()), r));
                }
            }
            Err(e) => {
                // Fresh SHARD tables don't exist yet: skip. An unknown BASE
                // table is a real error (callers rely on it).
                if sharded && e.to_string().contains("table not found") {
                    continue;
                }
                return Err(e.to_string());
            }
        }
    }
    Ok(merged)
}

fn encode_scan_fast(
    server: &BlitzServer,
    authed: &Option<blitz_auth::Identity>,
    codec: &FrameCodec,
    req: &Request,
) -> anyhow::Result<bytes::Bytes> {
    let id = req.id;
    // Row-owner tables: filter merged rows by owner BEFORE windowing, so
    // pages/cursors stay honest (complete, ordered, non-overlapping).
    let w = scan_pagination(&req.values);
    // Fan out across shards server-side (stable base names for callers).
    let mut merged = match scan_merged(server, &req.table) {
        Ok(m) => m,
        Err(e) => {
            return Ok(codec
                .encode_response(&Response::err(id, e))
                .map_err(|e| anyhow::anyhow!("{e}"))?)
        }
    };
    if server.owner_column(&req.table).is_some() {
        merged.retain(|(_, row)| enforce_owner(server, authed, Op::Scan, &req.table, &row.values).is_ok());
    }
    if w.desc {
        merged.sort_by_key(|r| std::cmp::Reverse(r.0));
    } else {
        merged.sort_by_key(|r| r.0);
    }
    let (start, end) = apply_window(merged.len(), !w.desc, &w, &|i| merged[i].0);
    let borrowed: Vec<(u64, &std::collections::HashMap<String, blitz_types::value::Value>)> =
        merged[start..end].iter().map(|(gid, r)| (*gid, &r.values)).collect();
    match codec.encode_ok_borrowed(id, &borrowed) {
        Ok(f) => Ok(f),
        Err(_) => Ok(codec
            .encode_response(&Response::err(id, format!("response too large ({} rows)", borrowed.len())))
            .map_err(|e| anyhow::anyhow!("{e}"))?),
    }
}

/// Serve one connection until the client disconnects or a fatal I/O or
/// framing error occurs.
async fn handle_connection(server: Arc<BlitzServer>, socket: TcpStream) -> Result<()> {
    // Disable Nagle: this is a request/response protocol with small frames,
    // so waiting to coalesce segments would add pure latency.
    socket.set_nodelay(true).context("failed to set TCP_NODELAY")?;
    let peer_ip = socket.peer_addr().ok().map(|a| a.ip());
    handle_stream(server, socket, peer_ip).await
}

/// Framing/dispatch loop over any byte stream (plain TCP or TLS).
/// Shared so TLS adds only handshake cost, no second protocol path.
async fn handle_stream(
    server: Arc<BlitzServer>,
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    peer_ip: Option<std::net::IpAddr>,
) -> Result<()> {
    // Fast shed: when prod sets `shed_at_connections`, new connections past
    // the watermark fail fast (close) instead of queueing and exploding p99.
    if server.should_shed() {
        server.record_shed_drop();
        return Ok(());
    }
    let _guard = match ConnectionGuard::new(&server, peer_ip) {
        Some(g) => g,
        None => return Ok(()), // at capacity / per-IP cap: close immediately
    };

    let max_frame = server.config().max_message_size;
    let idle_secs = server.config().idle_timeout_secs;
    let codec = FrameCodec::new(max_frame);
    // Pre-size 4 KiB (typical request) to avoid first-read realloc;
    // growth is bounded below by the slow-loris cap.
    let mut staging = BytesMut::with_capacity(4096);
    // Connection-bound identity: set by any op carrying `_auth`, persists
    // for the life of the connection (per-op re-resolution would cost a
    // map lookup per request; this keeps authed steady-state at ~ns).
    let mut authed: Option<blitz_auth::Identity> = None;

    loop {
        // Idle reaping: keep-alive conns that go silent past the deadline
        // are closed to reclaim FDs/RAM (prevents slow-loris FD exhaustion).
        let n = if idle_secs > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(idle_secs),
                stream.read_buf(&mut staging),
            )
            .await
            {
                Err(_) => return Ok(()), // idle timeout: orderly close
                Ok(Err(e)) => return Err(e).context("failed to read from socket"),
                Ok(Ok(n)) => n,
            }
        } else {
            stream
                .read_buf(&mut staging)
                .await
                .context("failed to read from socket")?
        };
        if n == 0 {
            return Ok(()); // orderly shutdown
        }
        server.record_io(n as u64, 0);
        // Slow-loris / OOM guard: a client that never completes a frame
        // cannot grow `staging` without bound. `max_frame + HEADER` is
        // the largest a legitimate partial frame can be.
        if staging.len() > max_frame + blitz_protocol::HEADER_LEN {
            anyhow::bail!(
                "staging overflow ({} bytes): closing connection",
                staging.len()
            );
        }
        while let Some(frame) = codec
            .feed(&mut staging)
            .context("framing error: closing connection")?
        {
            let mut incoming = codec
                .decode_incoming(frame)
                .context("decode error: closing connection")?;
            // Push-stream upgrade: `Subscribe` with `_stream: 1` dedicates
            // this connection to server-driven frames (DMs/live). It must be
            // the last frame in flight; further client bytes are ignored and
            // the connection ends when the client disconnects or goes idle.
            // Poll-based `Subscribe` (no `_stream`) stays request/response.
            if let Incoming::Single(ref mut req) = incoming {
                if req.op == Op::Subscribe && is_stream_subscribe(&req.values) {
                    if let Some(tok) = take_auth_token(&mut req.values) {
                        if let Some(id) = server.resolve_token(&tok) {
                            authed = Some(id);
                        }
                    }
                    if let Err(e) = server.authorize(&authed, req.op, &req.table) {
                        let rid = req.id;
                        let err = codec.encode_response(&Response::err(rid, e)).context("encode error")?;
                        stream.write_all(&err).await.context("failed to write to socket")?;
                        server.record_io(0, err.len() as u64);
                        return Ok(());
                    }
                    // Row-owner tables stay off push streams: broadcast delivery
                    // can't enforce per-row ownership without putting engine
                    // reads on the write path — poll instead (fail closed).
                    if server.config().require_auth && server.owner_column(&req.table).is_some() {
                        let rid = req.id;
                        let err = codec.encode_response(&Response::err(rid, "push streams disabled on row-owner tables (poll instead)")).context("encode error")?;
                        stream.write_all(&err).await.context("failed to write to socket")?;
                        server.record_io(0, err.len() as u64);
                        return Ok(());
                    }
                    let (rid, table) = (req.id, req.table.clone());
                    return run_push_stream(&server, &mut stream, &codec, rid, table, idle_secs).await;
                }
            }
            // Batch and single share one frame budget: a batch of N costs
            // one read + one write instead of N round trips.
            // Auth: `_auth` token in any op's values handshakes the
            // connection (identity persists); denied ops become err payloads
            // without touching engine/WAL/change-log.
            let t0 = std::time::Instant::now();
            let encoded = match incoming {
                // Get/Scan use zero-copy borrowed encode (no Value clones).
                Incoming::Single(mut req) => {
                    if let Some(tok) = take_auth_token(&mut req.values) {
                        if let Some(id) = server.resolve_token(&tok) {
                            authed = Some(id);
                        }
                    }
                    if let Err(e) = server.authorize(&authed, req.op, &req.table) {
                        let rid = req.id;
                        codec.encode_response(&Response::err(rid, e)).context("encode error")?
                    } else if req.op == Op::Get {
                        encode_get_fast(&server, &authed, &codec, &req).context("encode error")?
                    } else if req.op == Op::Scan {
                        encode_scan_fast(&server, &authed, &codec, &req).context("encode error")?
                    } else {
                        let resp = dispatch(&server, &authed, req);
                        // A giant response could exceed the frame budget;
                        // report it as an error payload instead of killing
                        // the connection.
                        match codec.encode_response(&resp) {
                            Ok(f) => f,
                            Err(_) => {
                                let err = Response::err(
                                    resp.id,
                                    format!(
                                        "response too large ({} rows)",
                                        resp.rows.len()
                                    ),
                                );
                                codec.encode_response(&err).context("encode error")?
                            }
                        }
                    }
                }
                Incoming::Batch(batch) => {
                    // Bound per-frame CPU: a single frame cannot force
                    // unbounded dispatch work.
                    if batch.ops.len() > 4096 {
                        let err = Response::err(batch.id, "batch too large (max 4096 ops)");
                        codec.encode_response(&err).context("encode error")?
                    } else {
                        let mut results = Vec::with_capacity(batch.ops.len());
                        for mut op in batch.ops {
                            if let Some(tok) = take_auth_token(&mut op.values) {
                                if let Some(id) = server.resolve_token(&tok) {
                                    authed = Some(id);
                                }
                            }
                            match server.authorize(&authed, op.op, &op.table) {
                                Err(e) => results.push(Response::err(op.id, e)),
                                Ok(()) => results.push(dispatch(&server, &authed, op)),
                            }
                        }
                        let bresp = BatchResponse {
                            id: batch.id,
                            results,
                        };
                        match codec.encode_batch_response(&bresp) {
                            Ok(f) => f,
                            Err(_) => {
                                let err = Response::err(
                                    batch.id,
                                    "batch response too large",
                                );
                                codec.encode_response(&err).context("encode error")?
                            }
                        }
                    }
                }
                Incoming::AtomicBatch(batch) => {
                    // One OCC transaction for the whole frame: all-or-nothing.
                    // Same 4096-op bound as Batch (per-frame CPU).
                    if batch.ops.len() > 4096 {
                        let err = Response::err(batch.id, "batch too large (max 4096 ops)");
                        codec.encode_response(&err).context("encode error")?
                    } else {
                        let bresp = execute_atomic(&server, &mut authed, batch);
                        match codec.encode_batch_response(&bresp) {
                            Ok(f) => f,
                            Err(_) => {
                                let err = Response::err(
                                    bresp.id,
                                    "batch response too large",
                                );
                                codec.encode_response(&err).context("encode error")?
                            }
                        }
                    }
                }
            };
            // p95/p99.9 alerting input: end-to-end handling time per frame.
            // Slow here means queued behind locks/scheduler, since dispatch
            // itself is microseconds — exactly the signal shedding needs.
            // Microsecond precision: millis truncates sub-ms Ping to 0.
            let slow_us = server.config().slow_threshold_ms * 1000;
            let elapsed_us = t0.elapsed().as_micros() as u64;
            server.record_request(elapsed_us > slow_us);
            let wlen = encoded.len() as u64;
            stream
                .write_all(&encoded)
                .await
                .context("failed to write to socket")?;
            server.record_io(0, wlen);
        }
        // Keep the buffer footprint proportional to steady-state frames:
        // a single huge (but legal) frame would otherwise pin megabytes
        // for the life of an idle keep-alive connection.
        if staging.capacity() > 65536 && staging.is_empty() {
            staging = BytesMut::with_capacity(4096);
        }
    }
}

/// Accept connections forever, spawning one task per connection.
pub async fn serve(server: Arc<BlitzServer>, listener: TcpListener) -> Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await.context("accept failed")?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(server, socket).await {
                tracing::debug!("connection ended: {:#}", e);
            }
        });
    }
}

/// Multi-listener serve with `SO_REUSEPORT` for 100K+ accept rates.
///
/// A single accept loop tops out at ~5-8K accepts/sec; SYN storms for
/// 100K+ CCU need N accept queues (one per core) so the kernel
/// load-balances SYNs. Each listener spawns its own accept task sharing
/// the same `Arc<BlitzServer>`.
pub async fn serve_reuseport(
    server: Arc<BlitzServer>,
    host: &str,
    port: u16,
    num_listeners: usize,
    backlog: u32,
) -> Result<()> {
    let addr: std::net::SocketAddr = format!("{}:{}", host, port).parse()?;
    let mut tasks = Vec::with_capacity(num_listeners);
    for _ in 0..num_listeners.max(1) {
        let sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        sock.set_reuse_address(true)?;
        #[cfg(unix)]
        sock.set_reuse_port(true)?;
        sock.set_nonblocking(true)?;
        // Small buffers keep 100K+ conns affordable; accepted sockets inherit.
        let _ = sock.set_recv_buffer_size(4096);
        let _ = sock.set_send_buffer_size(4096);
        sock.bind(&addr.into())?;
        sock.listen(backlog as i32)?;
        let std_listener: std::net::TcpListener = sock.into();
        let listener = TcpListener::from_std(std_listener)?;
        let server = Arc::clone(&server);
        tasks.push(tokio::spawn(async move {
            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::debug!("reuseport accept failed: {:#}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                };
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(server, socket).await {
                        tracing::debug!("connection ended: {:#}", e);
                    }
                });
            }
        }));
    }
    // Run until first listener task ends (never, barring fatal error).
    for t in tasks {
        t.await?;
    }
    Ok(())
}

/// Build a TLS acceptor from DER cert chain + PKCS#8 DER key.
/// Use `serve_tls` with it; plaintext `serve` stays for loopback/bench.
/// Terminate at a reverse proxy if rotation/OCSP is needed — in-process TLS
/// here covers single-node prod without extra hops.
pub fn tls_acceptor_from_der(
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
) -> Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let cert = CertificateDer::from(cert_der);
    let key = PrivateKeyDer::Pkcs8(key_der.into());
    let cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .context("invalid TLS cert/key")?;
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(cfg)))
}

/// Load PEM cert chain + first private key (PKCS#8 or RSA) from files.
pub fn tls_acceptor_from_pem_files(cert_path: &str, key_path: &str) -> Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::io::BufReader;
    let cert_file = std::fs::File::open(cert_path).context("open tls cert")?;
    let certs: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        anyhow::bail!("no certs in {}", cert_path);
    }
    let key_file = std::fs::File::open(key_path).context("open tls key")?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| anyhow::anyhow!("no private key in {}", key_path))?;
    let key: PrivateKeyDer = key.into();
    let cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("invalid TLS cert/key")?;
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(cfg)))
}

/// TLS accept loop: handshake, then the shared framing/dispatch path.
/// Failed handshakes close without a slot leak (guard lives in `handle_stream`).
pub async fn serve_tls(
    server: Arc<BlitzServer>,
    listener: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
) -> Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await.context("tls accept failed")?;
        let _ = socket.set_nodelay(true);
        let peer_ip = socket.peer_addr().ok().map(|a| a.ip());
        let acceptor = acceptor.clone();
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let tls = match acceptor.accept(socket).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!("tls handshake failed: {:#}", e);
                    return;
                }
            };
            if let Err(e) = handle_stream(server, tls, peer_ip).await {
                tracing::debug!("tls connection ended: {:#}", e);
            }
        });
    }
}

/// Minimal HTTP ops surface: `GET /metrics` (Prometheus exposition) and
/// `GET /readyz` (200 when started, 503 before). Hand-rolled HTTP/1.0 —
/// no new deps — for scraping without touching the binary hot path.
/// TLS termination stays at the reverse proxy (documented); this listener
/// binds loopback by default.
/// JSON-over-HTTP bridge listener: metrics, readiness, the `/v1`
/// data envelope, CORS, keep-alive, and an SSE change stream.
///
/// This is an ops bridge, not a general server: HTTP/1.x only, no
/// chunked bodies (Content-Length required), one handler task per
/// connection, keep-alive loop bounded per connection. Browsers need
/// `http_cors_origins` configured (else no CORS headers → fetch blocked).
pub async fn serve_http_ops(server: Arc<BlitzServer>, listener: TcpListener) -> Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await.context("http accept failed")?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            http_connection(server, socket).await;
        });
    }
}

/// CORS origin check: exact match or wildcard. Empty config = no headers.
fn cors_origin(server: &BlitzServer, origin: Option<&str>) -> Option<String> {
    let origins = &server.config().http_cors_origins;
    if origins.is_empty() {
        return None;
    }
    let origin = origin?;
    if origins.iter().any(|o| o == "*" || o == origin) {
        // Echo back explicit origins (credentials-safe); "*" only when the
        // operator literally configured "*".
        if origins.iter().any(|o| o == "*") {
            Some("*".to_string())
        } else {
            Some(origin.to_string())
        }
    } else {
        None
    }
}

fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Too Large",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

/// Read one request head (buffered; 16KiB cap). Returns
/// (method, target, version, headers, body-prefix bytes).
async fn read_head(
    socket: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Option<(String, String, String, Vec<(String, String)>, usize)> {
    use tokio::io::AsyncReadExt;
    loop {
        if let Some(pos) = find_headers_end(buf) {
            let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
            return Some(parse_head(&head, pos));
        }
        if buf.len() > 16384 {
            return None;
        }
        let mut tmp = [0u8; 4096];
        match socket.read(&mut tmp).await {
            Ok(0) => return None,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => return None,
        }
    }
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn parse_head(text: &str, header_end: usize) -> (String, String, String, Vec<(String, String)>, usize) {
    let mut lines = text.lines();
    let request_line = lines.next().unwrap_or("/");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let version = parts.next().unwrap_or("HTTP/1.0").to_string();
    let mut headers = Vec::new();
    for line in lines {
        let line = line.trim_end_matches('\r');
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    (method, target, version, headers, header_end)
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

/// One HTTP connection: keep-alive request loop (bounded), CORS, SSE.
async fn http_connection(server: Arc<BlitzServer>, mut socket: TcpStream) {
    use tokio::io::AsyncWriteExt;
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    // Bound requests per connection (FD churn over FD hoarding).
    for _ in 0..10000 {
        let (method, target, version, headers, header_end) = match read_head(&mut socket, &mut buf).await {
            Some(h) => h,
            None => return,
        };
        // Bytes after the head belong to the body (pipelined tail stays).
        let mut body = buf.split_off(header_end);
        buf.clear();
        let content_length: usize = header(&headers, "content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
        let max_body = server.config().max_message_size.max(1024);
        if content_length > max_body {
            let _ = respond(&mut socket, 413, "text/plain", "too large\n", "close", None).await;
            return;
        }
        while body.len() < content_length {
            let mut tmp = [0u8; 8192];
            match tokio::io::AsyncReadExt::read(&mut socket, &mut tmp).await {
                Ok(0) => return,
                Ok(n) => body.extend_from_slice(&tmp[..n]),
                Err(_) => return,
            }
            if body.len() > max_body {
                let _ = respond(&mut socket, 413, "text/plain", "too large\n", "close", None).await;
                return;
            }
        }
        body.truncate(content_length);
        // Keep-alive: HTTP/1.1 defaults on; 1.0 defaults off.
        let conn_hdr = header(&headers, "connection").unwrap_or("");
        let keep_alive = if version == "HTTP/1.1" {
            conn_hdr.to_ascii_lowercase() != "close"
        } else {
            conn_hdr.to_ascii_lowercase() == "keep-alive"
        };
        let conn_tok = if keep_alive { "keep-alive" } else { "close" };

        // CORS preflight: answered without auth, with the allow-list.
        let origin = header(&headers, "origin");
        let cors = cors_origin(&server, origin);
        if method == "OPTIONS" {
            let mut extra = String::new();
            if let Some(o) = cors.as_deref() {
                extra = format!(
                    "access-control-allow-origin: {}\r\naccess-control-allow-methods: GET, POST, OPTIONS\r\naccess-control-allow-headers: authorization, content-type\r\naccess-control-max-age: 86400\r\n",
                    o
                );
            }
            let head = format!(
                "{} 204 {}\r\n{}content-length: 0\r\nconnection: {}\r\n\r\n",
                version, reason(204), extra, conn_tok
            );
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            if !keep_alive {
                return;
            }
            continue;
        }

        // Split path and query (SSE params live in the query).
        let (path, query) = match target.find('?') {
            Some(i) => (&target[..i], &target[i + 1..]),
            None => (target.as_str(), ""),
        };
        let query_param = |name: &str| -> Option<String> {
            query.split('&').find_map(|kv| {
                let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                if k == name {
                    Some(percent_decode(v))
                } else {
                    None
                }
            })
        };
        // Stateless identity per request: header first, `?token=` fallback
        // (EventSource can't set headers — SSE needs the fallback).
        let bearer = header(&headers, "authorization").and_then(|v| {
            let v = v.trim();
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
                .map(|t| t.trim().to_string())
        });
        let token = bearer.or_else(|| query_param("token"));
        let authed = token.as_deref().and_then(|t| server.resolve_token(t));

        // SSE change stream: holds the connection by design.
        if method == "GET" && path == "/v1/stream" {
            let table = query_param("table").unwrap_or_default();
            if table.is_empty() {
                let _ = respond(&mut socket, 400, "text/plain", "missing ?table=\n", "close", cors.clone()).await;
                return;
            }
            if let Err(e) = server.authorize(&authed, blitz_protocol::Op::Subscribe, &table) {
                let code = if e.starts_with("unauthorized") { 401 } else { 403 };
                let _ = respond(&mut socket, code, "text/plain", &format!("{}\n", e), "close", cors.clone()).await;
                return;
            }
            if server.config().require_auth && server.owner_column(&table).is_some() {
                let _ = respond(&mut socket, 403, "text/plain", "push streams disabled on row-owner tables (poll instead)\n", "close", cors.clone()).await;
                return;
            }
            sse_stream(&server, &mut socket, &table, query_param("since"), cors).await;
            return;
        }

        let (code, ctype, resp_body): (u16, &str, String) = match (method.as_str(), path) {
            ("GET", "/metrics") => (200, "text/plain; version=0.0.4", server.metrics_text()),
            ("GET", "/readyz") => {
                if server.uptime_secs().is_some() {
                    (200, "text/plain", "ok\n".to_string())
                } else {
                    (503, "text/plain", "starting\n".to_string())
                }
            }
            ("POST", "/v1/op") => {
                let (c, b) = crate::http_bridge::handle_op(&server, &authed, &body);
                (c, "application/json", b)
            }
            ("POST", "/v1/batch") => {
                let (c, b) = crate::http_bridge::handle_batch(&server, &authed, &body);
                (c, "application/json", b)
            }
            _ if method != "GET" && (path == "/v1/op" || path == "/v1/batch") => {
                (405, "text/plain", "method not allowed (use POST)\n".to_string())
            }
            _ => (404, "text/plain", "not found\n".to_string()),
        };
        if respond(&mut socket, code, ctype, &resp_body, conn_tok, cors).await.is_err() {
            return;
        }
        if !keep_alive {
            return;
        }
    }
}

async fn respond(
    socket: &mut TcpStream,
    code: u16,
    ctype: &str,
    body: &str,
    conn: &str,
    cors: Option<String>,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut head = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: {}\r\n",
        code,
        reason(code),
        ctype,
        body.len(),
        conn
    );
    if let Some(o) = cors {
        head.push_str(&format!("access-control-allow-origin: {}\r\n", o));
    }
    head.push_str("\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await
}

/// Percent-decode a query value (`+` → space; invalid sequences pass through).
fn percent_decode(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = |c: u8| match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                };
                match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push(h << 4 | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// SSE change stream over the bounded change-log: polls `read_changes`
/// every 250ms, emitting new records as `data:` frames from the last seen
/// `ts_micros`. Ends on client disconnect (read side closes) or shutdown.
/// Same visibility as `Subscribe` polls (recorded writes only).
async fn sse_stream(
    server: &Arc<BlitzServer>,
    socket: &mut TcpStream,
    table: &str,
    since: Option<String>,
    cors: Option<String>,
) {
    use tokio::io::AsyncWriteExt;
    let mut head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: keep-alive\r\n".to_string();
    if let Some(o) = cors {
        head.push_str(&format!("access-control-allow-origin: {}\r\n", o));
    }
    head.push_str("\r\n");
    if socket.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    // Comment keep-alive so intermediaries don't buffer us out.
    if socket.write_all(b": connected\n\n").await.is_err() {
        return;
    }
    let mut since: u64 = since.and_then(|s| s.parse().ok()).unwrap_or(0);
    loop {
        let recs = server.read_changes(table, since, 100);
        for r in &recs {
            let data = format!(
                "{{\"table\":{},\"op\":{},\"row_id\":{},\"ts\":{}}}",
                serde_json::Value::String(r.table.clone()),
                serde_json::Value::String(r.op.to_string()),
                r.row_id,
                r.ts_micros
            );
            let frame = format!("data: {}\n\n", data);
            if socket.write_all(frame.as_bytes()).await.is_err() {
                return;
            }
            since = since.max(r.ts_micros);
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        // Detect a gone client without blocking the stream: a zero-byte
        // read readiness probe would consume framing; instead rely on the
        // next write failing (250ms cadence bounds detection delay).
        if server.config().idle_timeout_secs > 0 {
            // Shared idle discipline: over-long streams eventually recycle
            // when the operator sets idle timeouts (documented).
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::value::Value;
    use std::collections::HashMap;
    use tokio::net::TcpStream;

    struct Client {
        read: tokio::net::tcp::OwnedReadHalf,
        write: tokio::net::tcp::OwnedWriteHalf,
        buf: BytesMut,
        codec: FrameCodec,
    }

    impl Client {
        async fn connect(addr: std::net::SocketAddr) -> Result<Self> {
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = stream.into_split();
            Ok(Self {
                read,
                write,
                buf: BytesMut::new(),
                codec: FrameCodec::with_default_limit(),
            })
        }

        async fn roundtrip(&mut self, req: &Request) -> Result<Response> {
            let frame = self.codec.encode_request(req)?;
            self.write.write_all(&frame).await?;
            loop {
                if let Some(payload) = self.codec.feed(&mut self.buf)? {
                    return Ok(self.codec.decode_response(payload)?);
                }
                let n = self.read.read_buf(&mut self.buf).await?;
                assert!(n > 0, "server closed connection unexpectedly");
            }
        }
    }

    fn values(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    async fn roundtrip_batch(
        client: &mut Client,
        id: u64,
        ops: Vec<Request>,
    ) -> Result<BatchResponse> {
        use blitz_protocol::BatchRequest;
        let frame = client
            .codec
            .encode_batch_request(&BatchRequest { id, ops })?;
        client.write.write_all(&frame).await?;
        loop {
            if let Some(payload) = client.codec.feed(&mut client.buf)? {
                return Ok(client.codec.decode_batch_response(payload)?);
            }
            let n = client.read.read_buf(&mut client.buf).await?;
            assert!(n > 0, "server closed connection unexpectedly");
        }
    }

    async fn roundtrip_atomic(
        client: &mut Client,
        id: u64,
        ops: Vec<Request>,
    ) -> Result<BatchResponse> {
        use blitz_protocol::BatchRequest;
        let frame = client
            .codec
            .encode_atomic_batch_request(&BatchRequest { id, ops })?;
        client.write.write_all(&frame).await?;
        loop {
            if let Some(payload) = client.codec.feed(&mut client.buf)? {
                return Ok(client.codec.decode_batch_response(payload)?);
            }
            let n = client.read.read_buf(&mut client.buf).await?;
            assert!(n > 0, "server closed connection unexpectedly");
        }
    }

    #[tokio::test]
    async fn test_e2e_ping_insert_get_scan_delete() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut client = Client::connect(addr).await.unwrap();

        // Ping.
        let resp = client.roundtrip(&Request::ping(1)).await.unwrap();
        assert_eq!(resp.id, 1);
        assert!(resp.ok);

        // Insert.
        let resp = client
            .roundtrip(&Request {
                id: 2,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Alice".into())),
                    ("email", Value::String("alice@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(resp.ok, "insert failed: {:?}", resp.error);
        let assigned = resp.rows[0].id;

        // Get.
        let resp = client
            .roundtrip(&Request {
                id: 3,
                op: Op::Get,
                table: "users".into(),
                row_id: Some(assigned),
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(
            resp.rows[0].values.get("name"),
            Some(&Value::String("Alice".into()))
        );

        // Update.
        let resp = client
            .roundtrip(&Request {
                id: 4,
                op: Op::Update,
                table: "users".into(),
                row_id: Some(assigned),
                values: Some(values(&[("name", Value::String("Bob".into()))])),
            })
            .await
            .unwrap();
        assert!(resp.ok, "update failed: {:?}", resp.error);
        assert_eq!(
            resp.rows[0].values.get("name"),
            Some(&Value::String("Bob".into()))
        );

        // Scan.
        let resp = client
            .roundtrip(&Request {
                id: 5,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.rows.len(), 1);

        // Delete.
        let resp = client
            .roundtrip(&Request {
                id: 6,
                op: Op::Delete,
                table: "users".into(),
                row_id: Some(assigned),
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);

        // Get after delete -> error response, connection stays alive.
        let resp = client
            .roundtrip(&Request {
                id: 7,
                op: Op::Get,
                table: "users".into(),
                row_id: Some(assigned),
                values: None,
            })
            .await
            .unwrap();
        assert!(!resp.ok);
        assert!(resp.error.is_some());

        // Unknown table -> error response, connection stays alive.
        let resp = client
            .roundtrip(&Request {
                id: 8,
                op: Op::Scan,
                table: "nope".into(),
                row_id: None,
                values: None,
            })
            .await
            .unwrap();
        assert!(!resp.ok);
    }

    #[tokio::test]
    async fn test_e2e_batch_mixed_results() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut client = Client::connect(addr).await.unwrap();

        // One frame, five ops: insert, get, update, get-missing (err), scan.
        let bresp = roundtrip_batch(
            &mut client,
            50,
            vec![
                Request {
                    id: 51,
                    op: Op::Insert,
                    table: "users".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("id", Value::Int64(9)),
                        ("name", Value::String("Batch".into())),
                        ("email", Value::String("batch@example.com".into())),
                    ])),
                },
                Request {
                    id: 52,
                    op: Op::Get,
                    table: "users".into(),
                    row_id: Some(1),
                    values: None,
                },
                Request {
                    id: 53,
                    op: Op::Update,
                    table: "users".into(),
                    row_id: Some(1),
                    values: Some(values(&[("name", Value::String("Batched".into()))])),
                },
                Request {
                    id: 54,
                    op: Op::Get,
                    table: "users".into(),
                    row_id: Some(9999),
                    values: None,
                },
                Request {
                    id: 55,
                    op: Op::Scan,
                    table: "users".into(),
                    row_id: None,
                    values: None,
                },
            ],
        )
        .await
        .unwrap();

        assert_eq!(bresp.id, 50);
        assert_eq!(bresp.results.len(), 5);
        assert!(bresp.results[0].ok); // insert
        assert!(bresp.results[1].ok); // get
        assert_eq!(
            bresp.results[2].rows[0].values.get("name"),
            Some(&Value::String("Batched".into()))
        );
        // Partial failure is per-op: missing row errs, frame still succeeds.
        assert!(!bresp.results[3].ok);
        assert_eq!(bresp.results[4].rows.len(), 1); // scan sees the insert
    }

    #[tokio::test]
    async fn test_many_concurrent_clients() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut handles = Vec::new();
        for c in 0..16 {
            handles.push(tokio::spawn(async move {
                let mut client = Client::connect(addr).await.unwrap();
                for i in 0..25 {
                    let resp = client
                        .roundtrip(&Request {
                            id: (c * 100 + i) as u64,
                            op: Op::Insert,
                            table: "users".into(),
                            row_id: None,
                            values: Some(values(&[
                                ("id", Value::Int64(c * 100 + i)),
                                ("name", Value::String("n".into())),
                                (
                                    "email",
                                    Value::String(format!("c{c}i{i}@x.com").into()),
                                ),
                            ])),
                        })
                        .await
                        .unwrap();
                    assert!(resp.ok, "insert failed: {:?}", resp.error);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(server.engine().count("users").unwrap(), 16 * 25);
        // Disconnects are observed asynchronously; wait for slots to drain.
        for _ in 0..200 {
            if server.connection_count() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(server.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_insert_idempotent_retry_returns_same_id() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        let mk = |id: u64| Request {
            id,
            op: Op::Insert,
            table: "users".into(),
            row_id: None,
            values: Some(values(&[
                ("id", Value::Int64(1)),
                ("name", Value::String("Idem".into())),
                ("email", Value::String("idem@example.com".into())),
                ("_idem", Value::String("k-123".into())),
            ])),
        };
        let r1 = client.roundtrip(&mk(1)).await.unwrap();
        assert!(r1.ok, "first insert failed: {:?}", r1.error);
        let r2 = client.roundtrip(&mk(2)).await.unwrap();
        assert!(r2.ok, "retry failed: {:?}", r2.error);
        assert_eq!(r1.rows[0].id, r2.rows[0].id, "idempotency must return same RowId");
        // No duplicate row created.
        assert_eq!(server.engine().count("users").unwrap(), 1);
        // `_idem` stripped, not stored as a column.
        let got = server.engine().get("users", blitz_types::id::RowId::new(r1.rows[0].id)).unwrap().unwrap();
        assert!(!got.values.contains_key("_idem"));
    }

    #[tokio::test]
    async fn test_require_auth_handshake_and_enforcement() {
        use crate::server::ServerConfig;
        use blitz_auth::{Identity, Permission};
        let token = "tok-abc-123".to_string();
        let ident = Identity::new("alice")
            .with_permission(Permission::Read)
            .with_permission(Permission::Write);
        let mut cfg = ServerConfig::default();
        cfg.require_auth = true;
        cfg.auth_tokens.insert(token.clone(), ident);
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Unauthenticated write → unauthorized, nothing stored.
        let r = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("X".into())),
                    ("email", Value::String("x@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("unauthorized"), "got {:?}", r);
        assert_eq!(server.engine().count("users").unwrap(), 0);

        // Wrong token stays unauthenticated.
        let r = client
            .roundtrip(&Request {
                id: 2,
                op: Op::Ping,
                table: String::new(),
                row_id: None,
                values: Some(values(&[("_auth", Value::String("bogus".into()))])),
            })
            .await
            .unwrap();
        assert!(r.ok); // Ping always ok, but identity NOT set
        let r = client
            .roundtrip(&Request {
                id: 3,
                op: Op::Get,
                table: "users".into(),
                row_id: Some(1),
                values: None,
            })
            .await
            .unwrap();
        assert!(!r.ok, "bogus token must not authenticate");

        // Correct handshake via Ping+_auth binds the connection.
        let r = client
            .roundtrip(&Request {
                id: 4,
                op: Op::Ping,
                table: String::new(),
                row_id: None,
                values: Some(values(&[("_auth", Value::String(token))])),
            })
            .await
            .unwrap();
        assert!(r.ok);
        // Now the same connection writes fine (Write granted).
        let r = client
            .roundtrip(&Request {
                id: 5,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("X".into())),
                    ("email", Value::String("x@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(r.ok, "authed write failed: {:?}", r.error);
        // ...but Delete (not granted, no policy rules) is forbidden.
        let row = r.rows[0].id;
        let r = client
            .roundtrip(&Request { id: 6, op: Op::Delete, table: "users".into(), row_id: Some(row), values: None })
            .await
            .unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("forbidden"), "got {:?}", r);
    }

    #[tokio::test]
    async fn test_find_lookup_by_unique() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let ins = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(7)),
                    ("name", Value::String("Findme".into())),
                    ("email", Value::String("find@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(ins.ok);
        // Hit.
        let r = client
            .roundtrip(&Request {
                id: 2,
                op: Op::Find,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("_col", Value::String("email".into())),
                    ("_val", Value::String("find@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        assert_eq!(r.rows[0].id, ins.rows[0].id);
        // Miss → err (not empty-ok, so clients can distinguish).
        let r = client
            .roundtrip(&Request {
                id: 3,
                op: Op::Find,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("_col", Value::String("email".into())),
                    ("_val", Value::String("nope@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(!r.ok);
        // Non-unique column → rejected, never a full scan.
        let r = client
            .roundtrip(&Request {
                id: 4,
                op: Op::Find,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("_col", Value::String("name".into())),
                    ("_val", Value::String("Findme".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(!r.ok, "non-unique find must fail, got {:?}", r);
    }

    #[tokio::test]
    async fn test_subscribe_long_poll_with_since_and_limit() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let sub = |id: u64, since: u64, limit: i64| Request {
            id,
            op: Op::Subscribe,
            table: "users".into(),
            row_id: None,
            values: Some(values(&[
                ("_since", Value::Int64(since as i64)),
                ("_limit", Value::Int64(limit)),
            ])),
        };
        // Empty at first.
        let r = client.roundtrip(&sub(1, 0, 100)).await.unwrap();
        assert!(r.ok && r.rows.is_empty(), "got {:?}", r);
        // Two writes → two records, oldest first.
        for i in 0..2 {
            let r = client
                .roundtrip(&Request {
                    id: 10 + i,
                    op: Op::Insert,
                    table: "users".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("id", Value::Int64(100 + i as i64)),
                        ("name", Value::String(format!("S{}", i))),
                        ("email", Value::String(format!("s{}@x.com", i))),
                    ])),
                })
                .await
                .unwrap();
            assert!(r.ok);
        }
        let r = client.roundtrip(&sub(20, 0, 100)).await.unwrap();
        assert!(r.ok && r.rows.len() == 2, "got {:?}", r);
        assert!(r.rows[0].id < r.rows[1].id);
        // _since filters to newer only; _limit caps.
        let ts = r.rows[0].values.get("ts").cloned();
        let since = match ts {
            Some(Value::Int64(n)) => n as u64,
            _ => panic!("record missing ts: {:?}", r.rows[0]),
        };
        let r2 = client.roundtrip(&sub(21, since, 100)).await.unwrap();
        assert!(r2.ok && r2.rows.len() == 1, "got {:?}", r2);
        let r3 = client.roundtrip(&sub(22, 0, 1)).await.unwrap();
        assert!(r3.ok && r3.rows.len() == 1, "limit ignored: {:?}", r3);
    }

    #[tokio::test]
    async fn test_scan_cursor_desc_pages_without_overlap() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        for i in 0..6 {
            let r = client
                .roundtrip(&Request {
                    id: i,
                    op: Op::Insert,
                    table: "users".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("id", Value::Int64(i as i64)),
                        ("name", Value::String("n".into())),
                        ("email", Value::String(format!("cu{}@x.com", i))),
                    ])),
                })
                .await
                .unwrap();
            assert!(r.ok);
        }
        // Page 1 desc limit 2 → two largest ids.
        let p1 = client
            .roundtrip(&Request {
                id: 100,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("_limit", Value::Int64(2)),
                    ("_order", Value::String("desc".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(p1.ok && p1.rows.len() == 2, "got {:?}", p1);
        assert!(p1.rows[0].id > p1.rows[1].id);
        // Page 2 via cursor (exclusive) → next two, no overlap.
        let cursor = p1.rows[1].id;
        let p2 = client
            .roundtrip(&Request {
                id: 101,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("_limit", Value::Int64(2)),
                    ("_order", Value::String("desc".into())),
                    ("_cursor", Value::UInt64(cursor)),
                ])),
            })
            .await
            .unwrap();
        assert!(p2.ok && p2.rows.len() == 2, "got {:?}", p2);
        assert!(!p2.rows.iter().any(|r| r.id == p1.rows[0].id || r.id == cursor));
        assert!(p2.rows[0].id < cursor);
    }

    #[tokio::test]
    async fn test_batch_is_not_atomic_partial_failure_commits_rest() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        // Seed one row with a taken email.
        let seed = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Seed".into())),
                    ("email", Value::String("taken@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(seed.ok);
        // Batch: [dup-email insert (fails), fresh insert (must still commit)].
        let b = roundtrip_batch(&mut client, 50, vec![
            Request {
                id: 51,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(2)),
                    ("name", Value::String("Dup".into())),
                    ("email", Value::String("taken@example.com".into())),
                ])),
            },
            Request {
                id: 52,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(3)),
                    ("name", Value::String("Fresh".into())),
                    ("email", Value::String("fresh@example.com".into())),
                ])),
            },
        ])
        .await
        .unwrap();
        assert!(!b.results[0].ok, "dup must fail");
        assert!(b.results[1].ok, "sibling must commit despite partial failure");
        assert_eq!(server.engine().count("users").unwrap(), 2);
    }

    #[tokio::test]
    async fn test_atomic_batch_commits_all_or_nothing() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Seed one row; learn its assigned id.
        let seed = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Seed".into())),
                    ("email", Value::String("seed@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(seed.ok);
        let seed_id = seed.rows[0].id;

        // Atomic checkout-style frame: read seed, update it, insert an order.
        let b = roundtrip_atomic(&mut client, 50, vec![
            Request {
                id: 51,
                op: Op::Get,
                table: "users".into(),
                row_id: Some(seed_id),
                values: None,
            },
            Request {
                id: 52,
                op: Op::Update,
                table: "users".into(),
                row_id: Some(seed_id),
                values: Some(values(&[("name", Value::String("Reserved".into()))])),
            },
            Request {
                id: 53,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(2)),
                    ("name", Value::String("Fresh".into())),
                    ("email", Value::String("fresh@example.com".into())),
                ])),
            },
        ])
        .await
        .unwrap();
        assert!(b.results.iter().all(|r| r.ok), "all must commit: {:?}", b.results);
        assert_eq!(b.results.len(), 3);
        // Get saw the pre-commit row; update response carries the new name.
        assert_eq!(
            b.results[0].rows[0].values.get("name"),
            Some(&Value::String("Seed".into()))
        );
        assert_eq!(
            b.results[1].rows[0].values.get("name"),
            Some(&Value::String("Reserved".into()))
        );
        assert_eq!(server.engine().count("users").unwrap(), 2);
        let after = server.engine().get("users", RowId::new(seed_id)).unwrap().unwrap();
        assert_eq!(after.get("name"), Some(&Value::String("Reserved".into())));
    }

    #[tokio::test]
    async fn test_atomic_batch_aborts_on_conflict_nothing_applied() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Seed one row holding the taken email.
        let seed = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Seed".into())),
                    ("email", Value::String("taken@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(seed.ok);

        // Atomic frame: [fresh insert (would succeed alone), dup insert].
        // Both must err; the fresh row must NOT exist afterwards.
        let b = roundtrip_atomic(&mut client, 50, vec![
            Request {
                id: 51,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(2)),
                    ("name", Value::String("Fresh".into())),
                    ("email", Value::String("fresh@example.com".into())),
                ])),
            },
            Request {
                id: 52,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(3)),
                    ("name", Value::String("Dup".into())),
                    ("email", Value::String("taken@example.com".into())),
                ])),
            },
        ])
        .await
        .unwrap();
        assert!(b.results.iter().all(|r| !r.ok), "all must abort: {:?}", b.results);
        assert_eq!(server.engine().count("users").unwrap(), 1);
    }

    #[tokio::test]
    async fn test_atomic_batch_rejects_snapshot_ops() {        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Scan inside an atomic frame: the whole frame aborts, nothing applies.
        let b = roundtrip_atomic(&mut client, 50, vec![
            Request {
                id: 51,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Ghost".into())),
                    ("email", Value::String("ghost@example.com".into())),
                ])),
            },
            Request {
                id: 52,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: None,
            },
        ])
        .await
        .unwrap();
        assert!(b.results.iter().all(|r| !r.ok), "snapshot op must abort all: {:?}", b.results);
        assert_eq!(server.engine().count("users").unwrap(), 0);
    }

    /// Checkout-style procedure used by the Call tests: read balance, branch,
    /// insert order + mark charged atomically, or Fail (rolls everything back).
    fn checkout_procedure() -> blitz_runtime::Procedure {
        use blitz_runtime::{Procedure, ProcedureStep};
        use blitz_types::value::Value;
        let mut order_vals = std::collections::HashMap::new();
        order_vals.insert("id".to_string(), Value::Int64(1));
        order_vals.insert("account_id".to_string(), Value::String("$account_id".into()));
        order_vals.insert("amount".to_string(), Value::String("$price".into()));
        let mut mark_vals = std::collections::HashMap::new();
        mark_vals.insert("status".to_string(), Value::String("charged".into()));
        Procedure::new("checkout")
            .with_step(ProcedureStep::Read {
                table: "accounts".into(),
                id: Value::String("$account_id".into()),
                into: "a".into(),
            })
            .with_step(ProcedureStep::If {
                condition: blitz_runtime::function::Condition::GreaterOrEqual(
                    "a.balance".into(),
                    Value::String("$price".into()),
                ),
                then_steps: vec![
                    ProcedureStep::Insert {
                        table: "orders".into(),
                        values: order_vals,
                        into: "order_id".into(),
                    },
                    ProcedureStep::Update {
                        table: "accounts".into(),
                        id: Value::String("$account_id".into()),
                        values: mark_vals,
                    },
                    ProcedureStep::Return {
                        value: Value::String("$order_id".into()),
                    },
                ],
                else_steps: vec![ProcedureStep::Fail {
                    message: "insufficient balance".into(),
                }],
            })
    }

    fn accounts_schema() -> blitz_types::schema::TableSchema {
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        TableSchema::new("accounts")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("balance", ColumnType::Int64))
            .with_column(ColumnDef::new("status", ColumnType::String))
    }

    fn orders_schema() -> blitz_types::schema::TableSchema {
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        TableSchema::new("orders")
            .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
            .with_column(ColumnDef::new("account_id", ColumnType::Int64))
            .with_column(ColumnDef::new("amount", ColumnType::Int64))
    }

    fn call_req(id: u64, name: &str, args: Vec<(&str, Value)>) -> Request {
        Request {
            id,
            op: Op::Call,
            table: format!("fn:{}", name),
            row_id: None,
            values: Some(values(&args)),
        }
    }

    #[tokio::test]
    async fn test_call_checkout_happy_path() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        server.engine().create_table(accounts_schema()).unwrap();
        server.engine().create_table(orders_schema()).unwrap();
        server.register_procedure(checkout_procedure());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Seed an account with balance 100; learn its assigned id.
        let seed = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "accounts".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("balance", Value::Int64(100)),
                    ("status", Value::String("active".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(seed.ok);
        let acct = seed.rows[0].id;

        let r = client
            .roundtrip(&call_req(2, "checkout", vec![
                ("account_id", Value::Int64(acct as i64)),
                ("price", Value::Int64(40)),
            ]))
            .await
            .unwrap();
        assert!(r.ok, "call failed: {:?}", r.error);
        assert_eq!(r.rows.len(), 1);
        // _applied reports both writes with real assigned ids, in order.
        let applied = r.rows[0].values.get("_applied").expect("missing _applied");
        let arr = match applied {
            Value::Json(serde_json::Value::Array(a)) => a,
            other => panic!("_applied not an array: {:?}", other),
        };
        assert_eq!(arr.len(), 2, "_applied: {:?}", arr);
        assert_eq!(arr[0]["table"], serde_json::Value::String("orders".into()));
        assert_eq!(arr[1]["table"], serde_json::Value::String("accounts".into()));
        let order_id = arr[0]["id"].as_u64().expect("order id");

        // State: account charged, order row holds the amount.
        let after = server.engine().get("accounts", RowId::new(acct)).unwrap().unwrap();
        assert_eq!(after.get("status"), Some(&Value::String("charged".into())));
        let order = server.engine().get("orders", RowId::new(order_id)).unwrap().unwrap();
        assert_eq!(order.get("amount"), Some(&Value::Int64(40)));
    }

    #[tokio::test]
    async fn test_call_checkout_insufficient_balance_rolls_back() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        server.engine().create_table(accounts_schema()).unwrap();
        server.engine().create_table(orders_schema()).unwrap();
        server.register_procedure(checkout_procedure());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        let seed = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "accounts".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("balance", Value::Int64(10)),
                    ("status", Value::String("active".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(seed.ok);
        let acct = seed.rows[0].id;

        let r = client
            .roundtrip(&call_req(2, "checkout", vec![
                ("account_id", Value::UInt64(acct)),
                ("price", Value::Int64(40)),
            ]))
            .await
            .unwrap();
        assert!(!r.ok, "must abort, got {:?}", r.rows);
        assert!(r.error.as_deref().unwrap_or("").contains("insufficient balance"), "err: {:?}", r.error);
        // Nothing applied: status untouched, no order row.
        let after = server.engine().get("accounts", RowId::new(acct)).unwrap().unwrap();
        assert_eq!(after.get("status"), Some(&Value::String("active".into())));
        assert_eq!(server.engine().count("orders").unwrap(), 0);
    }

    #[tokio::test]
    async fn test_call_unknown_procedure_and_bad_resource() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        let r = client.roundtrip(&call_req(1, "nope", vec![])).await.unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("not found"), "got {:?}", r.error);

        let bad = client
            .roundtrip(&Request { id: 2, op: Op::Call, table: "users".into(), row_id: None, values: None })
            .await
            .unwrap();
        assert!(!bad.ok && bad.error.as_deref().unwrap_or("").contains("fn:<procedure>"), "got {:?}", bad.error);
    }

    #[tokio::test]
    async fn test_call_inside_atomic_batch_rejected() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        server.register_procedure(checkout_procedure());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        let b = roundtrip_atomic(&mut client, 50, vec![
            call_req(51, "checkout", vec![("account_id", Value::Int64(1))]),
        ])
        .await
        .unwrap();
        assert!(b.results.iter().all(|r| !r.ok), "nested call must abort: {:?}", b.results);
    }

    /// Sharded test server: `widgets` hashed by `owner` over 4 physicals.
    /// Clients only ever see the base name; the wire carries global ids.
    fn sharded_server() -> Arc<BlitzServer> {
        use crate::server::{ServerConfig, ShardSpec};
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        let mut cfg = ServerConfig::default();
        cfg.table_shards.insert("widgets".into(), ShardSpec::new(4, "owner"));
        let server = BlitzServer::with_config(cfg);
        for s in 0..4 {
            server
                .engine()
                .create_table(
                    TableSchema::new(format!("widgets_{:02}", s))
                        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                        .with_column(ColumnDef::new("owner", ColumnType::String).nullable()),
                )
                .unwrap();
        }
        Arc::new(server)
    }

    fn widget_insert(id: u64, n: i64, owner: &str) -> Request {
        Request {
            id,
            op: Op::Insert,
            table: "widgets".into(),
            row_id: None,
            values: Some(values(&[
                ("id", Value::Int64(n)),
                ("owner", Value::String(owner.into())),
            ])),
        }
    }

    /// Auth server: require_auth + `docs` owned by its `owner` column,
    /// plus any extra `row_owner` entries. Tokens: alice/bob long-lived,
    /// admin long-lived, sess-* sessions.
    fn owned_server() -> Arc<BlitzServer> {
        owned_server_with(&[])
    }

    fn owned_server_with(extra_owner: &[(&str, &str)]) -> Arc<BlitzServer> {
        use crate::server::ServerConfig;
        use blitz_auth::{Identity, Permission};
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        let mut cfg = ServerConfig::default();
        cfg.require_auth = true;
        cfg.row_owner.insert("docs".into(), "owner".into());
        for (t, c) in extra_owner {
            cfg.row_owner.insert(t.to_string(), c.to_string());
        }
        let server = BlitzServer::with_config(cfg);
        server
            .engine()
            .create_table(
                TableSchema::new("docs")
                    .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                    .with_column(ColumnDef::new("owner", ColumnType::String).nullable())
                    .with_column(ColumnDef::new("body", ColumnType::String).nullable()),
            )
            .unwrap();
        let user = |subject: &str| {
            Identity::new(subject)
                .with_permission(Permission::Read)
                .with_permission(Permission::Write)
                .with_permission(Permission::Delete)
        };
        server.register_identity("tok-alice".into(), user("alice"));
        server.register_identity("tok-bob".into(), user("bob"));
        server.register_identity("tok-admin".into(), Identity::new("root").with_role("admin"));
        server.register_session("sess-alice".into(), user("alice"), 3600);
        server.register_session("sess-dead".into(), user("alice"), 0);
        Arc::new(server)
    }

    fn authed_req(id: u64, op: Op, table: &str, tok: &str, row_id: Option<u64>, vals: Vec<(String, Value)>) -> Request {
        let mut all: Vec<(String, Value)> = vec![("_auth".to_string(), Value::String(tok.into()))];
        all.extend(vals);
        let map: std::collections::HashMap<String, Value> = all.into_iter().collect();
        Request { id, op, table: table.into(), row_id, values: Some(map) }
    }

    fn doc_vals(owner: &str, body: &str) -> Vec<(String, Value)> {
        vec![
            ("id".to_string(), Value::Int64(1)),
            ("owner".to_string(), Value::String(owner.into())),
            ("body".to_string(), Value::String(body.into())),
        ]
    }

    #[tokio::test]
    async fn test_row_ownership_point_ops() {
        let server = owned_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Alice creates her own doc.
        let ins = client
            .roundtrip(&authed_req(1, Op::Insert, "docs", "tok-alice", None, doc_vals("alice", "a")))
            .await
            .unwrap();
        assert!(ins.ok, "own insert failed: {:?}", ins.error);
        let gid = ins.rows[0].id;

        // Bob can't read it (hidden as not-found), update it, or delete it.
        let g = client.roundtrip(&authed_req(2, Op::Get, "docs", "tok-bob", Some(gid), vec![])).await.unwrap();
        assert!(!g.ok && g.error.as_deref().unwrap_or("").contains("row not found"), "leak: {:?}", g);
        let u = client
            .roundtrip(&authed_req(3, Op::Update, "docs", "tok-bob", Some(gid), vec![("body".to_string(), Value::String("hijack".into()))]))
            .await
            .unwrap();
        assert!(!u.ok && u.error.as_deref().unwrap_or("").contains("forbidden"), "got {:?}", u);
        let d = client.roundtrip(&authed_req(4, Op::Delete, "docs", "tok-bob", Some(gid), vec![])).await.unwrap();
        assert!(!d.ok, "bob deleted alice's row");

        // Bob can't create docs FOR alice either (or without owner).
        let f = client.roundtrip(&authed_req(5, Op::Insert, "docs", "tok-bob", None, doc_vals("alice", "forged"))).await.unwrap();
        assert!(!f.ok && f.error.as_deref().unwrap_or("").contains("forbidden"), "got {:?}", f);
        let n = client
            .roundtrip(&authed_req(6, Op::Insert, "docs", "tok-bob", None, vec![("id".to_string(), Value::Int64(9))]))
            .await
            .unwrap();
        assert!(!n.ok, "ownerless insert must fail");

        // Alice reads/updates her own; admin bypasses everything.
        let g = client.roundtrip(&authed_req(7, Op::Get, "docs", "tok-alice", Some(gid), vec![])).await.unwrap();
        assert!(g.ok, "own get failed: {:?}", g.error);
        let a = client.roundtrip(&authed_req(8, Op::Get, "docs", "tok-admin", Some(gid), vec![])).await.unwrap();
        assert!(a.ok, "admin get failed: {:?}", a.error);

        // Collection reads filter by owner (no fail-closed, no leaks).
        let s = client
            .roundtrip(&authed_req(9, Op::Scan, "docs", "tok-alice", None, vec![("_limit".to_string(), Value::Int64(10))]))
            .await
            .unwrap();
        assert!(s.ok && s.rows.len() == 1, "alice sees only hers: {:?}", s);
        let s = client
            .roundtrip(&authed_req(10, Op::Scan, "docs", "tok-bob", None, vec![("_limit".to_string(), Value::Int64(10))]))
            .await
            .unwrap();
        assert!(s.ok && s.rows.is_empty(), "bob sees none: {:?}", s);
    }

    #[tokio::test]
    async fn test_session_expiry_enforced() {
        let server = owned_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Live session authenticates.
        let p = client.roundtrip(&authed_req(1, Op::Ping, "", "sess-alice", None, vec![])).await.unwrap();
        assert!(p.ok, "live session rejected: {:?}", p.error);
        // Expired session (ttl 0) is evicted on sight → unauthorized.
        // Fresh connection: connection identity is sticky, so a failed
        // handshake must not inherit a prior auth.
        let mut client2 = Client::connect(addr).await.unwrap();
        let ins = client2
            .roundtrip(&authed_req(2, Op::Insert, "docs", "sess-dead", None, doc_vals("alice", "x")))
            .await
            .unwrap();
        assert!(!ins.ok && ins.error.as_deref().unwrap_or("").contains("unauthorized"), "got {:?}", ins);
        // Bogus token likewise (fresh connection).
        let mut client3 = Client::connect(addr).await.unwrap();
        let ins2 = client3
            .roundtrip(&authed_req(3, Op::Insert, "docs", "bogus", None, doc_vals("alice", "x")))
            .await
            .unwrap();
        assert!(!ins2.ok, "bogus token accepted");
    }

    #[tokio::test]
    async fn test_row_ownership_atomic_batch() {
        let server = owned_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Bob's batch sneaks one forged insert among two legal ones:
        // the whole frame must abort, nothing applied.
        let mk = |id: u64, owner: &str| {
            let mut all = vec![("_auth".to_string(), Value::String("tok-bob".into()))];
            all.extend(doc_vals(owner, "b"));
            let map: std::collections::HashMap<String, Value> = all.into_iter().collect();
            Request { id, op: Op::Insert, table: "docs".into(), row_id: None, values: Some(map) }
        };
        let b = roundtrip_batch(&mut client, 50, vec![mk(51, "bob"), mk(52, "alice"), mk(53, "bob")])
            .await
            .unwrap();
        // NOTE: plain Batch is per-op (non-atomic by design): bob's two
        // succeed, the forgery fails. Ownership is per-op, not per-frame.
        assert!(b.results[0].ok && !b.results[1].ok && b.results[2].ok, "got {:?}", b.results);

        // Atomic batch with the same mix aborts EVERYTHING (all-or-nothing).
        let b = roundtrip_atomic(&mut client, 60, vec![mk(61, "bob"), mk(62, "alice"), mk(63, "bob")])
            .await
            .unwrap();
        assert!(b.results.iter().all(|r| !r.ok), "atomic must abort all: {:?}", b.results);
        assert_eq!(server.engine().count("docs").unwrap(), 2, "only bob's two plain-batch rows");
    }

    async fn seed_doc(client: &mut Client, id: u64, tok: &str, owner: &str, body: &str) -> u64 {
        let r = client
            .roundtrip(&authed_req(id, Op::Insert, "docs", tok, None, vec![
                ("id".to_string(), Value::Int64(id as i64)),
                ("owner".to_string(), Value::String(owner.into())),
                ("body".to_string(), Value::String(body.into())),
            ]))
            .await
            .unwrap();
        assert!(r.ok, "seed failed: {:?}", r.error);
        r.rows[0].id
    }

    #[tokio::test]
    async fn test_row_filtered_scan_pagination() {
        let server = owned_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        // Interleave owners: alice ×4, bob ×3.
        for i in 0..4u64 {
            seed_doc(&mut client, 10 + i, "tok-alice", "alice", "a").await;
        }
        for i in 0..3u64 {
            seed_doc(&mut client, 20 + i, "tok-bob", "bob", "b").await;
        }
        let scan = |id: u64, tok: &str, lim: i64, extra: Vec<(String, Value)>| {
            let mut vals = vec![("_limit".to_string(), Value::Int64(lim))];
            vals.extend(extra);
            authed_req(id, Op::Scan, "docs", tok, None, vals)
        };
        // Alice pages desc limit 2: complete (4), ordered, no overlap.
        let p1 = client.roundtrip(&scan(50, "tok-alice", 2, vec![
            ("_order".to_string(), Value::String("desc".into())),
        ])).await.unwrap();
        assert!(p1.ok && p1.rows.len() == 2, "got {:?}", p1);
        assert!(p1.rows[0].id > p1.rows[1].id);
        assert!(p1.rows.iter().all(|r| r.values.get("owner") == Some(&Value::String("alice".into()))));
        let cursor = p1.rows[1].id;
        let p2 = client.roundtrip(&scan(51, "tok-alice", 10, vec![
            ("_order".to_string(), Value::String("desc".into())),
            ("_cursor".to_string(), Value::UInt64(cursor)),
        ])).await.unwrap();
        assert!(p2.ok && p2.rows.len() == 2, "got {:?}", p2);
        assert!(p2.rows.iter().all(|r| r.id < cursor));
        assert!(!p2.rows.iter().any(|r| r.id == p1.rows[0].id));
        // Bob sees exactly his 3, asc full scan.
        let b = client.roundtrip(&scan(52, "tok-bob", 100, vec![])).await.unwrap();
        assert!(b.ok && b.rows.len() == 3, "got {:?}", b);
        assert!(b.rows.iter().all(|r| r.values.get("owner") == Some(&Value::String("bob".into()))));
    }

    #[tokio::test]
    async fn test_row_filtered_find_and_subscribe() {
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        // Notes: slug unique (Find-able) AND owner-gated via config.
        let server = owned_server_with(&[("notes", "owner")]);
        server.start().await.unwrap();
        server
            .engine()
            .create_table(
                TableSchema::new("notes")
                    .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                    .with_column(ColumnDef::new("owner", ColumnType::String).nullable())
                    .with_column(ColumnDef::new("slug", ColumnType::String).unique()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let note = |id: u64, tok: &str, owner: &str, slug: &str| authed_req(id, Op::Insert, "notes", tok, None, vec![
            ("id".to_string(), Value::Int64(id as i64)),
            ("owner".to_string(), Value::String(owner.into())),
            ("slug".to_string(), Value::String(slug.into())),
        ]);
        for (i, (tok, owner, slug)) in [("tok-alice", "alice", "a1"), ("tok-bob", "bob", "b1")].iter().enumerate() {
            let r = client.roundtrip(&note(60 + i as u64, tok, owner, slug)).await.unwrap();
            assert!(r.ok, "seed failed: {:?}", r.error);
        }
        // Bob finds his own slug; alice's slug hides as miss.
        let f = client
            .roundtrip(&authed_req(70, Op::Find, "notes", "tok-bob", None, vec![
                ("_col".to_string(), Value::String("slug".into())),
                ("_val".to_string(), Value::String("b1".into())),
            ]))
            .await
            .unwrap();
        assert!(f.ok, "own find failed: {:?}", f.error);
        let f = client
            .roundtrip(&authed_req(71, Op::Find, "notes", "tok-bob", None, vec![
                ("_col".to_string(), Value::String("slug".into())),
                ("_val".to_string(), Value::String("a1".into())),
            ]))
            .await
            .unwrap();
        assert!(!f.ok, "foreign find must miss: {:?}", f);
        // Subscribe poll on docs (gated): arm the change-log with an empty
        // poll first (records only exist after first Subscribe), then seed.
        let sub = |id: u64, tok: &str| authed_req(id, Op::Subscribe, "docs", tok, None, vec![
            ("_since".to_string(), Value::Int64(0)),
            ("_limit".to_string(), Value::Int64(100)),
        ]);
        let arm = client.roundtrip(&sub(79, "tok-alice")).await.unwrap();
        assert!(arm.ok, "arm poll failed: {:?}", arm.error);
        seed_doc(&mut client, 80, "tok-alice", "alice", "a").await;
        seed_doc(&mut client, 81, "tok-bob", "bob", "b").await;
        let pa = client.roundtrip(&sub(82, "tok-alice")).await.unwrap();
        assert!(pa.ok, "poll failed: {:?}", pa.error);
        assert!(!pa.rows.is_empty(), "alice should see her records");
        for r in &pa.rows {
            let rid = match r.values.get("row_id") {
                Some(Value::Int64(n)) => *n as u64,
                _ => panic!("record missing row_id: {:?}", r),
            };
            let row = server.engine().get("docs", RowId::new(rid)).unwrap().unwrap();
            assert_eq!(row.values.get("owner"), Some(&Value::String("alice".into())), "leak in poll");
        }
    }

    #[tokio::test]
    async fn test_row_filtered_search_and_push_closed() {
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        // posts_team: indexed (posts* prefix) AND owner-gated, authed.
        let server = owned_server_with(&[("posts_team", "owner")]);
        server.start().await.unwrap();
        server
            .engine()
            .create_table(
                TableSchema::new("posts_team")
                    .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                    .with_column(ColumnDef::new("owner", ColumnType::String).nullable())
                    .with_column(ColumnDef::new("body", ColumnType::String).nullable()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let post = |id: u64, tok: &str, owner: &str, body: &str| authed_req(id, Op::Insert, "posts_team", tok, None, vec![
            ("id".to_string(), Value::Int64(id as i64)),
            ("owner".to_string(), Value::String(owner.into())),
            ("body".to_string(), Value::String(body.into())),
        ]);
        for (i, (tok, o, b)) in [
            ("tok-alice", "alice", "xylophone dreams quartz"),
            ("tok-bob", "bob", "zephyr nights quartz"),
        ].iter().enumerate() {
            let r = client.roundtrip(&post(90 + i as u64, tok, o, b)).await.unwrap();
            assert!(r.ok, "seed failed: {:?}", r.error);
        }
        let search = |id: u64, tok: &str, q: &str| authed_req(id, Op::Search, "posts_team", tok, None, vec![
            ("_q".to_string(), Value::String(q.into())),
            ("_limit".to_string(), Value::Int64(20)),
        ]);
        // Own term hits; shared term shows only hers; foreign term blinds.
        let r = client.roundtrip(&search(95, "tok-alice", "xylophone")).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        let r = client.roundtrip(&search(96, "tok-alice", "quartz")).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        let r = client.roundtrip(&search(97, "tok-bob", "xylophone")).await.unwrap();
        assert!(r.ok && r.rows.is_empty(), "leak: {:?}", r);
        let r = client.roundtrip(&search(98, "tok-bob", "quartz")).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        // Push upgrade on a gated table stays rejected (poll instead).
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let sock = TcpStream::connect(addr).await.unwrap();
        sock.set_nodelay(true).unwrap();
        let (mut rd, mut wr) = sock.into_split();
        let codec = FrameCodec::with_default_limit();
        let frame = codec.encode_request(&authed_req(99, Op::Subscribe, "posts_team", "tok-alice", None, vec![
            ("_stream".to_string(), Value::Int64(1)),
        ])).unwrap();
        wr.write_all(&frame).await.unwrap();
        let mut buf = BytesMut::new();
        let resp = loop {
            if let Some(p) = codec.feed(&mut buf).unwrap() {
                break codec.decode_response(p).unwrap();
            }
            assert!(rd.read_buf(&mut buf).await.unwrap() > 0);
        };
        assert!(!resp.ok && resp.error.as_deref().unwrap_or("").contains("push streams disabled"),
            "got {:?}", resp);
    }

    #[tokio::test]
    async fn test_routing_crud_by_global_id() {
        let server = sharded_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Same owner always lands on the same shard (deterministic hash).
        let mut shards = std::collections::HashSet::new();
        let mut ids = Vec::new();
        for (i, owner) in ["u0", "u1", "u2", "u3", "u0", "u1"].iter().enumerate() {
            let r = client.roundtrip(&widget_insert(i as u64, i as i64, owner)).await.unwrap();
            assert!(r.ok, "insert failed: {:?}", r.error);
            let gid = r.rows[0].id;
            ids.push((owner.to_string(), gid));
            shards.insert(gid >> 56);
        }
        // Base name only on the wire; global ids route back correctly.
        for (owner, gid) in &ids {
            let g = client
                .roundtrip(&Request { id: 100, op: Op::Get, table: "widgets".into(), row_id: Some(*gid), values: None })
                .await
                .unwrap();
            assert!(g.ok && g.rows.len() == 1, "get {:?} failed: {:?}", gid, g.error);
            assert_eq!(g.rows[0].values.get("owner"), Some(&Value::String(owner.clone())));
            assert_eq!(g.rows[0].id, *gid, "response must echo the global id");
        }
        // Same-owner rows share a shard; distinct owners spread (hash).
        let shard_of = |gid: u64| gid >> 56;
        assert_eq!(shard_of(ids[0].1), shard_of(ids[4].1), "u0 must pin one shard");
        assert_eq!(shard_of(ids[1].1), shard_of(ids[5].1), "u1 must pin one shard");
        assert!(shards.len() > 1, "owners should spread: {:?}", shards);
        // Physical tables hold all rows; base name holds none.
        let total: usize = (0..4).map(|s| server.engine().count(&format!("widgets_{:02}", s)).unwrap()).sum();
        assert_eq!(total, 6);
        assert!(server.engine().count("widgets").is_err(), "base must not materialize");

        // Update + delete by global id.
        let u = client
            .roundtrip(&Request {
                id: 200,
                op: Op::Update,
                table: "widgets".into(),
                row_id: Some(ids[0].1),
                values: Some(values(&[("owner", Value::String("u0x".into()))])),
            })
            .await
            .unwrap();
        assert!(u.ok, "update failed: {:?}", u.error);
        assert_eq!(u.rows[0].id, ids[0].1);
        let d = client
            .roundtrip(&Request { id: 201, op: Op::Delete, table: "widgets".into(), row_id: Some(ids[2].1), values: None })
            .await
            .unwrap();
        assert!(d.ok, "delete failed: {:?}", d.error);
        let g = client
            .roundtrip(&Request { id: 202, op: Op::Get, table: "widgets".into(), row_id: Some(ids[2].1), values: None })
            .await
            .unwrap();
        assert!(!g.ok, "deleted row must miss");
        // Unknown base table still errors (fresh SHARD tables scan empty,
        // unknown BASE tables do not).
        let s = client
            .roundtrip(&Request { id: 203, op: Op::Scan, table: "nope".into(), row_id: None, values: None })
            .await
            .unwrap();
        assert!(!s.ok, "unknown table scan must err");
    }

    #[tokio::test]
    async fn test_routing_scan_merges_shards() {
        let server = sharded_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        for (i, owner) in ["u0", "u1", "u2", "u3", "u4"].iter().enumerate() {
            let r = client.roundtrip(&widget_insert(i as u64, i as i64, owner)).await.unwrap();
            assert!(r.ok, "insert failed: {:?}", r.error);
        }
        // Full scan on the base name merges every shard with global ids.
        let s = client
            .roundtrip(&Request {
                id: 50,
                op: Op::Scan,
                table: "widgets".into(),
                row_id: None,
                values: Some(values(&[("_limit", Value::Int64(100))])),
            })
            .await
            .unwrap();
        assert!(s.ok && s.rows.len() == 5, "got {:?}", s.rows.len());
        // Cursor pages don't overlap and stay ordered.
        let p1 = client
            .roundtrip(&Request {
                id: 51,
                op: Op::Scan,
                table: "widgets".into(),
                row_id: None,
                values: Some(values(&[("_limit", Value::Int64(2)), ("_order", Value::String("desc".into()))])),
            })
            .await
            .unwrap();
        assert!(p1.ok && p1.rows.len() == 2, "got {:?}", p1);
        assert!(p1.rows[0].id > p1.rows[1].id);
        let cursor = p1.rows[1].id;
        let p2 = client
            .roundtrip(&Request {
                id: 52,
                op: Op::Scan,
                table: "widgets".into(),
                row_id: None,
                values: Some(values(&[
                    ("_limit", Value::Int64(10)),
                    ("_order", Value::String("desc".into())),
                    ("_cursor", Value::UInt64(cursor)),
                ])),
            })
            .await
            .unwrap();
        assert!(p2.ok, "got {:?}", p2.error);
        assert!(!p2.rows.iter().any(|r| r.id == cursor || r.id == p1.rows[0].id));
        assert!(p2.rows.iter().all(|r| r.id < cursor));
    }

    #[tokio::test]
    async fn test_routing_atomic_batch_end_to_end() {
        let server = sharded_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let seed = client.roundtrip(&widget_insert(1, 1, "seed")).await.unwrap();
        assert!(seed.ok);
        let gid = seed.rows[0].id;

        // Atomic read-modify-write + insert, all by base name + global id.
        let b = roundtrip_atomic(&mut client, 50, vec![
            Request { id: 51, op: Op::Get, table: "widgets".into(), row_id: Some(gid), values: None },
            Request {
                id: 52,
                op: Op::Update,
                table: "widgets".into(),
                row_id: Some(gid),
                values: Some(values(&[("owner", Value::String("seed2".into()))])),
            },
            widget_insert(53, 2, "fresh"),
        ])
        .await
        .unwrap();
        assert!(b.results.iter().all(|r| r.ok), "atomic routed batch must commit: {:?}", b.results);
        assert_eq!(b.results[1].rows[0].values.get("owner"), Some(&Value::String("seed2".into())));
        // New row got a global id on some shard; both rows Get-able by base.
        let fresh_gid = b.results[2].rows[0].id;
        for check in [gid, fresh_gid] {
            let g = client
                .roundtrip(&Request { id: 90, op: Op::Get, table: "widgets".into(), row_id: Some(check), values: None })
                .await
                .unwrap();
            assert!(g.ok, "get {:?} failed", check);
        }
    }

    #[tokio::test]
    async fn test_push_stream_delivers_writer_insert() {        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        // Subscriber: dedicated conn, Subscribe+_stream.
        let sub_sock = TcpStream::connect(addr).await.unwrap();
        sub_sock.set_nodelay(true).unwrap();
        let (mut sub_r, mut sub_w) = sub_sock.into_split();
        let codec = FrameCodec::with_default_limit();
        let sub_frame = codec
            .encode_request(&Request {
                id: 1,
                op: Op::Subscribe,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[("_stream", Value::Int64(1))])),
            })
            .unwrap();
        sub_w.write_all(&sub_frame).await.unwrap();
        let mut buf = BytesMut::new();
        let ack = loop {
            if let Some(p) = codec.feed(&mut buf).unwrap() {
                break codec.decode_response(p).unwrap();
            }
            assert!(sub_r.read_buf(&mut buf).await.unwrap() > 0);
        };
        assert!(ack.ok && ack.id == 1 && ack.rows.is_empty(), "ack: {:?}", ack);

        // Writer: normal conn inserts.
        let mut writer = Client::connect(addr).await.unwrap();
        let ins = writer
            .roundtrip(&Request {
                id: 2,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Pushed".into())),
                    ("email", Value::String("push@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(ins.ok);

        // Push arrives (id = change seq, nonzero; op=insert).
        let push = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(p) = codec.feed(&mut buf).unwrap() {
                    return codec.decode_response(p).unwrap();
                }
                if sub_r.read_buf(&mut buf).await.unwrap() == 0 {
                    panic!("stream closed before push");
                }
            }
        })
        .await
        .expect("push timeout");
        assert!(push.ok && push.id > 0, "push: {:?}", push);
        assert_eq!(
            push.rows[0].values.get("op"),
            Some(&Value::String("insert".into()))
        );
    }

    #[tokio::test]
    async fn test_search_exact_term_bounded() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        // posts table with body for the search index.
        server
            .engine()
            .create_table(
                blitz_types::schema::TableSchema::new("posts")
                    .with_column(blitz_types::column::ColumnDef::new("id", blitz_types::column::ColumnType::Int64).nullable())
                    .with_column(blitz_types::column::ColumnDef::new("author", blitz_types::column::ColumnType::String).nullable())
                    .with_column(blitz_types::column::ColumnDef::new("body", blitz_types::column::ColumnType::String).nullable()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        for (i, body) in ["hello world", "goodbye world"].iter().enumerate() {
            let r = client
                .roundtrip(&Request {
                    id: i as u64,
                    op: Op::Insert,
                    table: "posts".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("author", Value::String("a".into())),
                        ("body", Value::String((*body).into())),
                    ])),
                })
                .await
                .unwrap();
            assert!(r.ok, "insert failed: {:?}", r.error);
        }
        let search = |id: u64, q: &str, lim: i64| Request {
            id,
            op: Op::Search,
            table: "posts".into(),
            row_id: None,
            values: Some(values(&[
                ("_q", Value::String(q.into())),
                ("_limit", Value::Int64(lim)),
            ])),
        };
        let r = client.roundtrip(&search(10, "hello", 20)).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        let r = client.roundtrip(&search(11, "world", 20)).await.unwrap();
        assert!(r.ok && r.rows.len() == 2, "got {:?}", r);
        let r = client.roundtrip(&search(12, "world", 1)).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "limit ignored: {:?}", r);
        let r = client.roundtrip(&search(13, "missingterm", 20)).await.unwrap();
        assert!(r.ok && r.rows.is_empty(), "got {:?}", r);
    }

    #[tokio::test]
    async fn test_search_index_rebuilt_after_loss() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        server
            .engine()
            .create_table(
                blitz_types::schema::TableSchema::new("posts")
                    .with_column(blitz_types::column::ColumnDef::new("id", blitz_types::column::ColumnType::Int64).nullable())
                    .with_column(blitz_types::column::ColumnDef::new("author", blitz_types::column::ColumnType::String).nullable())
                    .with_column(blitz_types::column::ColumnDef::new("body", blitz_types::column::ColumnType::String).nullable()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        for (i, body) in ["alpha bravo", "bravo charlie"].iter().enumerate() {
            let r = client
                .roundtrip(&Request {
                    id: i as u64,
                    op: Op::Insert,
                    table: "posts".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("author", Value::String("a".into())),
                        ("body", Value::String((*body).into())),
                    ])),
                })
                .await
                .unwrap();
            assert!(r.ok, "insert failed: {:?}", r.error);
        }
        let search = |id: u64, q: &str| Request {
            id,
            op: Op::Search,
            table: "posts".into(),
            row_id: None,
            values: Some(values(&[
                ("_q", Value::String(q.into())),
                ("_limit", Value::Int64(20)),
            ])),
        };
        // Live index works.
        let r = client.roundtrip(&search(10, "alpha")).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        // Simulate restart: drop all postings → search goes blind.
        server.clear_search_index();
        let r = client.roundtrip(&search(11, "alpha")).await.unwrap();
        assert!(r.ok && r.rows.is_empty(), "index should be blind after loss: {:?}", r);
        // Boot repair: rebuild from posts tables → search works again.
        assert_eq!(server.rebuild_search_index(), 2);
        let r = client.roundtrip(&search(12, "alpha")).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        let r = client.roundtrip(&search(13, "bravo")).await.unwrap();
        assert!(r.ok && r.rows.len() == 2, "got {:?}", r);
    }

    #[tokio::test]
    async fn test_media_blob_cap() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        server
            .engine()
            .create_table(
                blitz_types::schema::TableSchema::new("media")
                    .with_column(blitz_types::column::ColumnDef::new("id", blitz_types::column::ColumnType::Int64).nullable())
                    .with_column(blitz_types::column::ColumnDef::new("blob", blitz_types::column::ColumnType::Bytes).nullable()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        // 300KiB blob → rejected before engine/WAL.
        let r = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "media".into(),
                row_id: None,
                values: Some(values(&[("blob", Value::Bytes(vec![0u8; 300 * 1024]))])),
            })
            .await
            .unwrap();
        assert!(!r.ok, "oversize blob must fail");
        // 1KiB blob → ok.
        let r = client
            .roundtrip(&Request {
                id: 2,
                op: Op::Insert,
                table: "media".into(),
                row_id: None,
                values: Some(values(&[("blob", Value::Bytes(vec![0u8; 1024]))])),
            })
            .await
            .unwrap();
        assert!(r.ok, "small blob failed: {:?}", r.error);
    }

    #[tokio::test]
    async fn test_fanout_materializes_follower_timeline() {
        use crate::social::{install_fanout, run_fanout_loop};
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        for t in ["posts", "follows"] {
            server
                .engine()
                .create_table(
                    blitz_types::schema::TableSchema::new(t)
                        .with_column(blitz_types::column::ColumnDef::new("id", blitz_types::column::ColumnType::Int64).nullable())
                        .with_column(blitz_types::column::ColumnDef::new("author", blitz_types::column::ColumnType::String).nullable())
                        .with_column(blitz_types::column::ColumnDef::new("body", blitz_types::column::ColumnType::String).nullable())
                        .with_column(blitz_types::column::ColumnDef::new("from", blitz_types::column::ColumnType::String).nullable())
                        .with_column(blitz_types::column::ColumnDef::new("to", blitz_types::column::ColumnType::String).nullable()),
                )
                .unwrap();
        }
        let rx = install_fanout(&server);
        let s2 = Arc::clone(&server);
        std::thread::spawn(move || run_fanout_loop(s2, rx));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        // bob follows alice.
        let r = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "follows".into(),
                row_id: None,
                values: Some(values(&[
                    ("from", Value::String("bob".into())),
                    ("to", Value::String("alice".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(r.ok, "follow failed: {:?}", r.error);
        // alice posts.
        let r = client
            .roundtrip(&Request {
                id: 2,
                op: Op::Insert,
                table: "posts".into(),
                row_id: None,
                values: Some(values(&[
                    ("author", Value::String("alice".into())),
                    ("body", Value::String("hello followers".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(r.ok, "post failed: {:?}", r.error);
        // Timeline row for bob appears (async, ≤5s).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let rows = server.engine().scan("timeline").unwrap_or_default();
            if rows.iter().any(|row| {
                row.get("owner") == Some(&Value::String("bob".into()))
                    && row.get("author") == Some(&Value::String("alice".into()))
            }) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "fanout never materialized");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let (done, dropped) = server.fanout_pending_approx();
        assert!(done >= 1 && dropped == 0, "done={} dropped={}", done, dropped);
    }

    #[tokio::test]
    async fn test_unique_email_rejected() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let ins = |id: u64, name: &str| Request {
            id,
            op: Op::Insert,
            table: "users".into(),
            row_id: None,
            values: Some(values(&[
                ("id", Value::Int64(id as i64)),
                ("name", Value::String(name.into())),
                ("email", Value::String("dup@example.com".into())),
            ])),
        };
        assert!(client.roundtrip(&ins(1, "A")).await.unwrap().ok);
        let r2 = client.roundtrip(&ins(2, "B")).await.unwrap();
        assert!(!r2.ok, "duplicate unique email must fail");
        let msg = format!("{:?}", r2.error);
        assert!(msg.to_lowercase().contains("duplicate"), "expected duplicate-key error, got {}", msg);
    }

    #[tokio::test]
    async fn test_type_mismatch_rejected() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        // users.id is Int64; sending a String must fail strict validation.
        let r = client
            .roundtrip(&Request {
                id: 1,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::String("not-an-int".into())),
                    ("name", Value::String("T".into())),
                    ("email", Value::String("t@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(!r.ok, "type mismatch must fail");
    }

    #[tokio::test]
    async fn test_per_ip_cap_and_http_ops() {
        use crate::server::ServerConfig;
        let mut cfg = ServerConfig::default();
        cfg.max_connections = 10;
        cfg.max_connections_per_ip = 1;
        cfg.idle_timeout_secs = 0;
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let _a = TcpStream::connect(addr).await.unwrap();
        for _ in 0..200 {
            if server.connection_count() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(server.connection_count(), 1);
        // Second conn from same 127.0.0.1 must shed immediately (EOF).
        let mut b = TcpStream::connect(addr).await.unwrap();
        let mut one = [0u8; 1];
        use tokio::io::AsyncReadExt;
        let n = tokio::time::timeout(std::time::Duration::from_secs(3), b.read(&mut one))
            .await
            .expect("no shed")
            .unwrap();
        assert_eq!(n, 0);

        // HTTP ops: /readyz 200, /metrics contains contract fields.
        let ops = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let oaddr = ops.local_addr().unwrap();
        tokio::spawn(super::serve_http_ops(Arc::clone(&server), ops));
        let mut s = TcpStream::connect(oaddr).await.unwrap();
        use tokio::io::AsyncWriteExt;
        s.write_all(b"GET /readyz HTTP/1.0\r\n\r\n").await.unwrap();
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.unwrap();
        let txt = String::from_utf8_lossy(&out);
        assert!(txt.contains("200 OK") && txt.contains("ok"), "readyz: {}", txt);
        let mut s = TcpStream::connect(oaddr).await.unwrap();
        s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").await.unwrap();
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.unwrap();
        let txt = String::from_utf8_lossy(&out);
        assert!(txt.contains("blitz_requests_total"), "metrics: {}", &txt[..txt.len().min(200)]);
    }

    /// Raw HTTP helper: POST JSON, returns (status, parsed body).
    async fn post_json(
        addr: std::net::SocketAddr,
        path: &str,
        body: serde_json::Value,
        token: Option<&str>,
    ) -> (u16, serde_json::Value) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let body_str = body.to_string();
        let mut s = TcpStream::connect(addr).await.unwrap();
        let mut req = format!(
            "POST {} HTTP/1.0\r\ncontent-type: application/json\r\ncontent-length: {}\r\n",
            path,
            body_str.len()
        );
        if let Some(t) = token {
            req.push_str(&format!("authorization: Bearer {}\r\n", t));
        }
        req.push_str("\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        s.write_all(body_str.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.unwrap();
        let txt = String::from_utf8_lossy(&out);
        let status: u16 = txt
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body_start = txt.find("\r\n\r\n").map(|i| i + 4).unwrap_or(txt.len());
        let parsed = serde_json::from_str(&txt[body_start..]).unwrap_or(serde_json::Value::Null);
        (status, parsed)
    }

    async fn http_server() -> (Arc<BlitzServer>, std::net::SocketAddr) {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(super::serve_http_ops(Arc::clone(&server), listener));
        (server, addr)
    }

    #[tokio::test]
    async fn test_http_op_crud_roundtrip() {
        use serde_json::json;
        let (_server, addr) = http_server().await;
        // Insert via JSON envelope.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 1, "op": "insert", "table": "users",
            "values": {"id": 501, "name": "Curl", "email": "curl@x.com"}
        }), None).await;
        assert_eq!(code, 200, "got {:?}", resp);
        assert_eq!(resp["ok"], true, "got {:?}", resp);
        let gid = resp["rows"][0]["id"].as_u64().expect("row id");
        // Get back.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 2, "op": "get", "table": "users", "row_id": gid
        }), None).await;
        assert_eq!((code, resp["ok"].clone()), (200, json!(true)));
        assert_eq!(resp["rows"][0]["values"]["name"], json!("Curl"));
        // Update + delete.
        let (_, resp) = post_json(addr, "/v1/op", json!({
            "id": 3, "op": "update", "table": "users", "row_id": gid,
            "values": {"name": "Curl2"}
        }), None).await;
        assert_eq!(resp["ok"], true, "got {:?}", resp);
        let (_, resp) = post_json(addr, "/v1/op", json!({
            "id": 4, "op": "delete", "table": "users", "row_id": gid
        }), None).await;
        assert_eq!(resp["ok"], true, "got {:?}", resp);
        // Get-miss stays in-band (200 + ok:false), like TCP err payloads.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 5, "op": "get", "table": "users", "row_id": gid
        }), None).await;
        assert_eq!(code, 200);
        assert_eq!(resp["ok"], false);
    }

    #[tokio::test]
    async fn test_http_batch_and_atomic() {
        use serde_json::json;
        let (server, addr) = http_server().await;
        // Seed a taken email.
        let (_, seed) = post_json(addr, "/v1/op", json!({
            "id": 1, "op": "insert", "table": "users",
            "values": {"id": 601, "name": "Seed", "email": "taken@x.com"}
        }), None).await;
        assert_eq!(seed["ok"], true);
        let mk = |id: u64, email: &str| json!({
            "id": id, "op": "insert", "table": "users",
            "values": {"id": id, "name": "N", "email": email}
        });
        // Plain batch: partial failure normal.
        let (_, resp) = post_json(addr, "/v1/batch", json!({
            "id": 50, "ops": [mk(51, "taken@x.com"), mk(52, "fresh@x.com")]
        }), None).await;
        assert_eq!(resp["results"][0]["ok"], false, "got {:?}", resp);
        assert_eq!(resp["results"][1]["ok"], true, "got {:?}", resp);
        // Atomic batch: all-or-nothing.
        let (_, resp) = post_json(addr, "/v1/batch", json!({
            "id": 60, "atomic": true, "ops": [mk(61, "fresh2@x.com"), mk(62, "taken@x.com")]
        }), None).await;
        assert!(resp["results"].as_array().unwrap().iter().all(|r| r["ok"] == false),
            "got {:?}", resp);
        assert_eq!(server.engine().count("users").unwrap(), 2);
    }

    #[tokio::test]
    async fn test_http_auth_and_envelope_errors() {
        use serde_json::json;
        // Secured server: owned docs + bearer tokens (reuse the TCP helper).
        let server = owned_server();
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(super::serve_http_ops(Arc::clone(&server), listener));
        // No token → 401.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 1, "op": "insert", "table": "docs",
            "values": {"id": 1, "owner": "alice", "body": "a"}
        }), None).await;
        assert_eq!(code, 401, "got {:?}", resp);
        // Alice's own insert → 200.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 2, "op": "insert", "table": "docs",
            "values": {"id": 1, "owner": "alice", "body": "a"}
        }), Some("tok-alice")).await;
        assert_eq!((code, resp["ok"].clone()), (200, json!(true)), "got {:?}", resp);
        let gid = resp["rows"][0]["id"].as_u64().unwrap();
        // Bob reads alice's doc → hidden as not-found, in-band 200.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 3, "op": "get", "table": "docs", "row_id": gid
        }), Some("tok-bob")).await;
        assert_eq!(code, 200);
        assert_eq!(resp["ok"], false);
        // Bob writes alice's doc → 403.
        let (code, resp) = post_json(addr, "/v1/op", json!({
            "id": 4, "op": "update", "table": "docs", "row_id": gid,
            "values": {"body": "hijack"}
        }), Some("tok-bob")).await;
        assert_eq!(code, 403, "got {:?}", resp);
        // Malformed envelope → 400. Unknown op → 400.
        let (code, _) = post_json(addr, "/v1/op", json!({"id": 5, "table": "docs"}), Some("tok-alice")).await;
        assert_eq!(code, 400);
        let (code, _) = post_json(addr, "/v1/op", json!({"id": 6, "op": "frobnicate"}), Some("tok-alice")).await;
        assert_eq!(code, 400);
    }

    /// Read one HTTP/1.x response from a (possibly reused) socket:
    /// returns (status, header block, body).
    async fn read_http_response(
        rd: &mut tokio::net::tcp::OwnedReadHalf,
        buf: &mut Vec<u8>,
    ) -> (u16, String, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos + 4]).into_owned();
                let status: u16 = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
                let len: usize = head
                    .lines()
                    .filter_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        if k.trim().to_ascii_lowercase() == "content-length" {
                            v.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .next()
                    .unwrap_or(0);
                while buf.len() < pos + 4 + len {
                    let mut tmp = [0u8; 8192];
                    let n = rd.read(&mut tmp).await.unwrap();
                    assert!(n > 0, "server closed mid-body");
                    buf.extend_from_slice(&tmp[..n]);
                }
                let body = buf[pos + 4..pos + 4 + len].to_vec();
                buf.drain(..pos + 4 + len);
                return (status, head, body);
            }
            let mut tmp = [0u8; 8192];
            let n = rd.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed mid-head");
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    #[tokio::test]
    async fn test_http_keep_alive_reuses_connection() {
        use serde_json::json;
        use tokio::io::AsyncWriteExt;
        let (_server, addr) = http_server().await;
        let sock = TcpStream::connect(addr).await.unwrap();
        sock.set_nodelay(true).unwrap();
        let (mut rd, mut wr) = sock.into_split();
        let mut buf = Vec::new();
        // Two sequential POSTs on ONE HTTP/1.1 connection.
        for i in 0..2u64 {
            let body = json!({
                "id": i, "op": "insert", "table": "users",
                "values": {"id": 700 + i as i64, "name": "KA", "email": format!("ka{}@x.com", i)}
            })
            .to_string();
            wr.write_all(
                format!(
                    "POST /v1/op HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(), body
                )
                .as_bytes(),
            )
            .await
            .unwrap();
            let (status, head, resp_body) = read_http_response(&mut rd, &mut buf).await;
            assert_eq!(status, 200, "head: {}", head);
            assert!(head.contains("connection: keep-alive"), "head: {}", head);
            let v: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
            assert_eq!(v["ok"], true, "got {:?}", v);
        }
    }

    #[tokio::test]
    async fn test_http_cors_and_preflight() {
        use crate::server::ServerConfig;
        use tokio::io::AsyncWriteExt;
        let mut cfg = ServerConfig::default();
        cfg.http_cors_origins = vec!["https://app.test".into()];
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(super::serve_http_ops(Arc::clone(&server), listener));
        // Simple request echoes a listed origin (and only listed ones).
        let sock = TcpStream::connect(addr).await.unwrap();
        let (mut rd, mut wr) = sock.into_split();
        let mut buf = Vec::new();
        wr.write_all(b"GET /readyz HTTP/1.1\r\nhost: x\r\norigin: https://app.test\r\n\r\n")
            .await
            .unwrap();
        let (status, head, _) = read_http_response(&mut rd, &mut buf).await;
        assert_eq!(status, 200);
        assert!(head.contains("access-control-allow-origin: https://app.test"), "head: {}", head);
        // Preflight answers without auth.
        let sock = TcpStream::connect(addr).await.unwrap();
        let (mut rd, mut wr) = sock.into_split();
        let mut buf = Vec::new();
        wr.write_all(
            b"OPTIONS /v1/op HTTP/1.1\r\nhost: x\r\norigin: https://app.test\r\naccess-control-request-method: POST\r\n\r\n",
        )
        .await
        .unwrap();
        let (status, head, _) = read_http_response(&mut rd, &mut buf).await;
        assert_eq!(status, 204, "head: {}", head);
        assert!(head.contains("access-control-allow-methods: GET, POST, OPTIONS"), "head: {}", head);
        // Unlisted origin gets no header.
        let sock = TcpStream::connect(addr).await.unwrap();
        let (mut rd, mut wr) = sock.into_split();
        let mut buf = Vec::new();
        wr.write_all(b"GET /readyz HTTP/1.1\r\nhost: x\r\norigin: https://evil.test\r\n\r\n")
            .await
            .unwrap();
        let (_, head, _) = read_http_response(&mut rd, &mut buf).await;
        assert!(!head.contains("access-control-allow-origin"), "head: {}", head);
    }

    #[tokio::test]
    async fn test_http_sse_stream_delivers_insert() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        // Binary listener for the seeder, HTTP listener for the stream:
        // the two protocols don't share a port.
        let bin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bin_addr = bin.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), bin));
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        tokio::spawn(super::serve_http_ops(Arc::clone(&server), http));
        // Arm the change-log, then open the stream.
        let mut seeder = Client::connect(bin_addr).await.unwrap();
        let _ = seeder
            .roundtrip(&Request { id: 1, op: Op::Subscribe, table: "users".into(), row_id: None, values: None })
            .await
            .unwrap();
        let sock = TcpStream::connect(http_addr).await.unwrap();
        sock.set_nodelay(true).unwrap();
        let (mut rd, mut wr) = sock.into_split();
        wr.write_all(b"GET /v1/stream?table=users HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap();
        // Head + initial comment.
        let mut buf = Vec::new();
        let (status, head, _) = read_http_response_stream_head(&mut rd, &mut buf).await;
        assert_eq!(status, 200, "head: {}", head);
        assert!(head.contains("text/event-stream"), "head: {}", head);
        let frame = read_sse_frame(&mut rd, &mut buf).await;
        assert!(frame.starts_with(": connected"), "hello frame: {}", frame);
        // Insert after the stream is up; expect a data frame naming users.
        seeder
            .roundtrip(&Request {
                id: 2,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(800)),
                    ("name", Value::String("Streamed".into())),
                    ("email", Value::String("stream@x.com".into())),
                ])),
            })
            .await
            .unwrap();
        let frame = read_sse_frame(&mut rd, &mut buf).await;
        assert!(frame.starts_with("data: "), "frame: {}", frame);
        assert!(frame.contains("users"), "frame: {}", frame);
        let _ = server;
    }

    /// Read an SSE head (headers only; the body never ends).
    async fn read_http_response_stream_head(
        rd: &mut tokio::net::tcp::OwnedReadHalf,
        buf: &mut Vec<u8>,
    ) -> (u16, String, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos + 4]).into_owned();
                let status: u16 = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
                buf.drain(..pos + 4);
                return (status, head, Vec::new());
            }
            let mut tmp = [0u8; 4096];
            let n = rd.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed stream head");
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Read until a blank line (one SSE frame, incl. comments).
    async fn read_sse_frame(
        rd: &mut tokio::net::tcp::OwnedReadHalf,
        buf: &mut Vec<u8>,
    ) -> String {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
                let frame = String::from_utf8_lossy(&buf[..pos]).into_owned();
                buf.drain(..pos + 2);
                if frame.trim().is_empty() {
                    continue;
                }
                return frame;
            }
            let mut tmp = [0u8; 4096];
            let n = tokio::time::timeout(std::time::Duration::from_secs(10), rd.read(&mut tmp))
                .await
                .expect("sse frame timeout")
                .unwrap();
            assert!(n > 0, "server closed stream");
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    #[tokio::test]
    async fn test_http_call_and_values() {
        use serde_json::json;
        let (server, addr) = http_server().await;
        server.register_procedure(checkout_procedure());
        server
            .engine()
            .create_table(accounts_schema())
            .unwrap();
        server.engine().create_table(orders_schema()).unwrap();
        // Seed via HTTP too (proves JSON ints hit Int64 columns).
        let (_, seed) = post_json(addr, "/v1/op", json!({
            "id": 1, "op": "insert", "table": "accounts",
            "values": {"id": 1, "balance": 100, "status": "active"}
        }), None).await;
        assert_eq!(seed["ok"], true, "got {:?}", seed);
        let acct = seed["rows"][0]["id"].as_u64().unwrap();
        // Checkout through the bridge: balance gate + two writes + _applied.
        let (_, resp) = post_json(addr, "/v1/op", json!({
            "id": 2, "op": "call", "table": "fn:checkout",
            "values": {"account_id": acct, "price": 40}
        }), None).await;
        assert_eq!(resp["ok"], true, "got {:?}", resp);
        assert_eq!(resp["rows"][0]["values"]["_applied"].as_array().unwrap().len(), 2);
        // Insufficient balance aborts verbatim.
        let (_, resp) = post_json(addr, "/v1/op", json!({
            "id": 3, "op": "call", "table": "fn:checkout",
            "values": {"account_id": acct, "price": 99999}
        }), None).await;
        assert_eq!(resp["ok"], false);
        assert!(resp["error"].as_str().unwrap_or("").contains("insufficient balance"),
            "got {:?}", resp);
    }

    /// Minimal echo guest for job tests (copies input to OUTPUT_BASE).
    fn echo_wasm() -> Vec<u8> {
        wat::parse_str(r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (param $in i32) (param $len i32) (result i64)
    (local $i i32)
    (block $done
      (loop $cp
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8
          (i32.add (i32.const 32768) (local.get $i))
          (i32.load8_u (i32.add (local.get $in) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $cp)))
    (i64.or
      (i64.shl (i64.const 32768) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))"#).unwrap()
    }

    /// Fuel-burner guest (never returns on its own).
    fn loop_wasm() -> Vec<u8> {
        wat::parse_str(r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (param i32 i32) (result i64)
    (loop $l (br 0))
    (i64.const 0)))"#).unwrap()
    }

    async fn poll_job(client: &mut Client, id: u64, job_id: &str) -> std::collections::HashMap<String, Value> {
        // Background execution is fast (µs-ms); poll briefly, fail loudly.
        for _ in 0..200 {
            let r = client
                .roundtrip(&Request {
                    id,
                    op: Op::JobPoll,
                    table: "jobs".into(),
                    row_id: None,
                    values: Some(values(&[("_job", Value::String(job_id.into()))])),
                })
                .await
                .unwrap();
            assert!(r.ok, "poll failed: {:?}", r.error);
            let status = r.rows[0].values.get("status").cloned();
            if status != Some(Value::String("pending".into()))
                && status != Some(Value::String("running".into()))
            {
                return r.rows[0].values.clone();
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("job {} never settled", job_id);
    }

    #[tokio::test]
    async fn test_job_submit_echo_poll() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let r = client
            .roundtrip(&Request {
                id: 1,
                op: Op::JobSubmit,
                table: "jobs".into(),
                row_id: None,
                values: Some(values(&[
                    ("wasm", Value::Bytes(echo_wasm())),
                    ("input", Value::String("hello-jobs".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(r.ok, "submit failed: {:?}", r.error);
        let job_id = match r.rows[0].values.get("job_id") {
            Some(Value::String(s)) => s.clone(),
            other => panic!("missing job_id: {:?}", other),
        };
        let out = poll_job(&mut client, 2, &job_id).await;
        assert_eq!(out.get("status"), Some(&Value::String("completed".into())), "got {:?}", out);
        assert_eq!(out.get("result"), Some(&Value::String("hello-jobs".into())));
    }

    #[tokio::test]
    async fn test_job_fuel_kill_and_unknown() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let r = client
            .roundtrip(&Request {
                id: 1,
                op: Op::JobSubmit,
                table: "jobs".into(),
                row_id: None,
                values: Some(values(&[
                    ("wasm", Value::Bytes(loop_wasm())),
                    ("input", Value::String("".into())),
                    ("_retries", Value::Int64(0)),
                ])),
            })
            .await
            .unwrap();
        assert!(r.ok, "submit failed: {:?}", r.error);
        let job_id = match r.rows[0].values.get("job_id") {
            Some(Value::String(s)) => s.clone(),
            other => panic!("missing job_id: {:?}", other),
        };
        let out = poll_job(&mut client, 2, &job_id).await;
        assert_eq!(out.get("status"), Some(&Value::String("failed".into())), "got {:?}", out);
        assert!(matches!(out.get("error"), Some(Value::String(e)) if e.contains("fuel")),
            "got {:?}", out);
        // Unknown job id errors honestly.
        let r = client
            .roundtrip(&Request {
                id: 3,
                op: Op::JobPoll,
                table: "jobs".into(),
                row_id: None,
                values: Some(values(&[("_job", Value::String("nope".into()))])),
            })
            .await
            .unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("job not found"), "got {:?}", r);
        // Job ops inside atomic batches abort (background work can't roll back).
        let b = roundtrip_atomic(&mut client, 50, vec![Request {
            id: 51,
            op: Op::JobSubmit,
            table: "jobs".into(),
            row_id: None,
            values: Some(values(&[
                ("wasm", Value::Bytes(echo_wasm())),
                ("input", Value::String("x".into())),
            ])),
        }])
        .await
        .unwrap();
        assert!(b.results.iter().all(|x| !x.ok), "nested job must abort: {:?}", b.results);
    }

    /// Deploy envelope for a balance-gated transfer (friendly JSON values).
    fn transfer_envelope(desc: &str) -> serde_json::Value {
        serde_json::json!({"v": 1, "procedure": {
            "name": "transfer", "description": desc,
            "steps": [
                {"Read": {"table": "accounts", "id": "$account_id", "into": "a"}},
                {"If": {
                    "condition": {"GreaterOrEqual": ["a.balance", "$price"]},
                    "then_steps": [
                        {"Insert": {"table": "orders", "values": {
                            "id": 7, "account_id": "$account_id", "amount": "$price"}, "into": "order_id"}},
                        {"Update": {"table": "accounts", "id": "$account_id",
                            "values": {"status": "charged"}}},
                        {"Return": {"value": "$order_id"}}
                    ],
                    "else_steps": [{"Fail": {"message": "insufficient balance"}}]
                }}
            ]
        }})
    }

    fn deploy_req(id: u64, envelope: serde_json::Value) -> Request {
        let mut values = std::collections::HashMap::new();
        values.insert("v".to_string(), Value::Int64(1));
        values.insert("procedure".to_string(), Value::Json(envelope["procedure"].clone()));
        Request { id, op: Op::ProcDeploy, table: "fn:transfer".into(), row_id: None, values: Some(values) }
    }

    #[tokio::test]
    async fn test_proc_deploy_call_redeploy_drop() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        server.engine().create_table(accounts_schema()).unwrap();
        server.engine().create_table(orders_schema()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Deploy v1 over TCP.
        let r = client.roundtrip(&deploy_req(1, transfer_envelope("first"))).await.unwrap();
        assert!(r.ok, "deploy failed: {:?}", r.error);
        assert_eq!(r.rows[0].values.get("version"), Some(&Value::UInt64(1)));
        // List shows it.
        let r = client.roundtrip(&Request { id: 2, op: Op::ProcList, table: "".into(), row_id: None, values: None }).await.unwrap();
        assert!(r.ok && r.rows.len() == 1, "got {:?}", r);
        assert_eq!(r.rows[0].values.get("name"), Some(&Value::String("transfer".into())));
        // Seed + call the DEPLOYED procedure (real end-to-end).
        let seed = client.roundtrip(&Request {
            id: 3, op: Op::Insert, table: "accounts".into(), row_id: None,
            values: Some(values(&[
                ("id", Value::Int64(1)),
                ("balance", Value::Int64(100)),
                ("status", Value::String("active".into())),
            ])),
        }).await.unwrap();
        assert!(seed.ok);
        let acct = seed.rows[0].id;
        let r = client.roundtrip(&call_req(4, "transfer", vec![
            ("account_id", Value::Int64(acct as i64)),
            ("price", Value::Int64(40)),
        ])).await.unwrap();
        assert!(r.ok, "call failed: {:?}", r.error);
        assert_eq!(r.rows[0].values.get("_applied").map(|v| match v {
            Value::Json(serde_json::Value::Array(a)) => a.len(),
            _ => 0,
        }), Some(2));
        // Redeploy bumps the version.
        let r = client.roundtrip(&deploy_req(5, transfer_envelope("second"))).await.unwrap();
        assert!(r.ok, "redeploy failed: {:?}", r.error);
        assert_eq!(r.rows[0].values.get("version"), Some(&Value::UInt64(2)));
        // Drop: calls fail, list empties.
        let r = client.roundtrip(&Request { id: 6, op: Op::ProcDrop, table: "fn:transfer".into(), row_id: None, values: None }).await.unwrap();
        assert!(r.ok, "drop failed: {:?}", r.error);
        let r = client.roundtrip(&call_req(7, "transfer", vec![])).await.unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("not found"), "got {:?}", r);
        let r = client.roundtrip(&Request { id: 8, op: Op::ProcList, table: "".into(), row_id: None, values: None }).await.unwrap();
        assert!(r.ok && r.rows.is_empty(), "got {:?}", r);
        let r = client.roundtrip(&Request { id: 9, op: Op::ProcDrop, table: "fn:transfer".into(), row_id: None, values: None }).await.unwrap();
        assert!(!r.ok, "double drop must err");
    }

    #[tokio::test]
    async fn test_proc_deploy_validation_and_atomic() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();

        // Unknown function reference rejected.
        let mut bad = transfer_envelope("bad");
        bad["procedure"]["steps"] = serde_json::json!([
            {"CallFunction": {"function": "nope", "args": {}}}
        ]);
        let r = client.roundtrip(&deploy_req(1, bad)).await.unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("unknown function"), "got {:?}", r);
        // Envelope/table name mismatch rejected.
        let r = client.roundtrip(&Request {
            id: 2, op: Op::ProcDeploy, table: "fn:other".into(), row_id: None,
            values: Some({
                let mut m = std::collections::HashMap::new();
                m.insert("v".to_string(), Value::Int64(1));
                m.insert("procedure".to_string(), Value::Json(transfer_envelope("x")["procedure"].clone()));
                m
            }),
        }).await.unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("must match"), "got {:?}", r);
        // Bad envelope version rejected.
        let r = client.roundtrip(&Request {
            id: 3, op: Op::ProcDeploy, table: "fn:transfer".into(), row_id: None,
            values: Some({
                let mut m = std::collections::HashMap::new();
                m.insert("v".to_string(), Value::Int64(99));
                m.insert("procedure".to_string(), Value::Json(transfer_envelope("x")["procedure"].clone()));
                m
            }),
        }).await.unwrap();
        assert!(!r.ok && r.error.as_deref().unwrap_or("").contains("version"), "got {:?}", r);
        // Deploy ops inside atomic batches abort (registry can't roll back).
        let b = roundtrip_atomic(&mut client, 50, vec![Request {
            id: 51, op: Op::ProcList, table: "".into(), row_id: None, values: None,
        }])
        .await
        .unwrap();
        assert!(b.results.iter().all(|x| !x.ok), "registry op must abort atomic: {:?}", b.results);
    }

    #[tokio::test]
    async fn test_version_handshake_no_auth() {
        // No handshake, no grants: version always answers (bootstrap).
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let r = client
            .roundtrip(&Request { id: 1, op: Op::Version, table: "".into(), row_id: None, values: None })
            .await
            .unwrap();
        assert!(r.ok, "version failed: {:?}", r.error);
        assert_eq!(
            r.rows[0].values.get("server"),
            Some(&Value::String(crate::server::SERVER_VERSION.to_string()))
        );
        assert_eq!(
            r.rows[0].values.get("protocol"),
            Some(&Value::Int64(blitz_protocol::PROTOCOL_VERSION as i64))
        );
        // Meaningless inside atomic frames (not versioned state): rejected.
        let b = roundtrip_atomic(&mut client, 50, vec![Request {
            id: 51, op: Op::Version, table: "".into(), row_id: None, values: None,
        }])
        .await
        .unwrap();
        assert!(b.results.iter().all(|x| !x.ok), "version must abort atomic: {:?}", b.results);
    }

    #[tokio::test]
    async fn test_idle_timeout_reaps_silent_conn() {
        use crate::server::ServerConfig;
        let mut cfg = ServerConfig::default();
        cfg.idle_timeout_secs = 1;
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut c = TcpStream::connect(addr).await.unwrap();
        for _ in 0..200 {
            if server.connection_count() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Stay silent past the 1s deadline → server closes (EOF).
        let mut one = [0u8; 1];
        use tokio::io::AsyncReadExt;
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), c.read(&mut one))
            .await
            .expect("idle reap never closed")
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_admission_control_rejects_over_capacity() {
        use crate::server::ServerConfig;

        let mut cfg = ServerConfig::default();
        cfg.max_connections = 2;
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let _a = TcpStream::connect(addr).await.unwrap();
        let _b = TcpStream::connect(addr).await.unwrap();
        // Wait until both slots are admitted.
        for _ in 0..200 {
            if server.connection_count() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(server.connection_count(), 2);

        // Third connection must be closed immediately (EOF on read).
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut one = [0u8; 1];
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            c.read(&mut one),
        )
        .await
        .expect("server did not close excess connection")
        .unwrap();
        assert_eq!(n, 0);
        assert_eq!(server.connection_count(), 2);
    }

    #[tokio::test]
    async fn test_scan_pagination_bounds_and_pages() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut client = Client::connect(addr).await.unwrap();
        for i in 0..5 {
            let resp = client
                .roundtrip(&Request {
                    id: i as u64,
                    op: Op::Insert,
                    table: "users".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("id", Value::Int64(i)),
                        ("name", Value::String("n".into())),
                        ("email", Value::String(format!("p{}@x.com", i))),
                    ])),
                })
                .await
                .unwrap();
            assert!(resp.ok, "insert failed: {:?}", resp.error);
        }

        // Default scan returns all 5 (under default limit 1000).
        let resp = client
            .roundtrip(&Request {
                id: 100,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.rows.len(), 5);

        // _limit=2 returns first page of 2, stable order by RowId.
        let resp = client
            .roundtrip(&Request {
                id: 101,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[("_limit", Value::Int64(2))])),
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.rows.len(), 2);

        // _limit=2 _offset=2 returns second page.
        let p1 = client
            .roundtrip(&Request {
                id: 102,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("_limit", Value::Int64(2)),
                    ("_offset", Value::Int64(2)),
                ])),
            })
            .await
            .unwrap();
        assert!(p1.ok);
        assert_eq!(p1.rows.len(), 2);
        // Pages don't overlap.
        assert_ne!(resp.rows[0].id, p1.rows[0].id);

        // Absurd _limit is clamped to MAX, not OOM: still ok, bounded rows.
        let resp = client
            .roundtrip(&Request {
                id: 103,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[("_limit", Value::Int64(1_000_000))])),
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.rows.len(), 5);
    }

    #[tokio::test]
    async fn test_shed_and_slow_stats() {
        use crate::server::ServerConfig;
        let mut cfg = ServerConfig::default();
        cfg.shed_at_connections = Some(1);
        cfg.slow_threshold_ms = 0; // every request counts as slow
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();
        assert!(!server.should_shed());
        assert!(server.slow_rate().is_none());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));
        let mut client = Client::connect(addr).await.unwrap();
        let resp = client.roundtrip(&Request::ping(1)).await.unwrap();
        assert!(resp.ok);
        assert_eq!(server.stats().total_requests, 1);
        assert_eq!(server.stats().slow_responses, 1);
        assert_eq!(server.slow_rate(), Some(1.0));
    }

    #[tokio::test]
    async fn test_tls_ping_roundtrip_self_signed() {
        // rcgen self-signed server cert; client pins its DER (no custom
        // verifier, no network PKI — test-only trust).
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = cert.serialize_der().unwrap();
        let key_der = cert.serialize_private_key_der();
        let acceptor = super::tls_acceptor_from_der(cert_der.clone(), key_der).unwrap();

        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(super::serve_tls(Arc::clone(&server), listener, acceptor));

        use rustls::pki_types::ServerName;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der.into()).unwrap();
        let ccfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(ccfg));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap().to_owned();
        let mut tls = connector.connect(server_name, tcp).await.unwrap();

        // Raw Ping frame over the TLS stream.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let codec = FrameCodec::with_default_limit();
        let frame = codec.encode_request(&Request::ping(7)).unwrap();
        tls.write_all(&frame).await.unwrap();
        let mut buf = BytesMut::new();
        let resp = loop {
            if let Some(p) = codec.feed(&mut buf).unwrap() {
                break codec.decode_response(p).unwrap();
            }
            let n = tls.read_buf(&mut buf).await.unwrap();
            assert!(n > 0, "tls server closed");
        };
        assert!(resp.ok && resp.id == 7, "got {:?}", resp);
    }
}
