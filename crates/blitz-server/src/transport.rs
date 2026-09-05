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
use blitz_protocol::{BatchResponse, FrameCodec, Incoming, Op, Request, Response, RowView};
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
fn dispatch(server: &BlitzServer, req: Request) -> Response {
    let id = req.id;
    match req.op {
        Op::Ping => Response::ok(id, Vec::new()),
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
            if let Some(ref key) = idem {
                if let Some(cached) = server.idem_lookup(key) {
                    return Response::ok(id, vec![RowView { id: cached, values }]);
                }
            }
            let mut row = Row::new(RowId::new(0));
            for (k, v) in &values {
                row.set(k.clone(), v.clone());
            }
            // Shard/cache fast path: trusted shape skips validation.
            let res = if server.config().skip_validation {
                server.engine().insert_unchecked(&req.table, row)
            } else {
                server.engine().insert(&req.table, row)
            };
            match res {
                Ok(assigned) => {
                    // Durability: never ack unwritten data in durable modes.
                    // On backpressure, compensate (delete just-inserted row)
                    // so engine/WAL can't diverge, then shed fast.
                    let data = crate::durability::values_to_json_bytes(&values);
                    if let Err(w) = server.wal_log(blitz_wal::EntryType::Insert, &req.table, assigned.as_u64(), data) {
                        let _ = server.engine().delete(&req.table, assigned);
                        return Response::err(id, format!("WAL backpressure: {}", w));
                    }
                    if let Some(key) = idem {
                        server.idem_record(key, assigned.as_u64());
                    }
                    server.record_change(&req.table, "insert", assigned.as_u64());
                    // Social derived state (post ack only): search index +
                    // async fanout job. Both fire-and-forget bounded; neither
                    // blocks the response (eventual, ~ms).
                    if req.table.starts_with("posts") {
                        if let Some(blitz_types::value::Value::String(body)) = values.get("body") {
                            server.index_post(&req.table, assigned.as_u64(), body);
                        }
                        if let Some(blitz_types::value::Value::String(author)) = values.get("author") {
                            server.fanout_enqueue(req.table.clone(), assigned.as_u64(), author.clone());
                        }
                    }
                    Response::ok(
                        id,
                        vec![RowView {
                            id: assigned.as_u64(),
                            values,
                        }],
                    )
                }
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Get => {
            let row_id = match req.row_id {
                Some(rid) => RowId::new(rid),
                None => return Response::err(id, "get requires row_id"),
            };
            // Zero-copy read: serialize straight off the shared handle.
            match server.engine().get_arc(&req.table, row_id) {
                Ok(Some(row)) => Response::ok(id, vec![row_to_view(row_id, &row)]),
                Ok(None) => Response::err(id, format!("row not found: {}", row_id)),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Update => {
            let row_id = match req.row_id {
                Some(rid) => RowId::new(rid),
                None => return Response::err(id, "update requires row_id"),
            };
            let values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "update requires values"),
            };
            // Move, don't clone: engine already returns an owned Row, so
            // moving its map into the view saves a second HashMap clone.
            match server.engine().update(&req.table, row_id, values) {
                Ok(row) => {
                    let data = crate::durability::values_to_json_bytes(&row.values);
                    if let Err(w) = server.wal_log(blitz_wal::EntryType::Update, &req.table, row_id.as_u64(), data) {
                        return Response::err(id, format!("WAL backpressure: {}", w));
                    }
                    server.record_change(&req.table, "update", row_id.as_u64());
                    Response::ok(
                        id,
                        vec![RowView {
                            id: row.id.as_u64(),
                            values: row.values,
                        }],
                    )
                }
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Delete => {
            let row_id = match req.row_id {
                Some(rid) => RowId::new(rid),
                None => return Response::err(id, "delete requires row_id"),
            };
            match server.engine().delete(&req.table, row_id) {
                Ok(true) => {
                    if let Err(w) = server.wal_log(blitz_wal::EntryType::Delete, &req.table, row_id.as_u64(), Vec::new()) {
                        return Response::err(id, format!("WAL backpressure: {}", w));
                    }
                    server.record_change(&req.table, "delete", row_id.as_u64());
                    Response::ok(id, Vec::new())
                }
                Ok(false) => Response::err(id, format!("row not found: {}", row_id)),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Scan => {
            let w = scan_pagination(&req.values);
            match server.engine().scan_arcs(&req.table) {
                // Paginated + cursor scan over RowId order. The limit bounds
                // CPU/frame; cursor (binary search) keeps pages stable under
                // concurrent inserts. NOTE: per-request full sort — hot
                // timeline paths must use precomputed feeds (Stage 9+), not
                // scans; this primitive is for admin/backfill pages.
                Ok(mut rows) => {
                    if w.desc {
                        rows.sort_by_key(|r| std::cmp::Reverse(r.id));
                    } else {
                        rows.sort_by_key(|r| r.id);
                    }
                    let (start, end) = apply_window(rows.len(), !w.desc, &w, &|i| rows[i].id.as_u64());
                    let views = rows[start..end]
                        .iter()
                        .map(|row| row_to_view(row.id, row))
                        .collect();
                    Response::ok(id, views)
                }
                Err(e) => Response::err(id, e.to_string()),
            }
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
            match server.engine().lookup_by_unique(&req.table, &col, &val) {
                Ok(Some(row)) => {
                    let rid = row.id;
                    Response::ok(id, vec![row_to_view(rid, &row)])
                }
                Ok(None) => Response::err(id, "not found"),
                Err(e) => Response::err(id, e.to_string()),
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
            let mut rows = Vec::new();
            for (table, rid) in server.search_posts(&q, limit) {
                match server.engine().get_arc(&table, RowId::new(rid)) {
                    Ok(Some(row)) => rows.push(row_to_view(RowId::new(rid), &row)),
                    _ => {}
                }
            }
            Response::ok(id, rows)
        }
    }
}

/// Zero-copy fast paths for the Single-frame hot path.
///
/// `dispatch` must return an owned `Response` (Batch needs it), which forces
/// a HashMap clone per read. These helpers encode straight off `Arc<Row>`
/// with `encode_ok_single/borrowed` — no `Value` clones — saving ~30-40%
/// of Get/Scan service time. Insert/Update/Delete stay on `dispatch`.
fn encode_get_fast(
    server: &BlitzServer,
    codec: &FrameCodec,
    req: &Request,
) -> anyhow::Result<bytes::Bytes> {
    let id = req.id;
    let row_id = match req.row_id {
        Some(rid) => RowId::new(rid),
        None => {
            return Ok(codec
                .encode_response(&Response::err(id, "get requires row_id"))
                .map_err(|e| anyhow::anyhow!("{e}"))?);
        }
    };
    match server.engine().get_arc(&req.table, row_id) {
        Ok(Some(row)) => Ok(codec
            .encode_ok_single(id, row_id.as_u64(), &row.values)
            .map_err(|e| anyhow::anyhow!("{e}"))?),
        Ok(None) => Ok(codec
            .encode_response(&Response::err(id, format!("row not found: {}", row_id)))
            .map_err(|e| anyhow::anyhow!("{e}"))?),
        Err(e) => Ok(codec
            .encode_response(&Response::err(id, e.to_string()))
            .map_err(|e| anyhow::anyhow!("{e}"))?),
    }
}

fn encode_scan_fast(
    server: &BlitzServer,
    codec: &FrameCodec,
    req: &Request,
) -> anyhow::Result<bytes::Bytes> {
    let id = req.id;
    let w = scan_pagination(&req.values);
    match server.engine().scan_arcs(&req.table) {
        Ok(mut rows) => {
            if w.desc {
                rows.sort_by_key(|r| std::cmp::Reverse(r.id));
            } else {
                rows.sort_by_key(|r| r.id);
            }
            let (start, end) = apply_window(rows.len(), !w.desc, &w, &|i| rows[i].id.as_u64());
            let borrowed: Vec<(u64, &std::collections::HashMap<String, blitz_types::value::Value>)> =
                rows[start..end].iter().map(|r| (r.id.as_u64(), &r.values)).collect();
            match codec.encode_ok_borrowed(id, &borrowed) {
                Ok(f) => Ok(f),
                Err(_) => Ok(codec
                    .encode_response(&Response::err(id, format!("response too large ({} rows)", borrowed.len())))
                    .map_err(|e| anyhow::anyhow!("{e}"))?),
            }
        }
        Err(e) => Ok(codec
            .encode_response(&Response::err(id, e.to_string()))
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
                        encode_get_fast(&server, &codec, &req).context("encode error")?
                    } else if req.op == Op::Scan {
                        encode_scan_fast(&server, &codec, &req).context("encode error")?
                    } else {
                        let resp = dispatch(&server, req);
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
                                Ok(()) => results.push(dispatch(&server, op)),
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
pub async fn serve_http_ops(server: Arc<BlitzServer>, listener: TcpListener) -> Result<()> {
    loop {
        let (mut socket, _peer) = listener.accept().await.context("http accept failed")?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];
            // Single-read request parse (paths are tiny; larger → 414).
            let n = match socket.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/");
            let (code, ctype, body) = match path {
                "/metrics" => (200, "text/plain; version=0.0.4", server.metrics_text()),
                "/readyz" => {
                    if server.uptime_secs().is_some() {
                        (200, "text/plain", "ok\n".to_string())
                    } else {
                        (503, "text/plain", "starting\n".to_string())
                    }
                }
                _ => (404, "text/plain", "not found\n".to_string()),
            };
            let reason = match code {
                200 => "OK",
                404 => "Not Found",
                503 => "Service Unavailable",
                _ => "OK",
            };
            let head = format!(
                "HTTP/1.0 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                code, reason, ctype, body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body.as_bytes()).await;
        });
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
    async fn test_push_stream_delivers_writer_insert() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
