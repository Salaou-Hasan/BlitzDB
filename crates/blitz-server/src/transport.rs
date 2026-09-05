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

/// Default/max page sizes for `Scan`. Unbounded scans are the OOM killer:
/// one slow client scanning a 1M-row table would pin a giant `Vec<Arc>`
/// under read lock, then a giant response frame. Pagination bounds both.
pub const DEFAULT_SCAN_LIMIT: usize = 1_000;
pub const MAX_SCAN_LIMIT: usize = 10_000;

/// Parse `_limit` / `_offset` from Scan `values` without a protocol bump.
/// Absent → (1000, 0). `_limit == 0` means "use default", clamped to
/// `MAX_SCAN_LIMIT`. Negative / wrong-typed values fall back to defaults.
fn scan_pagination(values: &Option<std::collections::HashMap<String, blitz_types::value::Value>>) -> (usize, usize) {
    let mut limit = DEFAULT_SCAN_LIMIT;
    let mut offset = 0usize;
    if let Some(map) = values {
        if let Some(v) = map.get("_limit") {
            let asked = match v {
                blitz_types::value::Value::Int64(n) => (*n).max(0) as usize,
                blitz_types::value::Value::Int32(n) => (*n).max(0) as usize,
                blitz_types::value::Value::UInt64(n) => *n as usize,
                blitz_types::value::Value::UInt32(n) => *n as usize,
                _ => DEFAULT_SCAN_LIMIT,
            };
            if asked > 0 {
                limit = asked.min(MAX_SCAN_LIMIT);
            }
        }
        if let Some(v) = map.get("_offset") {
            let asked = match v {
                blitz_types::value::Value::Int64(n) => (*n).max(0) as usize,
                blitz_types::value::Value::Int32(n) => (*n).max(0) as usize,
                blitz_types::value::Value::UInt64(n) => *n as usize,
                blitz_types::value::Value::UInt32(n) => *n as usize,
                _ => 0,
            };
            offset = asked;
        }
    }
    (limit, offset)
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
                    Response::ok(id, Vec::new())
                }
                Ok(false) => Response::err(id, format!("row not found: {}", row_id)),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Scan => {
            let (limit, offset) = scan_pagination(&req.values);
            match server.engine().scan_arcs(&req.table) {
                // Paginated scan: sort by RowId for stable pages, then
                // slice. Sorting N ids is cheaper than encoding N rows,
                // and the limit bounds both CPU and frame size.
                Ok(mut rows) => {
                    rows.sort_by_key(|r| r.id);
                    let total = rows.len();
                    let start = offset.min(total);
                    let end = (start + limit).min(total);
                    let views = rows[start..end]
                        .iter()
                        .map(|row| row_to_view(row.id, row))
                        .collect();
                    Response::ok(id, views)
                }
                Err(e) => Response::err(id, e.to_string()),
            }
        },
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
    let (limit, offset) = scan_pagination(&req.values);
    match server.engine().scan_arcs(&req.table) {
        Ok(mut rows) => {
            rows.sort_by_key(|r| r.id);
            let total = rows.len();
            let start = offset.min(total);
            let end = (start + limit).min(total);
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
async fn handle_connection(server: Arc<BlitzServer>, mut socket: TcpStream) -> Result<()> {
    // Fast shed: when prod sets `shed_at_connections`, new connections past
    // the watermark fail fast (close) instead of queueing and exploding p99.
    if server.should_shed() {
        server.record_shed_drop();
        return Ok(());
    }
    let peer_ip = socket.peer_addr().ok().map(|a| a.ip());
    let _guard = match ConnectionGuard::new(&server, peer_ip) {
        Some(g) => g,
        None => return Ok(()), // at capacity / per-IP cap: close immediately
    };

    // Disable Nagle: this is a request/response protocol with small frames,
    // so waiting to coalesce segments would add pure latency.
    socket.set_nodelay(true).context("failed to set TCP_NODELAY")?;

    let max_frame = server.config().max_message_size;
    let idle_secs = server.config().idle_timeout_secs;
    let codec = FrameCodec::new(max_frame);
    // Pre-size 4 KiB (typical request) to avoid first-read realloc;
    // growth is bounded below by the slow-loris cap.
    let mut staging = BytesMut::with_capacity(4096);

    loop {
        // Idle reaping: keep-alive conns that go silent past the deadline
        // are closed to reclaim FDs/RAM (prevents slow-loris FD exhaustion).
        let n = if idle_secs > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(idle_secs),
                socket.read_buf(&mut staging),
            )
            .await
            {
                Err(_) => return Ok(()), // idle timeout: orderly close
                Ok(Err(e)) => return Err(e).context("failed to read from socket"),
                Ok(Ok(n)) => n,
            }
        } else {
            socket
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
            let incoming = codec
                .decode_incoming(frame)
                .context("decode error: closing connection")?;
            // Batch and single share one frame budget: a batch of N costs
            // one read + one write instead of N round trips.
            let t0 = std::time::Instant::now();
            let encoded = match incoming {
                // Get/Scan use zero-copy borrowed encode (no Value clones).
                Incoming::Single(req) if req.op == Op::Get => {
                    encode_get_fast(&server, &codec, &req).context("encode error")?
                }
                Incoming::Single(req) if req.op == Op::Scan => {
                    encode_scan_fast(&server, &codec, &req).context("encode error")?
                }
                Incoming::Single(req) => {
                    let resp = dispatch(&server, req);
                    // A giant Scan could exceed the frame budget; report it
                    // as an error payload instead of killing the connection.
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
                Incoming::Batch(batch) => {
                    // Bound per-frame CPU: a single frame cannot force
                    // unbounded dispatch work.
                    if batch.ops.len() > 4096 {
                        let err = Response::err(batch.id, "batch too large (max 4096 ops)");
                        codec.encode_response(&err).context("encode error")?
                    } else {
                        let mut results = Vec::with_capacity(batch.ops.len());
                        for op in batch.ops {
                            results.push(dispatch(&server, op));
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
            socket
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
}
