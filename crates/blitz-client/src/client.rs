//! Reference client: one TCP connection, invisible autobatching.
//!
//! The programming model never changes with scale: every method looks like a
//! single op. Under the hood the worker flushes 25 buffered ops (or 2ms,
//! whichever first) as one batch frame — the exact wire shape the SLOs are
//! proven on. Single pending ops flush as SINGLE frames (Get/Scan fast
//! paths stay hot); only real batches go wide.
//!
//! Retry contract (v1, honest): a flush that fails before any response byte
//! is retried ONCE after reconnect iff every op is a read or an `_idem`
//! insert (the SDK auto-stamps `_idem` on every insert). Updates/deletes are
//! never auto-retried (applied-then-ack-lost is ambiguous): retry them
//! yourself with the same values (updates are effect-idempotent; treat
//! delete's "not found" as success).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use blitz_protocol::{BatchRequest, FrameCodec, Op, Request, Response, RowView};
use blitz_types::value::Value;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::error::{map_server_error, SdkError, SdkResult};

/// Ops buffered before this worker flushes one batch frame (SLO-proven N).
pub const BATCH_N: usize = 25;
/// Max age of the oldest buffered op before a partial flush (low-load p50
/// cost; still 10× inside the SLO budget at low CCU).
pub const BATCH_MAX_WAIT: Duration = Duration::from_millis(2);
/// Default per-call timeout (flush + server + read).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// A row with its (possibly shard-routed) global id.
pub type Row = RowView;

/// Result of a `call`: output values plus per-write `{table, id}` globals.
#[derive(Debug, Clone)]
pub struct CallResult {
    pub values: HashMap<String, Value>,
    pub applied: Vec<(String, u64)>,
}

struct Pending {
    req: Request,
    reply: oneshot::Sender<SdkResult<Response>>,
    retry_safe: bool,
}

enum Cmd {
    /// Goes through the batcher (N=25 or 2ms flush).
    Batch(Pending),
    /// Flushes immediately alone (latency probes, auth handshakes).
    Direct(Pending),
}

/// Cloneable handle. All clones share one connection + batcher.
#[derive(Clone)]
pub struct Client {
    tx: mpsc::UnboundedSender<Cmd>,
    next_id: Arc<AtomicU64>,
    timeout: Duration,
}

impl Client {
    /// Connect (eager; fails fast on refused/timeout).
    pub async fn connect(addr: SocketAddr) -> SdkResult<Self> {
        Self::connect_with_timeout(addr, DEFAULT_TIMEOUT).await
    }

    pub async fn connect_with_timeout(addr: SocketAddr, timeout: Duration) -> SdkResult<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| SdkError::Timeout(timeout.as_millis() as u64))?
            .map_err(|e| SdkError::Transport(format!("connect {}: {}", addr, e)))?;
        stream.set_nodelay(true).map_err(|e| SdkError::Transport(e.to_string()))?;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(worker_loop(addr, stream, rx, timeout));
        Ok(Self { tx, next_id: Arc::new(AtomicU64::new(1)), timeout })
    }

    fn alloc_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn exec(&self, req: Request, retry_safe: bool) -> SdkResult<Response> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Batch(Pending { req, reply: reply_tx, retry_safe }))
            .map_err(|_| SdkError::Closed)?;
        tokio::time::timeout(self.timeout, reply_rx)
            .await
            .map_err(|_| SdkError::Timeout(self.timeout.as_millis() as u64))?
            .map_err(|_| SdkError::Closed)?
    }

    /// Send one frame immediately (Ping/auth probes skip the batcher so
    /// handshakes don't wait 2ms and latency probes stay honest).
    async fn exec_direct(&self, req: Request) -> SdkResult<Response> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Direct(Pending { req, reply: reply_tx, retry_safe: true }))
            .map_err(|_| SdkError::Closed)?;
        tokio::time::timeout(self.timeout, reply_rx)
            .await
            .map_err(|_| SdkError::Timeout(self.timeout.as_millis() as u64))?
            .map_err(|_| SdkError::Closed)?
    }

    fn ok_rows(&self, resp: Response) -> SdkResult<Vec<Row>> {
        if resp.ok {
            Ok(resp.rows)
        } else {
            Err(map_server_error(&resp.error.unwrap_or_else(|| "unknown error".into())))
        }
    }

    // -- Primitive ops (each looks single; the worker batches) ---------

    /// Authenticate this connection (handshake; identity persists).
    pub async fn authenticate(&self, token: &str) -> SdkResult<()> {
        let mut values = HashMap::new();
        values.insert("_auth".to_string(), Value::String(token.to_string()));
        let req = Request { id: self.alloc_id(), op: Op::Ping, table: String::new(), row_id: None, values: Some(values) };
        let resp = self.exec_direct(req).await?;
        self.ok_rows(resp).map(|_| ())
    }

    pub async fn ping(&self) -> SdkResult<()> {
        let req = Request { id: self.alloc_id(), op: Op::Ping, table: String::new(), row_id: None, values: None };
        let resp = self.exec_direct(req).await?;
        self.ok_rows(resp).map(|_| ())
    }

    /// Insert with auto `_idem` (safe reconnect-replay inside the window).
    pub async fn insert(&self, table: &str, mut values: HashMap<String, Value>) -> SdkResult<Row> {
        values.insert("_idem".to_string(), Value::String(uuid::Uuid::new_v4().to_string()));
        let req = Request { id: self.alloc_id(), op: Op::Insert, table: table.into(), row_id: None, values: Some(values) };
        let mut rows = self.ok_rows(self.exec(req, true).await?)?;
        rows.pop().ok_or_else(|| SdkError::Server("insert returned no rows".into()))
    }

    pub async fn get(&self, table: &str, id: u64) -> SdkResult<Option<Row>> {
        let req = Request { id: self.alloc_id(), op: Op::Get, table: table.into(), row_id: Some(id), values: None };
        match self.exec(req, true).await {
            Ok(resp) if resp.ok => Ok(resp.rows.into_iter().next()),
            Ok(resp) => {
                let msg = resp.error.unwrap_or_else(|| "unknown error".into());
                // Row miss → None. Table miss stays an error (caller bug).
                if msg.starts_with("row not found") {
                    Ok(None)
                } else {
                    Err(map_server_error(&msg))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Not auto-retried (see module docs): retry manually on transport error.
    pub async fn update(&self, table: &str, id: u64, values: HashMap<String, Value>) -> SdkResult<Row> {
        let req = Request { id: self.alloc_id(), op: Op::Update, table: table.into(), row_id: Some(id), values: Some(values) };
        let mut rows = self.ok_rows(self.exec(req, false).await?)?;
        rows.pop().ok_or_else(|| SdkError::Server("update returned no rows".into()))
    }

    /// Not auto-retried: on transport error, re-Get first; treat a retry's
    /// "not found" as success (already deleted).
    pub async fn delete(&self, table: &str, id: u64) -> SdkResult<()> {
        let req = Request { id: self.alloc_id(), op: Op::Delete, table: table.into(), row_id: Some(id), values: None };
        self.ok_rows(self.exec(req, false).await?).map(|_| ())
    }

    pub async fn scan(&self, table: &str, limit: usize, cursor: Option<u64>, desc: bool) -> SdkResult<Vec<Row>> {
        let mut values = HashMap::new();
        values.insert("_limit".to_string(), Value::Int64(limit as i64));
        if desc {
            values.insert("_order".to_string(), Value::String("desc".into()));
        }
        if let Some(c) = cursor {
            values.insert("_cursor".to_string(), Value::UInt64(c));
        }
        let req = Request { id: self.alloc_id(), op: Op::Scan, table: table.into(), row_id: None, values: Some(values) };
        self.ok_rows(self.exec(req, true).await?)
    }

    pub async fn find(&self, table: &str, column: &str, value: Value) -> SdkResult<Option<Row>> {
        let mut values = HashMap::new();
        values.insert("_col".to_string(), Value::String(column.into()));
        values.insert("_val".to_string(), value);
        let req = Request { id: self.alloc_id(), op: Op::Find, table: table.into(), row_id: None, values: Some(values) };
        match self.exec(req, true).await {
            Ok(resp) if resp.ok => Ok(resp.rows.into_iter().next()),
            Ok(resp) => {
                let msg = resp.error.unwrap_or_else(|| "unknown error".into());
                if msg == "not found" {
                    Ok(None)
                } else {
                    Err(map_server_error(&msg))
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn search(&self, table: &str, query: &str, limit: usize) -> SdkResult<Vec<Row>> {
        let mut values = HashMap::new();
        values.insert("_q".to_string(), Value::String(query.into()));
        values.insert("_limit".to_string(), Value::Int64(limit as i64));
        let req = Request { id: self.alloc_id(), op: Op::Search, table: table.into(), row_id: None, values: Some(values) };
        self.ok_rows(self.exec(req, true).await?)
    }

    /// Execute a registered procedure transactionally. Not auto-retried:
    /// design procedures around an application `_idem` argument instead.
    pub async fn call(&self, name: &str, args: HashMap<String, Value>) -> SdkResult<CallResult> {
        let req = Request {
            id: self.alloc_id(),
            op: Op::Call,
            table: format!("fn:{}", name),
            row_id: None,
            values: Some(args),
        };
        let mut rows = self.ok_rows(self.exec(req, false).await?)?;
        let row = rows.pop().ok_or_else(|| SdkError::Server("call returned no rows".into()))?;
        let mut values = row.values;
        let applied = match values.remove("_applied") {
            Some(Value::Json(serde_json::Value::Array(arr))) => arr
                .iter()
                .filter_map(|v| match (v.get("table").and_then(|t| t.as_str()), v.get("id").and_then(|i| i.as_u64())) {
                    (Some(t), Some(i)) => Some((t.to_string(), i)),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        Ok(CallResult { values, applied })
    }
}

async fn worker_loop(
    addr: SocketAddr,
    mut stream: TcpStream,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    timeout: Duration,
) {
    let codec = FrameCodec::with_default_limit();
    let mut buf = BytesMut::new();
    let mut pending: Vec<Pending> = Vec::new();
    let mut tick = tokio::time::interval(BATCH_MAX_WAIT);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Drop the immediate first tick: an idle worker shouldn't flush empties.
    tick.tick().await;

    let mut dead = false;
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                match cmd {
                    Some(Cmd::Batch(p)) => {
                        pending.push(p);
                        if pending.len() >= BATCH_N {
                            dead = flush(&codec, &mut stream, &mut buf, &mut pending, addr, timeout).await.is_err();
                            if dead { fail_all(&mut pending, "connection lost during flush"); }
                        }
                    }
                    Some(Cmd::Direct(p)) => {
                        // Flush anything buffered first (order!), then the
                        // direct frame alone.
                        if !pending.is_empty() && !dead {
                            dead = flush(&codec, &mut stream, &mut buf, &mut pending, addr, timeout).await.is_err();
                            if dead { fail_all(&mut pending, "connection lost during flush"); }
                        }
                        if dead {
                            let _ = p.reply.send(Err(SdkError::Transport("connection lost".into())));
                        } else {
                            // flush() always replies (ok or err); the vec is
                            // drained either way.
                            let mut single = vec![p];
                            if flush(&codec, &mut stream, &mut buf, &mut single, addr, timeout).await.is_err() {
                                dead = true;
                            }
                        }
                    }
                    None => {
                        // All handles dropped: best-effort final flush, then exit.
                        if !pending.is_empty() && !dead {
                            let _ = flush(&codec, &mut stream, &mut buf, &mut pending, addr, timeout).await;
                        }
                        fail_all(&mut pending, "client closed");
                        return;
                    }
                }
            }
            _ = tick.tick() => {
                if !pending.is_empty() && !dead {
                    dead = flush(&codec, &mut stream, &mut buf, &mut pending, addr, timeout).await.is_err();
                    if dead { fail_all(&mut pending, "connection lost during flush"); }
                } else if dead {
                    // Try to re-establish for future work (lazy).
                    match TcpStream::connect(addr).await {
                        Ok(s) => { stream = s; dead = false; }
                        Err(_) => {}
                    }
                }
            }
        }
    }
}

fn fail_all(pending: &mut Vec<Pending>, msg: &str) {
    for p in pending.drain(..) {
        let _ = p.reply.send(Err(SdkError::Transport(msg.into())));
    }
}

/// Flush all buffered ops as one frame (single-op → SINGLE frame for the
/// Get/Scan fast paths; N>1 → one batch). Returns Err on transport failure;
/// on failure with an all-retry-safe batch, reconnects + resends once.
async fn flush(
    codec: &FrameCodec,
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    pending: &mut Vec<Pending>,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<(), ()> {
    let batch: Vec<Pending> = pending.drain(..).collect();
    if batch.is_empty() {
        return Ok(());
    }
    let retry_safe = batch.iter().all(|p| p.retry_safe);
    let reqs: Vec<Request> = batch.iter().map(|p| p.req.clone()).collect();
    match flush_once(codec, stream, buf, &reqs, timeout).await {
        Ok(resps) => {
            for (p, r) in batch.into_iter().zip(resps.into_iter()) {
                let _ = p.reply.send(r);
            }
            Ok(())
        }
        Err(_) if retry_safe => {
            // One reconnect + resend with identical payloads (same `_idem`).
            let mut fresh = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
                Ok(Ok(s)) => s,
                _ => {
                    for p in batch {
                        let _ = p.reply.send(Err(SdkError::Transport("flush failed; reconnect failed".into())));
                    }
                    return Err(());
                }
            };
            let mut fresh_buf = BytesMut::new();
            match flush_once(codec, &mut fresh, &mut fresh_buf, &reqs, timeout).await {
                Ok(resps) => {
                    for (p, r) in batch.into_iter().zip(resps.into_iter()) {
                        let _ = p.reply.send(r);
                    }
                    *stream = fresh;
                    *buf = fresh_buf;
                    Ok(())
                }
                Err(_) => {
                    for p in batch {
                        let _ = p.reply.send(Err(SdkError::Transport("flush failed after reconnect".into())));
                    }
                    Err(())
                }
            }
        }
        Err(_) => {
            for p in batch {
                let _ = p.reply.send(Err(SdkError::Transport("flush failed (not retry-safe; retry manually)".into())));
            }
            Err(())
        }
    }
}

/// One frame roundtrip. Responses map to per-op results (server errors stay
/// per-op payloads, mapped by the caller).
async fn flush_once(
    codec: &FrameCodec,
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    reqs: &[Request],
    timeout: Duration,
) -> Result<Vec<SdkResult<Response>>, ()> {
    let frame = if reqs.len() == 1 {
        codec.encode_request(&reqs[0]).map_err(|_| ())?
    } else {
        codec.encode_batch_request(&BatchRequest { id: reqs[0].id, ops: reqs.to_vec() }).map_err(|_| ())?
    };
    tokio::time::timeout(timeout, stream.write_all(&frame)).await.map_err(|_| ())?.map_err(|_| ())?;
    loop {
        if let Some(payload) = codec.feed(buf).map_err(|_| ())? {
            if reqs.len() == 1 {
                let resp = codec.decode_response(payload).map_err(|_| ())?;
                return Ok(vec![Ok(resp)]);
            }
            let bresp = codec.decode_batch_response(payload).map_err(|_| ())?;
            if bresp.results.len() != reqs.len() {
                return Err(());
            }
            return Ok(bresp.results.into_iter().map(Ok).collect());
        }
        let n = tokio::time::timeout(timeout, stream.read_buf(buf)).await.map_err(|_| ())?.map_err(|_| ())?;
        if n == 0 {
            return Err(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn kv(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    async fn test_server() -> (std::net::SocketAddr, Arc<blitz_server::BlitzServer>) {
        let server = Arc::new(blitz_server::BlitzServer::new());
        server.start().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(blitz_server::serve(Arc::clone(&server), listener));
        (addr, server)
    }

    #[tokio::test]
    async fn sdk_crud_roundtrip() {
        let (addr, _srv) = test_server().await;
        let c = Client::connect(addr).await.unwrap();
        c.ping().await.unwrap();

        let row = c.insert("users", kv(&[
            ("id", Value::Int64(1)),
            ("name", Value::String("Ada".into())),
            ("email", Value::String("ada@x.com".into())),
        ])).await.unwrap();
        let got = c.get("users", row.id).await.unwrap().expect("must exist");
        assert_eq!(got.values.get("name"), Some(&Value::String("Ada".into())));

        let upd = c.update("users", row.id, kv(&[("name", Value::String("Ada L.".into()))])).await.unwrap();
        assert_eq!(upd.values.get("name"), Some(&Value::String("Ada L.".into())));

        let found = c.find("users", "email", Value::String("ada@x.com".into())).await.unwrap();
        assert!(found.is_some());
        assert!(c.find("users", "email", Value::String("nope@x.com".into())).await.unwrap().is_none());

        let rows = c.scan("users", 100, None, false).await.unwrap();
        assert_eq!(rows.len(), 1);

        c.delete("users", row.id).await.unwrap();
        assert!(c.get("users", row.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn sdk_concurrent_sharing_batches() {
        // 30 tasks × 10 inserts on ONE client: the worker must batch (N=25)
        // and map every response to the right caller.
        let (addr, _srv) = test_server().await;
        let c = Client::connect(addr).await.unwrap();
        let mut hs = Vec::new();
        for t in 0..30 {
            let c = c.clone();
            hs.push(tokio::spawn(async move {
                let mut ids = Vec::new();
                for i in 0..10 {
                    let row = c.insert("users", kv(&[
                        ("id", Value::Int64(t * 100 + i)),
                        ("name", Value::String(format!("u{}-{}", t, i))),
                        ("email", Value::String(format!("u{}-{}@x.com", t, i))),
                    ])).await.unwrap();
                    ids.push(row.id);
                }
                ids
            }));
        }
        let mut all = Vec::new();
        for h in hs {
            all.extend(h.await.unwrap());
        }
        assert_eq!(all.len(), 300);
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 300, "every insert needs a distinct id");
        let rows = c.scan("users", 1000, None, false).await.unwrap();
        assert_eq!(rows.len(), 300);
    }

    #[tokio::test]
    async fn sdk_error_mapping() {
        let (addr, _srv) = test_server().await;
        let c = Client::connect(addr).await.unwrap();
        // Get missing → None (not an error).
        assert!(c.get("users", 424242).await.unwrap().is_none());
        // Update missing → NotFound.
        let err = c.update("users", 424242, kv(&[("name", Value::String("x".into()))])).await.unwrap_err();
        assert!(matches!(err, SdkError::NotFound(_)), "got {:?}", err);
        // Scan unknown table → NotFound (table not found).
        let err = c.scan("nope", 10, None, false).await.unwrap_err();
        assert!(matches!(err, SdkError::NotFound(_)), "got {:?}", err);
    }

    #[tokio::test]
    async fn sdk_call_procedure() {
        use blitz_runtime::{Procedure, ProcedureStep};
        let (addr, srv) = test_server().await;
        srv.register_procedure(
            Procedure::new("greet")
                .with_step(ProcedureStep::CallFunction {
                    function: "upper".into(),
                    args: kv(&[("0", Value::String("$name".into()))]),
                })
                .with_step(ProcedureStep::Return { value: Value::String("$name".into()) }),
        );
        let c = Client::connect(addr).await.unwrap();
        let out = c.call("greet", kv(&[("name", Value::String("ada".into()))])).await.unwrap();
        assert_eq!(out.values.get("result"), Some(&Value::String("ada".into())));
        assert!(out.applied.is_empty());
        // Unknown procedure → Server error (verbatim abort reason).
        let err = c.call("nope", HashMap::new()).await.unwrap_err();
        assert!(matches!(err, SdkError::NotFound(_)), "got {:?}", err);
    }
}
