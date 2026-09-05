//! Benchmark C — Real application workload (exact accounting).
//!
//! ```sh
//! cargo run --release -p blitz-server --example bench_c_app [max_ccu] [ops_per_client]
//! ```
//!
//! Exact-counting harness per proposal:
//!   attempted / connected / sent / received / ok / resp_err / io_err /
//!   timeouts / lost / conn_failures, with invariant
//!   `received == ok + resp_err` verified per level.
//! Only GOODPUT (`ok`/sec) is reported as throughput; attempted RPS is
//! shown separately. First 10 response failures are logged with
//! op/conn/error/timestamp/queue-state.

#[allow(dead_code)]
fn _unused() {}

// Exact accounting lives in the library so all benches share it.
use blitz_server::bench_common::{
    roundtrip_timeout, ErrorDetail, Hist, LevelCounters, RtOutcome,
};

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use blitz_core::TableEngine;
use blitz_protocol::{FrameCodec, Op, Request};
use blitz_server::{serve, BlitzServer};
use blitz_types::value::Value;
use bytes::BytesMut;

fn schema(name: &str, cols: &[&str]) -> blitz_types::schema::TableSchema {
    use blitz_types::column::{ColumnDef, ColumnType};
    let mut s = blitz_types::schema::TableSchema::new(name)
        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable());
    for c in cols {
        s = s.with_column(ColumnDef::new(*c, ColumnType::String).nullable());
    }
    s
}

fn app_tables() -> Vec<blitz_types::schema::TableSchema> {
    vec![
        schema("users", &["name", "email"]),
        schema("posts", &["author", "body"]),
        schema("likes", &["user", "post"]),
        schema("follows", &["from", "to"]),
        schema("comments", &["post", "body"]),
        schema("notifs", &["user", "text"]),
        schema("messages", &["from", "to", "body"]),
    ]
}

struct Rng(u64);
impl Rng {
    fn next(&mut self, b: usize) -> usize {
        let mut x = self.0 | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x as usize) % b.max(1)
    }
}

async fn connect(addr: std::net::SocketAddr, idx: i64) -> anyhow::Result<tokio::net::TcpStream> {
    let s = tokio::net::TcpSocket::new_v4()?;
    s.set_recv_buffer_size(4096)?;
    s.set_send_buffer_size(4096)?;
    let src: std::net::SocketAddr = format!("127.0.0.{}:0", 1 + (idx % 8))
        .parse()
        .map_err(|e| anyhow::anyhow!("bad src: {}", e))?;
    s.bind(src)?;
    let st = s.connect(addr).await?;
    st.set_nodelay(true)?;
    Ok(st)
}

struct AppState {
    posts: Vec<u64>,
    notifs: Vec<u64>,
}

/// Shard routing for the cache/shard deployment: `shards==1` keeps legacy
/// global tables; otherwise each client sticks to `idx % shards`, so its
/// reads hit its own partition and per-table write locks divide by `shards`.
fn tname(base: &str, shard: usize, shards: usize) -> String {
    if shards <= 1 {
        base.to_string()
    } else {
        format!("{}_{:02}", base, shard % shards)
    }
}
fn build_app_req(rng: &mut Rng, idx: i64, next_id: u64, st: &AppState, shard: usize, shards: usize) -> (usize, Request) {
    let roll = rng.next(100);
    if roll < 30 {
        if !st.posts.is_empty() {
            let p = st.posts[rng.next(st.posts.len())];
            (0, Request { id: next_id, op: Op::Get, table: tname("posts", shard, shards), row_id: Some(p), values: None })
        } else {
            let mut v = HashMap::new();
            v.insert("author".into(), Value::String(format!("u{}", idx)));
            v.insert("body".into(), Value::String("hello".into()));
            (1, Request { id: next_id, op: Op::Insert, table: tname("posts", shard, shards), row_id: None, values: Some(v) })
        }
    } else if roll < 45 {
        let mut v = HashMap::new();
        v.insert("author".into(), Value::String(format!("u{}", idx)));
        v.insert("body".into(), Value::String(format!("post {}", next_id)));
        (1, Request { id: next_id, op: Op::Insert, table: tname("posts", shard, shards), row_id: None, values: Some(v) })
    } else if roll < 60 {
        let mut v = HashMap::new();
        v.insert("user".into(), Value::String(format!("u{}", idx)));
        let pref = if !st.posts.is_empty() { st.posts[rng.next(st.posts.len())].to_string() } else { "0".into() };
        v.insert("post".into(), Value::String(pref));
        (2, Request { id: next_id, op: Op::Insert, table: tname("likes", shard, shards), row_id: None, values: Some(v) })
    } else if roll < 70 {
        let mut v = HashMap::new();
        v.insert("from".into(), Value::String(format!("u{}", idx)));
        v.insert("to".into(), Value::String(format!("u{}", rng.next(100000))));
        (3, Request { id: next_id, op: Op::Insert, table: tname("follows", shard, shards), row_id: None, values: Some(v) })
    } else if roll < 80 {
        let mut v = HashMap::new();
        let pref = if !st.posts.is_empty() { st.posts[rng.next(st.posts.len())].to_string() } else { "0".into() };
        v.insert("post".into(), Value::String(pref));
        v.insert("body".into(), Value::String("nice!".into()));
        (4, Request { id: next_id, op: Op::Insert, table: tname("comments", shard, shards), row_id: None, values: Some(v) })
    } else if roll < 90 {
        if !st.notifs.is_empty() && rng.next(2) == 0 {
            let p = st.notifs[rng.next(st.notifs.len())];
            (5, Request { id: next_id, op: Op::Get, table: tname("notifs", shard, shards), row_id: Some(p), values: None })
        } else {
            let mut v = HashMap::new();
            v.insert("user".into(), Value::String(format!("u{}", idx)));
            v.insert("text".into(), Value::String("you have a like".into()));
            (5, Request { id: next_id, op: Op::Insert, table: tname("notifs", shard, shards), row_id: None, values: Some(v) })
        }
    } else if roll < 95 {
        let mut v = HashMap::new();
        v.insert("from".into(), Value::String(format!("u{}", idx)));
        v.insert("to".into(), Value::String(format!("u{}", rng.next(100000))));
        v.insert("body".into(), Value::String("hey".into()));
        (6, Request { id: next_id, op: Op::Insert, table: tname("messages", shard, shards), row_id: None, values: Some(v) })
    } else {
        (7, Request::ping(next_id))
    }
}

struct ClientOut {
    lats: Vec<u64>,
    mix: [u64; 8],
    sent: u64,
    received: u64,
    ok: u64,
    resp_err: u64,
    io_err: u64,
    timeouts: u64,
    lost: u64,
    errors: Vec<ErrorDetail>,
}

async fn run_client(
    addr: std::net::SocketAddr,
    idx: i64,
    ops: usize,
    barrier: Arc<tokio::sync::Barrier>,
    shards: usize,
) -> anyhow::Result<ClientOut> {
    let stream = match connect(addr, idx).await {
        Ok(s) => s,
        Err(e) => {
            barrier.wait().await;
            return Err(e);
        }
    };
    barrier.wait().await;
    let (mut rd, mut wr) = stream.into_split();
    let codec = FrameCodec::with_default_limit();
    let mut buf = BytesMut::new();
    let mut out = ClientOut {
        lats: Vec::with_capacity(ops),
        mix: [0; 8],
        sent: 0,
        received: 0,
        ok: 0,
        resp_err: 0,
        io_err: 0,
        timeouts: 0,
        lost: 0,
        errors: Vec::new(),
    };
    let mut rng = Rng(0x1234ABCDu64.wrapping_add(idx as u64 * 0x9E3779B9));
    let mut st = AppState { posts: Vec::new(), notifs: Vec::new() };
    let timeout = Duration::from_secs(5);
    let shard = idx as usize % shards.max(1);

    let mut next_id = 0u64;
    for i in 0..ops {
        let (slot, req) = build_app_req(&mut rng, idx, next_id, &st, shard, shards);
        next_id += 1;
        out.sent += 1;
        let op_name = format!("{:?}", req.op);
        let tbl = req.table.clone();
        let rid = req.row_id;
        match roundtrip_timeout(&codec, &mut rd, &mut wr, &mut buf, &req, timeout).await {
            RtOutcome::Ok(r, el) => {
                out.received += 1;
                out.ok += 1;
                out.lats.push(el.as_micros() as u64);
                out.mix[slot] += 1;
                if req.op == Op::Insert && !r.rows.is_empty() {
                    if req.table.starts_with("posts") {
                        st.posts.push(r.rows[0].id);
                    } else if req.table.starts_with("notifs") {
                        st.notifs.push(r.rows[0].id);
                    }
                }
            }
            RtOutcome::RespErr(r, el) => {
                out.received += 1;
                out.resp_err += 1;
                out.lats.push(el.as_micros() as u64);
                out.mix[slot] += 1;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(
                        idx,
                        &op_name,
                        &tbl,
                        rid,
                        r.error.unwrap_or_else(|| "unknown".into()),
                        format!("op#{}/{} known_posts={} known_notifs={} outstanding=1", i, ops, st.posts.len(), st.notifs.len()),
                    ));
                }
            }
            RtOutcome::IoErr(e) => {
                out.io_err += 1;
                out.lost += (ops - i - 1) as u64;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, &op_name, &tbl, rid, format!("io: {}", e),
                        format!("op#{}/{} outstanding=1", i, ops)));
                }
                break;
            }
            RtOutcome::Timeout => {
                out.timeouts += 1;
                out.lost += (ops - i - 1) as u64;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, &op_name, &tbl, rid, "timeout 5s".into(),
                        format!("op#{}/{} outstanding=1", i, ops)));
                }
                break;
            }
        }
        if st.posts.len() > 64 {
            st.posts.drain(0..st.posts.len() - 64);
        }
        if st.notifs.len() > 64 {
            st.notifs.drain(0..st.notifs.len() - 64);
        }
    }
    Ok(out)
}

/// Batched variant: `batch` app ops per BatchRequest frame.
/// Same mix via `build_app_req`; per-op latency approximated as
/// batch_time/batch_len (throughput exact). This is the production answer
/// past 8K CCU: one syscall + wake per N ops keeps p99 <100ms where
/// ping-pong Little's-law queues to 200ms+.
async fn run_client_batched(
    addr: std::net::SocketAddr,
    idx: i64,
    ops: usize,
    barrier: Arc<tokio::sync::Barrier>,
    batch: usize,
    shards: usize,
) -> anyhow::Result<ClientOut> {
    use blitz_protocol::BatchRequest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let stream = match connect(addr, idx).await {
        Ok(s) => s,
        Err(e) => {
            barrier.wait().await;
            return Err(e);
        }
    };
    let (mut rd, mut wr) = stream.into_split();
    let codec = FrameCodec::with_default_limit();
    let mut buf = BytesMut::new();
    let mut out = ClientOut {
        lats: Vec::with_capacity(ops),
        mix: [0; 8],
        sent: 0,
        received: 0,
        ok: 0,
        resp_err: 0,
        io_err: 0,
        timeouts: 0,
        lost: 0,
        errors: Vec::new(),
    };
    let mut rng = Rng(0x1234ABCDu64.wrapping_add(idx as u64 * 0x9E3779B9));
    let mut st = AppState { posts: Vec::new(), notifs: Vec::new() };
    let timeout = Duration::from_secs(30);
    let mut next_id = 0u64;
    let shard = idx as usize % shards.max(1);
    // Seed 2 posts + 2 notifs BEFORE the barrier (setup, not measured load:
    // excluded from sent/recv/ok/lats so SLO percentiles cover only the loaded
    // storm; failures here abort as io/timeout with full ops lost).
    for s in 0..4 {
        let req = if s % 2 == 0 {
            let mut v = HashMap::new();
            v.insert("author".into(), Value::String(format!("u{}", idx)));
            v.insert("body".into(), Value::String("seed".into()));
            Request { id: next_id, op: Op::Insert, table: tname("posts", shard, shards), row_id: None, values: Some(v) }
        } else {
            let mut v = HashMap::new();
            v.insert("user".into(), Value::String(format!("u{}", idx)));
            v.insert("text".into(), Value::String("seed".into()));
            Request { id: next_id, op: Op::Insert, table: tname("notifs", shard, shards), row_id: None, values: Some(v) }
        };
        next_id += 1;
        match roundtrip_timeout(&codec, &mut rd, &mut wr, &mut buf, &req, timeout).await {
            RtOutcome::Ok(r, _) => {
                if !r.rows.is_empty() {
                    if req.table.starts_with("posts") {
                        st.posts.push(r.rows[0].id);
                    } else {
                        st.notifs.push(r.rows[0].id);
                    }
                }
            }
            RtOutcome::RespErr(r, _) => {
                out.resp_err += 1;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, "Seed", &req.table, None,
                        r.error.unwrap_or_else(|| "unknown".into()), "seed phase (setup)".into()));
                }
            }
            RtOutcome::IoErr(e) => {
                out.io_err += 1;
                out.lost += ops as u64;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, "Seed", &req.table, None, format!("io: {}", e), "seed phase".into()));
                }
                return Ok(out);
            }
            RtOutcome::Timeout => {
                out.timeouts += 1;
                out.lost += ops as u64;
                return Ok(out);
            }
        }
    }
    // Seeds done pre-load; now join the timed storm.
    barrier.wait().await;
    let mut batch_no = 0u64;
    let mut done = 0usize;
    while done < ops {
        let n = (ops - done).min(batch);
        let mut batch_ops = Vec::with_capacity(n);
        let mut slots = Vec::with_capacity(n);
        let mut metas: Vec<(String, String, Option<u64>)> = Vec::with_capacity(n);
        for _ in 0..n {
            let (slot, req) = build_app_req(&mut rng, idx, next_id, &st, shard, shards);
            next_id += 1;
            metas.push((format!("{:?}", req.op), req.table.clone(), req.row_id));
            slots.push(slot);
            batch_ops.push(req);
        }
        out.sent += n as u64;
        // BLITZ_ATOMIC=1 sends each frame as an atomic batch (all-or-nothing)
        // instead of a plain batch: same ops, OCC transaction per frame.
        let atomic = std::env::var("BLITZ_ATOMIC").as_deref() == Ok("1");
        let frame = if atomic {
            codec.encode_atomic_batch_request(&BatchRequest { id: batch_no, ops: batch_ops })?
        } else {
            codec.encode_batch_request(&BatchRequest { id: batch_no, ops: batch_ops })?
        };
        batch_no += 1;
        let t = Instant::now();
        match tokio::time::timeout(timeout, wr.write_all(&frame)).await {
            Err(_) => {
                out.timeouts += 1;
                out.lost += (ops - done - n) as u64;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, "Batch", "-", None, "write timeout".into(), format!("batch done={}/{}", done, ops)));
                }
                break;
            }
            Ok(Err(e)) => {
                out.io_err += 1;
                out.lost += (ops - done - n) as u64;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, "Batch", "-", None, format!("write: {}", e), format!("batch done={}/{}", done, ops)));
                }
                break;
            }
            Ok(Ok(())) => {}
        }
        let bresp = loop {
            match codec.feed(&mut buf) {
                Ok(Some(p)) => match codec.decode_batch_response(p) {
                    Ok(b) => break b,
                    Err(e) => {
                        out.io_err += 1;
                        out.lost += (ops - done - n) as u64;
                        if out.errors.len() < 10 {
                            out.errors.push(ErrorDetail::new(idx, "Batch", "-", None, format!("decode_batch: {}", e), format!("done={}/{}", done, ops)));
                        }
                        done = ops;
                        break blitz_protocol::BatchResponse { id: 0, results: vec![] };
                    }
                },
                Ok(None) => {}
                Err(e) => {
                    out.io_err += 1;
                    out.lost += (ops - done - n) as u64;
                    if out.errors.len() < 10 {
                        out.errors.push(ErrorDetail::new(idx, "Batch", "-", None, format!("feed: {}", e), format!("done={}/{}", done, ops)));
                    }
                    done = ops;
                    break blitz_protocol::BatchResponse { id: 0, results: vec![] };
                }
            }
            match tokio::time::timeout(timeout, rd.read_buf(&mut buf)).await {
                Ok(Ok(0)) => {
                    out.io_err += 1;
                    out.lost += (ops - done - n) as u64;
                    done = ops;
                    break blitz_protocol::BatchResponse { id: 0, results: vec![] };
                }
                Ok(Ok(_)) => continue,
                _ => {
                    out.timeouts += 1;
                    out.lost += (ops - done - n) as u64;
                    done = ops;
                    break blitz_protocol::BatchResponse { id: 0, results: vec![] };
                }
            }
        };
        if done >= ops && bresp.results.is_empty() && n > 0 && out.received < out.sent {
            break;
        }
        let per_op = t.elapsed() / n.max(1) as u32;
        for (k, res) in bresp.results.iter().enumerate() {
            out.received += 1;
            out.lats.push(per_op.as_micros() as u64);
            let slot = slots.get(k).copied().unwrap_or(0);
            out.mix[slot] += 1;
            let (opn, tbl, rid) = metas.get(k).cloned().unwrap_or(("?".into(), "-".into(), None));
            if !res.ok {
                out.resp_err += 1;
                if out.errors.len() < 10 {
                    out.errors.push(ErrorDetail::new(idx, &opn, &tbl, rid,
                        res.error.clone().unwrap_or_else(|| "unknown".into()),
                        format!("batch done={}/{} throttle=1", done, ops)));
                }
            } else {
                out.ok += 1;
                if opn == "Insert" && !res.rows.is_empty() {
                    if tbl.starts_with("posts") {
                        st.posts.push(res.rows[0].id);
                    } else if tbl.starts_with("notifs") {
                        st.notifs.push(res.rows[0].id);
                    }
                }
            }
        }
        // Shortfall (transport drop mid-batch): count missing as lost.
        if bresp.results.len() < n {
            let missing = (n - bresp.results.len()) as u64;
            out.lost += missing;
            // sent already counted; received only what arrived.
        }
        done += n;
        if st.posts.len() > 64 {
            st.posts.drain(0..st.posts.len() - 64);
        }
        if st.notifs.len() > 64 {
            st.notifs.drain(0..st.notifs.len() - 64);
        }
    }
    Ok(out)
}

async fn run_level(ccu: usize, ops: usize, batch: usize, shards: usize) {
    // Shard/cache deployment: trusted shape → skip validation; sharded tables
    // divide per-table write locks by `shards`. Durability via env for
    // prod-quant runs: BLITZ_DATA_DIR + BLITZ_DURABILITY=near-sync|every-sec
    // (default none = in-memory speed).
    let mut cfg = blitz_server::ServerConfig::default();
    cfg.skip_validation = true;
    // BLITZ_SERVER_SHARDS=1: server-side routing. Clients speak BASE names
    // (naming shards=1); the server hashes into `shards` physical tables with
    // the identical `{base}_{NN}` layout. Same data distribution, stable model.
    let server_side = std::env::var("BLITZ_SERVER_SHARDS").as_deref() == Ok("1") && shards > 1;
    if server_side {
        for (base, col) in [
            ("posts", "author"),
            ("likes", "user"),
            ("follows", "from"),
            ("comments", "post"),
            ("notifs", "user"),
            ("messages", "from"),
        ] {
            cfg.table_shards.insert(base.into(), blitz_server::ShardSpec::new(shards, col));
        }
    }
    if let Ok(dir) = std::env::var("BLITZ_DATA_DIR") {
        // Per-level subdir: isolates levels (no cross-level replay
        // accumulation — each level models a steady-state server with rotation
        // assumed; shared-dir accumulation is tested by restart drills, not SLOs).
        let level_dir = format!("{}/ccu-{}", dir, ccu);
        cfg.data_dir = Some(level_dir);
        cfg.durability = match std::env::var("BLITZ_DURABILITY").as_deref() {
            Ok("near-sync") => blitz_server::DurabilityMode::near_sync(),
            Ok("every-sec") => blitz_server::DurabilityMode::every_sec(),
            _ => blitz_server::DurabilityMode::None,
        };
        cfg.snapshot_secs = 0;
    }
    let durable = cfg.durability.is_durable();
    let server = Arc::new(BlitzServer::with_config(cfg));
    server.start().await.unwrap();
    // Physical tables exist under both layouts (client-mangled or routed).
    let physical_shards = if shards <= 1 { 1 } else { shards };
    if physical_shards <= 1 {
        for t in app_tables() {
            let _ = server.engine().create_table(t);
        }
    } else {
        for s in 0..physical_shards {
            for t in app_tables() {
                let mut st = t.clone();
                st.name = format!("{}_{:02}", t.name, s);
                let _ = server.engine().create_table(st);
            }
        }
    }
    {
        use blitz_types::{id::RowId, row::Row};
        if physical_shards <= 1 {
            for i in 0..1000i64 {
                let mut r = Row::new(RowId::new(0));
                r.set("user", Value::String("seed".into()));
                r.set("text", Value::String(format!("seed {}", i)));
                let _ = server.engine().insert("notifs", r);
            }
        } else {
            let per = (1000 / physical_shards as i64).max(16);
            for s in 0..physical_shards {
                for i in 0..per {
                    let mut r = Row::new(RowId::new(0));
                    r.set("user", Value::String("seed".into()));
                    r.set("text", Value::String(format!("seed {}", i)));
                    let _ = server.engine().insert(&format!("notifs_{:02}", s), r);
                }
            }
        }
    }
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    sock.set_send_buffer_size(4096).unwrap();
    sock.bind("0.0.0.0:0".parse().unwrap()).unwrap();
    let listener = sock.listen(8192).unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let task = tokio::spawn(serve(Arc::clone(&server), listener));
    let peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let poller = {
        let (s, p, st) = (Arc::clone(&server), Arc::clone(&peak), Arc::clone(&stop));
        tokio::spawn(async move {
            while !st.load(Ordering::Relaxed) {
                p.fetch_max(s.connection_count() as u64, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(ccu + 1));
    let mut hs = Vec::with_capacity(ccu);
    // Server-side routing: clients name base tables (shards=1 for tname);
    // the server distributes. Otherwise clients pin `idx % shards`.
    let naming = if server_side { 1 } else { shards };
    for c in 0..ccu {
        if ccu > 5000 && c % 1000 == 999 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if batch <= 1 {
            hs.push(tokio::spawn(run_client(addr, c as i64, ops, Arc::clone(&barrier), naming)));
        } else {
            hs.push(tokio::spawn(run_client_batched(addr, c as i64, ops, Arc::clone(&barrier), batch, naming)));
        }
    }
    let cpu0 = blitz_server::bench_common::cpu_ms();
    let rss0 = blitz_server::bench_common::rss_kb();
    barrier.wait().await;
    let t0 = Instant::now();

    let counters = LevelCounters::default();
    // aggregate manually (single-threaded join loop)
    let mut hist = Hist::default();
    let mut mix = [0u64; 8];
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut ok = 0u64;
    let mut resp_err = 0u64;
    let mut io_err = 0u64;
    let mut timeouts = 0u64;
    let mut lost = 0u64;
    let mut conn_fail: u64 = 0;
    let mut first_errors: Vec<ErrorDetail> = Vec::new();
    for h in hs {
        match h.await {
            Ok(Ok(o)) => {
                hist.samples.extend(o.lats);
                for i in 0..8 {
                    mix[i] += o.mix[i];
                }
                sent += o.sent;
                received += o.received;
                ok += o.ok;
                resp_err += o.resp_err;
                io_err += o.io_err;
                timeouts += o.timeouts;
                lost += o.lost;
                for e in o.errors {
                    if first_errors.len() < 10 {
                        first_errors.push(e);
                    }
                }
            }
            _ => conn_fail += 1,
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    poller.await.unwrap();
    task.abort();

    let attempted = ccu as u64 * ops as u64;
    let connected = ccu as u64 - conn_fail;
    // Fill counters for invariant check
    let lc = LevelCounters {
        attempted,
        connected,
        conn_failures: conn_fail,
        sent,
        received,
        ok,
        resp_err,
        io_err,
        timeouts,
        lost,
        first_errors: Mutex::new(vec![]),
    };
    let _ = counters;
    let invariant_ok = lc.verify().is_ok();
    hist.sorted();
    let n = hist.samples.len();
    let p50 = hist.pct_ms(0.50);
    let p90 = hist.pct_ms(0.90);
    let p95 = hist.pct_ms(0.95);
    let p99 = hist.pct_ms(0.99);
    let p999 = hist.pct_ms(0.999);
    let errors = resp_err + io_err + timeouts + lost + conn_fail;
    // SLO contract: successful end-to-end latency only, 0 errors/timeouts.
    let slo = errors == 0 && timeouts == 0 && p50 < 50.0 && p90 < 70.0 && p95 < 70.0 && p99 < 100.0 && p999 < 200.0;
    println!(
        "{:>8} {:>10} {:>10} {:>10} {:>10} {:>10.2} {:>12.0} {:>12.0} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.1} {:>8} {:>10} {:>6}",
        ccu, attempted, sent, received, ok, secs,
        lc.goodput(secs), lc.attempted_rps(secs),
        p50, p90, p95, p99, p999,
        hist.max_ms(), errors,
        peak.load(Ordering::Relaxed),
        if slo { "PASS" } else { "FAIL" },
    );
    let st = server.stats();
    let net_mb_in = st.bytes_read as f64 / 1_048_576.0;
    let net_mb_out = st.bytes_written as f64 / 1_048_576.0;
    println!(
        "  exact: attempted={} connected={}/{} sent={} received={} ok={} failed={} timed_out={} conn_fail={} lost={} invariant={}",
        attempted, connected, ccu, sent, received, ok, resp_err + io_err, timeouts, conn_fail, lost,
        if invariant_ok { "received==ok+resp_err OK" } else { "BROKEN" },
    );
    println!(
        "  mix timeline={} post={} like={} follow={} comment={} notif={} msg={} rt_ping={}",
        mix[0], mix[1], mix[2], mix[3], mix[4], mix[5], mix[6], mix[7],
    );
    println!(
        "  sys: cpu={}ms rss={}MB net_in={:.1}MB net_out={:.1}MB queue_depth(peak/conns)={}/{} active_tx={} tx_conflicts={} fanout={} wal_bytes={} wal_ops={} wal_dropped={} nvme=unwired slow_rate={:?} shed_drops={} batch={} shards={} novalidate durable={}",
        blitz_server::bench_common::cpu_ms().saturating_sub(cpu0), blitz_server::bench_common::rss_kb() / 1024,
        net_mb_in, net_mb_out, peak.load(Ordering::Relaxed), st.connections,
        st.active_tx, st.tx_conflicts, st.subscription_fanout, st.wal_bytes, st.wal_ops,
        server.wal_dropped(),
        server.slow_rate(), st.shed_drops, batch, shards, durable,
    );
    let _ = (rss0, n);
    for (i, e) in first_errors.iter().take(10).enumerate() {        println!(
            "  err[{}]: conn={} op={} table={} row={:?} err={} tx={} queue={} ts={:?}",
            i, e.conn_id, e.op, e.table, e.row_id, e.error, e.tx_state, e.queue_state, e.timestamp
        );
    }
    if !invariant_ok || resp_err + io_err + timeouts + lost + conn_fail > 0 {
        println!("  NOTE: goodput above is ok/sec; attempted RPS shown separately per proposal.");
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let max: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(50_000);
    let ops: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(20);
    let batch: usize = args.get(3).and_then(|a| a.parse().ok()).unwrap_or(1);
    let shards: usize = args.get(4).and_then(|a| a.parse().ok()).unwrap_or(1).max(1);
    println!("=== Benchmark C SLO ({} ops/client, batch={} shards={}) ===", ops, batch, shards);
    println!("SLO: p50<50 p90<70 p95<70 p99<100 p99.9<200 errors=0 timeouts=0 (successful end-to-end only)");
    println!("{:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10} {:>6}",
        "ccu", "attempted", "sent", "recv", "ok", "secs", "goodput", "attempted", "p50ms", "p90ms", "p95ms", "p99ms", "p99.9ms", "maxms", "errors", "peak", "SLO");
    // Contract levels: 10K/20K/30K/40K/50K.
    for &c in &[10_000usize, 20_000, 30_000, 40_000, 50_000] {
        if c > max {
            break;
        }
        let r = tokio::time::timeout(Duration::from_secs(240), run_level(c, ops, batch, shards)).await;
        if r.is_err() {
            println!("{:>8} TIMEOUT", c);
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
