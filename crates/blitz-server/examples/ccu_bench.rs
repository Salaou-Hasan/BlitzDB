//! BlitzDB benchmark + honest-CCU harness (run in RELEASE mode).
//!
//! ```sh
//! cargo run --release -p blitz-server --example ccu_bench [engine_n] [max_clients] [tables]
//! ```
//!
//! Phases:
//!   A. Engine throughput + latency (insert/get/update/scan, in-process).
//!   A-parallel. Engine thread scaling across disjoint tables.
//!   A-maplock. Table-map read-lock scaling (no allocs).
//!   B. Component attribution (codec vs engine).
//!   C-local. Mixed workload over tokio tasks, in-process (no TCP).
//!   C. TCP ramp: C concurrent ACTIVE connections, mixed workload.
//!   C-pipe. Pipelined variant (N outstanding reqs/conn).

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_protocol::{BatchRequest, BatchResponse, FrameCodec, Op, Request, Response};
use blitz_server::{serve, BlitzServer};
use blitz_types::id::RowId;
use blitz_types::row::Row;
use blitz_types::value::Value;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------
// Latency histogram (microseconds)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Hist {
    samples: Vec<u64>,
}

impl Hist {
    fn record(&mut self, d: Duration) {
        self.samples.push(d.as_micros() as u64);
    }

    fn summarize(&mut self, label: &str) {
        if self.samples.is_empty() {
            println!("{:>28}: no samples", label);
            return;
        }
        self.samples.sort_unstable();
        let n = self.samples.len();
        let sum: u64 = self.samples.iter().sum();
        let mean = sum as f64 / n as f64;
        let pct = |p: f64| self.samples[((p * n as f64) as usize).min(n - 1)];
        println!(
            "{:>28}: n={} mean={:.1}us p50={}us p99={}us p999={}us max={}us",
            label,
            n,
            mean,
            pct(0.50),
            pct(0.99),
            pct(0.999),
            self.samples[n - 1]
        );
    }
}

// ---------------------------------------------------------------------------
// Phase A: engine bench
// ---------------------------------------------------------------------------

fn user_schema() -> blitz_types::schema::TableSchema {
    user_schema_named("users")
}

fn user_schema_named(name: &str) -> blitz_types::schema::TableSchema {
    use blitz_types::column::{ColumnDef, ColumnType};
    blitz_types::schema::TableSchema::new(name)
        .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
        .with_column(ColumnDef::new("name", ColumnType::String))
        .with_column(ColumnDef::new("email", ColumnType::String))
}

fn make_user(i: i64) -> Row {
    let mut row = Row::new(RowId::new(0));
    row.set("id", Value::Int64(i));
    row.set("name", Value::String(format!("User {}", i)));
    row.set("email", Value::String(format!("user{}@example.com", i)));
    row
}

fn phase_a_engine(n: usize) {
    println!("--- Phase A: engine (in-process, {} rows) ---", n);
    let engine = InMemoryTableEngine::new();
    engine.create_table(user_schema()).unwrap();

    let mut h = Hist::default();
    let start = Instant::now();
    for i in 0..n as i64 {
        let t = Instant::now();
        let id = engine.insert("users", make_user(i)).unwrap();
        black_box(id);
        h.record(t.elapsed());
    }
    let secs = start.elapsed().as_secs_f64();
    println!("insert: {:.0} rows/sec", n as f64 / secs);
    h.summarize("insert latency");

    let mut h = Hist::default();
    let start = Instant::now();
    for i in 1..=n as u64 {
        let t = Instant::now();
        let row = engine.get("users", RowId::new(i)).unwrap().unwrap();
        black_box(row);
        h.record(t.elapsed());
    }
    let secs = start.elapsed().as_secs_f64();
    println!("get: {:.0} rows/sec", n as f64 / secs);
    h.summarize("get latency");

    let mut h = Hist::default();
    let start = Instant::now();
    for i in 1..=n as u64 {
        let mut values = HashMap::new();
        values.insert("name".into(), Value::String("Updated".into()));
        let t = Instant::now();
        let row = engine.update("users", RowId::new(i), values).unwrap();
        black_box(row);
        h.record(t.elapsed());
    }
    let secs = start.elapsed().as_secs_f64();
    println!("update: {:.0} rows/sec", n as f64 / secs);
    h.summarize("update latency");

    let start = Instant::now();
    for _ in 0..5 {
        let rows = engine.scan("users").unwrap();
        black_box(rows.len());
    }
    println!(
        "scan: {:.0} rows/sec (5 full scans)",
        5.0 * n as f64 / start.elapsed().as_secs_f64()
    );
}

fn phase_a_parallel() {
    println!("--- Phase A-parallel: engine thread scaling (50k inserts/thread) ---");
    for threads in [1usize, 4, 12, 24] {
        let engine = Arc::new(InMemoryTableEngine::new());
        let mut tnames = Vec::with_capacity(threads);
        for t in 0..threads {
            let name = format!("pt{}", t);
            engine.create_table(user_schema_named(&name)).unwrap();
            tnames.push(name);
        }
        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..threads {
                let engine = Arc::clone(&engine);
                let tname = tnames[t].clone();
                s.spawn(move || {
                    for i in 0..50_000i64 {
                        black_box(engine.insert(&tname, make_user(i)).unwrap());
                    }
                });
            }
        });
        let secs = start.elapsed().as_secs_f64();
        println!("threads={:>3} aggregate={:.0} rows/sec", threads, threads as f64 * 50_000.0 / secs);
    }
}

fn phase_a_maplock() {
    println!("--- Phase A-maplock: table_exists scaling (map read-lock only, no allocs) ---");
    for threads in [1usize, 4, 12, 24] {
        let engine = Arc::new(InMemoryTableEngine::new());
        engine.create_table(user_schema_named("probe")).unwrap();
        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..threads {
                let engine = Arc::clone(&engine);
                s.spawn(move || {
                    for _ in 0..200_000 {
                        black_box(engine.table_exists("probe"));
                    }
                });
            }
        });
        let secs = start.elapsed().as_secs_f64();
        println!("threads={:>3} aggregate={:.0} checks/sec", threads, threads as f64 * 200_000.0 / secs);
    }
}

// ---------------------------------------------------------------------------
// Phase B: component attribution
// ---------------------------------------------------------------------------

fn phase_b_attribution(iters: usize) {
    println!("--- Phase B: per-op cost attribution ({} iterations) ---", iters);
    let engine = InMemoryTableEngine::new();
    engine.create_table(user_schema()).unwrap();
    let codec = FrameCodec::with_default_limit();

    let mut values = HashMap::new();
    values.insert("id".into(), Value::Int64(1));
    values.insert("name".into(), Value::String("Alice Johnson".into()));
    values.insert("email".into(), Value::String("alice@example.com".into()));
    let req = Request {
        id: 1,
        op: Op::Insert,
        table: "users".into(),
        row_id: None,
        values: Some(values),
    };

    // Codec encode + decode.
    let start = Instant::now();
    for _ in 0..iters {
        let frame = codec.encode_request(&req).unwrap();
        let mut buf = BytesMut::from(frame.as_ref());
        let payload = codec.feed(&mut buf).unwrap().unwrap();
        let back: Request = codec.decode_request(payload).unwrap();
        black_box(back);
    }
    let codec_us = start.elapsed().as_micros() as f64 / iters as f64;

    // Engine insert alone.
    let start = Instant::now();
    for i in 0..iters as i64 {
        black_box(engine.insert("users", make_user(i)).unwrap());
    }
    let engine_us = start.elapsed().as_micros() as f64 / iters as f64;

    println!(
        "codec encode+feed+decode: {:.2}us/op | engine insert: {:.2}us/op",
        codec_us, engine_us
    );
}

// ---------------------------------------------------------------------------
// Phase C: TCP ramp
// ---------------------------------------------------------------------------

struct StepStats {
    clients: usize,
    total_reqs: usize,
    secs: f64,
    errors: u64,
    peak_conns: u64,
    hist: Hist,
    err_detail: String,
}

impl StepStats {
    fn req_per_sec(&self) -> f64 {
        self.total_reqs as f64 / self.secs
    }
}

#[derive(Default)]
struct ClientErrs {
    /// Error responses from the server.
    resp: u64,
    /// Transport failures (first one kills the connection).
    io: u64,
    /// Ops never attempted after a transport failure.
    lost_ops: u64,
    /// First transport error text, for diagnosis.
    first_io_error: Option<String>,
    /// First server error-response text, for diagnosis.
    first_resp_error: Option<String>,
}

struct SimpleRng(u64);

impl SimpleRng {
    fn next(&mut self, bound: usize) -> usize {
        // xorshift64
        let mut x = self.0 | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x as usize) % bound.max(1)
    }
}

/// Build one tuned client connection. Fully fallible: every failure funnels
/// into the caller's barrier-counted error path, never a panic (a panicking
/// task would starve the start barrier and hang the whole step).
async fn connect_client(
    addr: std::net::SocketAddr,
    client_idx: i64,
) -> anyhow::Result<TcpStream> {
    // Spread clients across 8 loopback IPs: each gets its own ~16k ephemeral
    // port range, defeating the single-IP port ceiling for large CCU runs.
    // Small buffers: at bench traffic levels 8KB is plenty and keeps 50k+
    // connections affordable in RAM.
    let sock = tokio::net::TcpSocket::new_v4()?;
    sock.set_recv_buffer_size(4096)?;
    sock.set_send_buffer_size(4096)?;
    let src: std::net::SocketAddr =
        format!("127.0.0.{}:0", 1 + (client_idx % 8)).parse().map_err(|e| anyhow::anyhow!("bad source addr: {}", e))?;
    sock.bind(src)?;
    let stream = sock.connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn run_client(
    addr: std::net::SocketAddr,
    client_idx: i64,
    ops: usize,
    table: String,
    barrier: Arc<tokio::sync::Barrier>,
) -> anyhow::Result<(Hist, ClientErrs)> {
    // Count in at the barrier even when connect fails: otherwise one
    // refused connection would starve the barrier and hang the step.
    let stream = match connect_client(addr, client_idx).await {
        Ok(s) => s,
        Err(e) => {
            barrier.wait().await;
            return Err(e);
        }
    };
    barrier.wait().await;
    let (mut read, mut write) = stream.into_split();
    let codec = FrameCodec::with_default_limit();
    let mut buf = BytesMut::new();
    let mut hist = Hist::default();
    let mut errs = ClientErrs::default();
    let mut rng = SimpleRng(0x9E3779B97F4A7C15u64.wrapping_add(client_idx as u64));
    let mut my_rows: Vec<u64> = Vec::new();

    async fn roundtrip(
        codec: &FrameCodec,
        read: &mut tokio::net::tcp::OwnedReadHalf,
        write: &mut tokio::net::tcp::OwnedWriteHalf,
        buf: &mut BytesMut,
        req: &Request,
    ) -> anyhow::Result<Response> {
        let frame = codec.encode_request(req)?;
        write.write_all(&frame).await?;
        loop {
            if let Some(payload) = codec.feed(buf)? {
                return Ok(codec.decode_response(payload)?);
            }
            let n = read.read_buf(buf).await?;
            if n == 0 {
                anyhow::bail!("server closed connection");
            }
        }
    }

    for i in 0..ops {
        let req = if i % 4 == 0 || my_rows.is_empty() {
            let mut values = HashMap::new();
            values.insert("id".into(), Value::Int64(client_idx * 1_000_000 + i as i64));
            values.insert("name".into(), Value::String("Load".into()));
            values.insert(
                "email".into(),
                Value::String(format!("c{}i{}@bench.local", client_idx, i)),
            );
            Request {
                id: i as u64,
                op: Op::Insert,
                table: table.clone(),
                row_id: None,
                values: Some(values),
            }
        } else {
            let pick = my_rows[rng.next(my_rows.len())];
            if i % 4 == 2 {
                let mut values = HashMap::new();
                values.insert("name".into(), Value::String("Load2".into()));
                Request {
                    id: i as u64,
                    op: Op::Update,
                    table: table.clone(),
                    row_id: Some(pick),
                    values: Some(values),
                }
            } else {
                Request {
                    id: i as u64,
                    op: Op::Get,
                    table: table.clone(),
                    row_id: Some(pick),
                    values: None,
                }
            }
        };

        let t = Instant::now();
        match roundtrip(&codec, &mut read, &mut write, &mut buf, &req).await {
            Ok(resp) => {
                hist.record(t.elapsed());
                if !resp.ok {
                    errs.resp += 1;
                    if errs.first_resp_error.is_none() {
                        errs.first_resp_error = resp.error.clone();
                    }
                    continue;
                } else if req.op == Op::Insert && !resp.rows.is_empty() {
                    my_rows.push(resp.rows[0].id);
                }
            }
            Err(e) => {
                errs.io += 1;
                errs.lost_ops += (ops - i - 1) as u64;
                if errs.first_io_error.is_none() {
                    errs.first_io_error = Some(format!("{:#}", e));
                }
                break;
            }
        }
    }
    Ok((hist, errs))
}

// Pipelined variant: `depth` requests in flight per connection instead of
// strict ping-pong. The server already answers in request order with
// echoed ids, so no server change is needed.
async fn run_client_pipelined(
    addr: std::net::SocketAddr,
    client_idx: i64,
    ops: usize,
    table: String,
    barrier: Arc<tokio::sync::Barrier>,
    depth: usize,
) -> anyhow::Result<(Hist, ClientErrs)> {
    let stream = match connect_client(addr, client_idx).await {
        Ok(s) => s,
        Err(e) => {
            barrier.wait().await;
            return Err(e);
        }
    };
    barrier.wait().await;
    let (mut read, mut write) = stream.into_split();
    let codec = FrameCodec::with_default_limit();
    let mut buf = BytesMut::new();
    let mut hist = Hist::default();
    let mut errs = ClientErrs::default();
    let mut rng = SimpleRng(0x9E3779B97F4A7C15u64.wrapping_add(client_idx as u64));
    let mut my_rows: Vec<u64> = Vec::new();
    let mut next_id = 0u64;
    let mut done = 0usize;
    while done < ops {
        let batch = (ops - done).min(depth);
        let mut sent: Vec<(u64, Instant, bool)> = Vec::with_capacity(batch);
        for _ in 0..batch {
            let i = next_id;
            next_id += 1;
            let is_insert = done % 4 == 0 || my_rows.is_empty();
            let req = if is_insert {
                let mut values = HashMap::new();
                values.insert("id".into(), Value::Int64(client_idx * 1_000_000 + done as i64));
                values.insert("name".into(), Value::String("Load".into()));
                values.insert(
                    "email".into(),
                    Value::String(format!("c{}i{}@bench.local", client_idx, done)),
                );
                Request { id: i, op: Op::Insert, table: table.clone(), row_id: None, values: Some(values) }
            } else {
                let pick = my_rows[rng.next(my_rows.len())];
                if done % 4 == 2 {
                    let mut values = HashMap::new();
                    values.insert("name".into(), Value::String("Load2".into()));
                    Request { id: i, op: Op::Update, table: table.clone(), row_id: Some(pick), values: Some(values) }
                } else {
                    Request { id: i, op: Op::Get, table: table.clone(), row_id: Some(pick), values: None }
                }
            };
            done += 1;
            let frame = codec.encode_request(&req)?;
            let t = Instant::now();
            write.write_all(&frame).await?;
            sent.push((i, t, req.op == Op::Insert));
        }
        let mut outstanding = batch;
        while outstanding > 0 {
            if let Some(payload) = codec.feed(&mut buf)? {
                let resp: Response = codec.decode_response(payload)?;
                outstanding -= 1;
                if let Some(pos) = sent.iter().position(|(id, _, _)| *id == resp.id) {
                    let (_, t, was_insert) = sent.remove(pos);
                    hist.record(t.elapsed());
                    if !resp.ok {
                        errs.resp += 1;
                        if errs.first_resp_error.is_none() {
                            errs.first_resp_error = resp.error.clone();
                        }
                    } else if was_insert && !resp.rows.is_empty() {
                        my_rows.push(resp.rows[0].id);
                    }
                }
            } else {
                let n = read.read_buf(&mut buf).await?;
                if n == 0 {
                    errs.io += 1;
                    errs.lost_ops += (ops - done) as u64;
                    done = ops;
                    outstanding = 0;
                }
            }
        }
    }
    Ok((hist, errs))
}

// Batched variant: `batch_size` ops per frame. Per-op latency is
// approximated as batch_time / batch_len (flatters intra-batch queueing
// slightly, but throughput is exact).
async fn run_client_batched(
    addr: std::net::SocketAddr,
    client_idx: i64,
    ops: usize,
    table: String,
    barrier: Arc<tokio::sync::Barrier>,
    batch_size: usize,
) -> anyhow::Result<(Hist, ClientErrs)> {
    let stream = match connect_client(addr, client_idx).await {
        Ok(s) => s,
        Err(e) => {
            barrier.wait().await;
            return Err(e);
        }
    };
    barrier.wait().await;
    let (mut read, mut write) = stream.into_split();
    let codec = FrameCodec::with_default_limit();
    let mut buf = BytesMut::new();
    let mut hist = Hist::default();
    let mut errs = ClientErrs::default();
    let mut rng = SimpleRng(0x9E3779B97F4A7C15u64.wrapping_add(client_idx as u64));
    let mut my_rows: Vec<u64> = Vec::new();
    let mut next_id = 1u64;
    let mut batch_no = 0u64;
    let mut done = 0usize;
    while done < ops {
        let n = (ops - done).min(batch_size);
        let mut batch_ops = Vec::with_capacity(n);
        let mut insert_positions = Vec::new();
        for _ in 0..n {
            let is_insert = done % 4 == 0 || my_rows.is_empty();
            if is_insert {
                let mut values = HashMap::new();
                values.insert("id".into(), Value::Int64(client_idx * 1_000_000 + done as i64));
                values.insert("name".into(), Value::String("Load".into()));
                values.insert(
                    "email".into(),
                    Value::String(format!("c{}i{}@bench.local", client_idx, done)),
                );
                insert_positions.push(batch_ops.len());
                batch_ops.push(Request { id: next_id, op: Op::Insert, table: table.clone(), row_id: None, values: Some(values) });
            } else {
                let pick = my_rows[rng.next(my_rows.len())];
                if done % 4 == 2 {
                    let mut values = HashMap::new();
                    values.insert("name".into(), Value::String("Load2".into()));
                    batch_ops.push(Request { id: next_id, op: Op::Update, table: table.clone(), row_id: Some(pick), values: Some(values) });
                } else {
                    batch_ops.push(Request { id: next_id, op: Op::Get, table: table.clone(), row_id: Some(pick), values: None });
                }
            }
            next_id += 1;
            done += 1;
        }
        let frame = codec.encode_batch_request(&BatchRequest { id: batch_no, ops: batch_ops })?;
        batch_no += 1;
        let t = Instant::now();
        write.write_all(&frame).await?;
        let bresp = loop {
            if let Some(payload) = codec.feed(&mut buf)? {
                break codec.decode_batch_response(payload)?;
            }
            let r = read.read_buf(&mut buf).await?;
            if r == 0 {
                errs.io += 1;
                errs.lost_ops += (ops - done) as u64;
                done = ops;
                break BatchResponse { id: 0, results: Vec::new() };
            }
        };
        let per_op = t.elapsed() / n as u32;
        for (pos, res) in bresp.results.iter().enumerate() {
            hist.record(per_op);
            if !res.ok {
                errs.resp += 1;
                if errs.first_resp_error.is_none() {
                    errs.first_resp_error = res.error.clone();
                }
            } else if insert_positions.contains(&pos) && !res.rows.is_empty() {
                my_rows.push(res.rows[0].id);
            }
        }
    }
    Ok((hist, errs))
}

async fn run_step(
    clients: usize,
    ops_per_client: usize,
    tables: usize,
    depth: usize,
    client_kind: u8,
) -> StepStats {
    let server = Arc::new(BlitzServer::new());
    server.start().await.unwrap();
    if tables > 1 {
        for t in 0..tables {
            server
                .engine()
                .create_table(user_schema_named(&format!("t{}", t)))
                .unwrap();
        }
    }
    // Tuned listener: big backlog absorbs connect bursts; small buffers keep
    // per-connection kernel memory low (accepted sockets inherit them).
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    sock.set_send_buffer_size(4096).unwrap();
    sock.bind("0.0.0.0:0".parse().unwrap()).unwrap();
    let listener = sock.listen(8192).unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let serve_task = tokio::spawn(serve(Arc::clone(&server), listener));

    // Peak-concurrency poller.
    let peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let poller = {
        let server = Arc::clone(&server);
        let peak = Arc::clone(&peak);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let c = server.connection_count() as u64;
                peak.fetch_max(c, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Start barrier: the timer begins once every client is connected,
    // so the window measures true simultaneity, not connect stagger.
    let barrier = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);
    for c in 0..clients {
        // Stagger massive connect storms: 50k simultaneous SYNs exhaust kernel
        // socket memory in one burst, while the same count accumulated over
        // seconds fits. Early arrivals simply wait at the barrier below.
        if clients > 5000 && c % 1000 == 999 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let barrier = Arc::clone(&barrier);
        let tname = if tables > 1 { format!("t{}", c % tables) } else { "users".to_string() };
        let barrier2 = Arc::clone(&barrier);
        if client_kind == 2 {
            // Batched: `depth` doubles as batch size.
            handles.push(tokio::spawn(run_client_batched(
                addr,
                c as i64,
                ops_per_client,
                tname,
                barrier2,
                depth,
            )));
        } else if depth <= 1 {
            handles.push(tokio::spawn(run_client(addr, c as i64, ops_per_client, tname, barrier2)));
        } else {
            handles.push(tokio::spawn(run_client_pipelined(
                addr,
                c as i64,
                ops_per_client,
                tname,
                barrier2,
                depth,
            )));
        }
    }
    barrier.wait().await;
    let start = Instant::now();
    let mut hist = Hist::default();
    let mut errs = ClientErrs::default();
    let mut connect_errors = 0u64;
    for h in handles {
        match h.await {
            Ok(Ok((mut ch, e))) => {
                hist.samples.extend(ch.samples.drain(..));
                errs.resp += e.resp;
                errs.io += e.io;
                errs.lost_ops += e.lost_ops;
                if errs.first_io_error.is_none() {
                    errs.first_io_error = e.first_io_error;
                }
                if errs.first_resp_error.is_none() {
                    errs.first_resp_error = e.first_resp_error;
                }
            }
            _ => connect_errors += 1,
        }
    }
    let secs = start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    poller.await.unwrap();
    serve_task.abort();

    StepStats {
        clients,
        total_reqs: clients * ops_per_client,
        secs,
        errors: errs.resp + errs.io,
        peak_conns: peak.load(Ordering::Relaxed),
        hist,
        err_detail: format!(
            "resp={} io={} lost_ops={} connect_failures={} first_io_error={:?} first_resp_error={:?}",
            errs.resp, errs.io, errs.lost_ops, connect_errors, errs.first_io_error, errs.first_resp_error
        ),
    }
}

// Same mixed workload as the TCP ramp, but in-process: codec + engine,
// tokio tasks, no sockets. If this scales to millions/sec, the TCP
// ceiling lives in the network stack, not the runtime or engine.
async fn phase_c_local(max_clients: usize) {
    println!("--- Phase C-local: mixed workload, in-process (no TCP) ---");
    println!("{:>8} {:>10} {:>10} {:>12}", "tasks", "reqs", "secs", "req/sec");
    for &tasks in &[100usize, 1000, 4000] {
        if tasks > max_clients {
            break;
        }
        let ops = (200_000 / tasks).clamp(25, 2000);
        let engine = Arc::new(InMemoryTableEngine::new());
        engine.create_table(user_schema()).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(tasks + 1));
        let mut handles = Vec::with_capacity(tasks);
        for c in 0..tasks {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let codec = FrameCodec::with_default_limit();
                let mut buf = BytesMut::new();
                let mut rng = SimpleRng(0x9E3779B97F4A7C15u64.wrapping_add(c as u64));
                let mut my_rows: Vec<u64> = Vec::new();
                barrier.wait().await;
                for i in 0..ops {
                    let req = if i % 4 == 0 || my_rows.is_empty() {
                        let mut values = HashMap::new();
                        values.insert("id".into(), Value::Int64(c as i64 * 1_000_000 + i as i64));
                        values.insert("name".into(), Value::String("Load".into()));
                        values.insert(
                            "email".into(),
                            Value::String(format!("c{}i{}@bench.local", c, i)),
                        );
                        Request { id: i as u64, op: Op::Insert, table: "users".into(), row_id: None, values: Some(values) }
                    } else {
                        let pick = my_rows[rng.next(my_rows.len())];
                        if i % 4 == 2 {
                            let mut values = HashMap::new();
                            values.insert("name".into(), Value::String("Load2".into()));
                            Request { id: i as u64, op: Op::Update, table: "users".into(), row_id: Some(pick), values: Some(values) }
                        } else {
                            Request { id: i as u64, op: Op::Get, table: "users".into(), row_id: Some(pick), values: None }
                        }
                    };
                    let frame = codec.encode_request(&req).unwrap();
                    buf.extend_from_slice(&frame);
                    let payload = codec.feed(&mut buf).unwrap().unwrap();
                    let req: Request = codec.decode_request(payload).unwrap();
                    match req.op {
                        Op::Insert => {
                            let mut row = Row::new(RowId::new(0));
                            for (k, v) in req.values.unwrap() {
                                row.set(k, v);
                            }
                            let assigned = engine.insert(&req.table, row).unwrap();
                            my_rows.push(assigned.as_u64());
                        }
                        Op::Get => {
                            black_box(engine.get_arc(&req.table, RowId::new(req.row_id.unwrap())).unwrap());
                        }
                        Op::Update => {
                            black_box(engine.update(&req.table, RowId::new(req.row_id.unwrap()), req.values.unwrap()).unwrap());
                        }
                        _ => unreachable!(),
                    }
                    let resp = Response::ok(req.id, Vec::new());
                    let out = codec.encode_response(&resp).unwrap();
                    buf.extend_from_slice(&out);
                    let payload = codec.feed(&mut buf).unwrap().unwrap();
                    let _: Response = codec.decode_response(payload).unwrap();
                }
            }));
        }
        barrier.wait().await;
        let start = Instant::now();
        for h in handles {
            h.await.unwrap();
        }
        let secs = start.elapsed().as_secs_f64();
        println!("{:>8} {:>10} {:>10.1} {:>12.0}", tasks, tasks * ops, secs, tasks as f64 * ops as f64 / secs);
    }
}

async fn phase_c_tcp(max_clients: usize, tables: usize) {
    println!("--- Phase C: TCP ramp (mixed insert/get/update workload, tables={}) ---", tables);
    println!(
        "{:>8} {:>10} {:>10} {:>10} {:>12} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "clients", "reqs", "done", "secs", "req/sec", "p50ms", "p99ms", "maxms", "errors", "peak_conns"
    );
    let steps = [1usize, 16, 100, 500, 1000, 2000, 4000, 8000, 16000];
    for &clients in &steps {
        if clients > max_clients {
            break;
        }
        let ops = (200_000 / clients).clamp(25, 2000);
        let step = tokio::time::timeout(Duration::from_secs(180), run_step(clients, ops, tables, 1, 0)).await;
        let mut s = match step {
            Ok(s) => s,
            Err(_) => {
                println!("{:>8} TIMEOUT after 180s", clients);
                break;
            }
        };
        s.hist.samples.sort_unstable();
        let n = s.hist.samples.len();
        let pct = |p: f64| {
            if n == 0 {
                0.0
            } else {
                s.hist.samples[((p * n as f64) as usize).min(n - 1)] as f64 / 1000.0
            }
        };
        let max_ms = if n == 0 {
            0.0
        } else {
            s.hist.samples[n - 1] as f64 / 1000.0
        };
        let rps = s.req_per_sec();
        println!(
            "{:>8} {:>10} {:>10} {:>10.1} {:>12.0} {:>8.2} {:>8.2} {:>8.1} {:>8} {:>10}",
            s.clients,
            s.total_reqs,
            n,
            s.secs,
            rps,
            pct(0.50),
            pct(0.99),
            max_ms,
            s.errors,
            s.peak_conns
        );
        if s.errors > 0 {
            println!("stopping ramp: {} errors at {} clients: {}", s.errors, clients, s.err_detail);
            break;
        }
    }
}

async fn phase_c_pipe() {
    println!("--- Phase C-pipe: pipelined clients (N outstanding reqs/conn) ---");
    println!("{:>8} {:>8} {:>10} {:>10} {:>10} {:>12} {:>8} {:>8} {:>10} {:>10}", "clients", "depth", "reqs", "done", "secs", "req/sec", "p50ms", "p99ms", "errors", "peak");
    for (clients, depth) in [(100usize, 10usize), (100, 50), (1000, 10), (50000, 10)] {
        let ops = (200_000 / clients).clamp(25, 2000);
        let step = tokio::time::timeout(Duration::from_secs(180), run_step(clients, ops, 1, depth, 1)).await;
        let mut s = match step {
            Ok(s) => s,
            Err(_) => {
                println!("PIPELINE TIMEOUT");
                break;
            }
        };
        s.hist.samples.sort_unstable();
        let n = s.hist.samples.len();
        let pct = |p: f64| {
            if n == 0 { 0.0 } else { s.hist.samples[((p * n as f64) as usize).min(n - 1)] as f64 / 1000.0 }
        };
        println!(
            "{:>8} {:>8} {:>10} {:>10} {:>10.1} {:>12.0} {:>8.2} {:>8.2} {:>10} {:>10}",
            s.clients, depth, s.total_reqs, n, s.secs, s.req_per_sec(), pct(0.50), pct(0.99), s.errors, s.peak_conns
        );
        println!("pipe detail: {}", s.err_detail);
        if s.errors > 0 || n != s.total_reqs {
            println!("pipe shortfall: completed {} of {}", n, s.total_reqs);
            break;
        }
    }
}

async fn phase_c_batch() {
    println!("--- Phase C-batch: batched clients (N ops/frame; per-op latency approx batch/n) ---");
    println!("{:>8} {:>8} {:>10} {:>10} {:>10} {:>12} {:>8} {:>8} {:>10} {:>10}", "clients", "batch", "reqs", "done", "secs", "req/sec", "p50ms~", "p99ms~", "errors", "peak");
    for (clients, batch) in [(100usize, 25usize), (100, 100), (1000, 50)] {
        let ops = (200_000 / clients).clamp(200, 2000);
        let step = tokio::time::timeout(Duration::from_secs(180), run_step(clients, ops, 1, batch, 2)).await;
        let mut s = match step {
            Ok(s) => s,
            Err(_) => {
                println!("BATCH TIMEOUT");
                break;
            }
        };
        s.hist.samples.sort_unstable();
        let n = s.hist.samples.len();
        let pct = |p: f64| {
            if n == 0 { 0.0 } else { s.hist.samples[((p * n as f64) as usize).min(n - 1)] as f64 / 1000.0 }
        };
        println!(
            "{:>8} {:>8} {:>10} {:>10} {:>10.1} {:>12.0} {:>8.2} {:>8.2} {:>10} {:>10}",
            s.clients, batch, s.total_reqs, n, s.secs, s.req_per_sec(), pct(0.50), pct(0.99), s.errors, s.peak_conns
        );
        println!("batch detail: {}", s.err_detail);
        if s.errors > 0 || n != s.total_reqs {
            println!("batch shortfall: completed {} of {}", n, s.total_reqs);
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let engine_n: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let max_clients: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(4000);
    let tables: usize = args.get(3).and_then(|a| a.parse().ok()).unwrap_or(1).max(1);

    println!("=== BlitzDB bench (release={}, parallelism={}) ===",
        if cfg!(debug_assertions) { "debug -- NUMBERS NOT REPRESENTATIVE" } else { "release" },
        std::thread::available_parallelism().map(|p| p.get()).unwrap_or(0));

    // Pipe-only mode for focused CCU runs: `ccu_bench 0 0 1 pipe`.
    if args.get(4).map(|a| a == "pipe").unwrap_or(false) {
        phase_c_pipe().await;
        return;
    }

    phase_a_engine(engine_n);
    phase_a_parallel();
    phase_a_maplock();
    phase_b_attribution(50_000);
    phase_c_local(max_clients).await;
    phase_c_tcp(max_clients, tables).await;
    phase_c_pipe().await;
    phase_c_batch().await;
}
