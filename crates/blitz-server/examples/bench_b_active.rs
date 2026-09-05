//! Benchmark B — Active request scalability.
//!
//! ```sh
//! cargo run --release -p blitz-server --example bench_b_active [max_clients] [tables]
//! # e.g. cargo run --release -p blitz-server --example bench_b_active 32000 1
//! ```
//!
//! Levels: 100, 500, 1K, 2K, 4K, 8K, 16K, 32K, 64K (stops at max_clients).
//! Workload per op (matches ccu_bench): 25% insert / 25% update / 50% get.
//!
//! Reports per level: RPS, p50/p95/p99/p99.9, CPU, RAM, errors.
//!
//! Tail-latency strategy under test: table sharding (`tables` arg spreads
//! clients over N tables so per-table write locks don't convoy) + batch
//! framing available in ccu_bench pipe/batch phases. Run twice:
//! `bench_b_active 16000 1` (single-table, worst case) vs
//! `bench_b_active 16000 16` (sharded, production-like).

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use blitz_core::{InMemoryTableEngine, TableEngine};
use blitz_protocol::{FrameCodec, Op, Request, Response};
use blitz_server::{serve, BlitzServer};
use blitz_types::value::Value;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn rss_kb() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest.split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0);
            }
        }
    }
    0
}
fn cpu_ms() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/stat") {
        if let Some(end) = s.rfind(')') {
            let p: Vec<&str> = s[end + 2..].split_whitespace().collect();
            if p.len() >= 15 {
                return (p[11].parse::<u64>().unwrap_or(0) + p[12].parse::<u64>().unwrap_or(0)) * 10;
            }
        }
    }
    0
}

fn user_schema_named(name: &str) -> blitz_types::schema::TableSchema {
    use blitz_types::column::{ColumnDef, ColumnType};
    blitz_types::schema::TableSchema::new(name)
        .with_column(ColumnDef::new("id", ColumnType::Int64).primary_key())
        .with_column(ColumnDef::new("name", ColumnType::String))
        .with_column(ColumnDef::new("email", ColumnType::String))
}

struct SimpleRng(u64);
impl SimpleRng {
    fn next(&mut self, bound: usize) -> usize {
        let mut x = self.0 | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x as usize) % bound.max(1)
    }
}

async fn connect_client(addr: std::net::SocketAddr, idx: i64) -> anyhow::Result<TcpStream> {
    let sock = tokio::net::TcpSocket::new_v4()?;
    sock.set_recv_buffer_size(4096)?;
    sock.set_send_buffer_size(4096)?;
    let src: std::net::SocketAddr = format!("127.0.0.{}:0", 1 + (idx % 8))
        .parse()
        .map_err(|e| anyhow::anyhow!("bad src: {}", e))?;
    sock.bind(src)?;
    let s = sock.connect(addr).await?;
    s.set_nodelay(true)?;
    Ok(s)
}

async fn run_client(
    addr: std::net::SocketAddr,
    idx: i64,
    ops: usize,
    table: String,
    barrier: Arc<tokio::sync::Barrier>,
) -> anyhow::Result<(Vec<u64>, u64, u64)> {
    let stream = match connect_client(addr, idx).await {
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
    let mut lats = Vec::with_capacity(ops);
    let mut err_resp = 0u64;
    let mut rng = SimpleRng(0x9E3779B97F4A7C15u64.wrapping_add(idx as u64));
    let mut my_rows: Vec<u64> = Vec::new();

    async fn rt(
        codec: &FrameCodec,
        rd: &mut tokio::net::tcp::OwnedReadHalf,
        wr: &mut tokio::net::tcp::OwnedWriteHalf,
        buf: &mut BytesMut,
        req: &Request,
    ) -> anyhow::Result<Response> {
        let f = codec.encode_request(req)?;
        wr.write_all(&f).await?;
        loop {
            if let Some(p) = codec.feed(buf)? {
                return Ok(codec.decode_response(p)?);
            }
            if rd.read_buf(buf).await? == 0 {
                anyhow::bail!("closed");
            }
        }
    }

    for i in 0..ops {
        let req = if i % 4 == 0 || my_rows.is_empty() {
            let mut v = HashMap::new();
            v.insert("id".into(), Value::Int64(idx * 1_000_000 + i as i64));
            v.insert("name".into(), Value::String("Load".into()));
            v.insert("email".into(), Value::String(format!("c{}i{}@bench.local", idx, i)));
            Request { id: i as u64, op: Op::Insert, table: table.clone(), row_id: None, values: Some(v) }
        } else {
            let pick = my_rows[rng.next(my_rows.len())];
            if i % 4 == 2 {
                let mut v = HashMap::new();
                v.insert("name".into(), Value::String("Load2".into()));
                Request { id: i as u64, op: Op::Update, table: table.clone(), row_id: Some(pick), values: Some(v) }
            } else {
                Request { id: i as u64, op: Op::Get, table: table.clone(), row_id: Some(pick), values: None }
            }
        };
        let t = Instant::now();
        match rt(&codec, &mut rd, &mut wr, &mut buf, &req).await {
            Ok(r) => {
                lats.push(t.elapsed().as_micros() as u64);
                if !r.ok {
                    err_resp += 1;
                } else if req.op == Op::Insert && !r.rows.is_empty() {
                    my_rows.push(r.rows[0].id);
                }
            }
            Err(_) => {
                // transport failure: remaining ops lost
                return Ok((lats, err_resp, (ops - i) as u64));
            }
        }
    }
    Ok((lats, err_resp, 0))
}

async fn run_level(clients: usize, ops: usize, tables: usize) {
    let server = Arc::new(BlitzServer::new());
    server.start().await.unwrap();
    if tables > 1 {
        for t in 0..tables {
            server.engine().create_table(user_schema_named(&format!("t{}", t))).unwrap();
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

    let barrier = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let mut hs = Vec::with_capacity(clients);
    for c in 0..clients {
        if clients > 5000 && c % 1000 == 999 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let tname = if tables > 1 { format!("t{}", c % tables) } else { "users".to_string() };
        hs.push(tokio::spawn(run_client(addr, c as i64, ops, tname, Arc::clone(&barrier))));
    }
    let rss0 = rss_kb();
    let cpu0 = cpu_ms();
    barrier.wait().await;
    let t0 = Instant::now();
    let mut all: Vec<u64> = Vec::new();
    let mut err_resp = 0u64;
    let mut lost = 0u64;
    let mut conn_fail = 0u64;
    for h in hs {
        match h.await {
            Ok(Ok((mut l, e, lo))) => {
                all.append(&mut l);
                err_resp += e;
                lost += lo;
            }
            _ => conn_fail += 1,
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    poller.await.unwrap();
    task.abort();
    all.sort_unstable();
    let n = all.len();
    let pct = |p: f64| if n == 0 { 0.0 } else { all[((p * n as f64) as usize).min(n - 1)] as f64 / 1000.0 };
    let rps = n as f64 / secs.max(0.001);
    let rss1 = rss_kb();
    let cpu1 = cpu_ms();
    println!(
        "{:>8} {:>10} {:>10} {:>10.2} {:>12.0} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.1} {:>8} {:>10} {:>10}",
        clients,
        clients * ops,
        n,
        secs,
        rps,
        pct(0.50),
        pct(0.95),
        pct(0.99),
        pct(0.999),
        if n == 0 { 0.0 } else { all[n - 1] as f64 / 1000.0 },
        err_resp + lost + conn_fail,
        peak.load(Ordering::Relaxed),
        format!("{}MB", rss1 / 1024),
    );
    let _ = (rss0, cpu0, cpu1);
    if err_resp + lost + conn_fail > 0 {
        println!("  detail: resp_err={} lost={} conn_fail={} cpu_used={}ms rss_delta={}MB",
            err_resp, lost, conn_fail, cpu1.saturating_sub(cpu0),
            (rss1 as i64 - rss0 as i64) / 1024);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let max: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(16_000);
    let tables: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(1).max(1);
    // Touch engine import so release builds keep it linked for profiling.
    let _ = InMemoryTableEngine::new();
    println!("=== Benchmark B: active request scalability (tables={}) ===", tables);
    println!("{:>8} {:>10} {:>10} {:>10} {:>12} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10} {:>10}",
        "clients", "reqs", "done", "secs", "req/sec", "p50ms", "p95ms", "p99ms", "p99.9ms", "maxms", "errors", "peak", "rss");
    for &c in &[100usize, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000] {
        if c > max {
            break;
        }
        let ops = (200_000usize / c).clamp(25, 2000);
        let r = tokio::time::timeout(Duration::from_secs(180), run_level(c, ops, tables)).await;
        if r.is_err() {
            println!("{:>8} TIMEOUT", c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
