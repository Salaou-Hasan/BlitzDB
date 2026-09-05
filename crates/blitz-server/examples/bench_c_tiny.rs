//! Tiny-workload + sequential verification for Benchmark C proposal.
//!
//! ```sh
//! cargo run --release -p blitz-server --example bench_c_tiny [max_ccu]
//! ```
//!
//! Part 1 — sequential check on ONE connection: 1, 10, 20, 100 sequential
//! requests must all succeed (proves no "1 success per conn" regression).
//! Part 2 — tiny workloads at scale (no realtime/notifications/cross-logic):
//! GET-only, INSERT-only, UPDATE-only at 1K/10K/50K with exact accounting.

#[allow(dead_code)]
fn _unused() {}

use blitz_server::bench_common::{roundtrip_timeout, Hist, RtOutcome};

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use blitz_core::TableEngine;
use blitz_protocol::{FrameCodec, Op, Request};
use blitz_server::{serve, BlitzServer};
use blitz_types::value::Value;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn tiny_schema() -> blitz_types::schema::TableSchema {
    use blitz_types::column::{ColumnDef, ColumnType};
    blitz_types::schema::TableSchema::new("t")
        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
        .with_column(ColumnDef::new("v", ColumnType::String).nullable())
}

async fn roundtrip(
    codec: &FrameCodec,
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
    buf: &mut BytesMut,
    req: &Request,
) -> anyhow::Result<blitz_protocol::Response> {
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

async fn sequential_check(addr: std::net::SocketAddr) {
    println!("--- sequential check: 1/10/20/100 reqs on one connection ---");
    for &n in &[1usize, 10, 20, 100] {
        let s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        let (mut rd, mut wr) = s.into_split();
        let codec = FrameCodec::with_default_limit();
        let mut buf = BytesMut::new();
        // insert one row first
        let mut v = HashMap::new();
        v.insert("id".into(), Value::Int64(1));
        v.insert("v".into(), Value::String("x".into()));
        let r = roundtrip(&codec, &mut rd, &mut wr, &mut buf,
            &Request { id: 0, op: Op::Insert, table: "t".into(), row_id: None, values: Some(v) }).await.unwrap();
        assert!(r.ok, "seed insert failed: {:?}", r.error);
        let row = r.rows[0].id;
        let mut ok = 0;
        for i in 0..n {
            let r = roundtrip(&codec, &mut rd, &mut wr, &mut buf,
                &Request { id: i as u64 + 1, op: Op::Get, table: "t".into(), row_id: Some(row), values: None }).await.unwrap();
            if r.ok {
                ok += 1;
            }
        }
        println!("  n={:>3}: {}/{} sequential Gets ok {}", n, ok, n, if ok == n { "PASS" } else { "FAIL" });
        assert_eq!(ok, n, "sequential check failed at n={}", n);
    }
}

#[derive(Clone, Copy)]
enum TinyMode {
    Get,
    Insert,
    Update,
}

async fn run_tiny_level(mode: TinyMode, ccu: usize, ops: usize) {
    let label = match mode {
        TinyMode::Get => "GET-only",
        TinyMode::Insert => "INSERT-only",
        TinyMode::Update => "UPDATE-only",
    };
    let server = Arc::new(BlitzServer::new());
    server.start().await.unwrap();
    let _ = server.engine().create_table(tiny_schema());
    // Pre-seed 20K rows for GET/UPDATE so reads always hit.
    if matches!(mode, TinyMode::Get | TinyMode::Update) {
        use blitz_types::{id::RowId, row::Row};
        for i in 0..20_000i64 {
            let mut r = Row::new(RowId::new(0));
            r.set("id", Value::Int64(i));
            r.set("v", Value::String("seed".into()));
            let _ = server.engine().insert("t", r);
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

    // Sequential sanity on this very server before the storm.
    sequential_check(addr).await;

    let barrier = Arc::new(tokio::sync::Barrier::new(ccu + 1));
    let mut hs = Vec::with_capacity(ccu);
    for c in 0..ccu {
        if ccu > 5000 && c % 1000 == 999 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let b = Arc::clone(&barrier);
        hs.push(tokio::spawn(async move {
            let s = tokio::net::TcpSocket::new_v4().unwrap();
            s.set_recv_buffer_size(4096).unwrap();
            s.set_send_buffer_size(4096).unwrap();
            let src: std::net::SocketAddr = format!("127.0.0.{}:0", 1 + (c % 8)).parse().unwrap();
            if s.bind(src).is_err() {
                b.wait().await;
                return Err(anyhow::anyhow!("bind"));
            }
            let st = match s.connect(addr).await {
                Ok(s) => s,
                Err(e) => {
                    b.wait().await;
                    return Err(e.into());
                }
            };
            let _ = st.set_nodelay(true);
            b.wait().await;
            let (mut rd, mut wr) = st.into_split();
            let codec = FrameCodec::with_default_limit();
            let mut buf = BytesMut::new();
            let mut lats = Vec::with_capacity(ops);
            let (mut sent, mut recv, mut okc, mut re, mut ioe, mut to, mut lost) = (0u64, 0, 0, 0, 0, 0, 0);
            let timeout = Duration::from_secs(5);
            // For UPDATE-only, each client first inserts its own rows then updates them.
            let mut my_rows: Vec<u64> = Vec::new();
            if matches!(mode, TinyMode::Update) {
                for i in 0..8 {
                    let mut v = HashMap::new();
                    v.insert("id".into(), Value::Int64(c as i64 * 1000 + i as i64));
                    v.insert("v".into(), Value::String("u".into()));
                    let req = Request { id: i as u64, op: Op::Insert, table: "t".into(), row_id: None, values: Some(v) };
                    sent += 1;
                    match roundtrip_timeout(&codec, &mut rd, &mut wr, &mut buf, &req, timeout).await {
                        RtOutcome::Ok(r, el) => {
                            recv += 1;
                            okc += 1;
                            lats.push(el.as_micros() as u64);
                            if !r.rows.is_empty() {
                                my_rows.push(r.rows[0].id);
                            }
                        }
                        RtOutcome::RespErr(_, el) => {
                            recv += 1;
                            re += 1;
                            lats.push(el.as_micros() as u64);
                        }
                        RtOutcome::IoErr(_) => {
                            ioe += 1;
                            lost += (ops - i - 1) as u64;
                            break;
                        }
                        RtOutcome::Timeout => {
                            to += 1;
                            lost += (ops - i - 1) as u64;
                            break;
                        }
                    }
                }
            }
            for i in 0..ops {
                let req = match mode {
                    TinyMode::Get => {
                        // deterministic hit: row 1..20000 exists
                        let pick = 1 + ((c * ops + i) % 20000) as u64;
                        Request { id: i as u64, op: Op::Get, table: "t".into(), row_id: Some(pick), values: None }
                    }
                    TinyMode::Insert => {
                        let mut v = HashMap::new();
                        v.insert("id".into(), Value::Int64((c * ops + i) as i64));
                        v.insert("v".into(), Value::String("x".into()));
                        Request { id: i as u64, op: Op::Insert, table: "t".into(), row_id: None, values: Some(v) }
                    }
                    TinyMode::Update => {
                        if my_rows.is_empty() {
                            let mut v = HashMap::new();
                            v.insert("id".into(), Value::Int64(0));
                            v.insert("v".into(), Value::String("x".into()));
                            Request { id: i as u64, op: Op::Insert, table: "t".into(), row_id: None, values: Some(v) }
                        } else {
                            let pick = my_rows[i % my_rows.len()];
                            let mut v = HashMap::new();
                            v.insert("v".into(), Value::String("y".into()));
                            Request { id: i as u64, op: Op::Update, table: "t".into(), row_id: Some(pick), values: Some(v) }
                        }
                    }
                };
                sent += 1;
                match roundtrip_timeout(&codec, &mut rd, &mut wr, &mut buf, &req, timeout).await {
                    RtOutcome::Ok(_, el) => {
                        recv += 1;
                        okc += 1;
                        lats.push(el.as_micros() as u64);
                    }
                    RtOutcome::RespErr(_, el) => {
                        recv += 1;
                        re += 1;
                        lats.push(el.as_micros() as u64);
                    }
                    RtOutcome::IoErr(_) => {
                        ioe += 1;
                        lost += (ops - i - 1) as u64;
                        break;
                    }
                    RtOutcome::Timeout => {
                        to += 1;
                        lost += (ops - i - 1) as u64;
                        break;
                    }
                }
            }
            Ok::<_, anyhow::Error>((lats, sent, recv, okc, re, ioe, to, lost))
        }));
    }
    barrier.wait().await;
    let t0 = Instant::now();
    let mut hist = Hist::default();
    let (mut sent, mut recv, mut okc, mut re, mut ioe, mut to, mut lost, mut cf) = (0u64, 0, 0, 0, 0, 0, 0, 0);
    for h in hs {
        match h.await {
            Ok(Ok((mut l, s, r, o, e, io, t, lo))) => {
                hist.samples.extend(l.drain(..));
                sent += s;
                recv += r;
                okc += o;
                re += e;
                ioe += io;
                to += t;
                lost += lo;
            }
            _ => cf += 1,
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    poller.await.unwrap();
    task.abort();
    hist.sorted();
    let attempted = ccu as u64 * ops as u64 + if matches!(mode, TinyMode::Update) { ccu as u64 * 8 } else { 0 };
    let inv = if recv == okc + re { "OK" } else { "BROKEN" };
    println!(
        "{:>12} ccu={:>6} attempted={:>8} sent={:>8} recv={:>8} ok={:>8} resp_err={} io={} to={} lost={} conn_fail={} inv={} secs={:>6.2} goodput={:>9.0} p50={:>6.2}ms p99={:>6.2}ms peak={}",
        label, ccu, attempted, sent, recv, okc, re, ioe, to, lost, cf, inv, secs,
        okc as f64 / secs.max(0.001), hist.pct_ms(0.50), hist.pct_ms(0.99), peak.load(Ordering::Relaxed)
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let max: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(50_000);
    println!("=== Tiny workloads + sequential verification ===");
    for &ccu in &[1_000usize, 10_000, 50_000] {
        if ccu > max {
            break;
        }
        let ops = (200_000usize / ccu).clamp(10, 50);
        run_tiny_level(TinyMode::Get, ccu, ops).await;
        run_tiny_level(TinyMode::Insert, ccu, ops).await;
        run_tiny_level(TinyMode::Update, ccu, ops).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
