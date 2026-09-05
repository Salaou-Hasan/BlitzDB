//! Hotspot bench: 1% of keys take 50% of traffic (celebrity skew).
//!
//! ```sh
//! cargo run --release -p blitz-server --example bench_hotspot [ccu] [skew_pct]
//! ```
//!
//! Pre-seeds 20K rows. Each op: with prob `skew_pct` pick from the 200 hot
//! rows (1%), else uniform. 50/50 Get/Update. Exact accounting + SLO verdict.
//! Expectation (documented): reads share the table lock so hot Gets look
//! like uniform; hot *writes* still serialize table-wide (per-table RwLock),
//! so skew shows up as fatter p99 vs uniform — row-level striping is the
//! stated next step, not claimed here.

use blitz_server::bench_common::{Hist, RtOutcome, roundtrip_timeout};
use blitz_server::{serve, BlitzServer};

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use blitz_core::TableEngine;
use blitz_protocol::{FrameCodec, Op, Request};
use blitz_types::value::Value;
use bytes::BytesMut;

fn schema() -> blitz_types::schema::TableSchema {
    use blitz_types::column::{ColumnDef, ColumnType};
    blitz_types::schema::TableSchema::new("t")
        .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
        .with_column(ColumnDef::new("v", ColumnType::String).nullable())
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

async fn run_level(ccu: usize, ops: usize, skew: u64) {
    let server = Arc::new(BlitzServer::new());
    server.start().await.unwrap();
    let _ = server.engine().create_table(schema());
    {
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
            let timeout = Duration::from_secs(5);
            let mut rng = Rng(0xBEEFu64.wrapping_add(c as u64));
            let mut lats = Vec::with_capacity(ops);
            let (mut sent, mut recv, mut okc, mut re, mut ioe, mut to, mut lost, mut hot_hits) = (0u64, 0, 0, 0, 0, 0, 0, 0);
            for i in 0..ops {
                let hot = rng.next(100) < skew as usize;
                // RowIds 1..=20000 (auto-assigned from 1). Hot = first 200.
                let pick = if hot {
                    hot_hits += 1;
                    1 + (rng.next(200) as u64)
                } else {
                    1 + (rng.next(20000) as u64)
                };
                let req = if i % 2 == 0 {
                    Request { id: i as u64, op: Op::Get, table: "t".into(), row_id: Some(pick), values: None }
                } else {
                    let mut v = HashMap::new();
                    v.insert("v".into(), Value::String("hot".into()));
                    Request { id: i as u64, op: Op::Update, table: "t".into(), row_id: Some(pick), values: Some(v) }
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
            Ok::<_, anyhow::Error>((lats, sent, recv, okc, re, ioe, to, lost, hot_hits))
        }));
    }
    barrier.wait().await;
    let t0 = Instant::now();
    let mut hist = Hist::default();
    let (mut sent, mut recv, mut okc, mut re, mut ioe, mut to, mut lost, mut cf, mut hot) = (0u64, 0, 0, 0, 0, 0, 0, 0, 0);
    for h in hs {
        match h.await {
            Ok(Ok((mut l, s, r, o, e, io, t, lo, hh))) => {
                hist.samples.extend(l.drain(..));
                sent += s;
                recv += r;
                okc += o;
                re += e;
                ioe += io;
                to += t;
                lost += lo;
                hot += hh;
            }
            _ => cf += 1,
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    poller.await.unwrap();
    task.abort();
    hist.sorted();
    let inv = if recv == okc + re { "OK" } else { "BROKEN" };
    let slo = re + ioe + to + lost + cf == 0 && hist.pct_ms(0.50) < 50.0 && hist.pct_ms(0.99) < 100.0 && hist.pct_ms(0.999) < 200.0;
    println!(
        "hotspot skew={}% ccu={} attempted={} sent={} recv={} ok={} err={} lost={} conn_fail={} hot_share={:.1}% inv={} secs={:.2} goodput={:.0} p50={:.2} p99={:.2} p99.9={:.2} peak={} {}",
        skew, ccu, ccu as u64 * ops as u64, sent, recv, okc, re + ioe + to, lost, cf,
        100.0 * hot as f64 / sent.max(1) as f64, inv, secs, okc as f64 / secs.max(0.001),
        hist.pct_ms(0.50), hist.pct_ms(0.99), hist.pct_ms(0.999), peak.load(Ordering::Relaxed),
        if slo { "PASS" } else { "FAIL" },
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ccu: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(10_000);
    let skew: u64 = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(50);
    let ops = (200_000usize / ccu).clamp(10, 50);
    run_level(ccu, ops, skew).await;
}
