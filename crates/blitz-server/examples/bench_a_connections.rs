//! Benchmark A — Connection scalability (idle CCU capability).
//!
//! Ignores application work. Measures how many *idle* connections one
//! BlitzDB node can hold on this hardware:
//!
//! ```sh
//! cargo run --release -p blitz-server --example bench_a_connections [max_ccu]
//! # e.g. cargo run --release -p blitz-server --example bench_a_connections 50000
//! ```
//!
//! Levels: 10K, 25K, 50K, 100K, 250K, 500K, 1M (stops early on error/OOM).
//!
//! Per level reports:
//!   - connection setup rate (conns/sec)
//!   - RSS total + per-connection overhead
//!   - idle overhead (RSS growth while holding, 3s window)
//!   - heartbeat cost (Ping p50/p99 over a sample, plus full-sweep time)
//!   - peak server-side connection count
//!
//! Design notes (what makes 50K+ fit):
//!   - tuned listener (backlog 8192, 4 KiB sock buffers; accepted sockets inherit)
//!   - client sockets also 4 KiB buffers + multi-IP bind (8 loopback IPs)
//!   - staggered connects (200ms / 1K conns past 5K) to avoid SYN-burst OOM
//!   - heartbeats sampled (min(N,2000)) so the probe itself doesn't OOM

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use blitz_protocol::{FrameCodec, Request, Response};
use blitz_server::{serve, BlitzServer};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn rss_kb() -> u64 {
    // VmRSS from /proc/self/status (Linux). Returns 0 elsewhere.
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
            }
        }
    }
    0
}

fn cpu_ms() -> u64 {
    // utime+stime in ms from /proc/self/stat (fields 14+15, USER_HZ=100).
    if let Ok(s) = std::fs::read_to_string("/proc/self/stat") {
        if let Some(end) = s.rfind(')') {
            let parts: Vec<&str> = s[end + 2..].split_whitespace().collect();
            if parts.len() >= 15 {
                let u: u64 = parts[11].parse().unwrap_or(0);
                let st: u64 = parts[12].parse().unwrap_or(0);
                return (u + st) * 10;
            }
        }
    }
    0
}

async fn connect_client(
    addr: std::net::SocketAddr,
    idx: usize,
) -> anyhow::Result<TcpStream> {
    let sock = tokio::net::TcpSocket::new_v4()?;
    sock.set_recv_buffer_size(4096)?;
    sock.set_send_buffer_size(4096)?;
    let src: std::net::SocketAddr = format!("127.0.0.{}:0", 1 + (idx % 8))
        .parse()
        .map_err(|e| anyhow::anyhow!("bad src addr: {}", e))?;
    sock.bind(src)?;
    let s = sock.connect(addr).await?;
    s.set_nodelay(true)?;
    Ok(s)
}

async fn ping_once(stream: &mut TcpStream, id: u64) -> anyhow::Result<Duration> {
    let codec = FrameCodec::with_default_limit();
    let (mut r, mut w) = stream.split();
    let frame = codec.encode_request(&Request::ping(id))?;
    let t = Instant::now();
    w.write_all(&frame).await?;
    let mut buf = BytesMut::new();
    loop {
        if let Some(p) = codec.feed(&mut buf)? {
            let resp: Response = codec.decode_response(p)?;
            debug_assert!(resp.ok);
            return Ok(t.elapsed());
        }
        let n = r.read_buf(&mut buf).await?;
        if n == 0 {
            anyhow::bail!("closed during heartbeat");
        }
    }
}

async fn run_level(target: usize) -> bool {
    println!(
        "\n=== A: {} idle connections ===",
        target
    );
    let server = Arc::new(BlitzServer::new());
    server.start().await.unwrap();

    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    sock.set_send_buffer_size(4096).unwrap();
    sock.bind("0.0.0.0:0".parse().unwrap()).unwrap();
    let listener = sock.listen(8192).unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let serve_task = tokio::spawn(serve(Arc::clone(&server), listener));

    let peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let poller = {
        let s = Arc::clone(&server);
        let p = Arc::clone(&peak);
        let st = Arc::clone(&stop);
        tokio::spawn(async move {
            while !st.load(Ordering::Relaxed) {
                p.fetch_max(s.connection_count() as u64, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let rss_before = rss_kb();
    let cpu_before = cpu_ms();

    // --- setup phase: staggered connects ---
    let t0 = Instant::now();
    let mut clients: Vec<TcpStream> = Vec::with_capacity(target.min(1_000_000));
    let mut failed = 0usize;
    for i in 0..target {
        if target > 5000 && i % 1000 == 999 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        match connect_client(addr, i).await {
            Ok(s) => clients.push(s),
            Err(e) => {
                failed += 1;
                if failed <= 3 {
                    eprintln!("connect #{} failed: {:#}", i, e);
                }
                if failed > target / 20 + 100 {
                    eprintln!("too many connect failures, aborting level");
                    break;
                }
            }
        }
        // let the acceptor catch up every 5k
        if i % 5000 == 4999 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let setup_secs = t0.elapsed().as_secs_f64();
    // wait for server-side count to converge
    for _ in 0..200 {
        if server.connection_count() + failed >= clients.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let setup_rate = clients.len() as f64 / setup_secs.max(0.001);
    let rss_after_connect = rss_kb();
    let per_conn_kb = if clients.is_empty() {
        0.0
    } else {
        (rss_after_connect.saturating_sub(rss_before)) as f64 / clients.len() as f64
    };

    // --- idle overhead: hold 3s, measure RSS drift + server count stability ---
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rss_idle = rss_kb();
    let idle_drift_kb = rss_idle.saturating_sub(rss_after_connect) as i64;

    // --- heartbeat: sample up to 2000 conns with Ping ---
    let sample = clients.len().min(2000);
    let mut lats: Vec<u64> = Vec::with_capacity(sample);
    let mut hb_err = 0usize;
    let hb_t0 = Instant::now();
    for (k, c) in clients.iter_mut().take(sample).enumerate() {
        match ping_once(c, k as u64).await {
            Ok(d) => lats.push(d.as_micros() as u64),
            Err(_) => hb_err += 1,
        }
    }
    let hb_secs = hb_t0.elapsed().as_secs_f64();
    lats.sort_unstable();
    let pct = |p: f64| {
        if lats.is_empty() {
            0
        } else {
            lats[((p * lats.len() as f64) as usize).min(lats.len() - 1)]
        }
    };
    let cpu_after = cpu_ms();
    let cpu_used_ms = cpu_after.saturating_sub(cpu_before);

    println!(
        "connected={}/{} failed={} setup={:.0} conns/sec ({:.1}s)",
        clients.len(),
        target,
        failed,
        setup_rate,
        setup_secs
    );
    println!(
        "rss_before={}MB rss_after={}MB per_conn={:.1}KB idle_drift_3s={}KB peak_server_conns={}",
        rss_before / 1024,
        rss_after_connect / 1024,
        per_conn_kb,
        idle_drift_kb,
        peak.load(Ordering::Relaxed)
    );
    println!(
        "heartbeat sample={} p50={}us p99={}us max={}us errors={} sweep={:.1}s",
        lats.len(),
        pct(0.50),
        pct(0.99),
        lats.last().copied().unwrap_or(0),
        hb_err,
        hb_secs
    );
    println!(
        "cpu_used={}ms wall={:.1}s est_net_idle≈{:.1}KB (4K+4K sock bufs, mostly uncommitted)",
        cpu_used_ms,
        setup_secs + 3.0 + hb_secs,
        per_conn_kb * 8.0 / 8.0
    );

    stop.store(true, Ordering::Relaxed);
    poller.await.unwrap();
    serve_task.abort();
    let n_connected = clients.len();
    drop(clients);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let ok = failed == 0 && hb_err == 0 && n_connected == target;
    if !ok {
        println!("LEVEL {} INCOMPLETE (connected {}, hb_err {})", target, n_connected, hb_err);
    } else {
        println!("LEVEL {} OK", target);
    }
    ok
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let max: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(50_000);
    println!("=== Benchmark A: connection scalability (release={}) ===",
        if cfg!(debug_assertions) { "debug -- NOT REPRESENTATIVE" } else { "release" });
    println!("hardware: {:?} cores, rss_now={}MB",
        std::thread::available_parallelism().map(|p| p.get()).unwrap_or(0),
        rss_kb() / 1024);

    let levels = [10_000usize, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000];
    for &l in &levels {
        if l > max {
            break;
        }
        let ok = run_level(l).await;
        if !ok {
            println!("stopping A ramp: level {} did not complete cleanly", l);
            break;
        }
        // breathing room between levels so TIME_WAIT/ports recycle
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
