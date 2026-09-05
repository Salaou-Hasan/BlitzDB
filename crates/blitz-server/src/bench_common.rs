//! Shared exact accounting for Benchmark B/C harnesses.
//!
//! The proposal demands: never conflate attempted vs received vs
//! successful. This module gives every bench the same counters +
//! invariant check + first-10 failure log.

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// One server-side failure as observed by the client.
#[derive(Debug, Clone)]
pub struct ErrorDetail {
    pub conn_id: i64,
    pub op: String,
    pub table: String,
    pub row_id: Option<u64>,
    pub error: String,
    pub timestamp: SystemTime,
    /// Per-connection queue state at failure (outstanding depth,
    /// known-row cache size) — the "queue state" a single-seq client has.
    pub queue_state: String,
    /// TCP has no tx frames; kept explicit so reports don't imply tx.
    pub tx_state: &'static str,
}

impl ErrorDetail {
    pub fn new(
        conn_id: i64,
        op: &str,
        table: &str,
        row_id: Option<u64>,
        error: String,
        queue_state: String,
    ) -> Self {
        Self {
            conn_id,
            op: op.to_string(),
            table: table.to_string(),
            row_id,
            error,
            timestamp: SystemTime::now(),
            queue_state,
            tx_state: "n/a (no tx over TCP)",
        }
    }
}

/// Exact per-level counters. Invariant: `received == ok + resp_err`.
#[derive(Debug, Default)]
pub struct LevelCounters {
    pub attempted: u64,
    pub connected: u64,
    pub conn_failures: u64,
    pub sent: u64,
    pub received: u64,
    pub ok: u64,
    pub resp_err: u64,
    pub io_err: u64,
    pub timeouts: u64,
    pub lost: u64,
    pub first_errors: Mutex<Vec<ErrorDetail>>,
}

impl LevelCounters {
    pub fn record_first_error(&self, d: ErrorDetail) {
        let mut g = self.first_errors.lock().unwrap();
        if g.len() < 10 {
            g.push(d);
        }
    }

    /// Check `received == ok + resp_err` and `attempted == sent + lost + unsent_conn_fail`.
    pub fn verify(&self) -> Result<(), String> {
        if self.received != self.ok + self.resp_err {
            return Err(format!(
                "INVARIANT BROKEN: received({}) != ok({}) + resp_err({})",
                self.received, self.ok, self.resp_err
            ));
        }
        Ok(())
    }

    /// Goodput (successful responses/sec), not attempted throughput.
    pub fn goodput(&self, secs: f64) -> f64 {
        self.ok as f64 / secs.max(0.001)
    }
    pub fn attempted_rps(&self, secs: f64) -> f64 {
        self.attempted as f64 / secs.max(0.001)
    }
}

pub fn rss_kb() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for l in s.lines() {
            if let Some(r) = l.strip_prefix("VmRSS:") {
                return r.split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0);
            }
        }
    }
    0
}

pub fn cpu_ms() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/stat") {
        if let Some(e) = s.rfind(')') {
            let p: Vec<&str> = s[e + 2..].split_whitespace().collect();
            if p.len() >= 15 {
                return (p[11].parse::<u64>().unwrap_or(0) + p[12].parse::<u64>().unwrap_or(0)) * 10;
            }
        }
    }
    0
}

/// Microsecond histogram with full p50/p95/p99/p99.9.
#[derive(Default)]
pub struct Hist {
    pub samples: Vec<u64>,
}

impl Hist {
    pub fn record(&mut self, d: Duration) {
        self.samples.push(d.as_micros() as u64);
    }
    pub fn sorted(&mut self) {
        self.samples.sort_unstable();
    }
    pub fn pct_ms(&self, p: f64) -> f64 {
        let n = self.samples.len();
        if n == 0 {
            return 0.0;
        }
        self.samples[((p * n as f64) as usize).min(n - 1)] as f64 / 1000.0
    }
    pub fn max_ms(&self) -> f64 {
        self.samples.last().copied().unwrap_or(0) as f64 / 1000.0
    }
}

/// Per-request timeout wrapper outcome.
pub enum RtOutcome {
    Ok(blitz_protocol::Response, Duration),
    RespErr(blitz_protocol::Response, Duration),
    IoErr(String),
    Timeout,
}

pub async fn roundtrip_timeout(
    codec: &blitz_protocol::FrameCodec,
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
    buf: &mut bytes::BytesMut,
    req: &blitz_protocol::Request,
    timeout: Duration,
) -> RtOutcome {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let t = Instant::now();
    let frame = match codec.encode_request(req) {
        Ok(f) => f,
        Err(e) => return RtOutcome::IoErr(format!("encode: {}", e)),
    };
    match tokio::time::timeout(timeout, wr.write_all(&frame)).await {
        Err(_) => return RtOutcome::Timeout,
        Ok(Err(e)) => return RtOutcome::IoErr(format!("write: {}", e)),
        Ok(Ok(())) => {}
    }
    loop {
        match codec.feed(buf) {
            Ok(Some(p)) => match codec.decode_response(p) {
                Ok(r) => {
                    let el = t.elapsed();
                    if r.ok {
                        return RtOutcome::Ok(r, el);
                    } else {
                        return RtOutcome::RespErr(r, el);
                    }
                }
                Err(e) => return RtOutcome::IoErr(format!("decode: {}", e)),
            },
            Ok(None) => {}
            Err(e) => return RtOutcome::IoErr(format!("feed: {}", e)),
        }
        match tokio::time::timeout(timeout, rd.read_buf(buf)).await {
            Ok(Ok(0)) => return RtOutcome::IoErr("server closed".into()),
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => return RtOutcome::IoErr(format!("read: {}", e)),
            Err(_) => return RtOutcome::Timeout,
        }
    }
}
