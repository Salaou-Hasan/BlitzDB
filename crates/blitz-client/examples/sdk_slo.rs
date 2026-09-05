//! SDK SLO spot check: C clients × ops through the autobatching SDK.
//! Usage: `sdk_slo [clients=1000] [ops=20]`.
//! PASS bar: p50<50ms p99<100ms errors=0 (same contract as bench_c_app).

use blitz_client::{Client, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

fn kv(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let clients: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(1000);
    let ops: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(20);

    let server = Arc::new(blitz_server::BlitzServer::new());
    server.start().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(blitz_server::serve(Arc::clone(&server), listener));

    let barrier = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let mut hs = Vec::with_capacity(clients);
    for c in 0..clients {
        let (b, a) = (Arc::clone(&barrier), addr);
        hs.push(tokio::spawn(async move {
            let client = Client::connect(a).await.unwrap();
            let mut lats = Vec::with_capacity(ops);
            let mut ids = Vec::new();
            let mut errors = 0u64;
            b.wait().await;
            for i in 0..ops {
                let t = Instant::now();
                // Alternate insert / get-back (get-your-own proves id mapping).
                let r = if i % 2 == 0 || ids.is_empty() {
                    client.insert("users", kv(&[
                        ("id", Value::Int64((c * 1000 + i) as i64)),
                        ("name", Value::String(format!("u{}-{}", c, i))),
                        ("email", Value::String(format!("u{}-{}@x.com", c, i))),
                    ])).await.map(|row| {
                        ids.push(row.id);
                    })
                } else {
                    let id = ids[i % ids.len()];
                    client.get("users", id).await.map(|_| ())
                };
                lats.push(t.elapsed().as_micros() as u64);
                if r.is_err() {
                    errors += 1;
                }
            }
            (lats, errors)
        }));
    }
    barrier.wait().await;
    let t0 = Instant::now();
    let mut all_lats = Vec::new();
    let mut errors = 0u64;
    for h in hs {
        let (lats, e) = h.await.unwrap();
        all_lats.extend(lats);
        errors += e;
    }
    let secs = t0.elapsed().as_secs_f64();
    all_lats.sort_unstable();
    let pct = |p: f64| all_lats[((all_lats.len() as f64 * p) as usize).min(all_lats.len() - 1)] as f64 / 1000.0;
    let goodput = all_lats.len() as f64 / secs;
    let pass = pct(0.5) < 50.0 && pct(0.99) < 100.0 && errors == 0;
    println!(
        "sdk_slo: clients={} ops={} goodput={:.0}/s p50={:.2}ms p99={:.2}ms errors={} {}",
        clients, ops, goodput, pct(0.5), pct(0.99), errors,
        if pass { "PASS" } else { "FAIL" }
    );
}
