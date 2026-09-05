//! SDK SLO spot check: C clients × ops through the autobatching SDK.
//! Usage: `sdk_slo [clients=1000] [ops=20]`.
//! PASS bar: p50<50ms p99<100ms errors=0 (same contract as bench_c_app).

use blitz_client::{Client, Value};
use blitz_core::TableEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

fn kv(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn main() {
    let threads: usize = std::env::var("TOKIO_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(24);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    let args: Vec<String> = std::env::args().collect();
    let clients: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(1000);
    let ops: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(20);

    let server = Arc::new(blitz_server::BlitzServer::new());
    server.start().await.unwrap();
    // 16 plain tables (no unique columns): spread write locks + indexes so
    // the run measures the wire path, not one table's locks.
    for t in 0..16 {
        use blitz_types::column::{ColumnDef, ColumnType};
        use blitz_types::schema::TableSchema;
        let _ = server.engine().create_table(
            TableSchema::new(format!("t{:02}", t))
                .with_column(ColumnDef::new("id", ColumnType::Int64).nullable())
                .with_column(ColumnDef::new("v", ColumnType::String).nullable()),
        );
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(blitz_server::serve(Arc::clone(&server), listener));

    let barrier = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let mut hs = Vec::with_capacity(clients);
    // SDK_SHARED=N: N clients shared across all tasks (round-robin) — the
    // idiomatic shape. Sharing lets the worker drain DEEP batches (the
    // invisible-batching vision); one-client-per-task can't batch awaited
    // ops and pays a task-hop per op (migration-heavy on many cores).
    let shared: usize = std::env::var("SDK_SHARED").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let pool: Vec<Client> = if shared > 0 {
        let mut v = Vec::with_capacity(shared);
        for _ in 0..shared {
            v.push(Client::connect(addr).await.unwrap());
        }
        v
    } else {
        Vec::new()
    };
    let pool = Arc::new(pool);
    for c in 0..clients {
        let (b, a) = (Arc::clone(&barrier), addr);
        let pool = Arc::clone(&pool);
        hs.push(tokio::spawn(async move {
            let owned;
            let client = if pool.is_empty() {
                owned = Client::connect(a).await.unwrap();
                &owned
            } else {
                &pool[c % pool.len()]
            };
            // Spread across 16 tables (like sharded app tables): a single
            // table's write lock + unique index would serialize everyone
            // and measure locks, not the SDK wire path.
            let table = format!("t{:02}", c % 16);
            let mut lats = Vec::with_capacity(ops);
            let mut ids = Vec::new();
            let mut errors = 0u64;
            // SDK_GETS_ONLY=1: seed untimed, then time pure Gets (no idem
            // write-lock traffic) to isolate read-path scaling.
            let gets_only = std::env::var("SDK_GETS_ONLY").as_deref() == Ok("1");
            if gets_only {
                for i in 0..ops {
                    let row = client.insert(&table, kv(&[
                        ("id", Value::Int64((c * 1000 + i) as i64)),
                        ("v", Value::String("seed".into())),
                    ])).await.unwrap();
                    ids.push(row.id);
                }
            }
            b.wait().await;
            for i in 0..ops {
                let t = Instant::now();
                // Alternate insert / get-back (get-your-own proves id mapping).
                let r = if gets_only {
                    let id = ids[i % ids.len()];
                    client.get(&table, id).await.map(|_| ())
                } else if i % 2 == 0 || ids.is_empty() {
                    client.insert(&table, kv(&[
                        ("id", Value::Int64((c * 1000 + i) as i64)),
                        ("v", Value::String(format!("u{}-{}", c, i))),
                    ])).await.map(|row| {
                        ids.push(row.id);
                    })
                } else {
                    let id = ids[i % ids.len()];
                    client.get(&table, id).await.map(|_| ())
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
