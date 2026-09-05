// Simple hash index throughput benchmark
use std::time::Instant;
use blitz_index::HashIndex;
use blitz_types::value::Value;

fn main() {
    let mut index = HashIndex::new();
    let n = 100_000u64;

    // Insert benchmark
    let start = Instant::now();
    for i in 0..n {
        index.insert(i, Value::Int64(i as i64));
    }
    let insert_time = start.elapsed();
    let insert_ops = n as f64 / insert_time.as_secs_f64();
    println!("HashIndex insert {} ops in {:.2?} ({:.} ops/sec)", n, insert_time, insert_ops);

    // Lookup benchmark
    let start = Instant::now();
    for i in 0..n {
        let _ = index.get(i);
    }
    let lookup_time = start.elapsed();
    let lookup_ops = n as f64 / lookup_time.as_secs_f64();
    println!("HashIndex lookup {} ops in {:.2?} ({:.} ops/sec)", n, lookup_time, lookup_ops);

    // Remove benchmark
    let start = Instant::now();
    for i in 0..n {
        index.remove(&Value::Int64(i as i64));
    }
    let remove_time = start.elapsed();
    let remove_ops = n as f64 / remove_time.as_secs_f64();
    println!("HashIndex remove {} ops in {:.2?} ({:.} ops/sec)", n, remove_time, remove_ops);

    println!("\nTotal: {} operations in {:.2?} ({:.} ops/sec)", 
        3u64 * n, 
        insert_time + lookup_time + remove_time, 
        3f64 * n / (insert_time + lookup_time + remove_time).as_secs_f64());
}