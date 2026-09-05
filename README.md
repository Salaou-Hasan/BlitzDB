# BlitzDB

A single-node, high-concurrency application database/server. TCP binary protocol,
in-memory engine with group-commit WAL durability, sharded to hold 50K
concurrent clients inside its latency SLOs.

## Status

Production-grade single node (cache + durable profiles). Multi-node
(replication/partitioning) is explicitly out of scope — see
`docs/architecture/ARCHITECTURE.md`.

| Suite | Result |
|---|---|
| `cargo test --workspace` | 47 suites green |
| Benchmark C app mix, 10K→50K CCU, batch-25/shards-16 | all `PASS`: p50<50 p90/p95<70 p99<100 p99.9<200ms, 0 errors |
| Durable `every-sec`, same load | all `PASS` (~40% below in-memory goodput) |
| Idle connections | 100K held, ~5.3KB/conn |

Latency contract (successful end-to-end only, 0 errors/timeouts): see
`docs/BENCHMARKS.md`. Ping-pong past ~8K active clients cannot meet it
(Little's law) — batch ≥25 is mandatory there, enforced by nothing but physics
and documented in `docs/PROTOCOL.md`.

## Layout

```
BlitzDB/
├── crates/
│   ├── blitz-types       # Value, Row, Schema, ColumnDef
│   ├── blitz-core        # InMemoryTableEngine (DashMap map, table locks, unique indexes)
│   ├── blitz-protocol    # Binary framing + codec (protocol v2, batching)
│   ├── blitz-tx          # Transaction engine (auto-commit over TCP; see docs)
│   ├── blitz-wal         # Write-ahead log (group-commit buffered appends, fdatasync)
│   ├── blitz-snapshot    # Snapshots + prune
│   ├── blitz-auth        # Identities, tokens
│   ├── blitz-policy      # Allow/deny rules (closed-deny under require_auth)
│   ├── blitz-events / blitz-realtime / blitz-api / blitz-search / blitz-jobs
│   │                     # Runtime subsystems (partially wired; see ARCHITECTURE.md)
│   ├── blitz-replication / blitz-cluster / blitz-observability
│   │                     # Stubs reserved for Stage 9+
│   ├── blitz-server      # TCP server: transport, durability, social, benches
│   └── blitz-cli         # `blitz` CLI (serve, rotate, metrics)
└── docs/
    ├── BENCHMARKS.md     # SLO contract, commands, last measured numbers
    ├── PROTOCOL.md       # Ops, batching, special keys, errors, retry rules
    ├── OPERATIONS.md     # Profiles, sysctl, metrics/alerts, backup, chaos
    └── architecture/     # ARCHITECTURE.md + ADRs
```

## Quick start

```bash
cargo build
cargo test --workspace          # 47 suites, must all pass

# In-memory cache shard (fastest; data lost on restart)
cargo run -p blitz-cli -- serve --port 7420 --metrics-port 9100

# Durable single node (crash-safe; ~40% below memory goodput)
cargo run -p blitz-cli -- serve --port 7420 --data-dir ./data \
  --durability every-sec --snapshot-secs 60 --metrics-port 9100

# With TLS (plaintext stays for loopback/bench)
cargo run -p blitz-cli -- serve --tls-cert cert.pem --tls-key key.pem --tls-port 7421

# Ops
curl localhost:9100/readyz
curl localhost:9100/metrics
cargo run -p blitz-cli -- rotate --dir ./data   # stopped-server WAL rotation
cargo run -p blitz-cli -- metrics               # Prometheus exposition
```

## Benchmarks (release only — debug numbers are meaningless)

```bash
# Full app SLO suite, 10K→50K, batch-25/shards-16:
cargo run --release -p blitz-server --example bench_c_app 50000 20 25 16
# Durable variant (isolated per-level dirs):
BLITZ_DATA_DIR=/tmp/b BLITZ_DURABILITY=every-sec \
  cargo run --release -p blitz-server --example bench_c_app 50000 20 25 16
# Idle scalability / active ping-pong / tiny / hotspot:
cargo run --release -p blitz-server --example bench_a_connections 100000
cargo run --release -p blitz-server --example bench_b_active 16000 1
cargo run --release -p blitz-server --example bench_c_tiny 10000
cargo run --release -p blitz-server --example bench_hotspot 10000 50
```

## SDK contract (all languages, same constants)

Batch ≥25 past 8K CCU · `_idem` on every insert · retry shed/timeout 3×
with jitter · `_auth` handshake · cursor pages only, never full scans ·
typed errors (`unauthorized`, `forbidden`, `WAL backpressure`). Details in
`docs/PROTOCOL.md`.

## License

MIT License - see [LICENSE](LICENSE) for details.
