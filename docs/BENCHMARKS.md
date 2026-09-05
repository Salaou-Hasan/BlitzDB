# BlitzDB Benchmarks & SLO Contract

Release builds only: `cargo run --release ...` (debug numbers are meaningless).

## Contract (successful end-to-end only)

`p50 < 50ms, p90 < 70ms, p95 < 70ms, p99 < 100ms, p99.9 < 200ms`,
`errors = 0`, `timeouts = 0`, invariant `received == ok + resp_err`.
Throughput reported is **goodput** (`ok`/s) next to attempted RPS —
never conflated. Every level prints
`attempted/sent/recv/ok/failed/timed_out/conn_fail/lost` + first-10 errors
with op/conn/error/tx-state/queue-state/timestamps.

## Suites

| Example | Command | What it proves |
|---|---|---|
| `bench_a_connections` | `... bench_a_connections 100000` | Idle CCU: setup rate, KB/conn, heartbeat p50/p99 |
| `bench_b_active` | `... bench_b_active 16000 1` | Active ping-pong ceiling (stops at first loss) |
| `bench_c_app` | `... bench_c_app 50000 20 25 16` | App mix SLOs 10K→50K (`max ops batch shards`); `BLITZ_DATA_DIR`+`BLITZ_DURABILITY` for durable runs; `BLITZ_ATOMIC=1` for all-or-nothing frames; `BLITZ_SERVER_SHARDS=1` for server-side routing (clients speak base names) |
| `bench_c_tiny` | `... bench_c_tiny 10000` | Sequential 1/10/20/100 per-conn + GET-/INSERT-/UPDATE-only isolation |
| `bench_hotspot` | `... bench_hotspot 10000 50` | 1%-keys/50%-traffic skew (celebrity rule) |
| `ccu_bench` | `... ccu_bench 20000 16000 1` | Legacy engine/codec/TCP-ramp/pipe/batch survey |

App mix (per client, realistic read-heavy): 30% timeline Get, 15% post,
15% like, 10% follow, 10% comment, 10% notif, 5% message, 5% realtime Ping.
Batch-25 groups 25 ops/frame; shards-16 routes clients over 16 table sets.

## Last measured (24c/14G box, `none` batch-25/shards-16)

| CCU | Goodput | p50 | p99 | Verdict |
|---|---|---|---|---|
| 10K | ~2.4–3.1M/s | ~2ms | ~3–5ms | PASS |
| 20K | ~2.1–2.8M/s | ~3–6ms | ~6–14ms | PASS |
| 30K | ~1.4–2.7M/s | ~5–8ms | ~9–18ms | PASS |
| 40K | ~1.4–2.7M/s | ~8–11ms | ~12–24ms | PASS |
| 50K | ~1.5–2.7M/s | ~10–13ms | ~14–28ms | PASS |

Durable `every-sec` same load: 50K 1.57M/s p99 28ms PASS (~0.6×);
`near-sync` 10K 2.66M/s p99 3.2ms PASS. Ping-pong (no batch): 10K/20K PASS,
30K+ FAIL (Little's law — physics, not a bug). Hotspot 50/50-update
ping-pong: 10K PASS (p99 ~60–85ms), 50K FAIL (p99 ~400–494ms, syscall-bound).

Atomic batches (`BLITZ_ATOMIC=1`, same 10K app mix): 1.52M/s, p50 3.3ms,
p99 5.3ms, 0 errors, PASS. Cost of all-or-nothing at 10K: ~0.75× goodput,
+1.4ms p99 vs plain batch (2.03M/s, p99 3.9ms same run) — same latency class,
0 tx_conflicts (blind inserts + stable-row reads don't contend).

Server-side routing (`BLITZ_SERVER_SHARDS=1`, 10K, 16 shards, base names on
the wire): 1.96M/s, p50 2.3ms, p99 4.2ms, 0 errors, PASS — indistinguishable
from client-mangled sharding (2.03M/s, p99 3.9ms). The model no longer
changes with scale.

Rust SDK (`blitz-client`, drain-driven autobatching, per-op await style).
Same code from 1 to 50K flows — only pool size changes. `sdk_slo` env:
`SDK_SHARED=N` (N clients shared round-robin; 0 = one per task),
`TOKIO_THREADS=T`, `SDK_FAST_INSERT=1` (no-`_idem` expert inserts, bench
wire parity), `SDK_GETS_ONLY=1` (read-path isolation). Shape matters, code
doesn't. Final shape: 64-shared pool, 8 runtime threads, 16-op flush chunks.

| Flows | Goodput | p50 | p99 | Verdict |
|---|---|---|---|---|
| 200 | ~187K/s | ~1.0ms | ~1.8ms | PASS |
| 1K (16 shared) | ~481K/s | ~1.9ms | ~3.6ms | PASS |
| 10K (64 shared) | ~2.0M/s | ~4.3ms | ~6.5ms | PASS |
| 50K (64 shared) | ~1.95M/s | ~23ms | ~30ms | PASS, 0 errors |

Native batch-25 for reference: 10K 2.03M/s p50 2.4ms p99 3.9ms; 50K
1.54M/s p50 15.2ms p99 27.6ms. The SDK meets-or-beats native throughput
at both scales with identical user code, p50 within ~1.5–1.7×, p99
within ~1.1–1.7× — and beats native-awaited (10K single-frame: 263K/s,
p99 79ms, FAIL) everywhere it is viable.

How the gap closed (each measured): timer-driven flush → drain-driven
(no 2ms tax, 2.1×); per-op timer wheel → worker queue-deadlines + one
timer per frame (+46%); global `_idem` write-lock → 16 shards (p99
halved); `insert_fast` without `_idem` (bench parity, p50 tied);
flush chunks 16 (native frame shape class, p99 parity at 50K).
Remaining delta is scheduler-queue amplification of dependent
caller→worker→caller wakeups plus per-connection multiplex queueing —
multiplexing physics, not server work (raw sockets: p50 0.35ms at 2K
conns). Guidance: share clients (pool ≈ CCU/150 at 10K+, wider at 50K
only to a point), size threads down until throughput bends (migration
beats parallelism past ~8 here), use `insert_fast` for naturally
idempotent hot paths.

## Reading results honestly

- Bands, not points: this box varies ±40% run-to-run; first level often
  slowest (frequency/warmup). SLO verdicts (not headline RPS) are the stable
  signal — all PASS with margin above.
- `peak` slightly under CCU on some lines is poller sampling (5ms), not loss:
  trust `sent/recv/ok` + invariant, which are exact.
- Durable runs isolate `ccu-N` subdirs (shared-dir replay accumulation would
  otherwise dominate — itself proof rotation is mandatory).
