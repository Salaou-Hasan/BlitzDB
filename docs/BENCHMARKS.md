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

## Reading results honestly

- Bands, not points: this box varies ±40% run-to-run; first level often
  slowest (frequency/warmup). SLO verdicts (not headline RPS) are the stable
  signal — all PASS with margin above.
- `peak` slightly under CCU on some lines is poller sampling (5ms), not loss:
  trust `sent/recv/ok` + invariant, which are exact.
- Durable runs isolate `ccu-N` subdirs (shared-dir replay accumulation would
  otherwise dominate — itself proof rotation is mandatory).
