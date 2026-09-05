# BlitzDB Architecture (single-node v1)

## Request path (what the SLOs actually measure)

```
Client
  │  TCP (4KiB bufs, NODELAY, backlog 8192, opt. TLS) or SO_REUSEPORT fan-in
  ▼
Admission: max_connections → shed watermark → per-IP cap → (close fast, shed counter)
  ▼
Framing: [ver u8][len u32 LE][payload], slow-loris cap, batch ≤4096 ops
  ▼
Auth: `_auth` handshake binds identity to the connection (one lookup/conn);
      Ping always passes; bypass when `!require_auth` (bench default);
      else direct permission → policy allow-rules → closed-deny
  ▼
Authorize per op → denied ops become err payloads (never touch engine/WAL)
  ▼
Dispatch (auto-commit per op; Batch is NOT atomic, partial failure normal):
  Insert (validate unless skip + unique-index reserve + id alloc + WAL + change-log + search-index + fanout job)
  Get/Scan zero-copy borrowed encode · Update (move) · Delete
  Find (O(1) unique lookup; non-unique rejected, never scanned)
  Subscribe (bounded long-poll) / Search (bounded postings) / push-stream upgrade
  ▼
Engine: DashMap table map → per-table RwLock → HashMap rows + per-col unique maps
  ▼
WAL (durable modes): 16-shard group-commit, 1ms/near-sync or 1000ms/every-sec,
fdatasync per group; full channel → backpressure error + Insert compensation
  ▼
Commit → change-log ring (128/table, gated free) + push hub + search/fanout enqueue
  ▼
Response encode (presized writer) → write → io/request/slow counters
```

## Consistency contract

- Auto-commit per op. No interactive transactions over TCP (`active_tx = 0`,
  reported honestly). `tx_manager` exists but owns a separate engine — do not
  use it on the serving path.
- Batch = ordered, non-atomic. Retry safety: Gets idempotent; Inserts need
  `_idem` (server dedups 256K bounded, in-memory RPO = group window).
- Unique/PK enforced O(1) via maintained indexes (insert/check/update/delete/
  replay all maintain; delete frees). Concurrent dup claimants: exactly one wins.
- Feed/search derived state: `timeline` rows skip WAL (recomputable);
  snapshots capture everything; replay is idempotent (`insert_preserving_id`
  + allocator bump; duplicate replays skipped).

## Durability modes

| Mode | Mechanism | Loss window | Relative goodput |
|---|---|---|---|
| `none` | no WAL | total on crash | 1.0× |
| `near-sync` | 16×1ms groups + fdatasync | ~1ms | ~0.9× @10K |
| `every-sec` | 16×1000ms groups + fdatasync | ≤1s | ~0.6× @50K |

Same-dir multi-level accumulation makes replay grow without bound —
`snapshot_and_rotate()` (quiesced) or offline `blitz rotate` is mandatory ops
procedure, plus bench isolation per `ccu-N` subdir.

## What is NOT on the hot path (by design)

- `blitz-tx` interactive tx, `blitz-query` planner, `blitz-search` crate TF
  index, `blitz-jobs`, replication/cluster (Stage 9+ stubs).
- Per-request full-sort timelines: cursor `Scan` exists for admin/backfill
  pages only; production feeds are precomputed (fanout worker) or pulled
  point reads. Hotspot ping-pong 50/50-update at 50K is syscall-bound
  (~250K rps → Little's-law floor) regardless of engine locking — measured,
  including a reverted row-striping attempt (see `docs/architecture/adr/`).

## Performance targets (measured, not wished)

SLO (successful end-to-end, 0 errors/timeouts):
p50<50, p90/p95<70, p99<100, p99.9<200ms at 10K→50K app CCU, batch-25/shards-16.
Internal budget <~25ms; network jitter owns the rest. Last numbers in
`docs/BENCHMARKS.md`. Ping-pong past ~8K cannot comply (Little's law).
