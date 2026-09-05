# BlitzDB Operations (single-node)

## Profiles

| Profile | Flags | SLO | Notes |
|---|---|---|---|
| cache | (defaults) | 50K PASS | Data lost on crash. Shard/cache, batch-25/shards-16. |
| durable every-sec | `--data-dir DIR --durability every-sec` | 50K PASS (~0.6×) | ≤1s loss window. Snapshots + rotation required. |
| durable near-sync | `--data-dir DIR --durability near-sync` | 10K PASS verified | ~1ms window. 50K needs NVMe measurement (not claimed here). |

## Host tuning

- `ulimit -n 524288` (50K conns × FDs; harness holds both ends in one process).
- `net.core.somaxconn = 8192` (container here caps 4096 — backlog silently clamps).
- 4KiB socket buffers are set by the server/CLI; raising them multiplies RSS
  (~8KB/conn budget at 4K → 5.3KB/conn measured idle).
- 8 loopback source IPs in benches defeat the ~28K single-IP ephemeral range.

## CLI reference

```
blitz serve [--host 127.0.0.1] [-p 7420] [-v]
  [--data-dir DIR --durability none|near-sync|every-sec]
  [--snapshot-secs N] [--metrics-port P]
  [--tls-cert C --tls-key K --tls-port T]
blitz rotate --dir DIR        # stopped-server WAL rotation (bounds replay)
blitz metrics                 # Prometheus exposition
```

## Metrics (`GET /metrics`, `/readyz` on `--metrics-port`)

`blitz_connections`, `blitz_requests_total`, `blitz_slow_responses_total`,
`blitz_slow_rate`, `blitz_shed_drops_total`, `blitz_wal_dropped_total`,
`blitz_bytes_read/written_total`, `blitz_wal_bytes/ops_total`,
`blitz_push_delivered_total`, `blitz_push_dropped_total`,
`blitz_fanout_done/dropped_total`, `blitz_active_tx` (0 = auto-commit),
`blitz_uptime_seconds`. `/readyz` is 200 once started.

Alert: `slow_rate > 0.01`, any `shed_drops`/`wal_dropped` growth,
`p99 > 100ms` (bench SLO), RSS drift >1%/h, replay time (log line at start).

## Durability procedures

- Snapshot cadence: `--snapshot-secs 60` (no-shed save) + `blitz rotate`
  from cron (quiesced truncate; brief shed window, retryable via `_idem`).
- Backup: copy `snapshots/` + `wal_*.log` (drill-tested in
  `durability::backup_tests`). Restore = place files, start (replay is
  idempotent; IDs preserved, allocator bumped).
- Restart replay grows without rotation — same-dir multi-run accumulation
  TIMEOUTed at 40K in testing; that is why rotation is mandatory, and why
  benches isolate `ccu-N` subdirs.

## Chaos / soak

- Kill-restart: covered by `crash_tests` + `rotation_tests` (drop without
  snapshot → recover → all flushed rows present, fresh IDs uncollided).
  Two-process kill -9 at 20K durable + 24h soak at 10K are procedures awaiting
  a home-lab window (explicitly deferred, not claimed).
- Hotspot provisioning rule: uniform batch mix passes 50K; 50/50-update
  ping-pong skew fails from Little's-law (~250K rps ceiling → ~200ms floor).
  Skewed writers must batch or shard further.

## Capacity table (measured, this hardware 24c/14G)

Idle 5.3KB/conn (100K = 779MB). App batch mix: 10K ~150MB → 50K ~640MB
(`none`); durable adds WAL (~16MB/600K ops) + same RSS. Setup ~7.7K conns/s
to 50K, ~1.3K/s to 100K (SYN storm bound, use `serve_reuseport` past that).
