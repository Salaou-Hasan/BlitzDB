# ADR-006: Sharded Group-Commit WAL with fdatasync

Date: 2026-09-05 · Status: accepted

## Context

Per-entry `flush+fsync` capped durable writes at ~10K ops/s. The SLO needs
>1M goodput with durability on.

## Decision

- 16 WAL files × threads, table-hash routed; 128K-deep channels; 1ms
  (`near-sync`) or 1000ms (`every-sec`) windows; one `fdatasync` per group.
- Buffered appends (no per-entry flush — that alone capped groups ~100K/s).
- Full channel sheds with typed backpressure + Insert compensation (delete),
  never silent loss. `snapshot_and_rotate()` quiesces for truncation.

## Consequences

- Single-file shed 13K/600K at 30K → sharded passes 50K (`every-sec`
  1.57M/s p99 28ms). Good: meets SLO. Bad: 16 files to operate; rotation
  briefly sheds (retryable via `_idem`).
- fdatasync (not fsync): correct for pre-created WAL files, cheaper under
  storms. NVMe numbers still unmeasured here (overlayfs only).
