# ADR-007: Batch-First Heavy Load; Auto-Commit on TCP

Date: 2026-09-05 · Status: accepted

## Context

Ping-pong past ~8K active clients misses p99<100ms at any engine speed
(Little's law: 50K/500K rps ≈ 100ms floor). Interactive transactions over
TCP would need a v3 protocol + client state machines.

## Decision

- Batch ≥25 past 8K CCU (one read+write per N ops; per-op latency ≈
  batch/N, throughput exact). Measured: ping-pong 50K p99 253ms FAIL →
  batch-25 p99 ~14–28ms PASS at 1.5–2.7M goodput.
- TCP is auto-commit per op; Batch is ordered and NON-atomic (partial
  failure normal). Retries: Gets idempotent, Inserts carry `_idem`
  (server dedups, bounded 256K, group-window RPO). `active_tx = 0` reported.

## Consequences

- Good: SLOs hold to 50K on one node; no v3 protocol tax.
- Bad: app code must batch hot paths and retry with same `_idem`; cross-op
  atomicity must live above (sagas) until Stage 9.
