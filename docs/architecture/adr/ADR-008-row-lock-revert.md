# ADR-008: Row-Striped Engine Reverted (Measured)

Date: 2026-09-05 · Status: accepted (revert)

## Context

Hotspot ping-pong 50/50-update at 50K failed p99 (~400ms). Suspected
table-wide write lock; built a full DashMap/row-striped engine.

## Decision

Revert. Measured before/after: hotspot 10K 400K/s → 244K/s (worse),
app batch unchanged-to-worse. Costs found: DashMap double-hash + shard
locks per op, two HashMap clones per update vs one in-place, entry-API
churn — ~40% overhead for parallelism the engine never needed (at 130K
updates/s the engine is ~5% of a core; the 50K-task syscall/scheduler rate
is the ceiling). Table `RwLock` + O(1) unique indexes stay.

## Consequences

- Skew rule stays architectural (batch/shard), not mechanical.
- Lesson recorded: profile the ceiling (strace: 81% send/recv+futex)
  before promoting a suspect to a rewrite.
