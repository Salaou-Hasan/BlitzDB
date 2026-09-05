# ADR-009: Poll-First Realtime, Push as Stream Upgrade

Date: 2026-09-05 · Status: accepted

## Context

Server-initiated frames break strict request/response accounting (bench
1:1 matching, p99 math, batching). But DMs/live need push.

## Decision

- Default `Subscribe` = bounded long-poll (`_since`/`_limit`, 128-ring per
  table, zero-cost gate until first use). Benches/SLOs measure this.
- Opt-in push: `Subscribe` + `_stream:1` dedicates the connection
  (ack + seq-ID frames until EOF/idle; must be last frame in flight).
  64-deep per subscriber, slow evicted + counted, never blocking writers.

## Consequences

- Good: one framing path, SLO math intact, push proven by test.
- Bad: one table per push conn; quiet tables hit idle reap (resubscribe);
  true multiplexed streams await a v3 protocol (Stage 9+).
