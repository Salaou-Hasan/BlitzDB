# ADR-004: Transaction Model

## Status

Accepted

## Context

Transactions must provide atomicity and isolation while supporting high concurrency.

## Decision

Use optimistic concurrency control (OCC) as the default:

1. Transaction begins with a snapshot
2. Reads and writes are buffered locally
3. At commit, validate that no conflicts occurred
4. Apply writes atomically if validation passes
5. If conflicts detected, retry or abort

Support configurable isolation levels:
- Read Uncommitted
- Read Committed
- Repeatable Read
- Serializable

## Consequences

**Positive:**
- No locking overhead during transaction execution
- High throughput for low-contention workloads
- Simple reasoning about transaction behavior
- Clean separation of read and write phases

**Negative:**
- Abort rate increases under high contention
- Retry logic adds complexity for applications
- Memory overhead for buffered writes

**Mitigation:**
- Conflict detection to fail fast
- Configurable retry policies
- Write buffering with bounded memory

## References

- PostgreSQL MVCC
- FoundationDB optimistic concurrency
- TiKV optimistic transactions
