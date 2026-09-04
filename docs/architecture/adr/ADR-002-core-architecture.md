# ADR-002: Core Architecture

## Status

Accepted

## Context

BlitzDB must support diverse workloads while maintaining simplicity and performance.

## Decision

BlitzDB is a modular monolith with clear internal boundaries:

```
Server
  └── Protocol / API
        └── Authentication
              └── Authorization
                └── Query / Function Execution
                      └── Transaction Engine
                            └── Table + Index Engine
                                  └── Memory Engine
                                        └── WAL / Persistence
```

Internal communication uses direct function calls, not network requests.

## Consequences

**Positive:**
- Minimal latency for internal operations
- Simple debugging and profiling
- No network serialization overhead
- Shared memory for zero-copy operations

**Negative:**
- Single process limits horizontal scaling
- Must use process-level concurrency for parallelism

**Mitigation:**
- Sharded locks and lock striping for concurrency
- Work-stealing thread pool for parallel execution
- Future clustering support for horizontal scaling

## References

- SQLite single-process architecture
- FoundationDB shared-nothing architecture
