# BlitzDB Architecture

## Overview

BlitzDB is a modular monolith that combines database, application runtime, authentication, events, and realtime capabilities into one coherent platform.

## System Architecture

```
                    BlitzDB PROCESS

 ┌─────────────────────────────────────────────┐
 │                  Server                     │
 │                                             │
 │   Protocol / API                            │
 │        ↓                                    │
 │   Authentication                            │
 │        ↓                                    │
 │   Authorization / Policies                  │
 │        ↓                                    │
 │   Query / Function Execution                │
 │        ↓                                    │
 │   Transaction Engine                        │
 │        ↓                                    │
 │   Table + Index Engine                     │
 │        ↓                                    │
 │   Memory Engine                             │
 │        ↓                                    │
 │   WAL / Persistence                         │
 │                                             │
 │   Event Engine                              │
 │   Realtime Engine                           │
 │   Background Jobs                           │
 │   Observability                             │
 └─────────────────────────────────────────────┘
```

## Data Flow

```
Client Request
  → Protocol decode
    → Authentication
      → Authorization
        → Query/Function execution
          → Transaction begin
            → Reads (from memory/snapshot)
            → Writes (buffered)
          → Transaction commit
            → Conflict validation
            → WAL write
            → State update
            → Event emission
            → Subscription propagation
          → Response
```

## Crate Dependencies

```
blitz-types (foundation)
  ↓
blitz-core (table engine trait)
  ↓
blitz-memory, blitz-table, blitz-index
  ↓
blitz-tx (transaction engine)
  ↓
blitz-query, blitz-storage, blitz-wal
  ↓
blitz-server (entry point)
```

## Key Invariants

1. Committed data is always consistent
2. Transactions are atomic and isolated
3. Realtime subscriptions reflect committed state
4. WAL ensures durability
5. Snapshots enable fast recovery

## Performance Targets

- Sub-100µs internal hot-path operations
- Millions/sec throughput for simple in-memory primitives
- Excellent p99 latency under contention
- Minimal lock contention
- High cache locality
