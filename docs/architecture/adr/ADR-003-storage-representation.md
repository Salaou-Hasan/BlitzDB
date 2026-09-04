# ADR-003: Storage Representation

## Status

Accepted

## Context

Data must be stored efficiently while supporting fast reads and writes.

## Decision

Use a hybrid approach:

- **Memory-resident hot data**: In-memory hash maps and B-tree maps
- **WAL for durability**: Append-only write-ahead log
- **Snapshots for recovery**: Periodic full snapshots with WAL replay
- **Page-based storage**: Fixed-size pages for persistent storage

The `Value` type is the fundamental data unit, supporting all primitive types and JSON documents.

## Consequences

**Positive:**
- Fast reads from memory-resident data
- Durability through WAL
- Efficient snapshots for backup and recovery
- Type-safe data representation

**Negative:**
- Memory usage scales with hot data size
- WAL growth requires compaction
- Snapshot creation has brief overhead

**Mitigation:**
- Background compaction for WAL
- Lazy loading for cold data
- Online snapshotting with minimal blocking

## References

- LMDB memory-mapped architecture
- RocksDB LSM-tree design
- PostgreSQL shared buffers
