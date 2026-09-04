# BlitzDB

A general-purpose high-performance application database/runtime.

BlitzDB combines the capabilities of a database, application runtime, authentication system, event engine, realtime subscriptions, and background job system into one coherent platform.

## Current Status

**Stage 1 - Core Types and In-Memory Engine**: Complete

- Core data types (Value, Row, Schema, ColumnDef)
- In-memory table engine with CRUD operations
- Hash index and B-tree index implementations
- Transaction engine with optimistic concurrency
- 30 passing tests

## Architecture

BlitzDB is a modular monolith designed around these principles:

1. **Performance first** - Cache-conscious data layouts, minimal allocations
2. **Correctness first** - Transactional integrity, crash recovery
3. **Realtime integrated** - Subscriptions synchronized with commits
4. **Developer experience** - Simple APIs, schema-driven development

## Crate Structure

```
BlitzDB/
├── crates/
│   ├── blitz-types       # Core data types (Value, Row, Schema)
│   ├── blitz-core        # Table engine trait and in-memory implementation
│   ├── blitz-memory      # Memory-resident storage
│   ├── blitz-table       # High-level table abstraction
│   ├── blitz-index       # Hash and B-tree indexes
│   ├── blitz-query       # Query planner and executor
│   ├── blitz-tx          # Transaction engine
│   ├── blitz-storage     # Persistent storage engine
│   ├── blitz-wal         # Write-ahead log
│   ├── blitz-snapshot    # Snapshot management
│   ├── blitz-runtime     # Application runtime
│   ├── blitz-auth        # Authentication
│   ├── blitz-policy      # Authorization policies
│   ├── blitz-events      # Event emission
│   ├── blitz-realtime    # Realtime subscriptions
│   ├── blitz-api         # API layer
│   ├── blitz-protocol    # Wire protocol
│   ├── blitz-search      # Full-text search
│   ├── blitz-jobs        # Background jobs
│   ├── blitz-replication # Replication
│   ├── blitz-cluster     # Cluster coordination
│   ├── blitz-observability # Metrics and logging
│   ├── blitz-cli         # CLI tool
│   └── blitz-server      # Server entry point
├── c/                    # C primitives (SIMD, hashing, etc.)
├── sdk/                  # Client SDKs
├── docs/                 # Documentation
└── tests/                # Integration tests
```

## Quick Start

```bash
# Build
cargo build

# Test
cargo test

# Run the CLI
cargo run -p blitz-cli
```

## Development

```bash
# Check
cargo check

# Test with output
cargo test -- --nocapture

# Run specific crate tests
cargo test -p blitz-types
cargo test -p blitz-core
```

## License

MIT License - see [LICENSE](LICENSE) for details.
