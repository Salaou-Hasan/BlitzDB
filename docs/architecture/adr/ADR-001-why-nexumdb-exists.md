# ADR-001: Why BlitzDB Exists

## Status

Accepted

## Context

Modern application backends typically require assembling multiple independent systems:

- Database (PostgreSQL, MySQL)
- Cache (Redis)
- Message broker (RabbitMQ, Kafka)
- WebSocket infrastructure
- Background job queue
- Search engine
- Authentication service

This creates significant operational complexity, consistency challenges, and developer cognitive load.

## Decision

BlitzDB combines these capabilities into one coherent platform:

- Database with transactions
- Application logic execution
- Built-in authentication and authorization
- Event emission tied to transactions
- Realtime subscriptions synchronized with commits
- Cache-like primitives
- Background job system
- Search capabilities

## Consequences

**Positive:**
- Simplified application architecture
- Consistent transaction semantics across all operations
- Realtime updates derived from transactional commits
- Reduced operational complexity

**Negative:**
- Larger initial implementation scope
- Must maintain correctness across all subsystems
- Single process becomes a single point of failure until clustering is implemented

**Mitigation:**
- Modular architecture allows incremental implementation
- Clear subsystem boundaries enable independent testing
- Single-node correctness must be proven before distributed features

## References

- PostgreSQL, Redis, FoundationDB, CockroachDB architecture documentation
- SpacetimeDB design principles
