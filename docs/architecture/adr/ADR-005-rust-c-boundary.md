# ADR-005: Rust + C Boundary

## Status

Accepted

## Context

Performance-critical primitives may benefit from C implementations, but safety must be maintained.

## Decision

Use a layered approach:

```
Rust application code
  └── Safe Rust wrapper
        └── C ABI boundary (unsafe)
              └── C implementation
```

Rules:
1. All C code must be wrapped in safe Rust APIs
2. Unsafe boundaries must be minimal and documented
3. Each C function must have clear ownership rules
4. C code must be tested with sanitizers
5. No undefined behavior in C code

Target C primitives:
- SIMD operations
- Hashing primitives
- Compression
- CRC/checksum
- Hot serialization kernels

## Consequences

**Positive:**
- Access to mature, optimized C libraries
- SIMD and platform-specific optimizations
- Clear safety boundaries

**Negative:**
- FFI overhead at boundaries
- Increased testing complexity
- Two language toolchains to maintain

**Mitigation:**
- Batch operations at FFI boundaries
- Comprehensive test coverage
- Automated sanitizer runs in CI

## References

- Rust FFI guidelines
- SQLite C API design
- RocksDB C API
