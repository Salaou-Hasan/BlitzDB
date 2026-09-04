# Contributing to BlitzDB

Thank you for considering contributing to BlitzDB.

## Development Setup

1. Install Rust stable toolchain
2. Clone the repository
3. Run `cargo check` to verify the build
4. Run `cargo test` to run all tests

## Code Style

- Follow Rust standard formatting (`cargo fmt`)
- Address all clippy warnings (`cargo clippy`)
- Write tests for new functionality
- Document public APIs

## Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Ensure all tests pass
5. Submit a pull request

## Testing

```bash
# Run all tests
cargo test

# Run specific crate tests
cargo test -p blitz-types

# Run with output
cargo test -- --nocapture
```

## Architecture Decisions

Major architectural decisions are recorded in `docs/architecture/adr/`. Before making significant changes, check if an ADR already exists or create one.

## Security

Report security vulnerabilities privately. See [SECURITY.md](SECURITY.md) for details.
