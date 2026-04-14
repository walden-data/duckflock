# Contributing to DuckFlock

Thanks for your interest in contributing to DuckFlock!

## Development Setup

### Prerequisites

- Rust 1.84+ (`rustup update stable`)
- PostgreSQL 16+
- protobuf compiler (`brew install protobuf` on macOS)

### Build

```bash
git clone https://github.com/walden-data/duckflock.git
cd duckflock
cargo build
```

### Test

```bash
cargo test
```

### Lint

```bash
cargo clippy -- -D warnings
cargo fmt --check
```

## Project Structure

```
duckflock/
├── crates/
│   ├── duckflock-core/      # Shared types, traits, config
│   ├── duckflock-engine/    # DuckDB execution, gRPC server
│   ├── duckflock-gateway/   # PG wire protocol
│   └── duckflock-server/    # Binary entrypoint
├── proto/                    # gRPC protocol definitions
├── docs/                     # Architecture documentation
└── duckflock.example.yaml    # Example configuration
```

## Pull Requests

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Ensure `cargo test`, `cargo clippy`, and `cargo fmt --check` pass
4. Write a clear PR description explaining the "why"
5. Link to any relevant issues

## Code Style

- Follow existing patterns in the codebase
- Use `thiserror` for error types, `anyhow` for application errors
- Add doc comments to public APIs
- Write tests for new functionality

## Reporting Issues

Please use [GitHub Issues](https://github.com/walden-data/duckflock/issues). Include:
- What you expected to happen
- What actually happened
- Steps to reproduce
- DuckFlock version, OS, and relevant configuration
