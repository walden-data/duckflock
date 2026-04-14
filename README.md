# DuckFlock 🦆

**A persistent PostgreSQL-compatible endpoint for [DuckLake](https://ducklake.select).**

DuckFlock turns your DuckLake into a multi-client SQL database. Point any PostgreSQL client — psql, Tableau, Metabase, DBeaver, your application — at DuckFlock, and query your lakehouse like it's Postgres.

```
BI Tools / Apps / Notebooks
  │ PostgreSQL wire protocol
  ▼
┌─────────────────────────────────┐
│          DuckFlock               │
│                                  │
│  Gateway ─── gRPC ──── Engine    │
│  (PG wire)           (DuckDB    │
│                       pool)      │
└──────────┬──────────┬────────────┘
      PostgreSQL    S3 / Local
      (DuckLake     (Parquet
       metadata)     files)
```

## Why DuckFlock?

[DuckLake](https://ducklake.select) is an open lakehouse format that stores metadata in PostgreSQL and data in Parquet files. It provides ACID transactions, time travel, schema evolution, and cross-catalog joins — powered by DuckDB.

But DuckLake by itself is an *embedded* library. There's no server. If you want multiple users, BI tools, or applications to query the same DuckLake concurrently, you need something in between.

**DuckFlock is that something.** It's a pool of DuckDB instances behind a persistent PostgreSQL-compatible endpoint.

## Quick Start

```bash
# Clone and start the full stack
git clone https://github.com/walden-data/duckflock.git
cd duckflock
docker compose up -d

# Connect with psql
psql -h localhost -p 5433 -U analyst

# Query your DuckLake
SELECT * FROM bronze.events LIMIT 10;
```

Or with a config file:

```bash
# Install
cargo install duckflock

# Configure
cp duckflock.example.yaml duckflock.yaml
# Edit duckflock.yaml with your PostgreSQL and S3 settings

# Run
duckflock --config duckflock.yaml
```

## Configuration

DuckFlock is configured via `duckflock.yaml`. See [`duckflock.example.yaml`](duckflock.example.yaml) for all options.

Minimal configuration:

```yaml
metadata:
  connection: postgres://localhost:5432/metadata_store

catalogs:
  my_lake:
    metadata_schema: lake_meta
    data_path: s3://my-bucket/lake/
```

## Architecture

DuckFlock has four crates:

| Crate | Role |
|-------|------|
| `duckflock-core` | Shared types, plugin traits (`AuthProvider`, `AuditLogger`, `CatalogSource`), config |
| `duckflock-engine` | DuckDB connection pool, query execution, gRPC server |
| `duckflock-gateway` | PostgreSQL wire protocol, client connection management |
| `duckflock-server` | Binary entrypoint, wires everything together |

### Plugin System

DuckFlock is designed to be extended. Three trait interfaces allow integrators to customize behavior without forking:

- **`AuthProvider`** — authenticate connections. Ships with `TrustAuthProvider` (dev) and `ScramAuthProvider` (SCRAM-SHA-256). Implement your own for JWT, LDAP, etc.
- **`AuditLogger`** — log queries. Ships with `NoOpAuditLogger` and `StdoutAuditLogger`. Implement your own for database-backed audit trails.
- **`CatalogSource`** — discover catalogs. Ships with `FileCatalogSource` (reads from config). Implement your own for dynamic catalog registries.

## How It Compares

| | DuckFlock | Vanilla DuckDB | Trino / Spark | MotherDuck |
|---|---|---|---|---|
| **DuckLake native** | ✅ | ✅ (embedded) | ❌ | ✅ (managed) |
| **Multi-client** | ✅ | ❌ | ✅ | ✅ |
| **PG wire protocol** | ✅ | ❌ | ❌ | ❌ |
| **Self-hosted** | ✅ | ✅ | ✅ | ❌ |
| **Config file** | ✅ | N/A | Complex | N/A |
| **Open source** | MIT | MIT | Apache/varies | Proprietary |

## Roadmap

- [x] Project scaffold and plugin trait definitions
- [ ] DuckDB connection pool and query execution
- [ ] PostgreSQL wire protocol gateway
- [ ] SCRAM-SHA-256 authentication
- [ ] Docker quick start
- [ ] Multi-node load balancing
- [ ] Process-level session isolation
- [ ] ADBC endpoint

## Built With

- [DuckDB](https://duckdb.org) — in-process analytical database
- [DuckLake](https://ducklake.select) — open lakehouse format (v1.0)
- [pgwire](https://github.com/sunng87/pgwire) — PostgreSQL wire protocol implementation
- [tonic](https://github.com/hyperium/tonic) — gRPC framework for Rust

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT — see [LICENSE](LICENSE).
