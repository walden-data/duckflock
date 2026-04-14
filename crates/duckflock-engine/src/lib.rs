//! DuckFlock Engine — DuckDB execution and gRPC service.
//!
//! This crate manages DuckDB connection pools, query execution,
//! DuckLake catalog attachment, and the gRPC server that the
//! gateway communicates with.

pub mod placeholder {
    /// Placeholder — engine implementation coming in DF-9.
    pub fn engine_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
