//! DuckFlock Gateway — PostgreSQL wire protocol server.
//!
//! This crate implements the PG wire protocol frontend that clients
//! (psql, BI tools, JDBC/ODBC drivers) connect to. It authenticates
//! connections via the [`AuthProvider`] trait and forwards queries
//! to the engine via gRPC.

pub mod placeholder {
    /// Placeholder — gateway implementation coming in DF-3.
    pub fn gateway_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
