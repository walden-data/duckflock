//! DuckFlock Engine — DuckDB connection pool and query execution.
//!
//! This crate provides:
//! - [`Engine`] — the main entry point for query execution
//! - [`EngineConfig`] — configuration for the engine
//! - [`EngineError`] — typed error handling
//! - [`QueryResult`] — query execution results with Arrow RecordBatches

pub mod catalog;
pub mod config;
pub mod engine;
pub mod error;
pub mod execute;
pub mod pool;

pub use catalog::FileCatalogSource;
pub use config::EngineConfig;
pub use engine::Engine;
pub use error::EngineError;
pub use execute::QueryResult;
