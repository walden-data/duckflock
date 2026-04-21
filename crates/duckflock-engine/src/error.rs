//! Error types for the DuckFlock engine.

use thiserror::Error;

/// Errors that can occur during engine operations.
#[derive(Error, Debug)]
pub enum EngineError {
    /// Error from DuckDB itself.
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    /// Error from Arrow data processing.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Catalog attachment failed.
    #[error("Catalog attachment failed for '{catalog}': {source}")]
    CatalogAttachment {
        catalog: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Invalid configuration provided.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Connection pool has no available connections.
    #[error("Connection pool exhausted")]
    PoolExhausted,

    /// Query execution timed out.
    #[error("Query execution timeout")]
    QueryTimeout,

    /// Internal engine error.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<EngineError> for duckflock_core::error::DuckFlockError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::DuckDb(e) => Self::QueryError(e.to_string()),
            EngineError::Arrow(e) => Self::QueryError(e.to_string()),
            EngineError::CatalogAttachment { catalog, source } => {
                Self::CatalogError(format!("{catalog}: {source}"))
            }
            EngineError::InvalidConfig(msg) => Self::ConfigError(msg),
            EngineError::PoolExhausted => Self::ConnectionError("pool exhausted".to_string()),
            EngineError::QueryTimeout => Self::ConnectionError("query timeout".to_string()),
            EngineError::Internal(msg) => Self::Internal(anyhow::anyhow!(msg)),
        }
    }
}
