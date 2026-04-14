/// Core error type for DuckFlock operations.
#[derive(Debug, thiserror::Error)]
pub enum DuckFlockError {
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Authorization denied: {0}")]
    AuthorizationDenied(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Catalog error: {0}")]
    CatalogError(String),

    #[error("Query execution error: {0}")]
    QueryError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}
