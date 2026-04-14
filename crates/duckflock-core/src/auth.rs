use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::DuckFlockError;
use crate::identity::Identity;

/// Credentials presented by a client during PG wire protocol startup.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: Option<String>,
    pub database: Option<String>,
    pub parameters: HashMap<String, String>,
}

/// Pluggable authentication provider.
///
/// DuckFlock ships with:
/// - `TrustAuthProvider` — accept all connections (dev mode)
/// - `ScramAuthProvider` — SCRAM-SHA-256 against users in config
///
/// Integrators can implement custom providers (e.g., JWT validation).
#[async_trait]
pub trait AuthProvider: Send + Sync + 'static {
    /// Authenticate a connection during PG startup.
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, DuckFlockError>;

    /// Optional: per-query authorization check.
    /// Default implementation allows all queries.
    async fn authorize(&self, _identity: &Identity, _query: &str) -> Result<(), DuckFlockError> {
        Ok(())
    }
}

/// Accept all connections without authentication. For development only.
pub struct TrustAuthProvider;

#[async_trait]
impl AuthProvider for TrustAuthProvider {
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, DuckFlockError> {
        tracing::warn!(
            username = %credentials.username,
            "Trust auth: accepting connection without authentication"
        );
        Ok(Identity::new(&credentials.username))
    }
}
