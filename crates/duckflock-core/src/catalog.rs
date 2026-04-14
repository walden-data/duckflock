use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::DuckFlockError;

/// Configuration for a single DuckLake catalog.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CatalogConfig {
    /// Display name / alias used in SQL (e.g., `bronze`)
    pub name: String,
    /// PostgreSQL schema holding DuckLake metadata
    pub metadata_schema: String,
    /// S3 or local path for Parquet data files
    pub data_path: String,
    /// Whether this catalog is read-only
    #[serde(default)]
    pub read_only: bool,
    /// Whether to auto-migrate older DuckLake versions
    #[serde(default)]
    pub auto_migrate: bool,
    /// Additional DuckLake ATTACH options
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Pluggable catalog discovery.
///
/// DuckFlock ships with:
/// - `FileCatalogSource` — reads catalogs from `duckflock.yaml`
///
/// Integrators can implement custom sources (e.g., read from a catalog registry).
#[async_trait]
pub trait CatalogSource: Send + Sync + 'static {
    /// Return the list of catalogs to attach.
    async fn list_catalogs(&self) -> Result<Vec<CatalogConfig>, DuckFlockError>;

    /// Refresh the catalog list. Called periodically or on-demand.
    /// Default implementation delegates to `list_catalogs`.
    async fn refresh(&self) -> Result<Vec<CatalogConfig>, DuckFlockError> {
        self.list_catalogs().await
    }
}
