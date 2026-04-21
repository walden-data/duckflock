//! Catalog attachment for DuckLake catalogs.

use duckdb::Connection;
use duckflock_core::catalog::{CatalogConfig, CatalogSource};
use duckflock_core::config::DuckFlockConfig;
use duckflock_core::error::DuckFlockError;

use crate::error::EngineError;

/// File-based catalog source that reads catalogs from DuckFlock configuration.
///
/// This is the default catalog source, reading catalog definitions
/// from `duckflock.yaml`.
pub struct FileCatalogSource {
    catalogs: Vec<CatalogConfig>,
    /// PostgreSQL metadata connection string for DuckLake catalogs.
    pub metadata_connection: String,
}

impl FileCatalogSource {
    /// Create a new file catalog source from DuckFlock configuration.
    pub fn new(config: &DuckFlockConfig) -> Self {
        let catalogs: Vec<CatalogConfig> = config
            .catalogs
            .iter()
            .map(|(name, entry)| CatalogConfig {
                name: name.clone(),
                metadata_schema: entry.metadata_schema.clone(),
                data_path: entry.data_path.clone(),
                read_only: entry.read_only,
                auto_migrate: entry.auto_migrate,
                options: entry.options.clone(),
            })
            .collect();

        Self {
            catalogs,
            metadata_connection: config.metadata.connection.clone(),
        }
    }
}

#[async_trait::async_trait]
impl CatalogSource for FileCatalogSource {
    async fn list_catalogs(&self) -> Result<Vec<CatalogConfig>, DuckFlockError> {
        Ok(self.catalogs.clone())
    }
}

/// Validate a catalog name: must start with a letter or underscore,
/// followed by letters, digits, or underscores.
pub fn is_valid_catalog_name(name: &str) -> bool {
    let re = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
    re.is_match(name)
}

/// Escape single quotes in a string for use in SQL literals.
fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

/// Attach a DuckLake catalog to a DuckDB connection.
///
/// Constructs and executes an `ATTACH` statement that connects DuckDB
/// to a DuckLake catalog backed by a PostgreSQL metadata store.
pub fn attach_catalog(
    conn: &Connection,
    catalog: &CatalogConfig,
    metadata_connection: &str,
) -> Result<(), EngineError> {
    // Validate catalog name
    if !is_valid_catalog_name(&catalog.name) {
        return Err(EngineError::CatalogAttachment {
            catalog: catalog.name.clone(),
            source: format!(
                "Invalid catalog name '{}': must match [a-zA-Z_][a-zA-Z0-9_]*",
                catalog.name
            )
            .into(),
        });
    }

    let escaped_conn = escape_single_quotes(metadata_connection);
    let escaped_data_path = escape_single_quotes(&catalog.data_path);
    let escaped_metadata_schema = escape_single_quotes(&catalog.metadata_schema);

    let mut sql = format!(
        "ATTACH 'ducklake:postgres:{}' AS {} (DATA_PATH '{}', METADATA_SCHEMA '{}')",
        escaped_conn, catalog.name, escaped_data_path, escaped_metadata_schema
    );

    if catalog.read_only {
        sql.push_str(" READ_ONLY");
    }

    tracing::info!("Attaching catalog: {}", catalog.name);
    tracing::debug!(
        "ATTACH SQL: ATTACH 'ducklake:postgres:[REDACTED]' AS {} (...)",
        catalog.name
    );

    conn.execute_batch(&sql)
        .map_err(|e| EngineError::CatalogAttachment {
            catalog: catalog.name.clone(),
            source: Box::new(e),
        })?;

    tracing::info!("Catalog '{}' attached successfully", catalog.name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_catalog_names() {
        assert!(is_valid_catalog_name("bronze"));
        assert!(is_valid_catalog_name("_private"));
        assert!(is_valid_catalog_name("catalog_1"));
        assert!(is_valid_catalog_name("MyCatalog"));
    }

    #[test]
    fn test_invalid_catalog_names() {
        assert!(!is_valid_catalog_name("123bad"));
        assert!(!is_valid_catalog_name("bad name"));
        assert!(!is_valid_catalog_name("bad;name"));
        assert!(!is_valid_catalog_name(""));
    }

    #[test]
    fn test_escape_single_quotes() {
        assert_eq!(escape_single_quotes("hello"), "hello");
        assert_eq!(escape_single_quotes("it's"), "it''s");
        assert_eq!(escape_single_quotes("'quoted'"), "''quoted''");
    }
}
