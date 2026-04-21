//! Engine configuration types.

use duckflock_core::catalog::CatalogConfig;

/// S3 / object-storage configuration for DuckDB.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3-compatible endpoint URL.
    pub endpoint: String,
    /// AWS region.
    pub region: String,
    /// Access key ID (optional for public buckets).
    pub access_key_id: Option<String>,
    /// Secret access key (optional for public buckets).
    pub secret_access_key: Option<String>,
    /// Whether to use SSL/TLS.
    pub use_ssl: bool,
    /// URL style: `path` or `vhost`.
    pub url_style: String,
}

/// Configuration for the DuckDB execution engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Database path (`:memory:` for in-memory, or a file path).
    pub database_path: String,
    /// Memory limit per DuckDB instance (e.g., `"2GB"`).
    pub max_memory: String,
    /// Number of worker threads (0 = auto).
    pub worker_threads: u32,
    /// Number of connections in the pool.
    pub pool_size: usize,
    /// DuckDB extensions to install and load.
    pub extensions: Vec<String>,
    /// Temporary directory for DuckDB spill-to-disk.
    pub temp_directory: String,
    /// Optional S3 / object-storage configuration.
    pub s3: Option<S3Config>,
    /// PostgreSQL metadata connection string for DuckLake catalogs.
    pub metadata_connection: Option<String>,
    /// DuckLake catalogs to attach on startup.
    pub catalogs: Vec<CatalogConfig>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            database_path: ":memory:".to_string(),
            max_memory: "2GB".to_string(),
            worker_threads: 0,
            pool_size: 4,
            extensions: vec![
                "postgres".to_string(),
                "ducklake".to_string(),
                "httpfs".to_string(),
                "parquet".to_string(),
            ],
            temp_directory: "/tmp/duckdb".to_string(),
            s3: None,
            metadata_connection: None,
            catalogs: Vec::new(),
        }
    }
}

impl EngineConfig {
    /// Create a new configuration with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the memory limit (e.g., `"2GB"`, `"512MB"`).
    pub fn with_memory(mut self, limit: impl Into<String>) -> Self {
        self.max_memory = limit.into();
        self
    }

    /// Set the connection pool size.
    pub fn with_pool_size(mut self, n: usize) -> Self {
        self.pool_size = n;
        self
    }

    /// Set S3 / object-storage configuration.
    pub fn with_s3(mut self, s3: S3Config) -> Self {
        self.s3 = Some(s3);
        self
    }

    /// Set the database file path (`:memory:` for in-memory).
    pub fn with_database_path(mut self, path: impl Into<String>) -> Self {
        self.database_path = path.into();
        self
    }

    /// Set DuckDB extensions to install and load.
    pub fn with_extensions(mut self, exts: Vec<String>) -> Self {
        self.extensions = exts;
        self
    }

    /// Set the PostgreSQL metadata connection string.
    pub fn with_metadata_connection(mut self, conn: impl Into<String>) -> Self {
        self.metadata_connection = Some(conn.into());
        self
    }

    /// Set the DuckLake catalogs to attach on startup.
    pub fn with_catalogs(mut self, catalogs: Vec<CatalogConfig>) -> Self {
        self.catalogs = catalogs;
        self
    }
}

impl From<&duckflock_core::config::DuckFlockConfig> for EngineConfig {
    fn from(config: &duckflock_core::config::DuckFlockConfig) -> Self {
        let s3 = if config.storage.endpoint.is_some() {
            Some(S3Config {
                endpoint: config.storage.endpoint.clone().unwrap_or_default(),
                region: config
                    .storage
                    .region
                    .clone()
                    .unwrap_or_else(|| "us-east-1".to_string()),
                access_key_id: None,
                secret_access_key: None,
                use_ssl: true,
                url_style: "vhost".to_string(),
            })
        } else {
            None
        };

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
            max_memory: config.compute.memory_per_session.clone(),
            pool_size: config.compute.max_sessions,
            s3,
            metadata_connection: Some(config.metadata.connection.clone()),
            catalogs,
            ..Self::default()
        }
    }
}
