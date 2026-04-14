use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::DuckFlockError;

/// Top-level DuckFlock configuration, typically loaded from `duckflock.yaml`.
#[derive(Debug, Clone, Deserialize)]
pub struct DuckFlockConfig {
    /// Listener configuration
    #[serde(default)]
    pub listen: ListenConfig,

    /// PostgreSQL metadata store connection
    pub metadata: MetadataConfig,

    /// Object storage configuration
    #[serde(default)]
    pub storage: StorageConfig,

    /// DuckLake catalogs to attach
    #[serde(default)]
    pub catalogs: HashMap<String, CatalogEntry>,

    /// Compute / resource configuration
    #[serde(default)]
    pub compute: ComputeConfig,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,

    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Metrics configuration
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub tls: Option<TlsConfig>,
}

impl Default for ListenConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            tls: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    /// PostgreSQL connection string for the DuckLake catalog database
    pub connection: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_storage_type")]
    pub r#type: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            r#type: default_storage_type(),
            endpoint: None,
            region: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CatalogEntry {
    pub metadata_schema: String,
    pub data_path: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub auto_migrate: bool,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ComputeConfig {
    #[serde(default = "default_isolation")]
    pub isolation: String,
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    #[serde(default = "default_memory_per_session")]
    pub memory_per_session: String,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: String,
}

impl Default for ComputeConfig {
    fn default() -> Self {
        Self {
            isolation: default_isolation(),
            max_sessions: default_max_sessions(),
            memory_per_session: default_memory_per_session(),
            idle_timeout: default_idle_timeout(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "default_auth_method")]
    pub method: String,
    #[serde(default)]
    pub users: Vec<UserEntry>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            method: default_auth_method(),
            users: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserEntry {
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_audit_mode")]
    pub audit: String,
    pub audit_file: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            audit: default_audit_mode(),
            audit_file: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            port: default_metrics_port(),
        }
    }
}

// Defaults
fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    5433
}
fn default_storage_type() -> String {
    "s3".to_string()
}
fn default_isolation() -> String {
    "connection".to_string()
}
fn default_max_sessions() -> usize {
    20
}
fn default_memory_per_session() -> String {
    "4GB".to_string()
}
fn default_idle_timeout() -> String {
    "5m".to_string()
}
fn default_auth_method() -> String {
    "trust".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_audit_mode() -> String {
    "none".to_string()
}
fn default_metrics_enabled() -> bool {
    true
}
fn default_metrics_port() -> u16 {
    9090
}

impl DuckFlockConfig {
    /// Load configuration from a YAML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, DuckFlockError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| DuckFlockError::ConfigError(format!("Failed to read config file: {e}")))?;
        Self::from_yaml(&content)
    }

    /// Parse configuration from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self, DuckFlockError> {
        serde_yaml::from_str(yaml)
            .map_err(|e| DuckFlockError::ConfigError(format!("Failed to parse config: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_config() {
        let yaml = r#"
metadata:
  connection: postgres://localhost:5432/metadata_store

catalogs:
  bronze:
    metadata_schema: bronze_meta
    data_path: s3://lake/bronze/
"#;
        let config = DuckFlockConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.listen.port, 5433);
        assert_eq!(config.catalogs.len(), 1);
        assert!(config.catalogs.contains_key("bronze"));
        assert_eq!(config.auth.method, "trust");
    }

    #[test]
    fn test_full_config() {
        let yaml = r#"
listen:
  host: 127.0.0.1
  port: 5434

metadata:
  connection: postgres://user:pass@localhost:5432/metadata_store

storage:
  type: s3
  endpoint: http://localhost:4566
  region: us-east-1

catalogs:
  bronze:
    metadata_schema: bronze_meta
    data_path: s3://lake/bronze/
    read_only: false
  silver:
    metadata_schema: silver_meta
    data_path: s3://lake/silver/
    read_only: true

compute:
  isolation: process
  max_sessions: 10
  memory_per_session: 8GB
  idle_timeout: 10m

auth:
  method: scram-sha-256
  users:
    - username: analyst
      password_hash: "scram-sha-256$4096:salt:stored_key:server_key"

logging:
  level: debug
  audit: stdout

metrics:
  enabled: true
  port: 9191
"#;
        let config = DuckFlockConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.listen.host, "127.0.0.1");
        assert_eq!(config.listen.port, 5434);
        assert_eq!(config.catalogs.len(), 2);
        assert_eq!(config.compute.isolation, "process");
        assert_eq!(config.compute.max_sessions, 10);
        assert_eq!(config.auth.method, "scram-sha-256");
        assert_eq!(config.auth.users.len(), 1);
        assert_eq!(config.metrics.port, 9191);
    }
}
