//! Engine facade — the main entry point for query execution.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use duckflock_core::config::DuckFlockConfig;

use crate::catalog;
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::execute::{self, QueryResult};
use crate::pool::ConnectionPool;

/// The main DuckFlock engine — manages a connection pool and executes queries.
pub struct Engine {
    pool: ConnectionPool,
    config: EngineConfig,
    query_count: AtomicU64,
    active_queries: AtomicU64,
    start_time: Instant,
}

/// RAII guard that increments `active_queries` on creation and decrements on drop.
struct ActiveQueryGuard<'a> {
    counter: &'a AtomicU64,
}

impl<'a> ActiveQueryGuard<'a> {
    fn new(counter: &'a AtomicU64) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveQueryGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Engine {
    /// Create a new engine with the given configuration.
    ///
    /// Initializes the connection pool and attaches all configured catalogs.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        let pool = ConnectionPool::new(&config)?;

        // Attach catalogs on all connections if any are configured
        if !config.catalogs.is_empty() {
            let metadata_conn = config.metadata_connection.as_ref().ok_or_else(|| {
                EngineError::InvalidConfig(
                    "metadata_connection is required when catalogs are configured".to_string(),
                )
            })?;

            let catalogs = config.catalogs.clone();
            let metadata_conn = metadata_conn.clone();

            // We need to attach on all connections - use a blocking approach since
            // this is initialization
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                pool.execute_all(move |conn| {
                    for catalog in &catalogs {
                        catalog::attach_catalog(conn, catalog, &metadata_conn)?;
                    }
                    Ok(())
                })
                .await
            })?;
        }

        Ok(Self {
            pool,
            config,
            query_count: AtomicU64::new(0),
            active_queries: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    /// Create a new engine from a DuckFlock configuration.
    ///
    /// Converts `DuckFlockConfig` to `EngineConfig` and then calls `Engine::new`.
    pub fn from_config(config: &DuckFlockConfig) -> Result<Self, EngineError> {
        let engine_config = EngineConfig::from(config);
        Self::new(engine_config)
    }

    /// Execute a SQL statement.
    ///
    /// Uses the connection pool to run the query and returns a `QueryResult`
    /// with Arrow RecordBatches for data queries.
    pub async fn execute(&self, sql: &str) -> Result<QueryResult, EngineError> {
        let _guard = ActiveQueryGuard::new(&self.active_queries);
        self.query_count.fetch_add(1, Ordering::Relaxed);

        let sql = sql.to_string();
        self.pool
            .execute(move |conn| execute::execute_sql(conn, &sql))
            .await?
    }

    /// Execute a SQL statement and return the result serialized as Arrow IPC.
    pub async fn execute_ipc(&self, sql: &str) -> Result<Vec<u8>, EngineError> {
        let result = self.execute(sql).await?;
        execute::serialize_to_ipc(&result.batches)
    }

    /// Check engine health by executing `SELECT 1`.
    pub async fn health_check(&self) -> Result<bool, EngineError> {
        let result = self.execute("SELECT 1").await?;
        Ok(result.is_data && !result.batches.is_empty())
    }

    /// Query DuckDB's current memory limit setting.
    pub async fn memory_usage_bytes(&self) -> Result<u64, EngineError> {
        let result = self
            .execute("SELECT current_setting('memory_limit')")
            .await?;

        if let Some(batch) = result.batches.first() {
            if batch.num_rows() > 0 {
                let col = batch.column(0);
                if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                    let val = str_arr.value(0);
                    return Ok(parse_memory_string(val));
                }
            }
        }

        Ok(0)
    }

    /// Return the total number of queries executed since engine startup.
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Return the number of currently active queries.
    pub fn active_queries(&self) -> u64 {
        self.active_queries.load(Ordering::Relaxed)
    }

    /// Return the engine uptime.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Return the connection pool size.
    pub fn pool_size(&self) -> usize {
        self.pool.pool_size()
    }

    /// Return a reference to the engine configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}

/// Parse a DuckDB memory string (e.g., "2GB", "512MB", "1TB") to bytes.
fn parse_memory_string(s: &str) -> u64 {
    let s = s.trim().to_uppercase();
    let (num_part, multiplier) = if s.ends_with("TB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
    } else if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else if s.ends_with("B") {
        (&s[..s.len() - 1], 1u64)
    } else {
        (s.as_str(), 1u64)
    };

    let num: f64 = num_part.trim().parse().unwrap_or(0.0);
    (num * multiplier as f64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_string() {
        assert_eq!(parse_memory_string("2GB"), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_string("512MB"), 512 * 1024 * 1024);
        assert_eq!(parse_memory_string("1TB"), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_string("1024"), 1024);
        assert_eq!(parse_memory_string("1KB"), 1024);
    }
}
