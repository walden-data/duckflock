//! DuckDB connection pool with semaphore-based concurrency control.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::Mutex;

use duckdb::Connection;
use tokio::sync::Semaphore;
use tracing;

use crate::config::EngineConfig;
use crate::error::EngineError;

/// A pool of DuckDB connections with semaphore-based concurrency control.
///
/// The pool creates `pool_size` connections at startup (one primary + N-1 clones).
/// Each `execute` call acquires a semaphore permit, takes a connection from the
/// pool, runs the closure, and returns the connection to the pool.
pub struct ConnectionPool {
    connections: Arc<Mutex<Vec<Connection>>>,
    semaphore: Arc<Semaphore>,
    pool_size: usize,
}

/// Validate an extension name: must start with a letter or underscore,
/// followed by letters, digits, or underscores.
fn is_valid_extension_name(name: &str) -> bool {
    let re = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
    re.is_match(name)
}

/// Initialize a DuckDB connection with the given engine configuration.
///
/// Sets threads, memory limit, temp directory, installs/loads extensions,
/// and configures S3 settings if provided.
fn initialize_connection(conn: &Connection, config: &EngineConfig) -> Result<(), EngineError> {
    // Set worker threads
    if config.worker_threads > 0 {
        conn.execute_batch(&format!("SET threads={}", config.worker_threads))?;
    }

    // Set memory limit
    conn.execute_batch(&format!("SET memory_limit='{}'", config.max_memory))?;

    // Set temp directory
    conn.execute_batch(&format!("SET temp_directory='{}'", config.temp_directory))?;

    // Disable insertion order preservation for performance
    conn.execute_batch("SET preserve_insertion_order=false")?;

    // Disable progress bar
    conn.execute_batch("SET enable_progress_bar=false")?;

    // Install and load extensions
    for ext in &config.extensions {
        if !is_valid_extension_name(ext) {
            tracing::warn!("Skipping invalid extension name: {}", ext);
            continue;
        }
        tracing::debug!("Installing extension: {}", ext);
        if let Err(e) = conn.execute_batch(&format!("INSTALL {}", ext)) {
            tracing::warn!("Failed to install extension '{}': {}", ext, e);
            continue;
        }
        tracing::debug!("Loading extension: {}", ext);
        if let Err(e) = conn.execute_batch(&format!("LOAD {}", ext)) {
            tracing::warn!("Failed to load extension '{}': {}", ext, e);
            continue;
        }
        tracing::info!("Extension {} loaded successfully", ext);
    }

    // Configure S3 settings if provided
    if let Some(ref s3) = config.s3 {
        conn.execute_batch(&format!("SET s3_region='{}'", s3.region))?;
        conn.execute_batch(&format!("SET s3_endpoint='{}'", s3.endpoint))?;

        if let Some(ref key) = s3.access_key_id {
            conn.execute_batch(&format!("SET s3_access_key_id='{}'", key))?;
        }
        if let Some(ref secret) = s3.secret_access_key {
            // Redact in logs but still set the value
            tracing::info!("Setting s3_secret_access_key='[REDACTED]'");
            conn.execute_batch(&format!("SET s3_secret_access_key='{}'", secret))?;
        }

        let ssl_val = if s3.use_ssl { "true" } else { "false" };
        conn.execute_batch(&format!("SET s3_use_ssl={}", ssl_val))?;
        conn.execute_batch(&format!("SET s3_url_style='{}'", s3.url_style))?;
        conn.execute_batch("SET s3_url_compatibility_mode=true")?;

        tracing::info!(
            "S3 configured: endpoint={}, region={}, url_style={}, ssl={}",
            s3.endpoint,
            s3.region,
            s3.url_style,
            s3.use_ssl
        );
    }

    Ok(())
}

impl ConnectionPool {
    /// Create a new connection pool from the given engine configuration.
    ///
    /// Opens a primary DuckDB connection, initializes it with configuration,
    /// then clones it `pool_size - 1` times to fill the pool.
    pub fn new(config: &EngineConfig) -> Result<Self, EngineError> {
        // Open the primary connection
        let primary = if config.database_path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(&config.database_path)?
        };

        // Initialize the primary connection
        initialize_connection(&primary, config)?;

        // Clone for the rest of the pool
        let mut connections = Vec::with_capacity(config.pool_size);
        connections.push(primary);

        for i in 1..config.pool_size {
            match connections[0].try_clone() {
                Ok(clone) => {
                    connections.push(clone);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to clone connection {}/{}: {}. Pool will be smaller.",
                        i + 1,
                        config.pool_size,
                        e
                    );
                }
            }
        }

        let actual_size = connections.len();
        tracing::info!(
            "Connection pool created: {}/{} connections",
            actual_size,
            config.pool_size
        );

        Ok(Self {
            connections: Arc::new(Mutex::new(connections)),
            semaphore: Arc::new(Semaphore::new(actual_size)),
            pool_size: actual_size,
        })
    }

    /// Execute a closure on a pooled connection.
    ///
    /// Acquires a semaphore permit, takes a connection from the pool,
    /// runs the closure in a blocking task, and returns the connection.
    /// Catches panics to prevent connection loss.
    pub async fn execute<F, R>(&self, f: F) -> Result<R, EngineError>
    where
        F: FnOnce(&Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        let semaphore = self.semaphore.clone();
        let _permit = semaphore
            .acquire_owned()
            .await
            .map_err(|_| EngineError::PoolExhausted)?;

        let connections = self.connections.clone();

        let result = tokio::task::spawn_blocking(move || {
            // Get a connection from the pool
            let conn = {
                let mut pool = connections.lock().expect("pool mutex poisoned");
                pool.pop()
            };

            if let Some(conn) = conn {
                // Run the closure, catching panics
                let result = catch_unwind(AssertUnwindSafe(|| f(&conn)));

                // Always return the connection to the pool
                {
                    let mut pool = connections.lock().expect("pool mutex poisoned");
                    pool.push(conn);
                }

                result.map_err(|panic_info| {
                    let msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "unknown panic".to_string()
                    };
                    EngineError::Internal(format!("query panicked: {}", msg))
                })
            } else {
                // No connection available despite having a permit — shouldn't happen
                Err(EngineError::PoolExhausted)
            }
        })
        .await
        .map_err(|e| EngineError::Internal(format!("spawn_blocking task failed: {}", e)))??;

        Ok(result)
    }

    /// Execute a closure on ALL connections in the pool.
    ///
    /// Acquires ALL semaphore permits, then runs the closure on each
    /// connection sequentially. Useful for operations that need to
    /// affect every connection (e.g., attaching catalogs).
    pub async fn execute_all<F>(&self, f: F) -> Result<(), EngineError>
    where
        F: Fn(&Connection) -> Result<(), EngineError> + Send + 'static,
    {
        // Acquire all permits as owned permits (so they're 'static)
        let mut permits = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            permits.push(
                self.semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| EngineError::PoolExhausted)?,
            );
        }

        let connections = self.connections.clone();

        tokio::task::spawn_blocking(move || -> Result<(), EngineError> {
            let pool = connections.lock().expect("pool mutex poisoned");
            for conn in pool.iter() {
                f(conn)?;
            }
            drop(permits);
            Ok(())
        })
        .await
        .map_err(|e| EngineError::Internal(format!("spawn_blocking task failed: {}", e)))??;

        Ok(())
    }

    /// Return the configured pool size.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }
}
