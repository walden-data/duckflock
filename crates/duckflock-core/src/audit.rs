use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::DuckFlockError;

/// A query audit log entry.
#[derive(Debug, Clone)]
pub struct QueryAuditEntry {
    pub timestamp: DateTime<Utc>,
    pub session_id: String,
    pub username: String,
    pub query_text: String,
    pub query_type: QueryType,
    pub duration_ms: u64,
    pub rows_affected: Option<u64>,
    pub success: bool,
    pub error: Option<String>,
    pub client_ip: Option<String>,
    pub metadata: HashMap<String, String>,
}

/// Classification of a SQL query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Other,
}

/// Pluggable audit logger.
///
/// DuckFlock ships with:
/// - `NoOpAuditLogger` — does nothing (default)
/// - `StdoutAuditLogger` — JSON log lines to stdout
///
/// Integrators can implement custom loggers (e.g., write to PostgreSQL).
#[async_trait]
pub trait AuditLogger: Send + Sync + 'static {
    async fn log_query(&self, entry: &QueryAuditEntry) -> Result<(), DuckFlockError>;
}

/// Default audit logger that does nothing.
pub struct NoOpAuditLogger;

#[async_trait]
impl AuditLogger for NoOpAuditLogger {
    async fn log_query(&self, _entry: &QueryAuditEntry) -> Result<(), DuckFlockError> {
        Ok(())
    }
}

/// Audit logger that writes JSON to stdout.
pub struct StdoutAuditLogger;

#[async_trait]
impl AuditLogger for StdoutAuditLogger {
    async fn log_query(&self, entry: &QueryAuditEntry) -> Result<(), DuckFlockError> {
        let json = serde_json::to_string(&AuditLogLine {
            timestamp: entry.timestamp.to_rfc3339(),
            session_id: &entry.session_id,
            username: &entry.username,
            query_type: format!("{:?}", entry.query_type),
            duration_ms: entry.duration_ms,
            rows_affected: entry.rows_affected,
            success: entry.success,
            error: entry.error.as_deref(),
        })
        .unwrap_or_default();
        println!("{json}");
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct AuditLogLine<'a> {
    timestamp: String,
    session_id: &'a str,
    username: &'a str,
    query_type: String,
    duration_ms: u64,
    rows_affected: Option<u64>,
    success: bool,
    error: Option<&'a str>,
}
