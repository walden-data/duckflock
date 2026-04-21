//! Query execution logic with SQL classification.

use std::io::Cursor;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use duckdb::Connection;

use crate::error::EngineError;

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    /// Number of rows affected (for DML) or returned (for queries).
    pub row_count: u64,
    /// Arrow RecordBatches (populated for data queries).
    pub batches: Vec<RecordBatch>,
    /// Wall-clock execution time.
    pub execution_time: Duration,
    /// Whether this result contains data batches.
    pub is_data: bool,
}

impl QueryResult {
    /// Create an empty result (e.g., for DDL or transaction control).
    pub fn empty(duration: Duration) -> Self {
        Self {
            row_count: 0,
            batches: Vec::new(),
            execution_time: duration,
            is_data: false,
        }
    }
}

/// Classify a SQL statement by its first keyword.
fn classify_sql(sql: &str) -> &'static str {
    let trimmed = sql.trim();
    let first_word = trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_uppercase();

    // Remove semicolons
    let first_word = first_word.trim_end_matches(';');

    match first_word {
        "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "END" => "transaction",
        "SELECT" | "WITH" | "DESCRIBE" | "EXPLAIN" | "SHOW" | "VALUES" | "TABLE" | "FROM" => "data",
        "CREATE" | "DROP" | "ALTER" | "INSERT" | "UPDATE" | "DELETE" | "ATTACH" | "DETACH"
        | "COPY" | "USE" | "SET" | "RESET" | "PRAGMA" | "EXPORT" | "IMPORT" | "VACUUM"
        | "CHECKPOINT" | "INSTALL" | "LOAD" => "non_query",
        _ => "unknown",
    }
}

/// Execute a SQL statement on a DuckDB connection.
///
/// Classifies the SQL by its first keyword and routes to the appropriate
/// execution method (batch, execute, or query_arrow).
pub fn execute_sql(conn: &Connection, sql: &str) -> Result<QueryResult, EngineError> {
    let start = std::time::Instant::now();

    let category = classify_sql(sql);
    tracing::debug!(
        "Executing SQL [{}]: {}",
        category,
        sql.split_whitespace().take(5).collect::<Vec<_>>().join(" ")
    );

    match category {
        "transaction" => {
            conn.execute_batch(sql)?;
            Ok(QueryResult::empty(start.elapsed()))
        }
        "non_query" => {
            let rows = conn.execute(sql, [])?;
            Ok(QueryResult {
                row_count: rows as u64,
                batches: Vec::new(),
                execution_time: start.elapsed(),
                is_data: false,
            })
        }
        "data" => {
            let mut stmt = conn.prepare(sql)?;
            let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
            let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            Ok(QueryResult {
                row_count,
                batches,
                execution_time: start.elapsed(),
                is_data: true,
            })
        }
        _ => {
            // Try as non-query first, fall back to data query
            match conn.execute(sql, []) {
                Ok(rows) => Ok(QueryResult {
                    row_count: rows as u64,
                    batches: Vec::new(),
                    execution_time: start.elapsed(),
                    is_data: false,
                }),
                Err(_) => {
                    // Retry as data query
                    let mut stmt = conn.prepare(sql)?;
                    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
                    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    Ok(QueryResult {
                        row_count,
                        batches,
                        execution_time: start.elapsed(),
                        is_data: true,
                    })
                }
            }
        }
    }
}

/// Serialize Arrow RecordBatches to IPC format.
///
/// Returns an empty `Vec<u8>` if there are no batches.
pub fn serialize_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, EngineError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    let mut writer = StreamWriter::try_new(&mut cursor, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    drop(writer);

    Ok(buffer)
}
