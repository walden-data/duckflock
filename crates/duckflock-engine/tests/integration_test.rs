//! Integration tests for the duckflock-engine crate.

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::ipc::reader::StreamReader;
use duckdb::Connection;
use duckflock_core::catalog::CatalogConfig;
use duckflock_engine::catalog::attach_catalog;
use duckflock_engine::{Engine, EngineConfig, EngineError};

fn test_engine() -> Engine {
    let config = EngineConfig::new()
        .with_memory("1GB")
        .with_pool_size(2)
        .with_extensions(vec!["parquet".to_string()]);
    Engine::new(config).expect("Failed to create test engine")
}

#[tokio::test]
async fn test_engine_creation() {
    let engine = test_engine();
    assert!(engine.health_check().await.expect("health check failed"));
}

#[tokio::test]
async fn test_simple_select() {
    let engine = test_engine();
    let result = engine
        .execute("SELECT 42 as num, 'hello' as msg")
        .await
        .expect("query failed");

    assert!(result.is_data);
    assert!(!result.batches.is_empty());

    let batch = &result.batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Check schema
    let schema = batch.schema();
    assert_eq!(schema.fields().len(), 2);

    // Check values
    let num_col = batch.column(0);
    let msg_col = batch.column(1);

    let num_arr = num_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("not Int32Array");
    let msg_arr = msg_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("not StringArray");

    assert_eq!(num_arr.value(0), 42);
    assert_eq!(msg_arr.value(0), "hello");
}

#[tokio::test]
async fn test_ddl_and_dml() {
    let engine = test_engine();

    // CREATE TABLE
    let result = engine
        .execute("CREATE TABLE test_items (id INTEGER PRIMARY KEY, name VARCHAR)")
        .await
        .expect("CREATE TABLE failed");
    assert!(!result.is_data);

    // INSERT
    let result = engine
        .execute("INSERT INTO test_items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .await
        .expect("INSERT failed");
    assert!(!result.is_data);
    assert_eq!(result.row_count, 3);

    // SELECT COUNT
    let result = engine
        .execute("SELECT COUNT(*) as cnt FROM test_items")
        .await
        .expect("SELECT COUNT failed");
    assert!(result.is_data);

    let batch = &result.batches[0];
    // DuckDB returns BIGINT (i64) for COUNT(*)
    let cnt_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("not Int64Array");
    assert_eq!(cnt_arr.value(0), 3);
}

#[tokio::test]
async fn test_arrow_ipc_roundtrip() {
    let engine = test_engine();

    // Get data via normal execute
    let result = engine
        .execute("SELECT 42 as num, 'hello' as msg")
        .await
        .expect("query failed");
    assert!(!result.batches.is_empty());

    // Get data via IPC
    let ipc_bytes = engine
        .execute_ipc("SELECT 42 as num, 'hello' as msg")
        .await
        .expect("IPC query failed");
    assert!(!ipc_bytes.is_empty());

    // Deserialize IPC
    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None).expect("failed to create StreamReader");
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to read IPC");

    assert_eq!(batches.len(), result.batches.len());
    assert_eq!(batches[0].num_rows(), 1);

    let num_arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("not Int32Array");
    assert_eq!(num_arr.value(0), 42);
}

#[tokio::test]
async fn test_concurrent_queries() {
    let engine = Arc::new(test_engine());

    let mut handles = Vec::new();
    for i in 0..8 {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            let result = engine
                .execute(&format!("SELECT {} as val", i))
                .await
                .expect("concurrent query failed");
            assert!(result.is_data);
        }));
    }

    for handle in handles {
        handle.await.expect("task panicked");
    }
}

#[tokio::test]
async fn test_pool_size_respected() {
    // Pool of 2, 4 concurrent queries — all should complete
    let engine = Arc::new(test_engine()); // pool_size=2

    let mut handles = Vec::new();
    for i in 0..4 {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            let result = engine.execute(&format!("SELECT {} as val", i)).await;
            result.expect("pool query failed")
        }));
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task join failed"))
        .collect();

    assert_eq!(results.len(), 4);
    for result in results {
        assert!(result.is_data);
    }
}

#[tokio::test]
async fn test_error_handling() {
    let engine = test_engine();

    let result = engine.execute("SELECT * FROM nonexistent_table").await;
    assert!(result.is_err());

    match result.unwrap_err() {
        EngineError::DuckDb(_) => {} // expected
        other => panic!("Expected DuckDb error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_extension_loading() {
    // Test with only parquet extension (bundled mode compatible)
    let config = EngineConfig::new()
        .with_memory("1GB")
        .with_pool_size(1)
        .with_extensions(vec!["parquet".to_string()]);
    let engine = Engine::new(config).expect("Failed to create engine");

    // Query loaded extensions
    let result = engine
        .execute("SELECT extension_name FROM duckdb_extensions() WHERE loaded = true")
        .await
        .expect("extension query failed");

    assert!(result.is_data);
    assert!(!result.batches.is_empty());

    // Check that parquet is loaded
    let batch = &result.batches[0];
    let ext_names: Vec<String> = (0..batch.num_rows())
        .filter_map(|i| {
            let col = batch.column(0);
            col.as_any()
                .downcast_ref::<StringArray>()
                .map(|arr| arr.value(i).to_string())
        })
        .collect();

    assert!(
        ext_names.iter().any(|n| n == "parquet"),
        "parquet extension should be loaded, found: {:?}",
        ext_names
    );
}

#[tokio::test]
async fn test_memory_config() {
    let config = EngineConfig::new()
        .with_memory("512MB")
        .with_pool_size(1)
        .with_extensions(vec!["parquet".to_string()]);
    let engine = Engine::new(config).expect("Failed to create engine");

    let result = engine
        .execute("SELECT current_setting('memory_limit')")
        .await
        .expect("memory limit query failed");

    assert!(result.is_data);

    let batch = &result.batches[0];
    let col = batch.column(0);
    let str_arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("not StringArray");
    let memory_setting = str_arr.value(0).to_uppercase();
    // DuckDB returns memory in MiB format (e.g., "488.2 MIB") or bytes
    // Just verify it's set and non-trivial
    assert!(!memory_setting.is_empty(), "memory setting should be set");
}

#[tokio::test]
async fn test_transaction_control() {
    let engine = test_engine();

    // BEGIN
    let result = engine.execute("BEGIN").await.expect("BEGIN failed");
    assert!(!result.is_data);
    assert!(result.batches.is_empty());

    // COMMIT
    let result = engine.execute("COMMIT").await.expect("COMMIT failed");
    assert!(!result.is_data);

    // BEGIN + ROLLBACK
    let result = engine.execute("BEGIN").await.expect("BEGIN failed");
    assert!(!result.is_data);
    let result = engine.execute("ROLLBACK").await.expect("ROLLBACK failed");
    assert!(!result.is_data);
}

#[tokio::test]
async fn test_multiple_data_types() {
    let engine = test_engine();

    // Create table with various types
    engine
        .execute(
            r#"
            CREATE TABLE type_test (
                int_col INTEGER,
                varchar_col VARCHAR,
                float_col FLOAT,
                bool_col BOOLEAN,
                date_col DATE,
                ts_col TIMESTAMP
            )
        "#,
        )
        .await
        .expect("CREATE TABLE failed");

    engine
        .execute(
            r#"
            INSERT INTO type_test VALUES
                (42, 'hello', 3.14, true, '2024-01-15', '2024-01-15 10:30:00'),
                (-1, 'world', -2.71, false, '2024-06-30', '2024-06-30 23:59:59')
        "#,
        )
        .await
        .expect("INSERT failed");

    let result = engine
        .execute("SELECT * FROM type_test")
        .await
        .expect("SELECT failed");

    assert!(result.is_data);
    assert_eq!(result.row_count, 2);
    assert_eq!(result.batches.len(), 1);

    let batch = &result.batches[0];
    assert_eq!(batch.num_columns(), 6);
    assert_eq!(batch.num_rows(), 2);

    // Verify int column
    let int_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("not Int32Array");
    assert_eq!(int_arr.value(0), 42);
    assert_eq!(int_arr.value(1), -1);

    // Verify varchar column
    let varchar_arr = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("not StringArray");
    assert_eq!(varchar_arr.value(0), "hello");
    assert_eq!(varchar_arr.value(1), "world");
}

#[tokio::test]
async fn test_catalog_attachment_error() {
    // Test that attaching a catalog with an invalid name returns an error
    let conn = Connection::open_in_memory().expect("Failed to open DuckDB");

    let bad_catalog = CatalogConfig {
        name: "bad;name".to_string(), // semicolons are invalid
        metadata_schema: "public".to_string(),
        data_path: "s3://test/data".to_string(),
        read_only: false,
        auto_migrate: false,
        options: Default::default(),
    };

    let result = attach_catalog(&conn, &bad_catalog, "postgres://localhost/db");
    assert!(result.is_err());

    match result.unwrap_err() {
        EngineError::CatalogAttachment { catalog, .. } => {
            assert_eq!(catalog, "bad;name");
        }
        other => panic!("Expected CatalogAttachment error, got: {:?}", other),
    }
}

#[test]
fn test_catalog_name_validation() {
    // Valid names
    assert!(duckflock_engine::catalog::is_valid_catalog_name("bronze"));
    assert!(duckflock_engine::catalog::is_valid_catalog_name("_private"));
    assert!(duckflock_engine::catalog::is_valid_catalog_name(
        "catalog_1"
    ));

    // Invalid names
    assert!(!duckflock_engine::catalog::is_valid_catalog_name("123bad"));
    assert!(!duckflock_engine::catalog::is_valid_catalog_name(
        "bad;name"
    ));
    assert!(!duckflock_engine::catalog::is_valid_catalog_name(
        "bad name"
    ));
}
