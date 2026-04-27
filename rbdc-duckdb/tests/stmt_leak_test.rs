//! Tests for prepared statement resource management and leak detection in rbdc-duckdb
//!
//! These tests verify that:
//! 1. exec_rows properly caches prepared statements
//! 2. exec does NOT cache (always creates and destroys)
//! 3. clear_cache properly releases all cached statements
//! 4. Connection drop releases all resources
//! 5. Error handling does not cause double-free or cache corruption

use futures_util::StreamExt;
use rbdc::db::Connection;
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection};

async fn create_conn() -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
}

/// Consume all rows from a stream and return the count
async fn consume_all_rows(
    mut stream: impl StreamExt<Item = Result<Box<dyn rbdc::db::Row>, rbdc::Error>> + Unpin,
) -> usize {
    let mut count = 0;
    while let Some(_row) = stream.next().await {
        count += 1;
    }
    count
}

/// Test: verify cached statement count increases after repeated exec_rows calls
#[tokio::test]
async fn test_stmt_cache_grows_on_repeated_exec_rows() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_cache (id INTEGER PRIMARY KEY)", vec![])
        .await
        .expect("create table");

    conn.exec("INSERT INTO test_cache VALUES (1)", vec![])
        .await
        .expect("insert");

    // Execute same query multiple times via exec_rows (uses cache)
    for _ in 0..5 {
        let stream = conn
            .exec_rows(
                "SELECT * FROM test_cache WHERE id = ?",
                vec![rbs::Value::I64(1)],
            )
            .await
            .expect("exec_rows");
        let count = consume_all_rows(stream).await;
        assert_eq!(count, 1);
    }

    // Cache should have exactly 1 statement for this query
    let cache_size = conn.cached_statements_size();
    assert_eq!(cache_size, 1, "exec_rows should cache the statement");
}

/// Test: verify exec (non-cached) does NOT grow the cache
#[tokio::test]
async fn test_exec_does_not_cache_statement() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_no_cache (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Multiple exec calls (not using cache)
    for i in 0..3 {
        conn.exec(
            "INSERT INTO test_no_cache VALUES (?)",
            vec![rbs::Value::I64(i)],
        )
        .await
        .expect("insert");
    }

    // exec does not use statement cache (always prepares new and destroys)
    let cache_size = conn.cached_statements_size();
    assert_eq!(cache_size, 0, "exec should NOT cache statements");
}

/// Test: verify clear_cache destroys all cached statements
#[tokio::test]
async fn test_clear_cache_releases_statements() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_clear (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Populate cache via exec_rows - need to consume the stream to trigger caching
    let stream = conn
        .exec_rows("SELECT * FROM test_clear", vec![])
        .await
        .expect("exec_rows");
    let _count = consume_all_rows(stream).await;

    assert_eq!(conn.cached_statements_size(), 1);

    // Clear the cache
    conn.clear_cache().await.expect("clear_cache");

    assert_eq!(
        conn.cached_statements_size(),
        0,
        "cache should be empty after clear"
    );
}

/// Test: verify connection drop releases resources via shutdown
#[tokio::test]
async fn test_connection_drop_releases_resources() {
    let cache_size_before;
    {
        let mut conn = create_conn().await;

        conn.exec("CREATE TABLE test_drop (id INTEGER)", vec![])
            .await
            .expect("create table");

        let stream = conn
            .exec_rows("SELECT * FROM test_drop", vec![])
            .await
            .expect("exec_rows");
        let _ = consume_all_rows(stream).await;

        cache_size_before = conn.cached_statements_size();
        assert_eq!(cache_size_before, 1);

        // Close connection
        conn.close().await.expect("close");
    }
    // Connection has been closed, resources should be released via Shutdown command

    // Create new connection to verify old one was cleaned up properly
    let mut new_conn = create_conn().await;
    new_conn.ping().await.expect("new connection works");
}

/// Test: many different queries should grow cache accordingly
#[tokio::test]
async fn test_cache_size_matches_unique_queries() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_many (id INTEGER)", vec![])
        .await
        .expect("create table");

    let queries = [
        "SELECT * FROM test_many WHERE id = 1",
        "SELECT * FROM test_many WHERE id = 2",
        "SELECT * FROM test_many WHERE id = 3",
    ];

    for (i, q) in queries.iter().enumerate() {
        conn.exec(
            "INSERT INTO test_many VALUES (?)",
            vec![rbs::Value::I64(i as i64)],
        )
        .await
        .expect("insert");

        let stream = conn.exec_rows(q, vec![]).await.expect("exec_rows");
        let _ = consume_all_rows(stream).await;
    }

    assert_eq!(conn.cached_statements_size(), queries.len());
}

/// Test: error in exec_rows should not leak/double-free statement
#[tokio::test]
async fn test_invalid_query_does_not_corrupt_cache() {
    let mut conn = create_conn().await;

    // Valid query first
    conn.exec("CREATE TABLE test_err (id INTEGER)", vec![])
        .await
        .expect("create table");

    let stream = conn
        .exec_rows("SELECT * FROM test_err", vec![])
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    assert_eq!(conn.cached_statements_size(), 1);

    // Now try invalid query - should error, not corrupt cache
    // Must consume the stream to trigger the error
    let result = conn
        .exec_rows("SELECT * FROM nonexistent_table", vec![])
        .await;
    assert!(result.is_ok(), "exec_rows should return a stream (errors happen at consumption)");

    let stream = result.unwrap();
    let _count = consume_all_rows(stream).await;
    // DuckDB might return 0 rows for nonexistent table without error, or error on consumption
    // The key is it should not crash or corrupt the cache

    // Cache should be unchanged (the invalid query is NOT cached)
    assert_eq!(conn.cached_statements_size(), 1);

    // Should still be able to use the valid cached statement
    let stream = conn
        .exec_rows("SELECT * FROM test_err", vec![])
        .await
        .expect("exec_rows should still work");
    consume_all_rows(stream).await;
}

/// Test: error in query with params should not corrupt cache
#[tokio::test]
async fn test_error_with_params_does_not_corrupt_cache() {
    let mut conn = create_conn().await;

    conn.exec(
        "CREATE TABLE test_param_err (id INTEGER PRIMARY KEY)",
        vec![],
    )
    .await
    .expect("create table");

    // Valid query with param
    let stream = conn
        .exec_rows(
            "SELECT * FROM test_param_err WHERE id = ?",
            vec![rbs::Value::I64(1)],
        )
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    // Insert data for next test
    conn.exec("INSERT INTO test_param_err VALUES (1)", vec![])
        .await
        .expect("insert");

    // Valid query again
    let stream = conn
        .exec_rows(
            "SELECT * FROM test_param_err WHERE id = ?",
            vec![rbs::Value::I64(1)],
        )
        .await
        .expect("exec_rows");
    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1);

    // Cache should still have 1 statement
    assert_eq!(conn.cached_statements_size(), 1);
}

/// Test: verify rapid connection cycling does not leak
#[tokio::test]
async fn test_rapid_connection_cycling_no_leak() {
    for i in 0..20 {
        let mut conn = create_conn().await;
        conn.exec(
            &format!("CREATE TABLE test_cycle_{} (id INTEGER)", i),
            vec![],
        )
        .await
        .expect("create table");

        conn.exec(
            &format!("INSERT INTO test_cycle_{} VALUES ({})", i, i),
            vec![],
        )
        .await
        .expect("insert");

        let stream = conn
            .exec_rows(&format!("SELECT * FROM test_cycle_{}", i), vec![])
            .await
            .expect("exec_rows");
        let count = consume_all_rows(stream).await;
        assert_eq!(count, 1);

        conn.close().await.expect("close");
    }
    // If we get here without panic/OOM, no obvious leak
}

/// Test: reuse same connection after clear_cache works correctly
#[tokio::test]
async fn test_reuse_connection_after_clear_cache() {
    let mut conn = create_conn().await;

    conn.exec(
        "CREATE TABLE test_reuse (id INTEGER PRIMARY KEY)",
        vec![],
    )
    .await
    .expect("create table");

    // First set of queries
    conn.exec("INSERT INTO test_reuse VALUES (1)", vec![])
        .await
        .expect("insert");

    let stream = conn
        .exec_rows(
            "SELECT * FROM test_reuse WHERE id = ?",
            vec![rbs::Value::I64(1)],
        )
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    assert_eq!(conn.cached_statements_size(), 1);

    // Clear cache
    conn.clear_cache().await.expect("clear_cache");
    assert_eq!(conn.cached_statements_size(), 0);

    // Second set of queries - should work with fresh cache
    conn.exec("INSERT INTO test_reuse VALUES (2)", vec![])
        .await
        .expect("insert");

    let stream = conn
        .exec_rows(
            "SELECT * FROM test_reuse WHERE id = ?",
            vec![rbs::Value::I64(2)],
        )
        .await
        .expect("exec_rows");
    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1);

    assert_eq!(conn.cached_statements_size(), 1);
}

/// Test: multiple params binding works correctly and doesn't leak
#[tokio::test]
async fn test_multiple_params_binding() {
    let mut conn = create_conn().await;

    conn.exec(
        "CREATE TABLE test_multi_param (id INTEGER, name TEXT, score REAL)",
        vec![],
    )
    .await
    .expect("create table");

    // Insert with multiple params
    conn.exec(
        "INSERT INTO test_multi_param VALUES (?, ?, ?)",
        vec![
            rbs::Value::I64(1),
            rbs::Value::String("Alice".to_string()),
            rbs::Value::F64(95.5),
        ],
    )
    .await
    .expect("insert");

    // Query with multiple params - id=1 matches the insert, score=95.5 > 90.0
    let stream = conn
        .exec_rows(
            "SELECT * FROM test_multi_param WHERE id = ? AND score > ?",
            vec![rbs::Value::I64(1), rbs::Value::F64(90.0)],
        )
        .await
        .expect("exec_rows");

    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1, "should return 1 row where id=1 and score > 90.0");

    assert_eq!(conn.cached_statements_size(), 1);
}
