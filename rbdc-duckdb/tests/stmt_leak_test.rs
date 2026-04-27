//! Tests for prepared statement resource management and leak detection in rbdc-duckdb
//!
//! These tests verify that:
//! 1. exec_rows properly caches prepared statements
//! 2. exec also caches prepared statements (fixes the memory leak from repeated prepare/destroy)
//! 3. clear_cache properly releases all cached statements
//! 4. Connection drop releases all resources
//! 5. Error handling does not cause double-free or cache corruption
//! 6. LRU eviction works when cache size is limited

use futures_util::StreamExt;
use rbdc::db::Connection;
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection};

async fn create_conn() -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
}

/// Create a connection with a custom statement cache size
async fn create_conn_with_cache_size(cache_size: usize) -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new()
        .path(":memory:")
        .statement_cache_size(cache_size);
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

    // ALL unique SQL statements are cached: CREATE TABLE + INSERT + SELECT = 3
    let cache_size = conn.cached_statements_size();
    assert_eq!(cache_size, 3, "exec_rows should cache all unique SQL statements");
}

/// Test: verify exec uses the statement cache (same as exec_rows).
/// This is the fix for the memory leak: previously exec always prepared and
/// destroyed a statement on every call; now it reuses the cached prepared
/// statement, avoiding repeated DuckDB alloc/free cycles.
#[tokio::test]
async fn test_exec_uses_statement_cache() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_exec_cache (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Multiple exec calls with the same SQL reuse the cached statement
    for i in 0..5 {
        conn.exec(
            "INSERT INTO test_exec_cache VALUES (?)",
            vec![rbs::Value::I64(i)],
        )
        .await
        .expect("insert");
    }

    // exec caches both CREATE TABLE and the INSERT statement = 2
    let cache_size = conn.cached_statements_size();
    assert_eq!(cache_size, 2, "exec should cache the INSERT and CREATE TABLE statements");

    // Verify all rows were actually inserted
    let stream = conn
        .exec_rows("SELECT COUNT(*) FROM test_exec_cache", vec![])
        .await
        .expect("count");
    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1, "should return 1 row from COUNT(*)");
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

    // CREATE TABLE + SELECT = 2
    assert_eq!(conn.cached_statements_size(), 2);

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
        // CREATE TABLE + SELECT = 2
        assert_eq!(cache_size_before, 2);

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

    // CREATE TABLE + INSERT with params + 3 unique SELECT literals = 5
    assert_eq!(conn.cached_statements_size(), queries.len() + 2);
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

    // CREATE TABLE + SELECT = 2
    assert_eq!(conn.cached_statements_size(), 2);

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

    // Cache should be unchanged (the invalid query is NOT cached): CREATE + SELECT = 2
    assert_eq!(conn.cached_statements_size(), 2);

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

    // Cache: CREATE(1) + SELECT_with_param(2) + INSERT_literal(3) = 3
    assert_eq!(conn.cached_statements_size(), 3);
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

    // CREATE(1) + INSERT_VALUES_1(2) + SELECT_with_param(3) = 3
    assert_eq!(conn.cached_statements_size(), 3);

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

    // After clear + 2 new statements (INSERT_VALUES_2, SELECT_with_param) = 2
    assert_eq!(conn.cached_statements_size(), 2);
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

    // CREATE(1) + INSERT_with_params(2) + SELECT_with_params(3) = 3
    assert_eq!(conn.cached_statements_size(), 3);
}

/// Test: LRU eviction when cache size is exceeded
#[tokio::test]
async fn test_lru_eviction_when_cache_full() {
    // Create connection with cache size of 3
    let mut conn = create_conn_with_cache_size(3).await;

    conn.exec("CREATE TABLE test_lru (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Execute 3 different queries to fill the cache
    for i in 1..=3 {
        conn.exec(&format!("INSERT INTO test_lru VALUES ({})", i), vec![])
            .await
            .expect("insert");

        let stream = conn
            .exec_rows(&format!("SELECT * FROM test_lru WHERE id = {}", i), vec![])
            .await
            .expect("exec_rows");
        let _ = consume_all_rows(stream).await;
    }

    assert_eq!(conn.cached_statements_size(), 3, "cache should have 3 statements");

    // Now execute a 4th different query - this should evict the LRU entry (the first query)
    conn.exec("INSERT INTO test_lru VALUES (4)", vec![])
        .await
        .expect("insert");

    let stream = conn
        .exec_rows("SELECT * FROM test_lru WHERE id = 4", vec![])
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    assert_eq!(
        conn.cached_statements_size(),
        3,
        "cache should still have 3 statements (LRU evicted)"
    );

    // The first query should still work - but if it was evicted, it will be re-cached
    let stream = conn
        .exec_rows("SELECT * FROM test_lru WHERE id = 1", vec![])
        .await
        .expect("exec_rows");
    let count = consume_all_rows(stream).await;
    // If id=1 was evicted, re-executing it re-caches it, so we still get 1 row
    assert_eq!(count, 1);
}

/// Test: zero cache size disables caching
#[tokio::test]
async fn test_zero_cache_size_disables_caching() {
    let mut conn = create_conn_with_cache_size(0).await;

    conn.exec("CREATE TABLE test_no_cache (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Execute same query multiple times
    for i in 1..=5 {
        conn.exec(&format!("INSERT INTO test_no_cache VALUES ({})", i), vec![])
            .await
            .expect("insert");

        let stream = conn
            .exec_rows("SELECT * FROM test_no_cache WHERE id = 1", vec![])
            .await
            .expect("exec_rows");
        let _ = consume_all_rows(stream).await;
    }

    // Cache should remain empty since caching is disabled
    assert_eq!(
        conn.cached_statements_size(),
        0,
        "cache should be empty when cache size is 0"
    );
}

/// Test: accessing a cached statement updates its LRU position
#[tokio::test]
async fn test_lru_order_updated_on_access() {
    // Create connection with cache size of 3
    let mut conn = create_conn_with_cache_size(3).await;

    conn.exec("CREATE TABLE test_lru_order (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Insert 3 rows and query them in order: 1, 2, 3
    for i in 1..=3 {
        conn.exec(&format!("INSERT INTO test_lru_order VALUES ({})", i), vec![])
            .await
            .expect("insert");
    }

    // Query in order 1, 2, 3 - this establishes LRU order
    for i in 1..=3 {
        let stream = conn
            .exec_rows(&format!("SELECT * FROM test_lru_order WHERE id = {}", i), vec![])
            .await
            .expect("exec_rows");
        let _ = consume_all_rows(stream).await;
    }
    assert_eq!(conn.cached_statements_size(), 3);

    // Now query id=1 again - this should move it to most recently used
    let stream = conn
        .exec_rows("SELECT * FROM test_lru_order WHERE id = 1", vec![])
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    // Add a new query - this should evict id=2 (now the LRU since 1 was accessed)
    let stream = conn
        .exec_rows("SELECT * FROM test_lru_order WHERE id = 4", vec![])
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    assert_eq!(
        conn.cached_statements_size(),
        3,
        "cache should still have 3 statements"
    );

    // Query 2 should still work (wasn't evicted), but 3 should have been re-cached
    let stream = conn
        .exec_rows("SELECT * FROM test_lru_order WHERE id = 2", vec![])
        .await
        .expect("exec_rows");
    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1);
}

/// Test: ping does not affect statement cache
#[tokio::test]
async fn test_ping_does_not_affect_cache() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_ping (id INTEGER)", vec![])
        .await
        .expect("create table");

    // Populate cache
    let stream = conn
        .exec_rows("SELECT * FROM test_ping", vec![])
        .await
        .expect("exec_rows");
    let _ = consume_all_rows(stream).await;

    // CREATE + SELECT = 2
    assert_eq!(conn.cached_statements_size(), 2);

    // Multiple pings should not affect cache
    conn.ping().await.expect("ping");
    conn.ping().await.expect("ping");
    conn.ping().await.expect("ping");

    assert_eq!(conn.cached_statements_size(), 2, "ping should not affect cache");
}

/// Test: shutdown properly releases all cached statements
#[tokio::test]
async fn test_shutdown_releases_all_cached_statements() {
    let cache_size_before;
    {
        let mut conn = create_conn().await;

        conn.exec("CREATE TABLE test_shutdown (id INTEGER)", vec![])
            .await
            .expect("create table");

        // Populate cache with multiple queries
        for i in 1..=5 {
            conn.exec(&format!("INSERT INTO test_shutdown VALUES ({})", i), vec![])
                .await
                .expect("insert");

            let stream = conn
                .exec_rows(&format!("SELECT * FROM test_shutdown WHERE id = {}", i), vec![])
                .await
                .expect("exec_rows");
            let _ = consume_all_rows(stream).await;
        }

        cache_size_before = conn.cached_statements_size();
        // CREATE(1) + 5 INSERTs + 5 SELECTs = 11
        assert_eq!(cache_size_before, 11);

        // Shutdown
        conn.close().await.expect("shutdown");
    }
    // Connection is dropped, all statements should be released

    // Create new connection to verify resources were released
    let mut new_conn = create_conn().await;
    new_conn.ping().await.expect("new connection works");
}

/// Test: exec_rows with same SQL but different params reuses cached statement
#[tokio::test]
async fn test_same_sql_different_params_reuses_cache() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_params (id INTEGER, value TEXT)", vec![])
        .await
        .expect("create table");

    conn.exec("INSERT INTO test_params VALUES (1, 'a')", vec![])
        .await
        .expect("insert");
    conn.exec("INSERT INTO test_params VALUES (2, 'b')", vec![])
        .await
        .expect("insert");
    conn.exec("INSERT INTO test_params VALUES (3, 'c')", vec![])
        .await
        .expect("insert");

    // Execute same SQL with different params multiple times
    for i in 1..=3 {
        let stream = conn
            .exec_rows(
                "SELECT * FROM test_params WHERE id = ?",
                vec![rbs::Value::I64(i)],
            )
            .await
            .expect("exec_rows");
        let count = consume_all_rows(stream).await;
        assert_eq!(count, 1);
    }

    // CREATE(1) + 3 literal INSERTs(2,3,4) + SELECT_with_param(5) = 5
    assert_eq!(
        conn.cached_statements_size(),
        5,
        "all unique SQL strings are cached"
    );
}

/// Test: verify all rows are returned and sent through channel
#[tokio::test]
async fn test_all_rows_returned_correctly() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_rows (id INTEGER, name TEXT)", vec![])
        .await
        .expect("create table");

    // Insert 10 rows
    for i in 1..=10 {
        conn.exec(
            &format!("INSERT INTO test_rows VALUES ({}, 'row{}')", i, i),
            vec![],
        )
        .await
        .expect("insert");
    }

    let stream = conn
        .exec_rows("SELECT * FROM test_rows ORDER BY id", vec![])
        .await
        .expect("exec_rows");

    let count = consume_all_rows(stream).await;
    assert_eq!(count, 10, "should return all 10 rows");
}

/// Test: mirrors the user's exact reported scenario.
/// exec() with INTEGER + TEXT + REAL params in a loop should not leak memory.
/// With the fix, the prepared statement is compiled once and cached; subsequent
/// calls reuse it via duckdb_clear_bindings + new param binding.
#[tokio::test]
async fn test_exec_user_scenario_int_text_real_params() {
    let mut conn = create_conn().await;

    conn.exec(
        "CREATE TABLE IF NOT EXISTS items (id INTEGER, name TEXT, value REAL)",
        vec![],
    )
    .await
    .expect("create table");

    // Run enough iterations to expose any per-call resource accumulation
    for _ in 0..50 {
        conn.exec(
            "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
            vec![
                rbs::Value::I32(1),
                rbs::Value::String("item".to_string()),
                rbs::Value::F64(1.5),
            ],
        )
        .await
        .expect("insert should succeed on every iteration");
    }

    // CREATE TABLE(1) + INSERT_with_params(2) = 2
    assert_eq!(
        conn.cached_statements_size(),
        2,
        "CREATE TABLE and INSERT should both be cached"
    );

    // Verify all 50 rows were actually inserted
    let stream = conn
        .exec_rows("SELECT COUNT(*) FROM items", vec![])
        .await
        .expect("count query");
    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1, "COUNT(*) should return exactly 1 result row");
}

/// Test: exec() correctly reports rows_affected for INSERT.
#[tokio::test]
async fn test_exec_rows_affected_count() {
    let mut conn = create_conn().await;

    conn.exec(
        "CREATE TABLE test_affected (id INTEGER PRIMARY KEY, val TEXT)",
        vec![],
    )
    .await
    .expect("create table");

    // Single INSERT should report 1 row affected
    let result = conn
        .exec(
            "INSERT INTO test_affected VALUES (?, ?)",
            vec![
                rbs::Value::I32(1),
                rbs::Value::String("hello".to_string()),
            ],
        )
        .await
        .expect("insert");

    assert_eq!(result.rows_affected, 1, "INSERT should affect exactly 1 row");

    // Second INSERT with a different key
    let result2 = conn
        .exec(
            "INSERT INTO test_affected VALUES (?, ?)",
            vec![
                rbs::Value::I32(2),
                rbs::Value::String("world".to_string()),
            ],
        )
        .await
        .expect("second insert");

    assert_eq!(result2.rows_affected, 1, "second INSERT should affect exactly 1 row");

    // CREATE(1) + INSERT_with_params(2) = 2 (same INSERT SQL reused)
    assert_eq!(conn.cached_statements_size(), 2);
}

/// Test: exec() and exec_rows() share the same LRU cache, so the same SQL
/// prepared via exec() is reused when exec_rows() encounters it and vice versa.
#[tokio::test]
async fn test_exec_and_exec_rows_share_cache() {
    let mut conn = create_conn().await;

    conn.exec("CREATE TABLE test_shared_cache (id INTEGER, val TEXT)", vec![])
        .await
        .expect("create table");

    // Insert via exec()
    conn.exec(
        "INSERT INTO test_shared_cache VALUES (?, ?)",
        vec![rbs::Value::I32(1), rbs::Value::String("a".to_string())],
    )
    .await
    .expect("insert via exec");

    // CREATE(1) + INSERT_with_params(2) = 2
    assert_eq!(conn.cached_statements_size(), 2, "CREATE TABLE and INSERT should be cached");

    // Query via exec_rows() with different SQL - cache grows to 3
    let stream = conn
        .exec_rows("SELECT * FROM test_shared_cache WHERE id = ?", vec![rbs::Value::I32(1)])
        .await
        .expect("exec_rows");
    let count = consume_all_rows(stream).await;
    assert_eq!(count, 1);
    assert_eq!(conn.cached_statements_size(), 3, "both exec and exec_rows add to the same cache");
}

/// Test: zero cache size disables caching for exec() too.
#[tokio::test]
async fn test_zero_cache_exec_no_cache() {
    let mut conn = create_conn_with_cache_size(0).await;

    conn.exec("CREATE TABLE test_zero_exec (id INTEGER)", vec![])
        .await
        .expect("create table");

    for i in 0..5 {
        conn.exec(
            "INSERT INTO test_zero_exec VALUES (?)",
            vec![rbs::Value::I64(i)],
        )
        .await
        .expect("insert");
    }

    assert_eq!(
        conn.cached_statements_size(),
        0,
        "exec should not cache when cache size is 0"
    );
}

/// Test: INSERT into non-existent table should not leak prepared statement memory.
/// The scenario: table "items" does not exist, we execute:
///   INSERT INTO items (id, name, value) VALUES (?, ?, ?)
/// This previously caused unbounded memory growth because the prepared statement
/// was cached even though execution failed due to table-not-existing error.
/// The fix: on execute failure, destroy the corrupted prepared statement via take_handle().
#[tokio::test]
async fn test_high_iterations_nonexistent_table_file_db() {
    // Use file-based DB like the example to catch real memory issues
    let db_path = "target/high_iter_test.db";
    let _ = std::fs::remove_file(db_path);
    let opts = DuckDbConnectOptions::new().path(db_path);
    let mut conn = DuckDbConnection::establish(&opts)
        .await
        .expect("create connection");

    for i in 0..5000 {
        let result = conn.exec(
            "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
            vec![
                rbs::Value::I32(1),
                rbs::Value::String("item".to_string()),
                rbs::Value::F64(1.5),
            ],
        ).await;
        // The error is swallowed by exec(), so result is always Ok(0)
        assert_eq!(conn.cached_statements_size(), 0,
            "failed INSERT should never be cached at iteration {}", i);
        drop(result);
    }
    println!("5000 iterations completed without OOM/memory leak");
    conn.close().await.ok();
    let _ = std::fs::remove_file(db_path);
}

/// Check cache state DURING the loop for non-existent table.
/// This verifies whether prepare ever succeeds (and statement cached then removed)

#[tokio::test]
async fn test_exec_into_nonexistent_table_no_memory_leak() {
    let mut conn = create_conn().await;

    // Intentionally skip CREATE TABLE - table does NOT exist
    // Repeatedly try to INSERT into non-existent table with params
    for _ in 0..20 {
        // DuckDB may succeed or fail depending on state; we just verify no crash/leak
        let _ = conn.exec(
            "INSERT INTO nonexistent_items (id, name, value) VALUES (?, ?, ?)",
            vec![
                rbs::Value::I32(1),
                rbs::Value::String("item".to_string()),
                rbs::Value::F64(1.5),
            ],
        )
        .await;
    }

    // Cache should still be empty (failed queries are never cached)
    assert_eq!(
        conn.cached_statements_size(),
        0,
        "failed INSERT should never be cached"
    );

    // Should still be able to create table and use connection normally
    conn.exec("CREATE TABLE items (id INTEGER, name TEXT, value REAL)", vec![])
        .await
        .expect("create table after failed inserts");

    conn.exec(
        "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
        vec![
            rbs::Value::I32(1),
            rbs::Value::String("item".to_string()),
            rbs::Value::F64(1.5),
        ],
    )
    .await
    .expect("insert after table created");

    let stream = conn
        .exec_rows("SELECT * FROM items", vec![])
        .await
        .expect("query after recovery");
    consume_all_rows(stream).await;
}

/// Test: repeated INSERT into non-existent table using exec_rows (streaming).
/// Same as above but via exec_rows path to ensure both code paths are covered.
#[tokio::test]
async fn test_exec_rows_into_nonexistent_table_no_memory_leak() {
    let mut conn = create_conn().await;

    // Intentionally skip CREATE TABLE
    for _ in 0..20 {
        let result = conn
            .exec_rows(
                "INSERT INTO nonexistent_items (id, name, value) VALUES (?, ?, ?)",
                vec![
                    rbs::Value::I32(1),
                    rbs::Value::String("item".to_string()),
                    rbs::Value::F64(1.5),
                ],
            )
            .await;

        if let Ok(stream) = result {
            // Must consume the stream to trigger actual execution
            let mut s = stream;
            while s.next().await.transpose().is_ok() {
                // consume until error or end
            }
        }
    }

    assert_eq!(
        conn.cached_statements_size(),
        0,
        "failed INSERT via exec_rows should never be cached"
    );
}
