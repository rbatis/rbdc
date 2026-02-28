//! Integration tests for the Turso adapter connection lifecycle.
//!
//! These tests verify the `rbdc::db::Connection` trait implementation against
//! an in-memory Turso database. They cover:
//!
//! - Connection establishment via `Driver::connect`
//! - Query execution (`get_rows`, `get_values`)
//! - Statement execution (`exec`)
//! - Transaction lifecycle (`begin`, `commit`, `rollback`)
//! - Ping and close semantics
//! - Error handling and fail-fast behavior
//! - No implicit fallback to any other backend

use rbdc::db::{Connection, Driver};
use rbdc_turso::TursoDriver;

/// Helper: create an in-memory connection via the driver.
async fn connect_in_memory() -> Box<dyn Connection> {
    let driver = TursoDriver {};
    driver
        .connect("turso://:memory:")
        .await
        .expect("in-memory connection should succeed")
}

/// Helper: create a connection and set up a test table.
async fn connect_with_table() -> Box<dyn Connection> {
    let mut conn = connect_in_memory().await;
    conn.exec(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
        vec![],
    )
    .await
    .expect("CREATE TABLE should succeed");
    conn
}

// ──────────────────────────────────────────────────────────────────────
// Connection Establishment
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_connect_in_memory() {
    let _conn = connect_in_memory().await;
    // Connection created successfully — the driver resolved the URI
    // and established a Turso/libSQL in-memory database.
}

#[tokio::test]
async fn test_connect_via_connect_opt() {
    use std::str::FromStr;

    let driver = TursoDriver {};
    let opt = rbdc_turso::TursoConnectOptions::from_str("turso://:memory:").unwrap();
    let _conn = driver
        .connect_opt(&opt)
        .await
        .expect("connect_opt should succeed");
}

#[tokio::test]
async fn test_connect_invalid_remote_fails() {
    let driver = TursoDriver {};
    // Remote URL without auth token should fail at validation time
    let result = driver.connect("turso://?url=libsql://fake.turso.io").await;
    assert!(result.is_err(), "connecting without auth token should fail");
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error"),
    };
    assert!(
        err.contains("auth_token"),
        "error should mention auth_token requirement, got: {}",
        err
    );
}

#[tokio::test]
async fn test_connect_local_file() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut path = std::env::temp_dir();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("rbdc_turso_local_{}_{}.db", std::process::id(), ts));

    let uri = format!("turso://{}", path.display());
    let driver = TursoDriver {};
    let mut conn = driver
        .connect(&uri)
        .await
        .expect("local-file connection should succeed");

    conn.exec(
        "CREATE TABLE local_file_test (id INTEGER PRIMARY KEY, name TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO local_file_test (id, name) VALUES (1, 'local')",
        vec![],
    )
    .await
    .unwrap();

    let rows = conn
        .get_rows("SELECT id, name FROM local_file_test", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    conn.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ──────────────────────────────────────────────────────────────────────
// Ping
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ping_succeeds() {
    let mut conn = connect_in_memory().await;
    conn.ping().await.expect("ping should succeed on live connection");
}

#[tokio::test]
async fn test_ping_after_operations() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'alice')", vec![])
        .await
        .unwrap();
    // Ping should still work after operations
    conn.ping().await.expect("ping should succeed after operations");
}

// ──────────────────────────────────────────────────────────────────────
// Close
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_close() {
    let mut conn = connect_in_memory().await;
    conn.close().await.expect("close should succeed");
}

#[tokio::test]
async fn test_close_after_operations() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'alice')", vec![])
        .await
        .unwrap();
    conn.close().await.expect("close should succeed after operations");
}

// ──────────────────────────────────────────────────────────────────────
// Exec (INSERT, UPDATE, DELETE, DDL)
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_exec_create_table() {
    let mut conn = connect_in_memory().await;
    let result = conn
        .exec(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)",
            vec![],
        )
        .await
        .expect("CREATE TABLE should succeed");
    // DDL typically affects 0 rows
    assert_eq!(result.rows_affected, 0);
}

#[tokio::test]
async fn test_exec_insert() {
    let mut conn = connect_with_table().await;
    let result = conn
        .exec(
            "INSERT INTO test (id, name, score) VALUES (1, 'bob', 95.5)",
            vec![],
        )
        .await
        .expect("INSERT should succeed");
    assert_eq!(result.rows_affected, 1);
    // last_insert_id should be the rowid
    assert_eq!(result.last_insert_id, rbs::Value::I64(1));
}

#[tokio::test]
async fn test_exec_insert_multiple() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (name) VALUES ('a')", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO test (name) VALUES ('b')", vec![])
        .await
        .unwrap();
    let result = conn
        .exec("INSERT INTO test (name) VALUES ('c')", vec![])
        .await
        .unwrap();
    assert_eq!(result.rows_affected, 1);
    assert_eq!(result.last_insert_id, rbs::Value::I64(3));
}

#[tokio::test]
async fn test_exec_update() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'old')", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO test (id, name) VALUES (2, 'old')", vec![])
        .await
        .unwrap();
    let result = conn
        .exec("UPDATE test SET name = 'new' WHERE name = 'old'", vec![])
        .await
        .expect("UPDATE should succeed");
    assert_eq!(result.rows_affected, 2);
}

#[tokio::test]
async fn test_exec_delete() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'del')", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO test (id, name) VALUES (2, 'keep')", vec![])
        .await
        .unwrap();
    let result = conn
        .exec("DELETE FROM test WHERE name = 'del'", vec![])
        .await
        .expect("DELETE should succeed");
    assert_eq!(result.rows_affected, 1);
}

#[tokio::test]
async fn test_exec_with_params() {
    let mut conn = connect_with_table().await;
    let result = conn
        .exec(
            "INSERT INTO test (id, name, score) VALUES (?, ?, ?)",
            vec![
                rbs::Value::I64(42),
                rbs::Value::String("parameterized".to_string()),
                rbs::Value::F64(88.8),
            ],
        )
        .await
        .expect("parameterized INSERT should succeed");
    assert_eq!(result.rows_affected, 1);
    assert_eq!(result.last_insert_id, rbs::Value::I64(42));
}

// ──────────────────────────────────────────────────────────────────────
// Query (get_rows)
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_rows_empty() {
    let mut conn = connect_with_table().await;
    let rows = conn
        .get_rows("SELECT * FROM test", vec![])
        .await
        .expect("SELECT should succeed");
    assert!(rows.is_empty(), "empty table should return no rows");
}

#[tokio::test]
async fn test_get_rows_with_data() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name, score) VALUES (1, 'alice', 90.0)", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO test (id, name, score) VALUES (2, 'bob', 85.5)", vec![])
        .await
        .unwrap();

    let rows = conn
        .get_rows("SELECT * FROM test ORDER BY id", vec![])
        .await
        .expect("SELECT should succeed");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_get_rows_metadata() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name, score) VALUES (1, 'meta', 77.0)", vec![])
        .await
        .unwrap();

    let rows = conn
        .get_rows("SELECT id, name, score FROM test", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    let md = row.meta_data();
    assert_eq!(md.column_len(), 3);
    assert_eq!(md.column_name(0), "id");
    assert_eq!(md.column_name(1), "name");
    assert_eq!(md.column_name(2), "score");
}

#[tokio::test]
async fn test_get_rows_values() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name, score) VALUES (7, 'val', 99.9)", vec![])
        .await
        .unwrap();

    let mut rows = conn
        .get_rows("SELECT id, name, score FROM test WHERE id = 7", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let row = &mut rows[0];
    // get() consumes values via Option::take() — any access order works
    let score = row.get(2).unwrap();
    let name = row.get(1).unwrap();
    let id = row.get(0).unwrap();

    assert_eq!(id, rbs::Value::I64(7));
    assert_eq!(name, rbs::Value::String("val".to_string()));
    assert_eq!(score, rbs::Value::F64(99.9));
}

#[tokio::test]
async fn test_get_rows_with_params() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'find_me')", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO test (id, name) VALUES (2, 'not_me')", vec![])
        .await
        .unwrap();

    let rows = conn
        .get_rows(
            "SELECT id, name FROM test WHERE name = ?",
            vec![rbs::Value::String("find_me".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_get_rows_index_out_of_range() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'oob')", vec![])
        .await
        .unwrap();

    let mut rows = conn
        .get_rows("SELECT id FROM test", vec![])
        .await
        .unwrap();
    let row = &mut rows[0];
    // Column index 5 is out of range (only 1 column)
    let err = row.get(5);
    assert!(err.is_err(), "out-of-range index should return error");
}

// ──────────────────────────────────────────────────────────────────────
// get_values (default trait implementation)
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_values() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name, score) VALUES (1, 'gv', 50.0)", vec![])
        .await
        .unwrap();

    let values = conn
        .get_values("SELECT id, name FROM test", vec![])
        .await
        .expect("get_values should succeed");

    // get_values returns Value::Array of Value::Map entries
    match &values {
        rbs::Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                rbs::Value::Map(m) => {
                    assert_eq!(m.len(), 2);
                }
                other => panic!("expected Map, got: {:?}", other),
            }
        }
        other => panic!("expected Array, got: {:?}", other),
    }
}

// ──────────────────────────────────────────────────────────────────────
// Transactions (begin / commit / rollback)
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_transaction_commit() {
    let mut conn = connect_with_table().await;

    conn.begin().await.expect("begin should succeed");
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'committed')", vec![])
        .await
        .unwrap();
    conn.commit().await.expect("commit should succeed");

    // Data should be visible after commit
    let rows = conn
        .get_rows("SELECT name FROM test WHERE id = 1", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_transaction_rollback() {
    let mut conn = connect_with_table().await;

    // Insert baseline data outside transaction
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'baseline')", vec![])
        .await
        .unwrap();

    // Begin transaction, insert, then rollback
    conn.begin().await.expect("begin should succeed");
    conn.exec("INSERT INTO test (id, name) VALUES (2, 'rolled_back')", vec![])
        .await
        .unwrap();
    conn.rollback().await.expect("rollback should succeed");

    // Only baseline data should exist
    let rows = conn
        .get_rows("SELECT * FROM test", vec![])
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "rollback should have undone the second insert"
    );
}

#[tokio::test]
async fn test_transaction_rollback_preserves_prior_data() {
    let mut conn = connect_with_table().await;

    conn.exec("INSERT INTO test (id, name) VALUES (1, 'keep')", vec![])
        .await
        .unwrap();

    conn.begin().await.unwrap();
    conn.exec("DELETE FROM test WHERE id = 1", vec![])
        .await
        .unwrap();
    conn.rollback().await.unwrap();

    // The DELETE should have been rolled back
    let rows = conn
        .get_rows("SELECT * FROM test WHERE id = 1", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "rollback should preserve prior data");
}

#[tokio::test]
async fn test_multiple_transactions() {
    let mut conn = connect_with_table().await;

    // First transaction: commit
    conn.begin().await.unwrap();
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'first')", vec![])
        .await
        .unwrap();
    conn.commit().await.unwrap();

    // Second transaction: rollback
    conn.begin().await.unwrap();
    conn.exec("INSERT INTO test (id, name) VALUES (2, 'second')", vec![])
        .await
        .unwrap();
    conn.rollback().await.unwrap();

    // Third transaction: commit
    conn.begin().await.unwrap();
    conn.exec("INSERT INTO test (id, name) VALUES (3, 'third')", vec![])
        .await
        .unwrap();
    conn.commit().await.unwrap();

    let rows = conn
        .get_rows("SELECT * FROM test ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "only committed transactions should persist");
}

// ──────────────────────────────────────────────────────────────────────
// SQL Error Handling
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_exec_invalid_sql() {
    let mut conn = connect_in_memory().await;
    let result = conn.exec("NOT VALID SQL", vec![]).await;
    assert!(result.is_err(), "invalid SQL should return error");
}

#[tokio::test]
async fn test_get_rows_invalid_sql() {
    let mut conn = connect_in_memory().await;
    let result = conn.get_rows("SELECT FROM WHERE", vec![]).await;
    assert!(result.is_err(), "invalid SQL should return error");
}

#[tokio::test]
async fn test_exec_table_not_found() {
    let mut conn = connect_in_memory().await;
    let result = conn
        .exec("INSERT INTO nonexistent (id) VALUES (1)", vec![])
        .await;
    assert!(result.is_err(), "referencing nonexistent table should fail");
}

#[tokio::test]
async fn test_exec_constraint_violation() {
    let mut conn = connect_with_table().await;
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'first')", vec![])
        .await
        .unwrap();
    // Duplicate primary key
    let result = conn
        .exec("INSERT INTO test (id, name) VALUES (1, 'dupe')", vec![])
        .await;
    assert!(
        result.is_err(),
        "duplicate primary key should return error"
    );
}

// ──────────────────────────────────────────────────────────────────────
// Fail-fast / No Fallback
// ──────────────────────────────────────────────────────────────────────

/// Verify that there is no fallback mechanism: when an operation fails,
/// it returns an error directly. The adapter does not attempt to retry
/// with a different backend or silently succeed.
#[tokio::test]
async fn test_no_fallback_on_error() {
    let mut conn = connect_in_memory().await;

    // This operation fails (table doesn't exist). The adapter should return
    // an error immediately — not silently fall back to any other behavior.
    let result = conn
        .exec("INSERT INTO does_not_exist (x) VALUES (1)", vec![])
        .await;
    assert!(result.is_err());

    // The connection should still be usable after the error (not poisoned)
    conn.exec("CREATE TABLE recovery_test (id INTEGER)", vec![])
        .await
        .expect("connection should still be usable after error");
}

/// Verify that a bad remote connection fails at connect time, not
/// silently falling back to local/in-memory.
#[tokio::test]
async fn test_no_fallback_bad_remote() {
    let driver = TursoDriver {};
    let result = driver
        .connect("turso://?url=libsql://nonexistent.example.com&token=fake")
        .await;
    // This should either fail at connect or return a connection that fails on first use.
    // It must NOT silently create a local/in-memory database.
    if let Ok(mut conn) = result {
        // If the connection was created (lazy connect), verify it fails on use
        let ping_result = conn.ping().await;
        let exec_result = conn.exec("SELECT 1", vec![]).await;
        // At least one of these should fail for a bad remote
        assert!(
            ping_result.is_err() || exec_result.is_err(),
            "operations on invalid remote should fail (no silent fallback)"
        );
    }
    // If connect itself failed, that's the expected fail-fast behavior
}

// ──────────────────────────────────────────────────────────────────────
// Value Type Round-trips
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_null_round_trip() {
    let mut conn = connect_with_table().await;
    conn.exec(
        "INSERT INTO test (id, name, score) VALUES (?, ?, ?)",
        vec![rbs::Value::I64(1), rbs::Value::Null, rbs::Value::Null],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT name, score FROM test WHERE id = 1", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &mut rows[0];
    let score = row.get(1).unwrap();
    let name = row.get(0).unwrap();
    assert_eq!(name, rbs::Value::Null);
    assert_eq!(score, rbs::Value::Null);
}

#[tokio::test]
async fn test_bool_as_integer() {
    let mut conn = connect_in_memory().await;
    conn.exec("CREATE TABLE bools (id INTEGER, flag INTEGER)", vec![])
        .await
        .unwrap();
    conn.exec(
        "INSERT INTO bools (id, flag) VALUES (?, ?)",
        vec![rbs::Value::I64(1), rbs::Value::Bool(true)],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT flag FROM bools WHERE id = 1", vec![])
        .await
        .unwrap();
    let row = &mut rows[0];
    let flag = row.get(0).unwrap();
    // Booleans are stored as integers (1/0) in Turso
    assert_eq!(flag, rbs::Value::I64(1));
}

#[tokio::test]
async fn test_binary_round_trip() {
    let mut conn = connect_in_memory().await;
    conn.exec("CREATE TABLE blobs (id INTEGER, data BLOB)", vec![])
        .await
        .unwrap();

    let blob_data = vec![0u8, 1, 2, 255, 128, 64];
    conn.exec(
        "INSERT INTO blobs (id, data) VALUES (?, ?)",
        vec![
            rbs::Value::I64(1),
            rbs::Value::Binary(blob_data.clone()),
        ],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT data FROM blobs WHERE id = 1", vec![])
        .await
        .unwrap();
    let row = &mut rows[0];
    let data = row.get(0).unwrap();
    assert_eq!(data, rbs::Value::Binary(blob_data));
}

// ──────────────────────────────────────────────────────────────────────
// Connection Resilience
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_connection_usable_after_sql_error() {
    let mut conn = connect_with_table().await;

    // Cause a SQL error
    let _ = conn.exec("INVALID SQL", vec![]).await;

    // Connection should still be functional
    conn.exec("INSERT INTO test (id, name) VALUES (1, 'recovery')", vec![])
        .await
        .expect("connection should recover from SQL error");

    let rows = conn
        .get_rows("SELECT name FROM test WHERE id = 1", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_connection_usable_after_constraint_error() {
    let mut conn = connect_with_table().await;

    conn.exec("INSERT INTO test (id, name) VALUES (1, 'first')", vec![])
        .await
        .unwrap();

    // Cause constraint violation
    let _ = conn
        .exec("INSERT INTO test (id, name) VALUES (1, 'dupe')", vec![])
        .await;

    // Connection should still work
    conn.exec("INSERT INTO test (id, name) VALUES (2, 'second')", vec![])
        .await
        .expect("connection should recover from constraint error");
}

// ──────────────────────────────────────────────────────────────────────
// Driver Identity
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_driver_name_is_turso() {
    let driver = TursoDriver {};
    assert_eq!(driver.name(), "turso");
}

#[test]
fn test_placeholder_passthrough() {
    use rbdc::db::Placeholder;
    let driver = TursoDriver {};
    let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
    assert_eq!(driver.exchange(sql), sql, "Turso uses ? placeholders (pass-through)");
}
