//! Broad integration coverage for the public `rbdc-sqlite` connection API.

use std::panic::AssertUnwindSafe;

use either::Either;
use futures_util::StreamExt;
use rbdc::db::{Connection, Row};
use rbdc_sqlite::query::SqliteQuery;
use rbdc_sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteQueryResult, SqliteRow, SqliteTypeInfo,
};
use rbs::Value;

async fn memory_conn() -> SqliteConnection {
    SqliteConnectOptions::new()
        .filename(":memory:")
        .connect()
        .await
        .expect("failed to open in-memory sqlite")
}

fn assert_description(columns: &[rbdc_sqlite::SqliteColumn]) {
    // Smoke-test the column accessors through a slice.
    for (i, column) in columns.iter().enumerate() {
        assert_eq!(column.ordinal(), i);
    }
}

async fn collect_stream(
    mut stream: futures_util::stream::BoxStream<
        '_,
        Result<Either<SqliteQueryResult, SqliteRow>, rbdc::Error>,
    >,
) -> (Vec<SqliteQueryResult>, Vec<SqliteRow>) {
    let mut results = Vec::new();
    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        match item.expect("stream item") {
            Either::Left(qr) => results.push(qr),
            Either::Right(row) => rows.push(row),
        }
    }
    (results, rows)
}

#[tokio::test]
async fn test_basic_crud_and_exec_result() {
    let mut conn = memory_conn().await;

    let created = conn
        .exec(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
            vec![],
        )
        .await
        .expect("create table");
    // DDL does not report rows affected
    assert_eq!(created.rows_affected, 0);

    let inserted = conn
        .exec(
            "INSERT INTO users (name, score) VALUES (?, ?)",
            vec![Value::String("Alice".into()), Value::F64(9.5)],
        )
        .await
        .expect("insert");
    assert_eq!(inserted.rows_affected, 1);
    assert_eq!(inserted.last_insert_id, Value::U64(1));

    // Multi-statement insert without params accumulates the affected row count.
    let inserted = conn
        .exec(
            "INSERT INTO users (name, score) VALUES ('Bob', 8.0); \
             INSERT INTO users (name, score) VALUES ('Carol', 7.0);",
            vec![],
        )
        .await
        .expect("multi insert");
    assert_eq!(inserted.rows_affected, 2);
    assert_eq!(inserted.last_insert_id, Value::U64(3));

    let updated = conn
        .exec(
            "UPDATE users SET score = ? WHERE name = ?",
            vec![Value::F64(10.0), Value::String("Bob".into())],
        )
        .await
        .expect("update");
    assert_eq!(updated.rows_affected, 1);

    let decoded = conn
        .exec_decode("SELECT id, name, score FROM users ORDER BY id", vec![])
        .await
        .expect("select");
    match decoded {
        Value::Array(rows) => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].as_map().unwrap()["id"], Value::I64(1));
            assert_eq!(rows[1].as_map().unwrap()["name"], Value::String("Bob".into()));
            assert_eq!(rows[1].as_map().unwrap()["score"], Value::F64(10.0));
        }
        other => panic!("expected array, got {:?}", other),
    }

    let deleted = conn
        .exec("DELETE FROM users WHERE id = ?", vec![Value::I64(2)])
        .await
        .expect("delete");
    assert_eq!(deleted.rows_affected, 1);
}

#[tokio::test]
async fn test_transactions() {
    let mut conn = memory_conn().await;
    conn.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", vec![])
        .await
        .unwrap();

    conn.begin().await.expect("begin");
    conn.exec("INSERT INTO t (v) VALUES (?)", vec![Value::String("a".into())])
        .await
        .unwrap();
    conn.rollback().await.expect("rollback");
    let decoded = conn.exec_decode("SELECT * FROM t", vec![]).await.unwrap();
    assert_eq!(decoded, Value::Array(vec![]));

    conn.begin().await.expect("begin");
    conn.exec("INSERT INTO t (v) VALUES (?)", vec![Value::String("b".into())])
        .await
        .unwrap();
    conn.commit().await.expect("commit");
    let decoded = conn.exec_decode("SELECT v FROM t", vec![]).await.unwrap();
    assert_eq!(decoded.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_exec_rows_metadata_and_typed_values() {
    let mut conn = memory_conn().await;
    conn.exec(
        "CREATE TABLE kinds (
            i INTEGER, i4 INT4, i8 INT8, real_col REAL, float_col FLOAT,
            txt TEXT, varchar_col VARCHAR(20), blob_col BLOB, flag BOOLEAN,
            d DATE, t TIME, dt DATETIME, ts TIMESTAMP,
            num NUMERIC, dec DECIMAL(10,2)
        )",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO kinds VALUES (1, 2, 3, 1.5, 2.5, 'text', 'v', x'0102', 1,
            '2020-01-01', '12:00:00', '2020-01-01 12:00:00', '2020-01-01 12:00:00', '7.25', '8.50')",
        vec![],
    )
    .await
    .unwrap();

    let mut stream = conn.exec_rows("SELECT * FROM kinds", vec![]).await.unwrap();
    let row = stream
        .next()
        .await
        .expect("one row")
        .expect("row result");
    drop(stream);

    let md = row.meta_data();
    assert_eq!(md.column_len(), 15);
    assert_eq!(md.column_name(0), "i");
    assert_eq!(md.column_name(5), "txt");
    // Declared type names are reported through metadata.
    assert_eq!(md.column_type(0), "INTEGER");
    assert_eq!(md.column_type(3), "REAL");
    assert_eq!(md.column_type(5), "TEXT");
    assert_eq!(md.column_type(7), "BLOB");
    assert_eq!(md.column_type(8), "BOOLEAN");
    assert_eq!(md.column_type(9), "DATE");
    assert_eq!(md.column_type(10), "TIME");
    assert_eq!(md.column_type(11), "DATETIME");
    assert_eq!(md.column_type(13), "NUMERIC");

    let mut row = row;
    assert_eq!(row.get(0).unwrap(), Value::I64(1));
    assert_eq!(row.get(3).unwrap(), Value::F64(1.5));
    assert_eq!(row.get(5).unwrap(), Value::String("text".into()));
    assert_eq!(row.get(7).unwrap(), Value::Binary(vec![1, 2]));
    // Runtime value types take precedence over declared column types.
    assert_eq!(row.get(8).unwrap(), Value::I64(1));
    assert_eq!(row.get(9).unwrap(), Value::String("2020-01-01".into()));
    assert_eq!(row.get(10).unwrap(), Value::String("12:00:00".into()));
    assert_eq!(
        row.get(11).unwrap(),
        Value::String("2020-01-01 12:00:00".into())
    );
    // NUMERIC affinity converts the text literals to REAL values.
    assert_eq!(row.get(13).unwrap(), Value::F64(7.25));
    assert_eq!(row.get(14).unwrap(), Value::F64(8.5));
}

#[tokio::test]
async fn test_exec_decode_declared_types() {
    let mut conn = memory_conn().await;
    conn.exec(
        "CREATE TABLE j (id INTEGER, payload TEXT, doc TEXT, arr TEXT, nil TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO j VALUES (1, '{\"a\":1}', '{\"b\":2}', '[1,2]', 'null')",
        vec![],
    )
    .await
    .unwrap();

    let decoded = conn.exec_decode("SELECT payload, doc, arr, nil FROM j", vec![]).await.unwrap();
    let map = decoded.as_array().unwrap()[0].as_map().unwrap();
    assert!(matches!(map["payload"], Value::Map(_)));
    assert!(matches!(map["doc"], Value::Map(_)));
    assert!(matches!(map["arr"], Value::Array(_)));
    // JSON "null" text decodes to a JSON null value.
    assert_eq!(map["nil"], Value::Null);
}

#[tokio::test]
async fn test_exec_rows_non_select_and_multiple_statements() {
    let mut conn = memory_conn().await;
    conn.exec("CREATE TABLE m (id INTEGER PRIMARY KEY, v TEXT)", vec![])
        .await
        .unwrap();

    // A statement with no result rows produces an empty stream.
    let mut stream = conn
        .exec_rows("INSERT INTO m (v) VALUES ('a')", vec![])
        .await
        .unwrap();
    assert!(stream.next().await.is_none());
    drop(stream);

    // A multi-statement query yields the rows of the SELECT.
    let mut stream = conn
        .exec_rows(
            "INSERT INTO m (v) VALUES ('b'); SELECT id, v FROM m ORDER BY id",
            vec![],
        )
        .await
        .unwrap();
    let mut count = 0;
    while let Some(row) = stream.next().await {
        let mut row = row.unwrap();
        let md = row.meta_data();
        assert_eq!(md.column_name(0), "id");
        assert!(matches!(row.get(1).unwrap(), Value::String(_)));
        count += 1;
    }
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_streaming_drop_early_releases_worker() {
    let mut conn = memory_conn().await;
    conn.exec("CREATE TABLE big (id INTEGER PRIMARY KEY)", vec![])
        .await
        .unwrap();
    for i in 0..50 {
        conn.exec("INSERT INTO big (id) VALUES (?)", vec![Value::I64(i)])
            .await
            .unwrap();
    }

    {
        let mut stream = conn.exec_rows("SELECT id FROM big", vec![]).await.unwrap();
        // Drop the stream before draining it. The worker must notice the
        // receiver is gone and stop sending rows.
        assert!(stream.next().await.is_some());
    }

    // The connection is still usable afterwards.
    let decoded = conn.exec_decode("SELECT COUNT(*) AS c FROM big", vec![]).await.unwrap();
    let count = decoded.as_array().unwrap()[0].as_map().unwrap()["c"].clone();
    assert_eq!(count, Value::I64(50));
}

#[tokio::test]
async fn test_fetch_many_and_fetch_optional() {
    let mut conn = memory_conn().await;

    let query = SqliteQuery {
        statement: Either::Left("SELECT 1 AS v".to_string()),
        arguments: vec![],
        persistent: false,
    };
    let (results, rows) = collect_stream(conn.fetch_many(query)).await;
    // A single statement always emits one completion result after its rows.
    assert_eq!(results.len(), 1);
    assert_eq!(rows.len(), 1);
    let mut row = rows.into_iter().next().unwrap();
    assert_eq!(row.get(0).unwrap(), Value::I64(1));

    let query = SqliteQuery {
        statement: Either::Left("SELECT ? AS v".to_string()),
        arguments: vec![Value::String("param".into())],
        persistent: true,
    };
    let (_results, rows) = collect_stream(conn.fetch_many(query)).await;
    assert_eq!(rows.len(), 1);
    let mut row = rows.into_iter().next().unwrap();
    assert_eq!(row.get(0).unwrap(), Value::String("param".into()));

    // `Either::Right` carries an already-prepared statement.
    let stmt = conn
        .prepare_with("SELECT 42 AS answer", &[])
        .await
        .expect("prepare");
    assert_eq!(stmt.sql(), "SELECT 42 AS answer");
    let query = SqliteQuery {
        statement: Either::Right(stmt),
        arguments: vec![],
        persistent: false,
    };
    let (_results, rows) = collect_stream(conn.fetch_many(query)).await;
    assert_eq!(rows.len(), 1);

    let query = SqliteQuery {
        statement: Either::Left("SELECT 1".to_string()),
        arguments: vec![],
        persistent: false,
    };
    let row = conn.fetch_optional(query).await.unwrap();
    assert!(row.is_some());

    let query = SqliteQuery {
        statement: Either::Left("SELECT 1 WHERE 0".to_string()),
        arguments: vec![],
        persistent: false,
    };
    let row = conn.fetch_optional(query).await.unwrap();
    assert!(row.is_none());

    // Multi-statement fetch_many reports query results for every statement.
    let query = SqliteQuery {
        statement: Either::Left(
            "CREATE TABLE q (id INTEGER); INSERT INTO q VALUES (7); SELECT id FROM q"
                .to_string(),
        ),
        arguments: vec![],
        persistent: false,
    };
    let (results, rows) = collect_stream(conn.fetch_many(query)).await;
    assert!(!results.is_empty());
    assert_eq!(rows.len(), 1);
    assert!(results.iter().any(|r| r.rows_affected() == 1));
    assert!(results.iter().any(|r| r.last_insert_rowid() == 1));
}

#[tokio::test]
async fn test_prepare_with_statement_apis() {
    let mut conn = memory_conn().await;
    let stmt = conn
        .prepare_with(
            "SELECT ? AS a, ? AS b",
            &[SqliteTypeInfo::null(), SqliteTypeInfo::null()],
        )
        .await
        .unwrap();

    assert_eq!(stmt.sql(), "SELECT ? AS a, ? AS b");
    match stmt.parameters() {
        Some(Either::Right(n)) => assert_eq!(n, 2),
        other => panic!("unexpected parameters: {:?}", other),
    }

    let cloned = stmt.to_owned();
    assert_eq!(cloned.sql(), stmt.sql());
    assert!(format!("{:?}", stmt).contains("SqliteStatement"));
    assert!(format!("{:?}", cloned).contains("SqliteStatement"));

    let columns = stmt.columns();
    assert_eq!(columns.len(), 2);
    assert!(format!("{:?}", columns[0]).contains("SqliteColumn"));
    assert_description(columns);
    assert_eq!(columns[0].ordinal(), 0);
    assert_eq!(columns[0].name(), "a");
    assert_eq!(columns[1].name(), "b");
    // Unbound parameters have no declared type at prepare time.
    assert!(columns[0].type_info().is_null());

    // Columns from a real table report their declared type.
    conn.exec("CREATE TABLE typed (id INTEGER, name TEXT)", vec![])
        .await
        .unwrap();
    let stmt = conn.prepare_with("SELECT id, name FROM typed", &[]).await.unwrap();
    let columns = stmt.columns();
    assert_eq!(columns[0].type_info().to_string(), "INTEGER");
    assert_eq!(columns[1].type_info().to_string(), "TEXT");
}

#[tokio::test]
async fn test_statement_cache_behavior() {
    let mut conn = SqliteConnectOptions::new()
        .filename(":memory:")
        .statement_cache_capacity(10)
        .connect()
        .await
        .unwrap();

    assert_eq!(conn.cached_statements_size(), 0);

    // Parameterized queries go through `prepare_with` and are cached.
    for _ in 0..3 {
        conn.exec("SELECT ? AS v", vec![Value::I64(1)])
            .await
            .unwrap();
    }
    assert_eq!(conn.cached_statements_size(), 1);

    // A different query adds another cache entry.
    conn.exec("SELECT ? AS w", vec![Value::I64(2)])
        .await
        .unwrap();
    assert_eq!(conn.cached_statements_size(), 2);

    // Non-parameterized queries use the temporary statement slot.
    conn.exec("SELECT 1", vec![]).await.unwrap();
    assert_eq!(conn.cached_statements_size(), 2);

    conn.clear_cached_statements().await.unwrap();
    assert_eq!(conn.cached_statements_size(), 0);

    // The connection remains usable after clearing the cache.
    conn.exec("SELECT ? AS v", vec![Value::I64(3)])
        .await
        .unwrap();
    assert_eq!(conn.cached_statements_size(), 1);

    // A single-entry cache evicts older entries.
    let mut small = SqliteConnectOptions::new()
        .filename(":memory:")
        .statement_cache_capacity(1)
        .connect()
        .await
        .unwrap();
    small.exec("SELECT ? AS a", vec![Value::I64(1)]).await.unwrap();
    small.exec("SELECT ? AS b", vec![Value::I64(2)]).await.unwrap();
    assert_eq!(small.cached_statements_size(), 1);
}

#[tokio::test]
async fn test_parameter_formats() {
    let mut conn = memory_conn().await;

    // Positional `?`.
    let decoded = conn.exec_decode("SELECT ? AS v", vec![Value::I64(1)]).await.unwrap();
    assert_eq!(decoded.as_array().unwrap()[0].as_map().unwrap()["v"], Value::I64(1));

    // `?NNN` and `$NNN`.
    let decoded = conn
        .exec_decode("SELECT ?2 AS b, ?1 AS a", vec![Value::I64(10), Value::I64(20)])
        .await
        .unwrap();
    let map = decoded.as_array().unwrap()[0].as_map().unwrap();
    assert_eq!(map["a"], Value::I64(10));
    assert_eq!(map["b"], Value::I64(20));

    let decoded = conn.exec_decode("SELECT $1 AS v", vec![Value::I64(5)]).await.unwrap();
    assert_eq!(decoded.as_array().unwrap()[0].as_map().unwrap()["v"], Value::I64(5));

    // Named parameters with non-integer names are rejected.
    let err = conn
        .exec_decode("SELECT $foo AS v", vec![Value::I64(1)])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("non-integer names"));

    // `:name` style parameters are unsupported.
    let err = conn
        .exec_decode("SELECT :foo AS v", vec![Value::I64(1)])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unsupported SQL parameter format"));

    // Fewer arguments than placeholders: SQLite treats missing args as NULL.
    let decoded = conn
        .exec_decode("SELECT ? AS a, ? AS b", vec![Value::I64(1)])
        .await
        .unwrap();
    let map = decoded.as_array().unwrap()[0].as_map().unwrap();
    assert_eq!(map["a"], Value::I64(1));
    assert_eq!(map["b"], Value::Null);
}

#[tokio::test]
async fn test_error_paths() {
    let mut conn = memory_conn().await;

    let err = conn.exec("THIS IS NOT SQL", vec![]).await.unwrap_err();
    assert!(!err.to_string().is_empty());

    // Errors from `exec_rows` are delivered through the returned stream.
    let err = {
        let mut stream = conn.exec_rows("NOT SQL", vec![]).await.expect("stream");
        match stream.next().await {
            Some(Err(e)) => e,
            other => panic!("expected a stream error, got {:?}", other),
        }
    };
    assert!(!err.to_string().is_empty());

    let err = conn
        .exec("NOT SQL ?", vec![Value::I64(1)])
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());

    let err = conn.exec_decode("NOT SQL", vec![]).await.unwrap_err();
    assert!(!err.to_string().is_empty());

    let err = conn.exec("SELECT * FROM missing_table", vec![]).await.unwrap_err();
    assert!(!err.to_string().is_empty());

    // Constraint violations surface as step errors.
    conn.exec("CREATE TABLE u (id INTEGER PRIMARY KEY)", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO u VALUES (1)", vec![]).await.unwrap();
    let err = conn.exec("INSERT INTO u VALUES (1)", vec![]).await.unwrap_err();
    assert!(!err.to_string().is_empty());

    // Foreign keys are enabled by default.
    conn.exec(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY); \
         CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
        vec![],
    )
    .await
    .unwrap();
    let err = conn.exec("INSERT INTO child VALUES (1, 99)", vec![]).await.unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn test_row_get_out_of_range_panics() {
    let mut conn = memory_conn().await;
    let mut stream = conn.exec_rows("SELECT 1 AS v", vec![]).await.unwrap();
    let mut row = stream.next().await.unwrap().unwrap();
    drop(stream);

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = row.get(10);
    }));
    assert!(result.is_err(), "out-of-range index should panic");
}

#[tokio::test]
async fn test_ping_flush_and_close() {
    let mut conn = memory_conn().await;
    conn.ping().await.expect("ping");
    rbdc::db::Connection::ping(&mut conn).await.expect("trait ping");
    conn.flush().await.expect("flush");
    assert!(!conn.should_flush());
    conn.exec("SELECT 1", vec![]).await.unwrap();
    conn.close().await.expect("close");
}

#[tokio::test]
async fn test_lock_handle_and_collations() {
    let mut conn = SqliteConnectOptions::new()
        .filename(":memory:")
        .collation("reverse", |a, b| b.cmp(a))
        .connect()
        .await
        .unwrap();

    conn.exec("CREATE TABLE words (w TEXT)", vec![]).await.unwrap();
    conn.exec(
        "INSERT INTO words VALUES ('a'); INSERT INTO words VALUES ('b'); \
         INSERT INTO words VALUES ('c');",
        vec![],
    )
    .await
    .unwrap();

    let decoded = conn
        .exec_decode("SELECT w FROM words ORDER BY w COLLATE reverse", vec![])
        .await
        .unwrap();
    let order: Vec<Value> = decoded
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_map().unwrap()["w"].clone())
        .collect();
    assert_eq!(
        order,
        vec![
            Value::String("c".into()),
            Value::String("b".into()),
            Value::String("a".into())
        ]
    );

    // Locked handle exposes the raw pointer and can register collations.
    let mut locked = conn.lock_handle().await.unwrap();
    // Touch the raw handle so the accessor is exercised (it is never null
    // while the handle is alive, so no null assertion is needed).
    let _ = locked.as_raw_handle().as_ptr();
    locked
        .create_collation("bylen", |a, b| a.len().cmp(&b.len()))
        .expect("create collation");
    drop(locked);

    conn.exec("INSERT INTO words VALUES ('dddd')", vec![]).await.unwrap();
    let decoded = conn
        .exec_decode("SELECT w FROM words ORDER BY w COLLATE bylen", vec![])
        .await
        .unwrap();
    assert_eq!(decoded.as_array().unwrap().len(), 4);

    // Invalid collation names (containing a nul byte) are rejected.
    let err_conn = SqliteConnectOptions::new()
        .filename(":memory:")
        .collation("bad\0name", |a, b| a.cmp(b));
    let err = err_conn.connect().await.unwrap_err();
    assert!(err.to_string().contains("collation name"));

    let mut locked = conn.lock_handle().await.unwrap();
    let err = locked
        .create_collation("bad\0name", |a, b| a.cmp(b))
        .unwrap_err();
    assert!(err.to_string().contains("collation name"));
}

#[tokio::test]
#[allow(deprecated)]
async fn test_deprecated_apis() {
    let mut conn = memory_conn().await;

    let raw = unsafe { conn.as_raw_handle() };
    assert!(!raw.is_null());

    conn.create_collation("rev", |a, b| b.cmp(a))
        .await
        .expect("deprecated create_collation");

    // Invalid names are logged by the worker rather than returned.
    conn.create_collation("bad\0name", |a, b| a.cmp(b))
        .await
        .expect("command sent");
    conn.ping().await.expect("worker still alive");
}

#[tokio::test]
async fn test_connection_debug_and_options() {
    let mut conn = SqliteConnectOptions::new()
        .filename(":memory:")
        .thread_name(|id| format!("custom-sqlite-{}", id))
        .command_buffer_size(8)
        .row_buffer_size(8)
        .foreign_keys(false)
        .shared_cache(true)
        .busy_timeout(std::time::Duration::from_millis(100))
        .serialized(true)
        .connect()
        .await
        .expect("connect with custom options");

    let debug = format!("{:?}", conn);
    assert!(debug.contains("SqliteConnection"));
    assert!(debug.contains("row_channel_size"));
    conn.exec("SELECT 1", vec![]).await.unwrap();
}

#[tokio::test]
async fn test_empty_statements_and_prepare_loop() {
    let mut conn = memory_conn().await;
    // Leading, consecutive and trailing semicolons are skipped by the
    // statement preparation loop.
    let decoded = conn
        .exec_decode(";;; SELECT 1 AS v;;;", vec![])
        .await
        .unwrap();
    assert_eq!(
        decoded.as_array().unwrap()[0].as_map().unwrap()["v"],
        Value::I64(1)
    );

    // A completely empty query produces no result rows.
    let decoded = conn.exec_decode("   ", vec![]).await.unwrap();
    assert_eq!(decoded, Value::Array(vec![]));
}

#[tokio::test]
async fn test_query_result_default_and_debug() {
    let mut result = SqliteQueryResult::default();
    assert_eq!(result.rows_affected(), 0);
    assert_eq!(result.last_insert_rowid(), 0);
    assert!(format!("{:?}", result).contains("SqliteQueryResult"));
    result.extend(vec![
        SqliteQueryResult::default(),
        SqliteQueryResult::default(),
    ]);
    assert_eq!(result.rows_affected(), 0);

    // Collected results from a stream also exercise `Extend`.
    let mut conn = memory_conn().await;
    conn.exec("CREATE TABLE e (id INTEGER PRIMARY KEY)", vec![])
        .await
        .unwrap();
    let query = SqliteQuery {
        statement: Either::Left("INSERT INTO e VALUES (1); INSERT INTO e VALUES (2)".to_string()),
        arguments: vec![],
        persistent: false,
    };
    let (results, _rows) = collect_stream(conn.fetch_many(query)).await;
    let total: u64 = results.iter().map(|r| r.rows_affected()).sum();
    assert_eq!(total, 2);
}

#[test]
fn test_database_marker_type() {
    let db = rbdc_sqlite::Sqlite;
    assert!(format!("{:?}", db).contains("Sqlite"));
}

#[tokio::test]
async fn test_bind_all_argument_variants() {
    let mut conn = memory_conn().await;
    conn.exec(
        "CREATE TABLE args (a INTEGER, b INTEGER, c REAL, d TEXT, e BLOB, f TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let inserted = conn
        .exec(
            "INSERT INTO args VALUES (?, ?, ?, ?, ?, ?)",
            vec![
                Value::Bool(true),
                Value::I64(2),
                Value::F64(3.5),
                Value::String("four".into()),
                Value::Binary(vec![5, 6]),
                Value::Null,
            ],
        )
        .await
        .expect("insert all argument kinds");
    assert_eq!(inserted.rows_affected, 1);

    let decoded = conn
        .exec_decode("SELECT a, b, c, d, e, f FROM args", vec![])
        .await
        .unwrap();
    let map = decoded.as_array().unwrap()[0].as_map().unwrap();
    assert_eq!(map["a"], Value::I64(1));
    assert_eq!(map["b"], Value::I64(2));
    assert_eq!(map["c"], Value::F64(3.5));
    assert_eq!(map["d"], Value::String("four".into()));
    assert_eq!(map["e"], Value::Binary(vec![5, 6]));
    assert_eq!(map["f"], Value::Null);
}
