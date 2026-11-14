//! Connection-level integration tests: exec/exec_rows/exec_decode, metadata,
//! multi-statement SQL, parameter binding and failure paths.

use futures_util::StreamExt;
use rbdc::db::Connection;
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection};
use rbs::Value;

async fn create_conn() -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
}

async fn create_conn_with_row_buffer(size: usize) -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new()
        .path(":memory:")
        .row_buffer_size(size);
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
}

async fn query_row(conn: &mut DuckDbConnection, sql: &str, params: Vec<Value>) -> Vec<Value> {
    let mut stream = conn
        .exec_rows(sql, params)
        .await
        .unwrap_or_else(|e| panic!("exec_rows({sql}) failed: {e}"));
    let mut row = stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no row for {sql}"))
        .unwrap_or_else(|e| panic!("row error for {sql}: {e}"));
    let cols = row.meta_data().column_len();
    (0..cols).map(|i| row.get(i).unwrap()).collect()
}

#[tokio::test]
async fn test_exec_decode_metadata() {
    let mut conn = create_conn().await;
    let value = conn
        .exec_decode("SELECT 1 AS one, 'x' AS two", vec![])
        .await
        .expect("exec_decode");
    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["one"].as_i64(), Some(1));
    assert_eq!(rows[0]["two"].as_str(), Some("x"));
}

#[tokio::test]
async fn test_exec_select_reports_zero_affected() {
    let mut conn = create_conn().await;
    let result = conn.exec("SELECT 1", vec![]).await.expect("exec select");
    assert_eq!(result.rows_affected, 0);
    assert_eq!(result.last_insert_id, Value::Null);
}

#[tokio::test]
async fn test_empty_result_set_stream() {
    let mut conn = create_conn().await;
    conn.exec("CREATE TABLE empty_table (id INTEGER)", vec![])
        .await
        .expect("create table");

    {
        let mut stream = conn
            .exec_rows("SELECT * FROM empty_table", vec![])
            .await
            .expect("exec_rows");
        assert!(stream.next().await.is_none());
    }

    // Connection still usable afterwards
    conn.ping().await.expect("ping after empty result");
}

#[tokio::test]
async fn test_bind_param_types() {
    let mut conn = create_conn().await;

    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::Null]).await,
        vec![Value::Null]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::Bool(true)]).await,
        vec![Value::Bool(true)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::I32(-3)]).await,
        vec![Value::I32(-3)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::I64(-4)]).await,
        vec![Value::I64(-4)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::U32(5)]).await,
        vec![Value::U64(5)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::U64(6)]).await,
        vec![Value::U64(6)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::F32(1.5)]).await,
        vec![Value::F32(1.5)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::F64(2.5)]).await,
        vec![Value::F64(2.5)]
    );
    assert_eq!(
        query_row(&mut conn, "SELECT ?", vec![Value::String("hi".into())]).await,
        vec![Value::String("hi".into())]
    );
    assert!(matches!(
        &query_row(
            &mut conn,
            "SELECT ?::HUGEINT",
            vec![Value::Ext(
                "HugeInt",
                Box::new(Value::String("12345678901234567890".into()))
            )]
        )
        .await[0],
        Value::Ext("HugeInt", value) if **value == Value::String("12345678901234567890".into())
    ));
    assert!(matches!(
        &query_row(
            &mut conn,
            "SELECT ?::UHUGEINT",
            vec![Value::Ext(
                "UHugeInt",
                Box::new(Value::String("340282366920938463463374607431768211455".into()))
            )]
        )
        .await[0],
        Value::Ext("UHugeInt", value)
            if **value == Value::String("340282366920938463463374607431768211455".into())
    ));
}

#[tokio::test]
async fn test_bind_binary_array_and_map_params() {
    let mut conn = create_conn().await;

    let value = query_row(&mut conn, "SELECT ?", vec![Value::Binary(vec![65, 66, 67])]).await;
    assert!(
        matches!(&value[0], Value::Binary(_)),
        "binary param should decode as binary, got {:?}",
        value[0]
    );

    let value = query_row(
        &mut conn,
        "SELECT ?",
        vec![Value::Array(vec![Value::I32(1), Value::I32(2)])],
    )
    .await;
    assert!(
        matches!(&value[0], Value::Array(a) if a.len() == 2),
        "array param should decode as array, got {:?}",
        value[0]
    );

    let mut map = rbs::value::map::ValueMap::new();
    map.insert(Value::String("k".into()), Value::I32(1));
    let value = query_row(&mut conn, "SELECT ?", vec![Value::Map(map)]).await;
    assert!(
        matches!(&value[0], Value::Map(m) if m.len() == 1),
        "map param should decode as map, got {:?}",
        value[0]
    );
}

#[tokio::test]
async fn test_bind_ext_params() {
    let mut conn = create_conn().await;

    // Numeric Timestamp binds as Int64
    assert_eq!(
        query_row(
            &mut conn,
            "SELECT ?",
            vec![Value::Ext("Timestamp", Box::new(Value::I64(123)))]
        )
        .await,
        vec![Value::I64(123)]
    );

    // Text Timestamp stays text
    assert_eq!(
        query_row(
            &mut conn,
            "SELECT ?",
            vec![Value::Ext(
                "Timestamp",
                Box::new(Value::String("abc".into()))
            )]
        )
        .await,
        vec![Value::String("abc".into())]
    );

    // Date is encoded as text
    assert_eq!(
        query_row(
            &mut conn,
            "SELECT ?",
            vec![Value::Ext(
                "Date",
                Box::new(Value::String("2024-01-15".into()))
            )]
        )
        .await,
        vec![Value::String("2024-01-15".into())]
    );
}

#[tokio::test]
async fn test_bind_typed_uuid() {
    let mut conn = create_conn().await;
    let values = query_row(
            &mut conn,
            "SELECT CAST(? AS UUID)",
            vec![Value::Ext(
                "Uuid",
                Box::new(Value::String("550e8400-e29b-41d4-a716-446655440000".into())),
            )]
        )
        .await;
    assert!(matches!(
        &values[0],
        Value::Ext("Uuid", value)
            if **value == Value::String("550e8400-e29b-41d4-a716-446655440000".into())
    ), "unexpected UUID: {:?}", values[0]);
}

#[tokio::test]
async fn test_bind_typed_bit() {
    let mut conn = create_conn().await;
    let values = query_row(
            &mut conn,
            "SELECT CAST(? AS BIT)",
            vec![Value::Ext("Bit", Box::new(Value::String("101".into())))]
        )
        .await;
    assert!(matches!(
        &values[0],
        Value::Ext("Bit", value) if **value == Value::String("101".into())
    ), "unexpected BIT: {:?}", values[0]);
}

#[tokio::test]
async fn test_bind_typed_timestamptz() {
    let mut conn = create_conn().await;
    assert!(matches!(
        &query_row(
            &mut conn,
            "SELECT CAST(? AS TIMESTAMPTZ)",
            vec![Value::Ext(
                "DateTime",
                Box::new(Value::String("2024-01-15 12:30:45+00".into())),
            )]
        )
        .await[0],
        Value::Ext("DateTime", _)
    ));
}

#[tokio::test]
async fn test_bind_typed_enum() {
    let mut conn = create_conn().await;
    conn.exec("CREATE TYPE test_mood AS ENUM ('happy', 'sad')", vec![])
        .await
        .expect("create enum");
    assert_eq!(
        query_row(
            &mut conn,
            "SELECT CAST(? AS test_mood)",
            vec![Value::Ext("Enum", Box::new(Value::String("happy".into())))]
        )
        .await,
        vec![Value::String("happy".into())]
    );
}

#[tokio::test]
async fn test_unbound_param_errors() {
    let mut conn = create_conn().await;

    // A string containing an interior NUL cannot be converted to a C string,
    // so it is never bound and execution must fail.
    let err = conn
        .exec("SELECT ?", vec![Value::String("a\0b".into())])
        .await;
    assert!(err.is_err(), "unbindable string parameter must fail");

    // Same for an Ext value whose text contains a NUL.
    let err = conn
        .exec(
            "SELECT ?",
            vec![Value::Ext("Date", Box::new(Value::String("a\0b".into())))],
        )
        .await;
    assert!(err.is_err(), "unbindable ext parameter must fail");

    // Unknown Ext types are rejected instead of being silently bound as NULL.
    let err = conn
        .exec(
            "SELECT ?",
            vec![Value::Ext("Unknown", Box::new(Value::I32(1)))],
        )
        .await;
    assert!(err.is_err(), "unknown ext parameter must fail to bind");
}

#[tokio::test]
async fn test_sql_with_interior_nul_is_rejected() {
    let mut conn = create_conn().await;
    let err = conn.exec("SELECT\0 1", vec![]).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_invalid_sql_errors() {
    let mut conn = create_conn().await;
    assert!(conn.exec("SELEC 1", vec![]).await.is_err());
    assert!(conn
        .exec("SELECT * FROM definitely_missing_table", vec![])
        .await
        .is_err());
}

#[tokio::test]
async fn test_exec_rows_error_stream() {
    let mut conn = create_conn().await;
    {
        let mut stream = conn
            .exec_rows("SELECT * FROM missing_stream_table", vec![])
            .await
            .expect("exec_rows returns a stream");

        let first = stream.next().await.expect("error item");
        assert!(first.is_err(), "expected an error from the stream");
        // After the error the underlying channel is closed.
        assert!(stream.next().await.is_none());
    }

    // And the connection still works.
    conn.ping().await.expect("ping after error stream");
}

#[tokio::test]
async fn test_multi_statement_exec() {
    let mut conn = create_conn().await;
    let result = conn
        .exec(
            "CREATE TABLE multi (id INTEGER); INSERT INTO multi VALUES (1); INSERT INTO multi VALUES (2)",
            vec![],
        )
        .await
        .expect("multi-statement exec");
    assert_eq!(result.rows_affected, 1, "final INSERT affects one row");

    let value = conn
        .exec_decode("SELECT COUNT(*) AS c FROM multi", vec![])
        .await
        .expect("count");
    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows[0]["c"].as_i64(), Some(2));
}

#[tokio::test]
async fn test_multi_statement_exec_rows_with_final_select() {
    let mut conn = create_conn().await;
    let mut stream = conn
        .exec_rows(
            "CREATE TABLE multi_select (id INTEGER); INSERT INTO multi_select VALUES (5); SELECT id FROM multi_select",
            vec![],
        )
        .await
        .expect("multi-statement exec_rows");

    let mut row = stream.next().await.expect("row").expect("ok row");
    assert_eq!(row.get(0).unwrap(), Value::I32(5));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn test_multi_statement_intermediate_execute_error() {
    let mut conn = create_conn().await;
    let err = conn
        .exec(
            "CREATE TABLE multi_err (id INTEGER); INSERT INTO missing_intermediate VALUES (1)",
            vec![],
        )
        .await;
    assert!(err.is_err(), "intermediate execution failure must surface");
}

#[tokio::test]
async fn test_multi_statement_intermediate_prepare_error() {
    let mut conn = create_conn().await;
    let err = conn
        .exec("INSERT INTO missing_prepare VALUES (1); SELECT 1", vec![])
        .await;
    assert!(err.is_err(), "intermediate prepare failure must surface");
}

#[tokio::test]
async fn test_dropping_stream_early_keeps_connection_usable() {
    let mut conn = create_conn_with_row_buffer(1).await;
    conn.exec("CREATE TABLE many_rows (id INTEGER)", vec![])
        .await
        .expect("create table");
    for i in 0..64 {
        conn.exec(&format!("INSERT INTO many_rows VALUES ({i})"), vec![])
            .await
            .expect("insert");
    }

    {
        let stream = conn
            .exec_rows("SELECT * FROM many_rows", vec![])
            .await
            .expect("exec_rows");
        // Drop without consuming any row; the worker must observe the closed
        // channel and stop sending without deadlocking.
        drop(stream);
    }

    conn.ping()
        .await
        .expect("ping must complete after dropped stream");
    let value = conn
        .exec_decode("SELECT COUNT(*) AS c FROM many_rows", vec![])
        .await
        .expect("count");
    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows[0]["c"].as_i64(), Some(64));
}

#[tokio::test]
async fn test_use_after_close_errors() {
    let mut conn = create_conn().await;
    conn.close().await.expect("close");
    assert!(conn.ping().await.is_err(), "ping after close must error");
    assert!(
        conn.exec("SELECT 1", vec![]).await.is_err(),
        "exec after close must error"
    );
    assert!(
        conn.exec_rows("SELECT 1", vec![]).await.is_err(),
        "exec_rows after close must error"
    );
    // A second shutdown has no worker left to receive the command.
    assert!(conn.close().await.is_err(), "second close must error");
}

#[tokio::test]
async fn test_intermediate_statement_execution_error_is_reported() {
    let mut conn = create_conn().await;
    // The first (intermediate) statement of a multi-statement query prepares
    // successfully but fails at execution time.
    let err = conn
        .exec("SELECT CAST('not_an_int' AS INTEGER); SELECT 1", vec![])
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
    // The connection remains usable after the failed intermediate statement.
    let value = conn.exec_decode("SELECT 42", vec![]).await.unwrap();
    assert_eq!(
        value.as_array().unwrap()[0].as_map().unwrap()["42"].as_i64(),
        Some(42)
    );
}

#[tokio::test]
async fn test_cached_statements_and_clear_cache() {
    let mut conn = create_conn().await;
    conn.exec("CREATE TABLE cache_api (id INTEGER)", vec![])
        .await
        .expect("create");
    // Same SQL twice -> one cached statement for the CREATE plus one for INSERT
    conn.exec("INSERT INTO cache_api VALUES (1)", vec![])
        .await
        .expect("insert");
    assert_eq!(conn.cached_statements_size(), 2);

    conn.clear_cache().await.expect("clear");
    assert_eq!(conn.cached_statements_size(), 0);
}

/// A prepared statement reused through the cache must be re-bound on each
/// execution (bindings reset), not accumulate stale parameters.
#[tokio::test]
async fn test_cached_statement_rebinds_params() {
    let mut conn = create_conn().await;
    conn.exec("CREATE TABLE rebind (id INTEGER)", vec![])
        .await
        .expect("create");

    for i in 0..10 {
        conn.exec("INSERT INTO rebind VALUES (?)", vec![Value::I64(i)])
            .await
            .expect("insert");
    }

    let value = conn
        .exec_decode("SELECT COUNT(*) AS c FROM rebind", vec![])
        .await
        .expect("count");
    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows[0]["c"].as_i64(), Some(10));
}
