//! Tests for the driver implementation, connect options parsing/builders,
//! error conversion, and row/metadata helpers.

use futures_util::StreamExt;
use rbdc::db::{ConnectOptions, Connection, Driver, MetaData, Placeholder, Row};
use rbdc_duckdb::meta_data::DuckDbMetaData;
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection, DuckDbDriver, DuckDbError, DuckDbRow};
use rbs::Value;

fn temp_path(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rbdc_duckdb_{}_{}_{}",
        std::process::id(),
        tag,
        nanos
    ))
}

#[test]
fn test_driver_name_default_option_and_exchange() {
    let driver = DuckDbDriver::new();
    assert_eq!(driver.name(), "duckdb");
    assert_eq!(driver.exchange("SELECT ?"), "SELECT ?");

    // Default construction must behave like new()
    let default_driver = DuckDbDriver::default();
    assert_eq!(default_driver.name(), "duckdb");

    // default_option returns a DuckDbConnectOptions configured for :memory:
    let opt = default_driver.default_option();
    let opt = opt
        .downcast_ref::<DuckDbConnectOptions>()
        .expect("default_option should be DuckDbConnectOptions");
    assert_eq!(opt.path, ":memory:");
}

#[test]
fn test_driver_column_type_all_variants() {
    let driver = DuckDbDriver::new();
    let cases: Vec<(Value, &str)> = vec![
        (Value::Null, "NULL"),
        (Value::Bool(true), "BOOLEAN"),
        (Value::I32(1), "INTEGER"),
        (Value::I64(1), "BIGINT"),
        (Value::U32(1), "INTEGER"),
        (Value::U64(1), "BIGINT"),
        (Value::F32(1.0), "FLOAT"),
        (Value::F64(1.0), "DOUBLE"),
        (Value::String("x".into()), "VARCHAR"),
        (Value::Binary(vec![1, 2]), "BLOB"),
        (Value::Array(vec![Value::I32(1)]), "VARCHAR"),
        (Value::Map(rbs::value::map::ValueMap::new()), "VARCHAR"),
        (Value::Ext("Date", Box::new(Value::Null)), "DATE"),
        (Value::Ext("DateTime", Box::new(Value::Null)), "TIMESTAMP"),
        (Value::Ext("Datetime", Box::new(Value::Null)), "TIMESTAMP"),
        (Value::Ext("Time", Box::new(Value::Null)), "TIME"),
        (Value::Ext("Timestamp", Box::new(Value::Null)), "BIGINT"),
        (Value::Ext("Decimal", Box::new(Value::Null)), "DECIMAL"),
        (Value::Ext("Json", Box::new(Value::Null)), "VARCHAR"),
        (Value::Ext("Uuid", Box::new(Value::Null)), "UUID"),
        (Value::Ext("SomethingElse", Box::new(Value::Null)), "VARCHAR"),
    ];
    for (value, expected) in cases {
        assert_eq!(
            driver.column_type(&value),
            expected,
            "unexpected column type for {value:?}"
        );
    }
}

#[tokio::test]
async fn test_driver_connect_and_connect_opt() {
    let driver = DuckDbDriver::new();

    let mut conn = driver
        .connect("duckdb://:memory:")
        .await
        .expect("driver.connect should work");
    conn.ping().await.expect("ping");

    let opt = DuckDbConnectOptions::new().path(":memory:");
    let mut conn2 = driver
        .connect_opt(&opt)
        .await
        .expect("driver.connect_opt should work");
    conn2.ping().await.expect("ping");
}

#[tokio::test]
async fn test_driver_connect_uri_forms() {
    let driver = DuckDbDriver::new();
    for url in [":memory:", "duckdb://memory", "duckdb:/:memory:", "duckdb://:memory:"] {
        let mut conn = driver
            .connect(url)
            .await
            .unwrap_or_else(|e| panic!("connect({url}) failed: {e}"));
        conn.ping().await.expect("ping");
        conn.close().await.ok();
    }
}

#[test]
fn test_error_conversion() {
    let err = DuckDbError::General("boom".to_string());
    assert_eq!(err.to_string(), "DuckDB error: boom");

    let rbdc_err: rbdc::Error = err.into();
    assert!(rbdc_err.to_string().contains("DuckDB error: boom"));
}

#[tokio::test]
async fn test_connect_options_defaults_and_builders() {
    let opt = DuckDbConnectOptions::new();
    assert_eq!(opt.path, ":memory:");

    let opt = opt
        .path("some.db")
        .thread_name(|id| format!("custom-worker-{id}"))
        .command_buffer_size(4)
        .row_buffer_size(8)
        .statement_cache_size(16);
    assert_eq!(opt.path, "some.db");

    // Default impl matches new()
    let def = DuckDbConnectOptions::default();
    assert_eq!(def.path, ":memory:");

    // Exercise all builder-configured values end-to-end.
    let opts = DuckDbConnectOptions::new()
        .path(":memory:")
        .thread_name(|id| format!("custom-worker-{id}"))
        .command_buffer_size(4)
        .row_buffer_size(8)
        .statement_cache_size(16);
    let mut conn = DuckDbConnection::establish(&opts)
        .await
        .expect("connect with custom options");
    conn.exec("CREATE TABLE builder_test (id INTEGER)", vec![])
        .await
        .expect("create table");
    conn.ping().await.expect("ping");
    assert_eq!(conn.cached_statements_size(), 1);
}

#[test]
fn test_connect_options_set_uri_variants() {
    let mut opt = DuckDbConnectOptions::new();

    opt.set_uri("").expect("empty uri");
    assert_eq!(opt.path, ":memory:");

    opt.set_uri(":memory:").expect("memory uri");
    assert_eq!(opt.path, ":memory:");

    opt.set_uri("memory").expect("memory alias");
    assert_eq!(opt.path, ":memory:");

    opt.set_uri("duckdb://my.db").expect("scheme uri");
    assert_eq!(opt.path, "my.db");

    opt.set_uri("duckdb:/other.db").expect("single slash uri");
    assert_eq!(opt.path, "other.db");

    opt.set_uri("plain.db").expect("plain uri");
    assert_eq!(opt.path, "plain.db");
}

#[tokio::test]
async fn test_options_with_custom_buffers_and_empty_path() {
    // Empty path is treated as in-memory by the worker.
    let opts = DuckDbConnectOptions::new()
        .path("")
        .command_buffer_size(2)
        .row_buffer_size(2)
        .thread_name(|id| format!("rbdc-test-worker-{id}"));
    let mut conn = DuckDbConnection::establish(&opts)
        .await
        .expect("establish with empty path");
    conn.exec("CREATE TABLE empty_path (id INTEGER)", vec![])
        .await
        .expect("create table");
    conn.ping().await.expect("ping");
}

#[tokio::test]
async fn test_file_database_persists_across_connections() {
    let path = temp_path("persist");
    let path_str = path.to_str().unwrap().to_string();

    {
        let opts = DuckDbConnectOptions::new().path(&path_str);
        let mut conn = DuckDbConnection::establish(&opts)
            .await
            .expect("establish file db");
        conn.exec("CREATE TABLE persisted (id INTEGER)", vec![])
            .await
            .expect("create table");
        conn.exec("INSERT INTO persisted VALUES (42)", vec![])
            .await
            .expect("insert");
        conn.close().await.expect("close");
    }

    {
        // A fresh options instance opens the same file database.
        let opts = DuckDbConnectOptions::new().path(&path_str);
        let mut conn = DuckDbConnection::establish(&opts)
            .await
            .expect("re-establish file db");
        let stream = conn
            .exec_rows("SELECT COUNT(*) FROM persisted", vec![])
            .await
            .expect("select");
        drop(stream);
        conn.close().await.ok();
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path_str}.wal"));
}

#[tokio::test]
async fn test_connections_share_database_from_same_options() {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    let mut conn1 = opts.connect().await.expect("first connection");
    let mut conn2 = opts.connect().await.expect("second connection");

    conn1
        .exec("CREATE TABLE shared_table (id INTEGER)", vec![])
        .await
        .expect("create table on first conn");

    // The second connection from the same options shares the in-memory database.
    conn2
        .exec("INSERT INTO shared_table VALUES (7)", vec![])
        .await
        .expect("insert via second conn");

    {
        let mut stream = conn1
            .exec_rows("SELECT id FROM shared_table", vec![])
            .await
            .expect("select on first conn");
        let mut row = stream.next().await.unwrap().expect("row");
        assert_eq!(row.get(0).unwrap(), Value::I32(7));
    }

    conn1.close().await.ok();
    conn2.close().await.ok();
}

#[tokio::test]
async fn test_open_failure_returns_error() {
    let dir = temp_path("open_fail");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    // Put a regular file where the database's parent directory would be, so
    // duckdb_open cannot create/open the database file.
    let blocker = dir.join("blocker");
    std::fs::write(&blocker, b"not a directory").expect("write blocker");
    let bad_db = blocker.join("db.duckdb");

    let opts = DuckDbConnectOptions::new().path(bad_db.to_str().unwrap());
    let result = DuckDbConnection::establish(&opts).await;
    assert!(result.is_err(), "opening a db under a file must fail");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_row_and_meta_data() {
    let mut row = DuckDbRow::new(
        vec![Value::I64(1), Value::String("abc".into())],
        2,
        vec!["id".to_string(), "name".to_string()],
    );

    assert_eq!(format!("{row:?}"), "DuckDbRow { values: [I64(1), String(\"abc\")] }");

    {
        let md = row.meta_data();
        assert_eq!(md.column_len(), 2);
        assert_eq!(md.column_name(0), "id");
        assert_eq!(md.column_name(1), "name");
        // Out-of-range column names get a synthetic name.
        assert_eq!(md.column_name(9), "col_9");
        assert_eq!(md.column_type(0), "ANY");
        assert_eq!(
            format!("{md:?}"),
            "DuckDbMetaData { column_count: 2 }"
        );
    }

    assert_eq!(row.get(0).unwrap(), Value::I64(1));
    assert_eq!(row.get(1).unwrap(), Value::String("abc".into()));
    assert!(row.get(2).is_err(), "out-of-range index must error");

    // Direct construction of the metadata type works too.
    let md = DuckDbMetaData {
        column_names: vec![],
        column_count: 0,
    };
    assert_eq!(md.column_len(), 0);
    assert_eq!(md.column_name(0), "col_0");
    assert_eq!(md.column_type(0), "ANY");
}
