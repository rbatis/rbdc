//! Tests for DuckDB types: Date, Time, DateTime, Timestamp, Decimal, Json, Uuid

use rbdc::db::Connection;
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection};
use rbs::Value;

async fn create_conn() -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
}

async fn setup_basic(conn: &mut DuckDbConnection) {
    conn.exec(
        "CREATE TABLE IF NOT EXISTS test_basic (
            id INTEGER PRIMARY KEY,
            name TEXT,
            bool_val BOOLEAN,
            int_val INTEGER,
            bigint_val BIGINT,
            real_val REAL,
            double_val DOUBLE,
            text_val TEXT,
            blob_val BLOB
        )",
        vec![],
    )
    .await
    .expect("create table");
}

async fn setup_ext(conn: &mut DuckDbConnection, table_name: &str, col_type: &str) {
    conn.exec(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, val {})",
            table_name, col_type
        ),
        vec![],
    )
    .await
    .expect("create table");
}

fn make_ext(type_name: &'static str, value: Value) -> Value {
    Value::Ext(type_name, Box::new(value))
}

#[tokio::test]
async fn test_basic_types() {
    let mut conn = create_conn().await;
    setup_basic(&mut conn).await;

    conn.exec(
        "INSERT INTO test_basic (id, name, bool_val, int_val, bigint_val, real_val, double_val) VALUES (?, ?, ?, ?, ?, ?, ?)",
        vec![
            Value::I32(1),
            Value::String("test".to_string()),
            Value::Bool(true),
            Value::I32(42),
            Value::I64(12345678901234),
            Value::F32(3.14),
            Value::F64(2.718281828),
        ],
    )
    .await
    .expect("insert basic types");

    let value = conn
        .exec_decode("SELECT id, name, bool_val, int_val, bigint_val, real_val, double_val FROM test_basic WHERE id = 1", vec![])
        .await
        .expect("select");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1, "expected 1 row, got {}", arr.len());
            match &arr[0] {
                Value::Map(m) => {
                    assert_eq!(m.len(), 7, "expected 7 columns");
                }
                _ => panic!("expected Map for row"),
            }
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_date_type() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_date", "VARCHAR").await;

    conn.exec(
        "INSERT INTO test_date (id, val) VALUES (?, ?)",
        vec![Value::I32(1), make_ext("Date", Value::String("2024-01-15".to_string()))],
    )
    .await
    .expect("insert date");

    let value = conn
        .exec_decode("SELECT id, val FROM test_date WHERE id = 1", vec![])
        .await
        .expect("select date");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_time_type() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_time", "VARCHAR").await;

    conn.exec(
        "INSERT INTO test_time (id, val) VALUES (?, ?)",
        vec![Value::I32(1), make_ext("Time", Value::String("12:30:45".to_string()))],
    )
    .await
    .expect("insert time");

    let value = conn
        .exec_decode("SELECT id, val FROM test_time WHERE id = 1", vec![])
        .await
        .expect("select time");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_datetime_type() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_datetime", "VARCHAR").await;

    conn.exec(
        "INSERT INTO test_datetime (id, val) VALUES (?, ?)",
        vec![Value::I32(1), make_ext("DateTime", Value::String("2024-01-15 12:30:45".to_string()))],
    )
    .await
    .expect("insert datetime");

    let value = conn
        .exec_decode("SELECT id, val FROM test_datetime WHERE id = 1", vec![])
        .await
        .expect("select datetime");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_timestamp_type() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_timestamp", "VARCHAR").await;

    conn.exec(
        "INSERT INTO test_timestamp (id, val) VALUES (?, ?)",
        vec![Value::I32(1), make_ext("Timestamp", Value::String("2024-01-15 12:30:45.123456".to_string()))],
    )
    .await
    .expect("insert timestamp");

    let value = conn
        .exec_decode("SELECT id, val FROM test_timestamp WHERE id = 1", vec![])
        .await
        .expect("select timestamp");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_decimal_type() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_decimal", "VARCHAR").await;

    conn.exec(
        "INSERT INTO test_decimal (id, val) VALUES (?, ?)",
        vec![Value::I32(1), make_ext("Decimal", Value::String("123.456789".to_string()))],
    )
    .await
    .expect("insert decimal");

    let value = conn
        .exec_decode("SELECT id, val FROM test_decimal WHERE id = 1", vec![])
        .await
        .expect("select decimal");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_blob_type() {
    let mut conn = create_conn().await;
    setup_basic(&mut conn).await;

    conn.exec(
        "INSERT INTO test_basic (id, blob_val) VALUES (?, ?)",
        vec![Value::I32(1), Value::Binary(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f])],
    )
    .await
    .expect("insert blob");

    let value = conn
        .exec_decode("SELECT id, blob_val FROM test_basic WHERE id = 1", vec![])
        .await
        .expect("select blob");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}

#[tokio::test]
async fn test_null_type() {
    let mut conn = create_conn().await;
    setup_basic(&mut conn).await;

    conn.exec(
        "INSERT INTO test_basic (id, name) VALUES (?, ?)",
        vec![Value::I32(1), Value::Null],
    )
    .await
    .expect("insert null");

    let value = conn
        .exec_decode("SELECT id, name FROM test_basic WHERE id = 1", vec![])
        .await
        .expect("select null");

    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("expected Array, got {:?}", value),
    }
}
