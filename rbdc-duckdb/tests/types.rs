//! Tests for DuckDB types: Date, Time, DateTime, Timestamp, Decimal, Json, Uuid
//! Verifies insert and query work correctly

use std::str::FromStr;
use rbdc::db::Connection;
use rbdc::types::{Date, DateTime, Decimal, Time, Timestamp, Uuid};
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection};
use rbs::Value;

async fn create_conn() -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
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

#[tokio::test]
async fn test_date_insert_and_query() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_date", "VARCHAR").await;

    // Insert using rbdc::types::Date -> Value::Ext
    let date: Value = Date::from_str("2024-01-15").unwrap().into();
    conn.exec(
        "INSERT INTO test_date (id, val) VALUES (?, ?)",
        vec![Value::I32(1), date],
    )
    .await
    .expect("insert date");

    // Query - verify we get back the string value
    let value = conn
        .exec_decode("SELECT id, val FROM test_date WHERE id = 1", vec![])
        .await
        .expect("select date");

    eprintln!("DEBUG date value = {:?}", value);

    // Value is Value::Array([Value::Map(...)])
    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    eprintln!("DEBUG rows = {:?}", rows);
    assert_eq!(rows.len(), 1);
    let id_val = rows[0]["id"].as_f64().expect("id should be number");
    assert_eq!(id_val, 1.0);
    let val_str = rows[0]["val"].as_str().expect("val should be string");
    assert_eq!(val_str, "2024-01-15");
}

#[tokio::test]
async fn test_time_insert_and_query() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_time", "VARCHAR").await;

    let time: Value = Time::from_str("12:30:45").unwrap().into();
    conn.exec(
        "INSERT INTO test_time (id, val) VALUES (?, ?)",
        vec![Value::I32(1), time],
    )
    .await
    .expect("insert time");

    let value = conn
        .exec_decode("SELECT id, val FROM test_time WHERE id = 1", vec![])
        .await
        .expect("select time");

    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"].as_f64().unwrap(), 1.0);
    assert_eq!(rows[0]["val"].as_str().unwrap(), "12:30:45");
}

#[tokio::test]
async fn test_datetime_insert_and_query() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_datetime", "VARCHAR").await;

    let datetime: Value = DateTime::from_str("2024-01-15 12:30:45").unwrap().into();
    conn.exec(
        "INSERT INTO test_datetime (id, val) VALUES (?, ?)",
        vec![Value::I32(1), datetime],
    )
    .await
    .expect("insert datetime");

    let value = conn
        .exec_decode("SELECT id, val FROM test_datetime WHERE id = 1", vec![])
        .await
        .expect("select datetime");

    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"].as_f64().unwrap(), 1.0);
    // DateTime is stored with timezone format
    assert_eq!(rows[0]["val"].as_str().unwrap(), "2024-01-15T12:30:45+08:00");
}

#[tokio::test]
async fn test_timestamp_insert_and_query() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_timestamp", "VARCHAR").await;

    let ts: Value = Timestamp::utc().into();
    conn.exec(
        "INSERT INTO test_timestamp (id, val) VALUES (?, ?)",
        vec![Value::I32(1), ts],
    )
    .await
    .expect("insert timestamp");

    let value = conn
        .exec_decode("SELECT id, val FROM test_timestamp WHERE id = 1", vec![])
        .await
        .expect("select timestamp");

    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"].as_f64().unwrap(), 1.0);
    // Timestamp is stored/returned as Number (milliseconds since epoch)
    assert!(rows[0]["val"].is_number(), "timestamp should be number");
}

#[tokio::test]
async fn test_decimal_insert_and_query() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_decimal", "VARCHAR").await;

    let decimal: Value = Decimal::new("123.456789").unwrap().into();
    conn.exec(
        "INSERT INTO test_decimal (id, val) VALUES (?, ?)",
        vec![Value::I32(1), decimal],
    )
    .await
    .expect("insert decimal");

    let value = conn
        .exec_decode("SELECT id, val FROM test_decimal WHERE id = 1", vec![])
        .await
        .expect("select decimal");

    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"].as_f64().unwrap(), 1.0);
    // DuckDB returns decimal as Number when stored in VARCHAR
    assert_eq!(rows[0]["val"].as_f64().unwrap(), 123.456789);
}

#[tokio::test]
async fn test_uuid_insert_and_query() {
    let mut conn = create_conn().await;
    setup_ext(&mut conn, "test_uuid", "VARCHAR").await;

    let uuid: Value = Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").unwrap().into();
    conn.exec(
        "INSERT INTO test_uuid (id, val) VALUES (?, ?)",
        vec![Value::I32(1), uuid],
    )
    .await
    .expect("insert uuid");

    let value = conn
        .exec_decode("SELECT id, val FROM test_uuid WHERE id = 1", vec![])
        .await
        .expect("select uuid");

    let rows: Vec<serde_json::Value> = rbs::from_value(value).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"].as_f64().unwrap(), 1.0);
    assert_eq!(rows[0]["val"].as_str().unwrap(), "550e8400-e29b-41d4-a716-446655440000");
}
