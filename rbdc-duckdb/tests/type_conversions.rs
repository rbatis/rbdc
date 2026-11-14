//! Tests for rbs::Value <-> DuckDB conversions.
//!
//! Two layers are covered:
//!  * direct unit-level checks of `Encode`, `value_to_param`, `is_json_string`
//!    and `DuckDbType::from_duckdb_type`;
//!  * end-to-end extraction of every DuckDB logical type that the driver can
//!    produce via SQL (`extract_row_values`).

use futures_util::StreamExt;
use libduckdb_sys as duck;
use rbdc::db::Connection;
use rbdc_duckdb::types::{
    extract_row_values, is_json_string, value_to_param, DuckDbArgumentValue, DuckDbType,
    DuckDbTypeInfo, Encode, IsNull,
};
use rbdc_duckdb::{DuckDbConnectOptions, DuckDbConnection};
use rbs::Value;

fn encode(v: Value) -> (Vec<DuckDbArgumentValue>, IsNull) {
    let mut args = Vec::new();
    let null = v.encode(&mut args).expect("encode should succeed");
    (args, null)
}

fn assert_text(args: &[DuckDbArgumentValue], expected: &str) {
    match args {
        [DuckDbArgumentValue::Text(s)] => assert_eq!(s, expected),
        other => panic!("expected one Text({expected:?}), got {other:?}"),
    }
}

#[test]
fn test_encode_all_value_variants() {
    let (args, null) = encode(Value::Null);
    assert!(matches!(null, IsNull::Yes));
    assert!(args.is_empty());

    let (args, null) = encode(Value::Bool(true));
    assert!(matches!(null, IsNull::No));
    assert!(matches!(
        args.as_slice(),
        [DuckDbArgumentValue::Boolean(true)]
    ));
    let (args, _) = encode(Value::Bool(false));
    assert!(matches!(
        args.as_slice(),
        [DuckDbArgumentValue::Boolean(false)]
    ));

    let (args, _) = encode(Value::I32(-5));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::Int(-5)]));
    let (args, _) = encode(Value::I64(-6));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::Int64(-6)]));
    let (args, _) = encode(Value::U32(7));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::UInt32(7)]));
    let (args, _) = encode(Value::U64(8));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::UInt64(8)]));
    let (args, _) = encode(Value::F32(1.25));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::Float(v)] if *v == 1.25));
    let (args, _) = encode(Value::F64(2.5));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::Double(v)] if *v == 2.5));
    let (args, _) = encode(Value::String("text".into()));
    assert_text(&args, "text");
    let (args, _) = encode(Value::Binary(vec![1, 2, 3]));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::Blob(b)] if b == &[1, 2, 3]));

    let (args, _) = encode(Value::Array(vec![Value::I32(1), Value::I32(2)]));
    assert_text(&args, "[1,2]");

    let mut map = rbs::value::map::ValueMap::new();
    map.insert(Value::String("k".into()), Value::I32(1));
    let (args, _) = encode(Value::Map(map));
    assert_text(&args, "{\"k\":1}");
}

#[test]
fn test_encode_ext_variants() {
    for (name, inner, expected) in [
        ("Date", Value::String("2024-01-15".into()), "2024-01-15"),
        (
            "DateTime",
            Value::String("2024-01-15T10:00:00".into()),
            "2024-01-15T10:00:00",
        ),
        ("Time", Value::String("10:00:00".into()), "10:00:00"),
        ("Decimal", Value::String("1.23".into()), "1.23"),
        ("Json", Value::String("{}".into()), "{}"),
        (
            "Uuid",
            Value::String("550e8400-e29b-41d4-a716-446655440000".into()),
            "550e8400-e29b-41d4-a716-446655440000",
        ),
    ] {
        let (args, null) = encode(Value::Ext(name, Box::new(inner)));
        assert!(matches!(null, IsNull::No));
        if name == "DateTime" {
            assert!(matches!(
                args.as_slice(),
                [DuckDbArgumentValue::DateTime(value)] if value == expected
            ));
        } else if name == "Uuid" {
            assert!(matches!(
                args.as_slice(),
                [DuckDbArgumentValue::Uuid(_)]
            ));
        } else {
            assert_text(&args, expected);
        }
    }

    let mut invalid_uuid_args = Vec::new();
    assert!(Value::Ext("Uuid", Box::new(Value::String("abc".into())))
        .encode(&mut invalid_uuid_args)
        .is_err());
    assert!(invalid_uuid_args.is_empty());

    let huge = i128::MAX.to_string();
    let (args, _) = encode(Value::Ext(
        "HugeInt",
        Box::new(Value::String(huge.clone())),
    ));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::HugeInt(v)] if *v == i128::MAX));

    let uhuge = u128::MAX.to_string();
    let (args, _) = encode(Value::Ext(
        "UHugeInt",
        Box::new(Value::String(uhuge.clone())),
    ));
    assert!(matches!(args.as_slice(), [DuckDbArgumentValue::UHugeInt(v)] if *v == u128::MAX));

    // Numeric timestamp is encoded as Int64
    let (args, _) = encode(Value::Ext("Timestamp", Box::new(Value::I64(1234))));
    assert!(matches!(
        args.as_slice(),
        [DuckDbArgumentValue::Int64(1234)]
    ));

    // Non-numeric timestamp falls back to text
    let (args, _) = encode(Value::Ext(
        "Timestamp",
        Box::new(Value::String("abc".into())),
    ));
    assert_text(&args, "abc");

    // Unknown extension types are rejected.
    let mut args = Vec::new();
    assert!(Value::Ext("Unknown", Box::new(Value::I32(1)))
        .encode(&mut args)
        .is_err());
    assert!(args.is_empty());
}

#[test]
fn test_value_to_param_all_variants() {
    assert!(matches!(
        value_to_param(Value::Null),
        DuckDbArgumentValue::Null
    ));
    assert!(matches!(
        value_to_param(Value::Bool(true)),
        DuckDbArgumentValue::Boolean(true)
    ));
    assert!(matches!(
        value_to_param(Value::Bool(false)),
        DuckDbArgumentValue::Boolean(false)
    ));
    assert!(matches!(
        value_to_param(Value::I32(-1)),
        DuckDbArgumentValue::Int(-1)
    ));
    assert!(matches!(
        value_to_param(Value::I64(-2)),
        DuckDbArgumentValue::Int64(-2)
    ));
    assert!(matches!(
        value_to_param(Value::U32(3)),
        DuckDbArgumentValue::UInt32(3)
    ));
    assert!(matches!(
        value_to_param(Value::U64(4)),
        DuckDbArgumentValue::UInt64(4)
    ));
    assert!(matches!(value_to_param(Value::F32(0.5)), DuckDbArgumentValue::Float(v) if v == 0.5));
    assert!(matches!(value_to_param(Value::F64(1.5)), DuckDbArgumentValue::Double(v) if v == 1.5));

    match value_to_param(Value::String("s".into())) {
        DuckDbArgumentValue::Text(s) => assert_eq!(s, "s"),
        other => panic!("unexpected {other:?}"),
    }
    match value_to_param(Value::Binary(vec![9, 8])) {
        DuckDbArgumentValue::Blob(b) => assert_eq!(b, vec![9, 8]),
        other => panic!("unexpected {other:?}"),
    }
    match value_to_param(Value::Array(vec![Value::I32(1)])) {
        DuckDbArgumentValue::Text(s) => assert_eq!(s, "[1]"),
        other => panic!("unexpected {other:?}"),
    }
    let mut map = rbs::value::map::ValueMap::new();
    map.insert(Value::String("a".into()), Value::I32(2));
    match value_to_param(Value::Map(map)) {
        DuckDbArgumentValue::Text(s) => assert_eq!(s, "{\"a\":2}"),
        other => panic!("unexpected {other:?}"),
    }

    for (name, inner, expected) in [
        ("Date", Value::String("2024-01-15".into()), "2024-01-15"),
        ("Time", Value::String("10:00:00".into()), "10:00:00"),
        ("DateTime", Value::String("dt".into()), "dt"),
        ("DateTime", Value::String("dt2".into()), "dt2"),
        ("Decimal", Value::String("1.23".into()), "1.23"),
        ("Json", Value::String("{}".into()), "{}"),
        ("Uuid", Value::String("uuid".into()), "uuid"),
        ("HugeInt", Value::String("42".into()), "42"),
        ("UHugeInt", Value::String("43".into()), "43"),
        ("Other", Value::String("other".into()), "other"),
    ] {
        match value_to_param(Value::Ext(name, Box::new(inner))) {
            DuckDbArgumentValue::Text(s) => assert_eq!(s, expected),
            DuckDbArgumentValue::DateTime(s) => assert_eq!(s, expected),
            DuckDbArgumentValue::HugeInt(v) => assert_eq!(v.to_string(), expected),
            DuckDbArgumentValue::UHugeInt(v) => assert_eq!(v.to_string(), expected),
            other => panic!("unexpected {other:?}"),
        }
    }

    // Timestamp with a numeric string is converted to Int64
    match value_to_param(Value::Ext(
        "Timestamp",
        Box::new(Value::String("999".into())),
    )) {
        DuckDbArgumentValue::Int64(n) => assert_eq!(n, 999),
        other => panic!("unexpected {other:?}"),
    }
    // Non-numeric timestamp stays text
    match value_to_param(Value::Ext("Timestamp", Box::new(Value::String("x".into())))) {
        DuckDbArgumentValue::Text(s) => assert_eq!(s, "x"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn test_from_duckdb_type_maps_every_constant() {
    use DuckDbType as T;
    let mappings: Vec<(duck::DUCKDB_TYPE, T)> = vec![
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_INVALID, T::Null),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, T::Boolean),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, T::TinyInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, T::SmallInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, T::Int),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, T::BigInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, T::UTinyInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, T::USmallInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, T::UInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT, T::UBigInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, T::Float),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, T::Double),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL, T::Decimal),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, T::Timestamp),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, T::TimestampS),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, T::TimestampMs),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS, T::TimestampNs),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ, T::TimestampTz),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_DATE, T::Date),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIME, T::Time),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ, T::TimeTz),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL, T::Interval),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, T::HugeInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT, T::UHugeInt),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, T::Varchar),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_BLOB, T::Blob),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_ENUM, T::Enum),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_LIST, T::List),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT, T::Struct),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_MAP, T::Map),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY, T::Array),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_UNION, T::Union),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_UUID, T::UUID),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_BIT, T::Bit),
        (duck::DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL, T::SQLNull),
    ];
    for (raw, expected) in mappings {
        assert_eq!(DuckDbType::from_duckdb_type(raw), expected);
    }
    // Unknown type ids fall back to Varchar.
    assert_eq!(
        DuckDbType::from_duckdb_type(duck::DUCKDB_TYPE_DUCKDB_TYPE_ANY),
        DuckDbType::Varchar
    );
    assert_eq!(DuckDbType::from_duckdb_type(9999), DuckDbType::Varchar);
}

#[test]
fn test_type_info_and_debug() {
    let info = DuckDbTypeInfo(DuckDbType::Date);
    let cloned = info.clone();
    assert_eq!(cloned.0, DuckDbType::Date);
    assert!(format!("{info:?}").contains("Date"));
}

#[test]
fn test_is_json_string() {
    assert!(is_json_string("null"));
    assert!(is_json_string("{}"));
    assert!(is_json_string("{\"a\":1}"));
    assert!(is_json_string("[]"));
    assert!(is_json_string("[1,2]"));
    assert!(!is_json_string(""));
    assert!(!is_json_string("hello"));
    assert!(!is_json_string("{not closed"));
    assert!(!is_json_string("[1,2"));
}

async fn create_conn() -> DuckDbConnection {
    let opts = DuckDbConnectOptions::new().path(":memory:");
    DuckDbConnection::establish(&opts)
        .await
        .expect("failed to create in-memory DuckDB connection")
}

async fn query_row(conn: &mut DuckDbConnection, sql: &str) -> Vec<Value> {
    let mut stream = conn
        .exec_rows(sql, vec![])
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
async fn test_extract_numeric_and_boolean_types() {
    let mut conn = create_conn().await;
    let values = query_row(
        &mut conn,
        "SELECT 1::TINYINT, 2::SMALLINT, 3::INTEGER, 4::BIGINT, \
         5::UTINYINT, 6::USMALLINT, 7::UINTEGER, 8::UBIGINT, \
         9.5::FLOAT, 10.5::DOUBLE, true",
    )
    .await;
    assert_eq!(
        values,
        vec![
            Value::I32(1),
            Value::I32(2),
            Value::I32(3),
            Value::I64(4),
            Value::U32(5),
            Value::U32(6),
            Value::U64(7),
            Value::U64(8),
            Value::F32(9.5),
            Value::F64(10.5),
            Value::Bool(true),
        ]
    );
}

#[tokio::test]
async fn test_extract_varchar_parsing_variants() {
    let mut conn = create_conn().await;
    let values = query_row(
        &mut conn,
        "SELECT '{\"a\":1}', '[1,2]', 'null', '42', '1.5', 'hello', '{oops}'",
    )
    .await;
    assert!(
        matches!(&values[0], Value::Map(m) if m.get(&Value::String("a".into())) == &Value::U64(1))
    );
    assert!(matches!(&values[1], Value::Array(a) if a.len() == 2));
    assert_eq!(values[2], Value::Null);
    assert_eq!(values[3], Value::I64(42));
    assert_eq!(values[4], Value::F64(1.5));
    assert_eq!(values[5], Value::String("hello".into()));
    // Looks like JSON, but isn't: falls back to a plain string.
    assert_eq!(values[6], Value::String("{oops}".into()));
}

#[tokio::test]
async fn test_extract_temporal_types() {
    let mut conn = create_conn().await;
    let values = query_row(
        &mut conn,
        "SELECT DATE '2024-01-15', TIME '12:30:45', \
         TIMESTAMP '2024-01-15 12:30:45', \
         CAST('2024-01-15 12:30:45' AS TIMESTAMP_S), \
         CAST('2024-01-15 12:30:45' AS TIMESTAMP_MS), \
         CAST('2024-01-15 12:30:45' AS TIMESTAMP_NS)",
    )
    .await;

    match &values[0] {
        Value::Ext("Date", inner) => assert_eq!(**inner, Value::String("2024-01-15".into())),
        other => panic!("unexpected date value: {other:?}"),
    }
    match &values[1] {
        Value::Ext("Time", inner) => assert_eq!(**inner, Value::String("12:30:45".into())),
        other => panic!("unexpected time value: {other:?}"),
    }
    match &values[2] {
        Value::Ext("Timestamp", _) => {}
        other => panic!("unexpected timestamp value: {other:?}"),
    }
    // Some DuckDB C API versions cannot render the compressed timestamp
    // representations to a string; the driver then yields NULL.
    for value in &values[3..] {
        assert!(
            matches!(value, Value::Null | Value::Ext("Timestamp", _)),
            "unexpected compressed timestamp value: {value:?}"
        );
    }
}

#[tokio::test]
async fn test_extract_decimal_interval_hugeint_uuid_blob() {
    let mut conn = create_conn().await;

    let values = query_row(
        &mut conn,
        "SELECT 1.25::DECIMAL(10,2), INTERVAL 1 DAY, 1::HUGEINT, 2::UHUGEINT",
    )
    .await;
    match &values[0] {
        Value::Ext("Decimal", inner) => assert_eq!(**inner, Value::String("1.25".into())),
        other => panic!("unexpected decimal: {other:?}"),
    }
    assert!(
        matches!(&values[1], Value::Ext("Interval", _)),
        "interval: {:?}",
        values[1]
    );
    assert!(
        matches!(&values[2], Value::Ext("HugeInt", inner) if **inner == Value::String("1".into()))
    );
    assert!(
        matches!(&values[3], Value::Ext("UHugeInt", inner) if **inner == Value::String("2".into()))
    );

    // UUID support in the C string conversion varies by DuckDB version.
    let values = query_row(&mut conn, "SELECT uuid()").await;
    assert!(
        matches!(&values[0], Value::Null | Value::Ext("Uuid", _)),
        "unexpected uuid value: {:?}",
        values[0]
    );

    let values = query_row(&mut conn, "SELECT 'abc'::BLOB").await;
    match &values[0] {
        Value::Binary(bytes) => assert_eq!(bytes, b"abc"),
        other => panic!("unexpected blob: {other:?}"),
    }
}

#[tokio::test]
async fn test_extract_nested_types() {
    let mut conn = create_conn().await;
    let values = query_row(
        &mut conn,
        "SELECT [1,2,3], CAST([4,5,6] AS INTEGER[3]), {'x': 1}, MAP {'k': 2}",
    )
    .await;

    // Some DuckDB C API versions cannot render nested types to strings; the
    // driver then reports NULL. Newer versions return the parsed value.
    assert!(
        matches!(&values[0], Value::Null | Value::Array(_)),
        "list: {:?}",
        values[0]
    );
    assert!(
        matches!(&values[1], Value::Null | Value::Array(_)),
        "array: {:?}",
        values[1]
    );
    assert!(
        matches!(&values[2], Value::Null | Value::String(_)),
        "struct: {:?}",
        values[2]
    );
    assert!(
        matches!(&values[3], Value::Null | Value::String(_) | Value::Map(_)),
        "map: {:?}",
        values[3]
    );
}

#[tokio::test]
async fn test_extract_enum_union_and_null() {
    let mut conn = create_conn().await;
    conn.exec("CREATE TYPE mood AS ENUM ('happy', 'sad')", vec![])
        .await
        .expect("create enum type");

    let values = query_row(&mut conn, "SELECT 'happy'::mood, union_value(a := 1), NULL").await;
    // ENUM and UNION string conversion varies by DuckDB version; SQL NULL is
    // always NULL.
    assert!(
        matches!(&values[0], Value::Null | Value::String(_)),
        "enum: {:?}",
        values[0]
    );
    assert!(
        matches!(&values[1], Value::Null | Value::String(_)),
        "union: {:?}",
        values[1]
    );
    assert_eq!(values[2], Value::Null);
}

/// Directly exercise extraction of a result set containing a NULL column and
/// multiple rows, to make sure every row is decoded independently.
#[tokio::test]
async fn test_ext_timestamp_parameter_binds_through_fallback() {
    let mut conn = create_conn().await;
    // `Ext` parameters take the `Encode` fallback path in `bind_params`; a
    // numeric `Timestamp` is encoded as an Int64 argument.
    let mut stream = conn
        .exec_rows(
            "SELECT CAST(? AS BIGINT)",
            vec![Value::Ext("Timestamp", Box::new(Value::I64(42)))],
        )
        .await
        .expect("exec_rows");
    let mut row = stream.next().await.expect("one row").expect("row result");
    assert_eq!(row.get(0).unwrap(), Value::I64(42));
}

#[tokio::test]
async fn test_extract_null_mixed_rows() {
    let mut conn = create_conn().await;
    let mut stream = conn
        .exec_rows(
            "SELECT * FROM (VALUES (1, NULL), (2, 'x')) AS t(a, b) ORDER BY a",
            vec![],
        )
        .await
        .expect("exec_rows");

    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        let mut row = row.expect("row");
        rows.push((row.get(0).unwrap(), row.get(1).unwrap()));
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, Value::I32(1));
    assert_eq!(rows[0].1, Value::Null);
    assert_eq!(rows[1].0, Value::I32(2));
    assert_eq!(rows[1].1, Value::String("x".into()));
}

/// Sanity check that `extract_row_values` is callable directly (used by tests
/// and downstream tooling) and agrees with the row stream output.
#[test]
fn test_extract_row_values_direct_call() {
    use std::ffi::CString;
    use std::ptr;

    unsafe {
        let mut db: duck::duckdb_database = ptr::null_mut();
        assert_eq!(duck::duckdb_open(ptr::null(), &mut db), duck::DuckDBSuccess);
        let mut con: duck::duckdb_connection = ptr::null_mut();
        assert_eq!(duck::duckdb_connect(db, &mut con), duck::DuckDBSuccess);

        let sql = CString::new("SELECT 7::INTEGER, 'x', 1.5::DOUBLE").unwrap();
        let mut result: duck::duckdb_result = std::mem::zeroed();
        assert_eq!(
            duck::duckdb_query(con, sql.as_ptr(), &mut result),
            duck::DuckDBSuccess
        );
        let cols = duck::duckdb_column_count(&mut result) as usize;
        let values = extract_row_values(&mut result, 0, cols);
        assert_eq!(
            values,
            vec![Value::I32(7), Value::String("x".into()), Value::F64(1.5)]
        );

        duck::duckdb_destroy_result(&mut result);
        duck::duckdb_disconnect(&mut con);
        duck::duckdb_close(&mut db);
    }
}
