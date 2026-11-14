//! Coverage for value conversions, encoding/decoding and type information.

use std::ffi::CString;
use std::ptr;

use either::Either;
use libsqlite3_sys::{
    sqlite3, sqlite3_close, sqlite3_column_value, sqlite3_finalize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt, SQLITE_OK, SQLITE_ROW,
};
use rbdc::date::Date;
use rbdc::datetime::DateTime;
use rbdc::decimal::Decimal;
use rbdc::json::Json;
use rbdc::timestamp::Timestamp;
use rbdc::types::time::Time;
use rbdc::uuid::Uuid;
use rbdc_sqlite::decode::Decode;
use rbdc_sqlite::encode::{Encode, IsNull};
use rbdc_sqlite::query::SqliteQuery;
use rbdc_sqlite::type_info::Type;
use rbdc_sqlite::value::{SqliteValue, SqliteValueRef};
use rbdc_sqlite::{
    SqliteArgumentValue, SqliteArguments, SqliteConnectOptions, SqliteStatement, SqliteTypeInfo,
};
use rbs::Value;

/// Evaluate a single SQL expression and duplicate the resulting SQLite value.
fn sqlite_value(sql: &str) -> SqliteValue {
    unsafe {
        let filename = CString::new(":memory:").unwrap();
        let query = CString::new(sql).unwrap();

        let mut db: *mut sqlite3 = ptr::null_mut();
        assert_eq!(sqlite3_open(filename.as_ptr(), &mut db), SQLITE_OK);

        let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(db, query.as_ptr(), -1, &mut stmt, ptr::null_mut()),
            SQLITE_OK
        );
        assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

        let raw = sqlite3_column_value(stmt, 0);
        let value = SqliteValue::new(raw, SqliteTypeInfo::null());

        sqlite3_finalize(stmt);
        sqlite3_close(db);

        value
    }
}

#[test]
fn test_sqlite_value_accessors() {
    use rbdc_sqlite::SqliteValueRef as ValueRef;

    // NULL
    let null = sqlite_value("SELECT NULL");
    assert!(null.is_null());
    assert!(null.type_info_opt().is_none());
    // Falls back to the stored type information when the value is NULL.
    assert!(null.type_info().is_null());
    assert_eq!(null.blob(), &[] as &[u8]);
    assert_eq!(null.text().unwrap(), "");
    assert!(null.as_ref().is_null());

    // Integer
    let int = sqlite_value("SELECT 123");
    assert!(!int.is_null());
    assert_eq!(int.int(), 123);
    assert_eq!(int.int64(), 123);
    assert_eq!(int.double(), 123.0);
    assert!(int.type_info_opt().is_some());
    let int_ref: ValueRef<'_> = int.as_ref();
    assert!(!int_ref.is_null());
    assert!(int_ref.type_info_opt().is_some());
    assert_eq!(int_ref.to_owned().int(), 123);

    // Float
    let float = sqlite_value("SELECT 2.5");
    assert_eq!(float.double(), 2.5);
    assert!(!float.as_ref().is_null());

    // Text
    let text = sqlite_value("SELECT 'hello'");
    assert_eq!(text.text().unwrap(), "hello");
    assert_eq!(text.blob(), b"hello");
    assert_eq!(text.as_ref().to_owned().text().unwrap(), "hello");

    // Blob
    let blob = sqlite_value("SELECT x'00FF'");
    assert_eq!(blob.blob(), &[0x00, 0xFF]);
    // Binary data is not valid UTF-8 here.
    assert!(blob.text().is_err());

    // Empty blob returns an empty slice.
    let empty_blob = sqlite_value("SELECT x''");
    assert_eq!(empty_blob.blob(), &[] as &[u8]);

    // Text storage holding invalid UTF-8.
    let invalid = sqlite_value("SELECT CAST(x'80' AS TEXT)");
    assert!(invalid.text().is_err());

    // Cloning a value duplicates the handle.
    let cloned = text.clone();
    assert_eq!(cloned.text().unwrap(), "hello");
}

#[test]
fn test_typed_decode() {
    macro_rules! decode {
        ($ty:ty, $sql:expr) => {{
            let v = sqlite_value($sql);
            <$ty as Decode>::decode(SqliteValueRef::value(&v))
        }};
    }

    assert_eq!(decode!(i8, "SELECT 5").unwrap(), 5i8);
    assert_eq!(decode!(i16, "SELECT 300").unwrap(), 300i16);
    assert_eq!(decode!(i32, "SELECT 70000").unwrap(), 70000i32);
    assert_eq!(decode!(i64, "SELECT 9000000000").unwrap(), 9_000_000_000i64);
    assert_eq!(decode!(u8, "SELECT 200").unwrap(), 200u8);
    assert_eq!(decode!(u16, "SELECT 60000").unwrap(), 60000u16);
    assert_eq!(decode!(u32, "SELECT 4000000000").unwrap(), 4_000_000_000u32);
    assert_eq!(decode!(u64, "SELECT 9000000000").unwrap(), 9_000_000_000u64);
    assert_eq!(decode!(f32, "SELECT 1.5").unwrap(), 1.5f32);
    assert_eq!(decode!(f64, "SELECT 2.25").unwrap(), 2.25f64);
    assert_eq!(decode!(String, "SELECT 'abc'").unwrap(), "abc".to_string());
    assert_eq!(decode!(Vec<u8>, "SELECT x'0102'").unwrap(), vec![1u8, 2u8]);
    assert!(decode!(bool, "SELECT 1").unwrap());
    assert!(!decode!(bool, "SELECT 0").unwrap());

    assert_eq!(
        decode!(Date, "SELECT '2024-01-15'").unwrap().to_string(),
        "2024-01-15"
    );
    assert_eq!(
        decode!(DateTime, "SELECT '2024-01-15T12:30:45Z'")
            .unwrap()
            .to_string(),
        "2024-01-15T12:30:45Z"
    );
    assert_eq!(
        decode!(Time, "SELECT '12:30:45'").unwrap().to_string(),
        "12:30:45"
    );
    assert_eq!(decode!(Timestamp, "SELECT 1234").unwrap(), Timestamp(1234));
    assert_eq!(
        decode!(Decimal, "SELECT '123.45'").unwrap().to_string(),
        "123.45"
    );
    assert_eq!(decode!(Json, "SELECT '{\"a\":1}'").unwrap().0, "{\"a\":1}");
    assert_eq!(
        decode!(Uuid, "SELECT '550e8400-e29b-41d4-a716-446655440000'")
            .unwrap()
            .to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    // Out-of-range integer conversions error instead of wrapping.
    assert!(decode!(i8, "SELECT 300").is_err());
    assert!(decode!(u8, "SELECT -1").is_err());
    assert!(decode!(u16, "SELECT -1").is_err());
    assert!(decode!(u32, "SELECT -1").is_err());
}

#[test]
fn test_typed_encode_and_type_info() {
    fn encode<T: Encode>(value: T) -> (Vec<SqliteArgumentValue>, IsNull) {
        let mut args = Vec::new();
        let is_null = value.encode(&mut args).unwrap();
        (args, is_null)
    }

    let (args, _) = encode(1i8);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(1)]));
    let (args, _) = encode(2i16);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(2)]));
    let (args, _) = encode(3i32);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(3)]));
    let (args, _) = encode(4i64);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int64(4)]));
    let (args, _) = encode(5u8);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(5)]));
    let (args, _) = encode(6u16);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(6)]));
    let (args, _) = encode(7u32);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int64(7)]));
    let (args, _) = encode(8u64);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int64(8)]));
    let mut too_large = Vec::new();
    assert!((u64::MAX).encode(&mut too_large).is_err());
    let (args, _) = encode(1.5f32);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Double(v)] if *v == 1.5));
    let (args, _) = encode(2.5f64);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Double(v)] if *v == 2.5));
    let (args, _) = encode("text".to_string());
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Text(v)] if v == "text"));
    let (args, _) = encode(vec![1u8, 2]);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Blob(v)] if v == &vec![1, 2]));
    let (args, _) = encode(true);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(1)]));
    let (args, _) = encode(false);
    assert!(matches!(args.as_slice(), [SqliteArgumentValue::Int(0)]));

    assert_eq!(1i8.type_info().name(), "INTEGER");
    assert_eq!(1i16.type_info().name(), "INTEGER");
    assert_eq!(1i32.type_info().name(), "INTEGER");
    assert_eq!(1i64.type_info().name(), "INTEGER");
    assert_eq!(1u8.type_info().name(), "INTEGER");
    assert_eq!(1u16.type_info().name(), "INTEGER");
    assert_eq!(1u32.type_info().name(), "INTEGER");
    assert_eq!(1u64.type_info().name(), "INTEGER");
    assert_eq!(1.5f32.type_info().name(), "REAL");
    assert_eq!(1.5f64.type_info().name(), "REAL");
    assert_eq!("x".to_string().type_info().name(), "TEXT");
    assert_eq!(vec![1u8].type_info().name(), "BLOB");
    assert_eq!(true.type_info().name(), "BOOLEAN");

    // Option<T> maps to itself, or NULL when absent.
    assert!(None::<i32>.type_info().is_null());
    assert_eq!(Some(1i32).type_info().name(), "INTEGER");
}

#[test]
fn test_value_encode_variants() {
    fn encode(value: Value) -> (Vec<SqliteArgumentValue>, IsNull) {
        let mut args = Vec::new();
        let is_null = value.encode(&mut args).unwrap();
        (args, is_null)
    }

    let (args, is_null) = encode(Value::Null);
    assert!(args.is_empty());
    assert!(matches!(is_null, IsNull::Yes));

    let cases = vec![
        Value::Bool(true),
        Value::I32(1),
        Value::I64(2),
        Value::U32(3),
        Value::U64(4),
        Value::F32(1.5),
        Value::F64(2.5),
        Value::String("s".into()),
        Value::Binary(vec![1, 2]),
        Value::Array(vec![Value::I64(1)]),
        Value::Map({
            let mut m = rbs::value::map::ValueMap::new();
            m.insert(Value::String("k".into()), Value::I64(1));
            m
        }),
        Value::Ext("Date", Box::new(Value::String("2020-01-01".into()))),
        Value::Ext("DateTime", Box::new(Value::String("2020-01-01".into()))),
        Value::Ext("Time", Box::new(Value::String("12:00".into()))),
        Value::Ext("Timestamp", Box::new(Value::I64(10))),
        Value::Ext("Decimal", Box::new(Value::String("1.25".into()))),
        Value::Ext("Json", Box::new(Value::String("{\"a\":1}".into()))),
        Value::Ext("Uuid", Box::new(Value::String("abc".into()))),
    ];

    for value in cases {
        let (args, is_null) = encode(value);
        assert_eq!(args.len(), 1);
        assert!(matches!(is_null, IsNull::No));
    }

    // Unknown extension values encode as SQL NULL.
    let (args, is_null) = encode(Value::Ext("Unknown", Box::new(Value::Null)));
    assert!(args.is_empty());
    assert!(matches!(is_null, IsNull::Yes));
}

#[test]
fn test_value_decode_dynamic_types() {
    let cases = [
        ("SELECT NULL", Value::Null),
        ("SELECT 42", Value::I64(42)),
        ("SELECT 4.5", Value::F64(4.5)),
        ("SELECT 'hello'", Value::String("hello".into())),
        ("SELECT x'0001'", Value::Binary(vec![0, 1])),
        // JSON-looking text is parsed into structured values.
        ("SELECT '{\"a\":1}'", {
            let mut m = rbs::value::map::ValueMap::new();
            m.insert(Value::String("a".into()), Value::U64(1));
            Value::Map(m)
        }),
        (
            "SELECT '[1,2]'",
            Value::Array(vec![Value::U64(1), Value::U64(2)]),
        ),
        ("SELECT 'null'", Value::Null),
        // Non-JSON text is preserved as a string.
        ("SELECT 'not json'", Value::String("not json".into())),
        ("SELECT '{bad}'", Value::String("{bad}".into())),
        ("SELECT '[1,2'", Value::String("[1,2".into())),
    ];

    for (sql, expected) in cases {
        let value = sqlite_value(sql);
        let decoded = Value::decode(SqliteValueRef::value(&value)).unwrap();
        assert_eq!(decoded, expected, "sql: {}", sql);
    }
}

#[test]
fn test_value_type_info_and_is_json_string() {
    assert!(Value::Null.type_info().is_null());
    assert_eq!(Value::Bool(true).type_info().name(), "BOOLEAN");
    assert_eq!(Value::I32(1).type_info().name(), "INTEGER");
    assert_eq!(Value::I64(1).type_info().name(), "INTEGER");
    assert_eq!(Value::U32(1).type_info().name(), "INTEGER");
    assert_eq!(Value::U64(1).type_info().name(), "INTEGER");
    assert_eq!(Value::F32(1.0).type_info().name(), "REAL");
    assert_eq!(Value::F64(1.0).type_info().name(), "REAL");
    assert_eq!(Value::String("x".into()).type_info().name(), "TEXT");
    assert_eq!(Value::Binary(vec![1]).type_info().name(), "BLOB");
    assert_eq!(Value::Array(vec![]).type_info().name(), "TEXT");
    assert_eq!(
        Value::Map(rbs::value::map::ValueMap::new())
            .type_info()
            .name(),
        "TEXT"
    );
    assert_eq!(
        Value::Ext("Date", Box::new(Value::Null)).type_info().name(),
        "TEXT"
    );
    assert_eq!(
        Value::Ext("DateTime", Box::new(Value::Null))
            .type_info()
            .name(),
        "TEXT"
    );
    assert_eq!(
        Value::Ext("Time", Box::new(Value::Null)).type_info().name(),
        "TEXT"
    );
    assert_eq!(
        Value::Ext("Timestamp", Box::new(Value::Null))
            .type_info()
            .name(),
        "INTEGER"
    );
    assert_eq!(
        Value::Ext("Decimal", Box::new(Value::Null))
            .type_info()
            .name(),
        "NUMERIC"
    );
    assert_eq!(
        Value::Ext("Json", Box::new(Value::Null)).type_info().name(),
        "BLOB"
    );
    assert_eq!(
        Value::Ext("Uuid", Box::new(Value::Null)).type_info().name(),
        "TEXT"
    );
    assert!(Value::Ext("Unknown", Box::new(Value::Null))
        .type_info()
        .is_null());
}

#[test]
fn test_type_info_display() {
    assert_eq!(SqliteTypeInfo::null().name(), "NULL");
    assert_eq!(format!("{}", SqliteTypeInfo::null()), "NULL");
    assert_eq!(format!("{:>6}", SqliteTypeInfo::null()), "  NULL");
    assert!(SqliteTypeInfo::null().is_null());
}

#[test]
fn test_sqlite_arguments_api() {
    let mut args = SqliteArguments::default();
    args.reserve(4, 0);
    args.add(Value::Null).unwrap();
    args.add(Value::I64(1)).unwrap();
    // Clone/Debug implementations; the values themselves are private, so we
    // verify them through the debug representation.
    let cloned = args.clone();
    let debug = format!("{:?}", cloned);
    assert!(debug.contains("Null"));
    assert!(debug.contains("Int64(1)"));

    let from_args =
        SqliteArguments::from_args(vec![Value::Bool(true), Value::I64(2), Value::Array(vec![])])
            .unwrap();
    let debug = format!("{:?}", from_args);
    assert!(debug.contains("Int(1)"));
    assert!(debug.contains("Int64(2)"));
    assert!(debug.contains("Text("));
}

#[tokio::test]
async fn test_query_helpers() {
    // `Either::Left` (raw SQL) variant.
    let query = SqliteQuery {
        statement: Either::Left("SELECT 1".to_string()),
        arguments: vec![],
        persistent: false,
    };
    assert_eq!(query.sql(), "SELECT 1");
    assert!(query.statement().is_none());
    assert!(!query.persistent());
    assert!(query.take_arguments().unwrap().is_none());

    // `Either::Right` (prepared statement) variant.
    let mut conn = SqliteConnectOptions::new()
        .filename(":memory:")
        .connect()
        .await
        .unwrap();
    let stmt = conn.prepare_with("SELECT ? AS v", &[]).await.unwrap();
    let stmt: SqliteStatement = stmt;
    let query = SqliteQuery {
        statement: Either::Right(stmt),
        arguments: vec![Value::I64(5)],
        persistent: true,
    };
    assert_eq!(query.sql(), "SELECT ? AS v");
    assert!(query.statement().is_some());
    assert!(query.persistent());
    let args = query.take_arguments().unwrap().expect("arguments");
    assert!(format!("{:?}", args).contains("Int64(5)"));

    // Also exercise `SqliteQuery::take_arguments` via a direct call on a
    // non-persistent raw query with arguments.
    let query = SqliteQuery {
        statement: Either::Left("SELECT ?".to_string()),
        arguments: vec![Value::I64(1)],
        persistent: false,
    };
    assert!(query.take_arguments().unwrap().is_some());
}
