//! Offline coverage for every branch of `rbdc_mssql::encode`.

use rbdc_mssql::encode::Encode;
use rbs::Value;
use tiberius::Query;

fn encode(value: Value) -> Result<(), rbdc::Error> {
    let mut query = Query::new("insert into t values (@P1)");
    value.encode(&mut query)
}

#[test]
fn encode_scalar_values() {
    assert!(encode(Value::Null).is_ok());
    assert!(encode(Value::Bool(true)).is_ok());
    assert!(encode(Value::I32(-1)).is_ok());
    assert!(encode(Value::I64(-2)).is_ok());
    assert!(encode(Value::U32(3)).is_ok());
    assert!(encode(Value::U64(4)).is_ok());
    assert!(encode(Value::U32(u32::MAX)).is_err());
    assert!(encode(Value::U64(u64::MAX)).is_err());
    assert!(encode(Value::F32(1.5)).is_ok());
    assert!(encode(Value::F64(2.5)).is_ok());
    assert!(encode(Value::String("hello".to_string())).is_ok());
    assert!(encode(Value::Binary(vec![1, 2, 3])).is_ok());
}

#[test]
fn encode_composite_values_as_json() {
    assert!(encode(Value::Array(vec![Value::I32(1), Value::String("a".into())])).is_ok());
    assert!(encode(Value::Map(rbs::value::map::ValueMap::new())).is_ok());
}

#[test]
fn encode_ext_values() {
    assert!(encode(Value::Ext(
        "Date",
        Box::new(Value::String("2020-01-02".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext(
        "DateTime",
        Box::new(Value::String("2020-01-02T03:04:05Z".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext(
        "Time",
        Box::new(Value::String("12:34:56".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext(
        "Decimal",
        Box::new(Value::String("123.45".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext("Timestamp", Box::new(Value::I64(1_600_000_000)))).is_ok());
    assert!(encode(Value::Ext(
        "Uuid",
        Box::new(Value::String("01234567-89ab-cdef-0123-456789abcdef".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext(
        "DateTimeOffset",
        Box::new(Value::String("2020-01-02T03:04:05+08:00".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext(
        "Json",
        Box::new(Value::String("{\"a\":1}".into()))
    ))
    .is_ok());
    assert!(encode(Value::Ext("Xml", Box::new(Value::String("<a/>".into())))).is_ok());
    assert!(encode(Value::Ext(
        "Geometry",
        Box::new(Value::Binary(vec![1, 2, 3]))
    ))
    .is_ok());
}

#[test]
fn encode_ext_fallbacks_do_not_error() {
    // Invalid UUID strings are rejected instead of silently binding nil.
    assert!(encode(Value::Ext(
        "Uuid",
        Box::new(Value::String("not-a-uuid".into()))
    ))
    .is_err());

    // Missing/non-numeric timestamp payloads are rejected instead of becoming 0.
    assert!(encode(Value::Ext("Timestamp", Box::new(Value::Null))).is_err());
    assert!(encode(Value::Ext(
        "Timestamp",
        Box::new(Value::String("not-a-number".into()))
    ))
    .is_err());
}

#[test]
fn encode_ext_errors() {
    assert!(encode(Value::Ext("UnknownType", Box::new(Value::Null))).is_err());

    // Non-string payload makes the date/time parsers fail.
    assert!(encode(Value::Ext("Date", Box::new(Value::I32(1)))).is_err());
    assert!(encode(Value::Ext("Time", Box::new(Value::I32(1)))).is_err());
    assert!(encode(Value::Ext("DateTime", Box::new(Value::I32(1)))).is_err());
    assert!(encode(Value::Ext("Decimal", Box::new(Value::I32(1)))).is_err());

    // Malformed string payloads fail to parse.
    assert!(encode(Value::Ext(
        "Date",
        Box::new(Value::String("not-a-date".into()))
    ))
    .is_err());
    assert!(encode(Value::Ext("Time", Box::new(Value::String("99:99".into())))).is_err());
    assert!(encode(Value::Ext(
        "DateTime",
        Box::new(Value::String("not-a-datetime".into()))
    ))
    .is_err());
    assert!(encode(Value::Ext(
        "Decimal",
        Box::new(Value::String("not-a-decimal".into()))
    ))
    .is_err());
}
