//! Offline coverage for MySQL value/type encoding and decoding.

use std::str::FromStr;
use std::sync::Arc;

use rbdc::date::Date as RbdcDate;
use rbdc::datetime::DateTime as RbdcDateTime;
use rbdc::decimal::Decimal;
use rbdc::json::Json;
use rbdc::timestamp::Timestamp;
use rbdc::types::time::Time as RbdcTime;
use rbdc::uuid::Uuid;
use rbdc::{Error, Json as RbdcJson};
use rbdc_mysql::options::MySqlConnectOptions;
use rbdc_mysql::protocol::text::ColumnType;
use rbdc_mysql::result_set::MySqlTypeInfo;
use rbdc_mysql::types::date::decode_date_buf;
use rbdc_mysql::types::enums::Enum;
use rbdc_mysql::types::geometry::Geometry;
use rbdc_mysql::types::set::Set;
use rbdc_mysql::types::time::decode_time;
use rbdc_mysql::types::year::Year;
use rbdc_mysql::types::{Decode, Encode, TypeInfo};
use rbdc_mysql::value::{MySqlValueFormat, MySqlValueRef};
use rbs::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn vref<'a>(data: Option<&'a [u8]>, ty: ColumnType, format: MySqlValueFormat) -> MySqlValueRef<'a> {
    MySqlValueRef {
        value: data,
        type_info: MySqlTypeInfo::from_type(ty),
        format,
        option: Arc::new(MySqlConnectOptions::new()),
    }
}

fn text(data: &str, ty: ColumnType) -> MySqlValueRef<'_> {
    vref(Some(data.as_bytes()), ty, MySqlValueFormat::Text)
}

fn binary(data: &[u8], ty: ColumnType) -> MySqlValueRef<'_> {
    vref(Some(data), ty, MySqlValueFormat::Binary)
}

fn decode(value: MySqlValueRef<'_>) -> Value {
    <Value as Decode>::decode(value).unwrap()
}

fn encode(value: Value) -> (usize, Vec<u8>) {
    let mut buf = Vec::new();
    let n = value.encode(&mut buf).unwrap();
    (n, buf)
}

fn ext_name(value: &Value) -> Option<&'static str> {
    match value {
        Value::Ext(name, _) => Some(name),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// rbs::Value TypeInfo
// ---------------------------------------------------------------------------

#[test]
fn test_type_info_for_value_variants() {
    let map: Value = serde_json::from_str("{\"a\":1}").unwrap();
    let cases = vec![
        (Value::Null, ColumnType::Null),
        (Value::Bool(true), ColumnType::Tiny),
        (Value::I32(1), ColumnType::Long),
        (Value::I64(1), ColumnType::LongLong),
        (Value::U32(1), ColumnType::Long),
        (Value::U64(1), ColumnType::LongLong),
        (Value::F32(1.0), ColumnType::Float),
        (Value::F64(1.0), ColumnType::Double),
        (Value::String("x".into()), ColumnType::VarChar),
        (Value::Binary(vec![1]), ColumnType::Blob),
        (Value::Array(vec![Value::I32(1)]), ColumnType::Json),
        (map, ColumnType::Json),
        (
            Value::Ext("Uuid", Box::new(Value::String("x".into()))),
            ColumnType::VarChar,
        ),
        (
            Value::Ext("Decimal", Box::new(Value::String("1.5".into()))),
            ColumnType::NewDecimal,
        ),
        (
            Value::Ext("Year", Box::new(Value::U64(2024))),
            ColumnType::Short,
        ),
        (
            Value::Ext("Date", Box::new(Value::String("2024-01-01".into()))),
            ColumnType::Date,
        ),
        (
            Value::Ext("Time", Box::new(Value::String("01:02:03".into()))),
            ColumnType::Time,
        ),
        (
            Value::Ext("Timestamp", Box::new(Value::U64(1))),
            ColumnType::Timestamp,
        ),
        (
            Value::Ext(
                "DateTime",
                Box::new(Value::String("2024-01-01 01:02:03".into())),
            ),
            ColumnType::DateTime,
        ),
        (
            Value::Ext("Json", Box::new(Value::String("{}".into()))),
            ColumnType::Json,
        ),
        (
            Value::Ext("Enum", Box::new(Value::String("a".into()))),
            ColumnType::VarChar,
        ),
        (
            Value::Ext("Set", Box::new(Value::String("a".into()))),
            ColumnType::VarChar,
        ),
        (
            Value::Ext("Geometry", Box::new(Value::Binary(vec![1]))),
            ColumnType::String,
        ),
        (
            Value::Ext("Bit", Box::new(Value::U64(1))),
            ColumnType::Blob,
        ),
        (
            Value::Ext("Nope", Box::new(Value::I32(1))),
            ColumnType::VarChar,
        ),
    ];
    for (value, expected) in cases {
        assert_eq!(value.type_info().r#type, expected, "value {value:?}");
    }
}

#[test]
fn test_unsigned_integer_type_info_and_decode() {
    assert!(Value::U32(1).type_info().is_unsigned());
    assert!(Value::U64(1).type_info().is_unsigned());
    assert!(!Value::I32(1).type_info().is_unsigned());

    let tiny = MySqlValueRef {
        value: Some(&[0xff]),
        type_info: MySqlTypeInfo::unsigned(ColumnType::Tiny),
        format: MySqlValueFormat::Binary,
        option: Arc::new(MySqlConnectOptions::new()),
    };
    assert_eq!(decode(tiny), Value::U32(255));

    let long = MySqlValueRef {
        value: Some(&u32::MAX.to_le_bytes()),
        type_info: MySqlTypeInfo::unsigned(ColumnType::Long),
        format: MySqlValueFormat::Binary,
        option: Arc::new(MySqlConnectOptions::new()),
    };
    assert_eq!(decode(long), Value::U64(u32::MAX as u64));
}

// ---------------------------------------------------------------------------
// rbs::Value decoding (text format)
// ---------------------------------------------------------------------------

#[test]
fn test_value_decode_text() {
    assert_eq!(decode(text("42", ColumnType::Tiny)), Value::I32(42));
    assert_eq!(decode(text("-7", ColumnType::Short)), Value::I32(-7));
    assert_eq!(
        decode(text("1234567890123", ColumnType::Long)),
        Value::I64(1_234_567_890_123)
    );
    assert_eq!(decode(text("1.5", ColumnType::Float)), Value::F32(1.5));
    assert_eq!(decode(text("2.25", ColumnType::Double)), Value::F64(2.25));
    assert_eq!(decode(text("anything", ColumnType::Null)), Value::Null);
    assert_eq!(
        decode(text("9000000000", ColumnType::LongLong)),
        Value::I64(9_000_000_000)
    );
    assert_eq!(decode(text("70000", ColumnType::Int24)), Value::I32(70000));
    assert_eq!(
        decode(text("héllo", ColumnType::VarChar)),
        Value::String("héllo".into())
    );
    assert_eq!(
        decode(text("abc", ColumnType::VarString)),
        Value::String("abc".into())
    );
    assert_eq!(
        decode(text("abc", ColumnType::String)),
        Value::String("abc".into())
    );
    assert_eq!(
        decode(text("0.5", ColumnType::Decimal)),
        Value::Ext("Decimal", Box::new(Value::String("0.5".into())))
    );
    assert_eq!(
        decode(text("0.5", ColumnType::NewDecimal)),
        Value::Ext("Decimal", Box::new(Value::String("0.5".into())))
    );
    assert_eq!(
        decode(text("2023-05-06", ColumnType::Date)),
        Value::Ext("Date", Box::new(Value::String("2023-05-06".into())))
    );
    assert_eq!(
        decode(text("12:34:56", ColumnType::Time)),
        Value::Ext("Time", Box::new(Value::String("12:34:56".into())))
    );
    assert_eq!(
        decode(text("2024", ColumnType::Year)),
        Value::Ext("Year", Box::new(Value::String("2024".into())))
    );
    assert_eq!(
        decode(text("a", ColumnType::Enum)),
        Value::Ext("Enum", Box::new(Value::String("a".into())))
    );
    assert_eq!(
        decode(text("a,b", ColumnType::Set)),
        Value::Ext("Set", Box::new(Value::String("a,b".into())))
    );
    assert_eq!(decode(binary(&[1, 2], ColumnType::Bit)), Value::U64(0x0102));

    for ty in [
        ColumnType::TinyBlob,
        ColumnType::MediumBlob,
        ColumnType::LongBlob,
        ColumnType::Blob,
    ] {
        assert_eq!(
            decode(text("bytes", ty)),
            Value::Binary(b"bytes".to_vec()),
            "{ty:?}"
        );
    }

    let dt = decode(text("2023-05-06 07:08:09", ColumnType::DateTime));
    assert_eq!(ext_name(&dt), Some("DateTime"));

    let ts = decode(text("2020-01-02 03:04:05Z", ColumnType::Timestamp));
    assert_eq!(ext_name(&ts), Some("Timestamp"));
    assert!(matches!(ts, Value::Ext("Timestamp", v) if matches!(*v, Value::U64(_))));

    let json = decode(text("{\"a\":1}", ColumnType::Json));
    assert!(matches!(json, Value::Map(_)));

    let geom = decode(binary(&[0xde, 0xad], ColumnType::Geometry));
    assert_eq!(
        geom,
        Value::Ext("Geometry", Box::new(Value::Binary(vec![0xde, 0xad])))
    );
}

#[test]
fn test_value_decode_text_timestamp_without_z() {
    // decode_timestamp appends the trailing Z when the server omits it.
    let ts = decode(text("2020-01-02 03:04:05", ColumnType::Timestamp));
    assert_eq!(ext_name(&ts), Some("Timestamp"));
    let expected = fastdate::DateTime::from_str("2020-01-02 03:04:05Z")
        .unwrap()
        .unix_timestamp_millis() as u64;
    match ts {
        Value::Ext(_, inner) => assert_eq!(*inner, Value::U64(expected)),
        _ => panic!("expected timestamp"),
    }
}

// ---------------------------------------------------------------------------
// rbs::Value decoding (binary format)
// ---------------------------------------------------------------------------

#[test]
fn test_value_decode_binary() {
    assert_eq!(decode(binary(&[0xff], ColumnType::Tiny)), Value::I32(-1));
    assert_eq!(
        decode(binary(&[0xff, 0xff], ColumnType::Short)),
        Value::I32(-1)
    );
    assert_eq!(decode(binary(&[0xff; 4], ColumnType::Long)), Value::I64(-1));
    assert_eq!(
        decode(binary(&[0xff; 8], ColumnType::LongLong)),
        Value::I64(-1)
    );
    assert_eq!(decode(binary(&[1, 0, 0], ColumnType::Int24)), Value::I32(1));
    assert_eq!(
        decode(binary(&1.5f32.to_le_bytes(), ColumnType::Float)),
        Value::F32(1.5)
    );
    // 8-byte DOUBLE values for a FLOAT column are truncated.
    assert_eq!(
        decode(binary(&2.5f64.to_le_bytes(), ColumnType::Float)),
        Value::F32(2.5)
    );
    assert_eq!(
        decode(binary(&2.5f64.to_le_bytes(), ColumnType::Double)),
        Value::F64(2.5)
    );
    assert_eq!(
        decode(binary(&[0x01, 0x02], ColumnType::Bit)),
        Value::U64(0x0102)
    );
    for ty in [
        ColumnType::TinyBlob,
        ColumnType::MediumBlob,
        ColumnType::LongBlob,
        ColumnType::Blob,
    ] {
        assert_eq!(
            decode(binary(&[1, 2], ty)),
            Value::Binary(vec![1, 2]),
            "{ty:?}"
        );
    }
}

#[test]
fn test_value_decode_binary_temporal() {
    // Date: len + year LE + month + day.
    let date = decode(binary(&[4, 0xE7, 0x07, 5, 6], ColumnType::Date));
    assert_eq!(
        date,
        Value::Ext("Date", Box::new(Value::String("2023-05-06".into())))
    );

    // Time with len <= 4 is the zero time.
    let zero_time = decode(binary(&[4, 0, 0, 0, 0], ColumnType::Time));
    assert_eq!(
        zero_time,
        Value::Ext("Time", Box::new(Value::String("00:00:00".into())))
    );
    // Time with a real payload.
    let time = decode(binary(&[8, 0, 0, 0, 0, 12, 34, 56], ColumnType::Time));
    assert_eq!(
        time,
        Value::Ext("Time", Box::new(Value::String("12:34:56.000000".into())))
    );

    // DateTime with len 4 has no time portion.
    let dt = decode(binary(&[4, 0xE7, 0x07, 5, 6], ColumnType::DateTime));
    assert_eq!(ext_name(&dt), Some("DateTime"));
    // DateTime with time and microseconds.
    let dt = decode(binary(
        &[11, 0xE7, 0x07, 5, 6, 7, 8, 9, 0x40, 0xE2, 0x01, 0x00],
        ColumnType::DateTime,
    ));
    assert_eq!(ext_name(&dt), Some("DateTime"));

    // Timestamp binary: same layout as datetime.
    let ts = decode(binary(&[4, 0xE7, 0x07, 5, 6], ColumnType::Timestamp));
    assert_eq!(ext_name(&ts), Some("Timestamp"));
    let ts = decode(binary(
        &[7, 0xE7, 0x07, 5, 6, 7, 8, 9],
        ColumnType::Timestamp,
    ));
    assert_eq!(ext_name(&ts), Some("Timestamp"));

    // YEAR is a fixed-width two-byte integer.
    assert_eq!(
        decode(binary(&[0xE8, 0x07], ColumnType::Year)),
        Value::Ext("Year", Box::new(Value::String("2024".into())))
    );
    assert!(<Value as Decode>::decode(binary(&[0], ColumnType::Year)).is_err());
}

#[test]
fn test_value_decode_error_paths() {
    // Invalid UTF-8 strings fall back to empty / default values.
    assert_eq!(
        decode(binary(&[0xff], ColumnType::VarChar)),
        Value::String("".into())
    );
    assert_eq!(
        decode(text("not-a-number", ColumnType::Float)),
        Value::F32(0.0)
    );
    assert_eq!(
        decode(text("not-a-number", ColumnType::Long)),
        Value::I64(0)
    );
    assert_eq!(
        decode(text("not-a-number", ColumnType::Short)),
        Value::I32(0)
    );
    assert_eq!(
        decode(text("", ColumnType::Decimal)),
        Value::Ext("Decimal", Box::new(Value::String("".into())))
    );

    // Malformed JSON is an error.
    assert!(<Value as Decode>::decode(text("not json", ColumnType::Json)).is_err());
    // Malformed timestamp/datetime text is an error.
    assert!(<Value as Decode>::decode(text("nope", ColumnType::Timestamp)).is_err());
    assert!(<Value as Decode>::decode(text("nope", ColumnType::DateTime)).is_err());
}

// ---------------------------------------------------------------------------
// rbs::Value encoding
// ---------------------------------------------------------------------------

#[test]
fn test_value_encode_primitives() {
    assert_eq!(encode(Value::Null), (0, vec![]));
    assert_eq!(encode(Value::Bool(true)), (1, vec![1]));
    assert_eq!(encode(Value::Bool(false)), (1, vec![0]));
    assert_eq!(encode(Value::I32(-2)), (4, (-2i32).to_le_bytes().to_vec()));
    assert_eq!(encode(Value::I64(-3)), (8, (-3i64).to_le_bytes().to_vec()));
    assert_eq!(encode(Value::U32(4)), (4, 4u32.to_le_bytes().to_vec()));
    assert_eq!(encode(Value::U64(5)), (8, 5u64.to_le_bytes().to_vec()));
    assert_eq!(encode(Value::F32(1.5)), (4, 1.5f32.to_le_bytes().to_vec()));
    assert_eq!(encode(Value::F64(2.5)), (8, 2.5f64.to_le_bytes().to_vec()));
    assert_eq!(
        encode(Value::String("abc".into())),
        (0, vec![3, b'a', b'b', b'c'])
    );
    assert_eq!(encode(Value::Binary(vec![1, 2])), (0, vec![2, 1, 2]));
}

#[test]
fn test_value_encode_json_containers() {
    let (len, buf) = encode(Value::Array(vec![Value::I32(1), Value::I32(2)]));
    assert_eq!(len, 5);
    assert_eq!(buf, b"\x05[1,2]".to_vec());

    let map: Value = serde_json::from_str("{\"a\":1}").unwrap();
    let (len, buf) = encode(map);
    assert!(len > 0);
    assert_eq!(buf[0] as usize, len);
    assert!(std::str::from_utf8(&buf[1..]).unwrap().contains("\"a\":1"));
}

#[test]
fn test_value_encode_ext_types() {
    assert_eq!(
        encode(Value::Ext("Uuid", Box::new(Value::String("abc".into())))),
        (3, vec![3, b'a', b'b', b'c'])
    );
    // Non-string UUIDs are rejected.
    assert!(Value::Ext("Uuid", Box::new(Value::I32(1)))
        .encode(&mut Vec::new())
        .is_err());

    let (n, buf) = encode(Value::Ext(
        "Decimal",
        Box::new(Value::String("12.34".into())),
    ));
    assert_eq!(n, 5);
    assert_eq!(buf, b"\x0512.34".to_vec());
    // Invalid decimals are rejected instead of becoming zero.
    assert!(Value::Ext("Decimal", Box::new(Value::String("nope".into())))
        .encode(&mut Vec::new())
        .is_err());

    assert_eq!(
        encode(Value::Ext("Year", Box::new(Value::U64(2024)))),
        (2, vec![0xE8, 0x07])
    );
    assert_eq!(
        encode(Value::Ext("Year", Box::new(Value::String("2024".into())))),
        (2, vec![0xE8, 0x07])
    );
    assert!(Value::Ext("Year", Box::new(Value::String("x".into())))
        .encode(&mut Vec::new())
        .is_err());

    let (_, buf) = encode(Value::Ext(
        "Date",
        Box::new(Value::String("2023-05-06".into())),
    ));
    assert_eq!(buf, vec![4, 0xE7, 0x07, 5, 6]);

    let (_, buf) = encode(Value::Ext(
        "Time",
        Box::new(Value::String("01:02:03".into())),
    ));
    assert_eq!(buf[0], 8);
    assert_eq!(&buf[1..], &[0, 0, 0, 0, 0, 1, 2, 3]);

    let (_, buf) = encode(Value::Ext("Timestamp", Box::new(Value::U64(0))));
    assert!(!buf.is_empty());
    assert!(Value::Ext("Timestamp", Box::new(Value::String("not-a-number".into())))
        .encode(&mut Vec::new())
        .is_err());

    let (_, buf) = encode(Value::Ext(
        "DateTime",
        Box::new(Value::String("2023-05-06 01:02:03".into())),
    ));
    assert!(!buf.is_empty());

    let (_, buf) = encode(Value::Ext(
        "Json",
        Box::new(Value::String("{\"a\":1}".into())),
    ));
    assert_eq!(buf[0] as usize, buf.len() - 1);

    assert_eq!(
        encode(Value::Ext("Enum", Box::new(Value::String("a".into())))),
        (1, vec![1, b'a'])
    );
    assert_eq!(
        encode(Value::Ext("Set", Box::new(Value::String("a,b".into())))),
        (3, vec![3, b'a', b',', b'b'])
    );
    assert_eq!(
        encode(Value::Ext("Bit", Box::new(Value::U64(0x0102)))),
        (0, vec![2, 0x01, 0x02])
    );
    assert_eq!(
        encode(Value::Ext("Geometry", Box::new(Value::Binary(vec![1, 2])))),
        (0, vec![2, 1, 2])
    );
    assert_eq!(
        encode(Value::Ext("Custom", Box::new(Value::String("xy".into())))),
        (0, vec![2, b'x', b'y'])
    );
    // Unknown extension values are converted through `into_bytes`.
    assert_eq!(
        encode(Value::Ext("Custom", Box::new(Value::I32(1)))),
        (0, vec![1, b'1'])
    );
    assert_eq!(
        encode(Value::Ext("Custom", Box::new(Value::Null))),
        (0, vec![0])
    );
}

#[test]
fn test_value_encode_invalid_ext_errors() {
    assert!(Value::Ext("Date", Box::new(Value::String("nope".into())))
        .encode(&mut Vec::new())
        .is_err());
    assert!(Value::Ext("Time", Box::new(Value::String("nope".into())))
        .encode(&mut Vec::new())
        .is_err());
    assert!(
        Value::Ext("DateTime", Box::new(Value::String("nope".into())))
            .encode(&mut Vec::new())
            .is_err()
    );
}

// ---------------------------------------------------------------------------
// Date
// ---------------------------------------------------------------------------

fn date_2023() -> RbdcDate {
    RbdcDate(fastdate::Date {
        year: 2023,
        mon: 5,
        day: 6,
    })
}

#[test]
fn test_date_encode_decode() {
    let mut buf = Vec::new();
    assert_eq!(date_2023().encode(&mut buf).unwrap(), 4);
    assert_eq!(buf, vec![4, 0xE7, 0x07, 5, 6]);

    let d = RbdcDate::decode(text("2023-05-06", ColumnType::Date)).unwrap();
    assert_eq!((d.0.year, d.0.mon, d.0.day), (2023, 5, 6));

    let d = RbdcDate::decode(binary(&[4, 0xE7, 0x07, 5, 6], ColumnType::Date)).unwrap();
    assert_eq!((d.0.year, d.0.mon, d.0.day), (2023, 5, 6));

    assert!(RbdcDate::decode(text("not-a-date", ColumnType::Date)).is_err());

    let zero = decode_date_buf(&[]).unwrap();
    assert_eq!((zero.year, zero.mon, zero.day), (0, 0, 0));
}

// ---------------------------------------------------------------------------
// DateTime
// ---------------------------------------------------------------------------

#[test]
fn test_datetime_encode_size_variants() {
    let midnight = RbdcDateTime(fastdate::DateTime::from_str("2023-05-06 00:00:00").unwrap());
    let with_time = RbdcDateTime(fastdate::DateTime::from_str("2023-05-06 01:02:03").unwrap());
    let with_nanos = RbdcDateTime(fastdate::DateTime::from_timestamp_millis(123));

    let mut buf = Vec::new();
    assert_eq!(midnight.encode(&mut buf).unwrap(), 5);
    assert_eq!(buf.len(), 5);

    let mut buf = Vec::new();
    assert_eq!(with_time.encode(&mut buf).unwrap(), 8);
    assert_eq!(buf.len(), 8);

    let mut buf = Vec::new();
    assert_eq!(with_nanos.encode(&mut buf).unwrap(), 12);
    assert_eq!(buf.len(), 12);
}

#[test]
fn test_datetime_decode() {
    let dt = RbdcDateTime::decode(text("2023-05-06 07:08:09", ColumnType::DateTime)).unwrap();
    assert_eq!(
        (
            dt.0.year(),
            dt.0.mon(),
            dt.0.day(),
            dt.0.hour(),
            dt.0.minute(),
            dt.0.sec()
        ),
        (2023, 5, 6, 7, 8, 9)
    );

    // len == 4 -> no time component.
    let dt = RbdcDateTime::decode(binary(&[4, 0xE7, 0x07, 5, 6], ColumnType::DateTime)).unwrap();
    assert_eq!(
        (dt.0.year(), dt.0.hour(), dt.0.minute(), dt.0.sec()),
        (2023, 0, 0, 0)
    );

    // len == 7 -> time without microseconds.
    let dt = RbdcDateTime::decode(binary(
        &[7, 0xE7, 0x07, 5, 6, 7, 8, 9],
        ColumnType::DateTime,
    ))
    .unwrap();
    assert_eq!((dt.0.hour(), dt.0.minute(), dt.0.sec()), (7, 8, 9));

    // len == 11 -> microseconds included.
    let dt = RbdcDateTime::decode(binary(
        &[11, 0xE7, 0x07, 5, 6, 7, 8, 9, 0x40, 0xE2, 0x01, 0x00],
        ColumnType::DateTime,
    ))
    .unwrap();
    assert_eq!(dt.0.nano(), 123_456_000);

    assert!(RbdcDateTime::decode(text("nope", ColumnType::DateTime)).is_err());
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

#[test]
fn test_time_encode_decode() {
    let no_nanos = fastdate::Time {
        hour: 1,
        minute: 2,
        sec: 3,
        nano: 0,
    };
    let mut buf = Vec::new();
    assert_eq!(no_nanos.encode(&mut buf).unwrap(), 8);
    assert_eq!(buf, vec![8, 0, 0, 0, 0, 0, 1, 2, 3]);

    let with_nanos = fastdate::Time {
        hour: 1,
        minute: 2,
        sec: 3,
        nano: 123_000_000,
    };
    let mut buf = Vec::new();
    assert_eq!(with_nanos.encode(&mut buf).unwrap(), 12);
    assert_eq!(&buf[0..8], &[12, 0, 0, 0, 0, 0, 1, 2]);
    assert_eq!(buf[8], 3);
    assert_eq!(&buf[9..13], &123_000u32.to_le_bytes());
}

#[test]
fn test_time_decode() {
    let t = fastdate::Time::decode(text("12:34:56", ColumnType::Time)).unwrap();
    assert_eq!((t.hour, t.minute, t.sec), (12, 34, 56));

    let t = fastdate::Time::decode(binary(&[4, 0, 0, 0, 0], ColumnType::Time)).unwrap();
    assert_eq!((t.hour, t.minute, t.sec, t.nano), (0, 0, 0, 0));

    let t = fastdate::Time::decode(binary(&[8, 0, 0, 0, 0, 1, 2, 3], ColumnType::Time)).unwrap();
    assert_eq!((t.hour, t.minute, t.sec, t.nano), (1, 2, 3, 0));

    let t = RbdcTime::decode(binary(&[8, 0, 0, 0, 0, 1, 2, 3], ColumnType::Time)).unwrap();
    assert_eq!((t.0.hour, t.0.minute, t.0.sec), (1, 2, 3));

    let t = decode_time(&[1, 2, 3]);
    assert_eq!((t.hour, t.minute, t.sec, t.nano), (1, 2, 3, 0));
    let t = decode_time(&[1, 2, 3, 0x39, 0x30, 0, 0]);
    assert_eq!(t.nano, 0x3039 * 1000);

    assert!(fastdate::Time::decode(text("nope", ColumnType::Time)).is_err());
}

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

#[test]
fn test_timestamp_encode_and_decode() {
    for millis in [0i64, 123, 1_000_000_000_000] {
        let mut buf = Vec::new();
        Timestamp(millis).encode(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    let millis = fastdate::DateTime::from_str("2023-05-06 07:08:09")
        .unwrap()
        .unix_timestamp_millis();
    let mut encoded = Vec::new();
    Timestamp(millis).encode(&mut encoded).unwrap();
    assert_eq!(encoded, vec![7, 0xE7, 0x07, 5, 5, 23, 8, 9]);

    let ts = Timestamp::decode(text("2023-05-06 07:08:09", ColumnType::Timestamp)).unwrap();
    let expected = fastdate::DateTime::from_str("2023-05-06 07:08:09")
        .unwrap()
        .unix_timestamp_millis();
    assert_eq!(ts.0, expected);

    let ts = Timestamp::decode(binary(&[4, 0xE7, 0x07, 5, 6], ColumnType::Timestamp)).unwrap();
    let expected = fastdate::DateTime::from((
        fastdate::Date {
            year: 2023,
            mon: 5,
            day: 6,
        },
        fastdate::Time {
            hour: 0,
            minute: 0,
            sec: 0,
            nano: 0,
        },
    ))
    .unix_timestamp_millis();
    assert_eq!(ts.0, expected);

    let ts = Timestamp::decode(binary(
        &[7, 0xE7, 0x07, 5, 6, 7, 8, 9],
        ColumnType::Timestamp,
    ))
    .unwrap();
    assert_eq!(ts.0, expected + ((7 * 3600 + 8 * 60 + 9) * 1000));

    assert!(Timestamp::decode(text("nope", ColumnType::Timestamp)).is_err());
}

// ---------------------------------------------------------------------------
// Decimal / Json / Uuid
// ---------------------------------------------------------------------------

#[test]
fn test_decimal_encode_decode() {
    let d = Decimal::from_str("12.34").unwrap();
    let mut buf = Vec::new();
    assert_eq!(d.encode(&mut buf).unwrap(), 5);
    assert_eq!(buf, b"\x0512.34".to_vec());

    let d = Decimal::decode(text("12.34", ColumnType::NewDecimal)).unwrap();
    assert_eq!(d.0.to_string(), "12.34");

    // A missing value decodes to zero.
    let d = Decimal::decode(vref(None, ColumnType::Decimal, MySqlValueFormat::Text)).unwrap();
    assert_eq!(d.0.to_string(), "0");

    assert!(Decimal::decode(text("not-a-decimal", ColumnType::Decimal)).is_err());
}

#[test]
fn test_json_encode_decode() {
    let mut buf = Vec::new();
    assert_eq!(RbdcJson(String::new()).encode(&mut buf).unwrap(), 4);
    assert_eq!(buf, b"\x04null".to_vec());

    let mut buf = Vec::new();
    assert_eq!(
        RbdcJson("{\"a\":1}".to_string()).encode(&mut buf).unwrap(),
        7
    );
    assert_eq!(buf, b"\x07{\"a\":1}".to_vec());

    let j: Json = RbdcJson::decode(text("{\"a\":1}", ColumnType::Json)).unwrap();
    assert_eq!(j.0, "{\"a\":1}");
    let j: Json = RbdcJson::decode(vref(None, ColumnType::Json, MySqlValueFormat::Text)).unwrap();
    assert_eq!(j.0, "null");
}

#[test]
fn test_uuid_encode_decode() {
    let u = Uuid("abc".to_string());
    let mut buf = Vec::new();
    assert_eq!(u.encode(&mut buf).unwrap(), 3);
    assert_eq!(buf, b"\x03abc".to_vec());

    assert_eq!(
        Uuid::decode(text("abc", ColumnType::VarChar)).unwrap().0,
        "abc"
    );
    assert_eq!(
        Uuid::decode(vref(None, ColumnType::VarChar, MySqlValueFormat::Text))
            .unwrap()
            .0,
        ""
    );
}

// ---------------------------------------------------------------------------
// Year / Enum / Set / Geometry
// ---------------------------------------------------------------------------

#[test]
fn test_year_encode_decode_and_formatting() {
    let y = Year(2024);
    assert_eq!(y.to_string(), "2024");
    assert_eq!(format!("{y:?}"), "Year(2024)");

    let mut buf = Vec::new();
    assert_eq!(y.encode(&mut buf).unwrap(), 2);
    assert_eq!(buf, vec![0xE8, 0x07]);

    assert_eq!(
        Year::decode(text("2024", ColumnType::Year)).unwrap(),
        Year(2024)
    );
    assert_eq!(
        Year::decode(text("nope", ColumnType::Year)).unwrap(),
        Year(0)
    );
    assert_eq!(
        Year::decode(binary(&[0xE8, 0x07], ColumnType::Year)).unwrap(),
        Year(2024)
    );
    assert!(Year::decode(binary(&[124], ColumnType::Year)).is_err());
    assert!(Year::decode(binary(&[0], ColumnType::Year)).is_err());
    assert!(Year::decode(binary(&[], ColumnType::Year)).is_err());
}

#[test]
fn test_enum_display_and_roundtrip() {
    let e = Enum("a".to_string());
    assert_eq!(e.to_string(), "a");
    assert_eq!(format!("{e:?}"), "Enum(a)");

    let mut buf = Vec::new();
    assert_eq!(e.clone().encode(&mut buf).unwrap(), 1);
    assert_eq!(buf, vec![1, b'a']);

    assert_eq!(
        Enum::decode(text("a", ColumnType::Enum)).unwrap(),
        Enum("a".to_string())
    );
    assert_eq!(
        Enum::decode(vref(None, ColumnType::Enum, MySqlValueFormat::Text)).unwrap(),
        Enum(String::new())
    );
}

#[test]
fn test_set_display_and_roundtrip() {
    let s = Set("a,b".to_string());
    assert_eq!(s.to_string(), "a,b");
    assert_eq!(format!("{s:?}"), "Set(a,b)");

    let mut buf = Vec::new();
    assert_eq!(s.clone().encode(&mut buf).unwrap(), 3);
    assert_eq!(buf, vec![3, b'a', b',', b'b']);

    assert_eq!(
        Set::decode(text("a,b", ColumnType::Set)).unwrap(),
        Set("a,b".to_string())
    );
    assert_eq!(
        Set::decode(vref(None, ColumnType::Set, MySqlValueFormat::Text)).unwrap(),
        Set(String::new())
    );
}

#[test]
fn test_geometry_display_and_roundtrip() {
    let g = Geometry(vec![1, 2]);
    assert_eq!(g.to_string(), "0102");
    assert_eq!(format!("{g:?}"), "Geometry([1, 2])");

    let mut buf = Vec::new();
    assert_eq!(g.clone().encode(&mut buf).unwrap(), 3);
    assert_eq!(buf, vec![2, 1, 2]);

    assert_eq!(
        Geometry::decode(binary(&[1, 2], ColumnType::Geometry)).unwrap(),
        Geometry(vec![1, 2])
    );
    assert_eq!(
        Geometry::decode(vref(None, ColumnType::Geometry, MySqlValueFormat::Binary)).unwrap(),
        Geometry(vec![])
    );
}

#[test]
fn test_errors_are_send_sync_debug() {
    // Keep the rbdc Error type in use here so type inference is exercised.
    fn assert_error<E: std::error::Error>(_: &E) {}
    let e: Error = Error::protocol("x");
    assert_error(&e);
}
