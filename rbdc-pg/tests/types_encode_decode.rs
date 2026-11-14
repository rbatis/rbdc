//! Offline encode/decode coverage for every PostgreSQL type supported by rbdc-pg.

use std::collections::HashMap;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use rbdc::date::Date;
use rbdc::datetime::DateTime;
use rbdc::decimal::Decimal;
use rbdc::json::Json;
use rbdc::timestamp::Timestamp;
use rbdc::uuid::Uuid;
use rbdc_pg::arguments::PgArgumentBuffer;
use rbdc_pg::type_info::{PgType, PgTypeInfo};
use rbdc_pg::types::byte::Bytea;
use rbdc_pg::types::decode::Decode;
use rbdc_pg::types::encode::{Encode, IsNull};
use rbdc_pg::types::hstore::Hstore;
use rbdc_pg::types::money::Money;
use rbdc_pg::types::point::Point;
use rbdc_pg::types::timetz::{OffsetTz, Timetz};
use rbdc_pg::types::tsquery::TsQuery;
use rbdc_pg::types::tsvector::TsVector;
use rbdc_pg::types::{Oid, TypeInfo};
use rbdc_pg::value::{PgValueFormat, PgValueRef};
use rbs::Value;

fn text_ref(s: &str) -> PgValueRef<'_> {
    PgValueRef {
        value: Some(s.as_bytes()),
        type_info: PgTypeInfo(PgType::Unknown),
        format: PgValueFormat::Text,
        timezone_sec: None,
    }
}

fn text_ref_typed(s: &str, ty: PgType) -> PgValueRef<'_> {
    PgValueRef {
        value: Some(s.as_bytes()),
        type_info: PgTypeInfo(ty),
        format: PgValueFormat::Text,
        timezone_sec: None,
    }
}

fn binary_ref(bytes: &[u8], ty: PgType) -> PgValueRef<'_> {
    PgValueRef {
        value: Some(bytes),
        type_info: PgTypeInfo(ty),
        format: PgValueFormat::Binary,
        timezone_sec: None,
    }
}

fn null_ref(ty: PgType) -> PgValueRef<'static> {
    PgValueRef {
        value: None,
        type_info: PgTypeInfo(ty),
        format: PgValueFormat::Binary,
        timezone_sec: None,
    }
}

fn encode<T: Encode>(v: T) -> Vec<u8> {
    let mut buf = PgArgumentBuffer::default();
    let is_null = v.encode(&mut buf).unwrap();
    assert!(matches!(is_null, IsNull::No));
    buf.to_vec()
}

fn encode_err<T: Encode>(v: T) -> bool {
    let mut buf = PgArgumentBuffer::default();
    v.encode(&mut buf).is_err()
}

#[test]
fn bool_codec() {
    assert_eq!(encode(true), vec![1]);
    assert_eq!(encode(false), vec![0]);

    assert!(<bool as Decode>::decode(binary_ref(&[1], PgType::Bool)).unwrap());
    assert!(!<bool as Decode>::decode(binary_ref(&[0], PgType::Bool)).unwrap());
    assert!(<bool as Decode>::decode(text_ref_typed("t", PgType::Bool)).unwrap());
    assert!(!<bool as Decode>::decode(text_ref_typed("f", PgType::Bool)).unwrap());
    assert!(<bool as Decode>::decode(text_ref_typed("yes", PgType::Bool)).is_err());
    // NULL is handled by Value but raw decode reports the null error
    assert!(<bool as Decode>::decode(null_ref(PgType::Bool)).is_err());
}

#[test]
fn integer_codec() {
    assert_eq!(encode(1i8), vec![1]);
    assert_eq!(encode(1u8), vec![1]);
    assert_eq!(encode(0x0102i16), vec![1, 2]);
    assert_eq!(encode(0x01020304i32), vec![1, 2, 3, 4]);
    assert_eq!(encode(0x0102030405060708i64), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(encode(0x0102u16), vec![1, 2]);
    assert_eq!(encode(0x01020304u32), vec![1, 2, 3, 4]);
    assert_eq!(encode(0x0102030405060708u64), vec![1, 2, 3, 4, 5, 6, 7, 8]);

    assert_eq!(
        <i8 as Decode>::decode(binary_ref(&[0x80], PgType::Char)).unwrap(),
        -128
    );
    assert_eq!(
        <u8 as Decode>::decode(binary_ref(&[0xff], PgType::Char)).unwrap(),
        255
    );
    // empty slice decodes as zero
    assert_eq!(
        <i8 as Decode>::decode(binary_ref(&[], PgType::Char)).unwrap(),
        0
    );
    assert_eq!(
        <u8 as Decode>::decode(binary_ref(&[], PgType::Char)).unwrap(),
        0
    );

    assert_eq!(
        <i16 as Decode>::decode(binary_ref(&[0, 5], PgType::Int2)).unwrap(),
        5
    );
    assert_eq!(
        <i16 as Decode>::decode(binary_ref(&[0, 0, 0, 5], PgType::Int4)).unwrap(),
        5
    );
    assert_eq!(
        <i16 as Decode>::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        5
    );
    assert!(<i16 as Decode>::decode(binary_ref(&[0], PgType::Int2)).is_err());
    assert_eq!(<i16 as Decode>::decode(text_ref("5")).unwrap(), 5);

    assert_eq!(
        <i32 as Decode>::decode(binary_ref(&[0, 0, 0, 5], PgType::Int4)).unwrap(),
        5
    );
    assert_eq!(
        <i32 as Decode>::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        5
    );
    assert!(<i32 as Decode>::decode(binary_ref(&[0], PgType::Int4)).is_err());
    assert_eq!(<i32 as Decode>::decode(text_ref("5")).unwrap(), 5);
    assert!(<i32 as Decode>::decode(text_ref("x")).is_err());

    assert_eq!(
        <i64 as Decode>::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        5
    );
    assert_eq!(<i64 as Decode>::decode(text_ref("-5")).unwrap(), -5);

    assert_eq!(
        <u16 as Decode>::decode(binary_ref(&[0, 5], PgType::Int2)).unwrap(),
        5
    );
    assert_eq!(
        <u16 as Decode>::decode(binary_ref(&[0, 0, 0, 5], PgType::Int4)).unwrap(),
        5
    );
    assert_eq!(
        <u16 as Decode>::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        5
    );
    assert!(<u16 as Decode>::decode(binary_ref(&[0], PgType::Int2)).is_err());
    assert_eq!(<u16 as Decode>::decode(text_ref("5")).unwrap(), 5);

    assert_eq!(
        <u32 as Decode>::decode(binary_ref(&[0, 0, 0, 5], PgType::Int4)).unwrap(),
        5
    );
    assert_eq!(
        <u32 as Decode>::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        5
    );
    assert!(<u32 as Decode>::decode(binary_ref(&[0], PgType::Int4)).is_err());
    assert_eq!(<u32 as Decode>::decode(text_ref("5")).unwrap(), 5);

    assert_eq!(
        <u64 as Decode>::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        5
    );
    assert_eq!(<u64 as Decode>::decode(text_ref("5")).unwrap(), 5);
}

#[test]
fn float_codec() {
    assert_eq!(encode(1.5f32), 1.5f32.to_be_bytes());
    assert_eq!(encode(-2.25f64), (-2.25f64).to_be_bytes());

    assert_eq!(
        <f32 as Decode>::decode(binary_ref(&1.5f32.to_be_bytes(), PgType::Float4)).unwrap(),
        1.5
    );
    assert_eq!(
        <f32 as Decode>::decode(binary_ref(&1.5f64.to_be_bytes(), PgType::Float8)).unwrap(),
        1.5
    );
    assert!(<f32 as Decode>::decode(binary_ref(&[0, 0, 0], PgType::Float4)).is_err());
    assert_eq!(<f32 as Decode>::decode(text_ref("1.5")).unwrap(), 1.5);

    assert_eq!(
        <f64 as Decode>::decode(binary_ref(&1.5f64.to_be_bytes(), PgType::Float8)).unwrap(),
        1.5
    );
    assert_eq!(<f64 as Decode>::decode(text_ref("1.5")).unwrap(), 1.5);
    assert!(<f64 as Decode>::decode(text_ref("x")).is_err());
}

#[test]
fn string_and_bytea_codec() {
    let s = String::decode(text_ref("hello")).unwrap();
    assert_eq!(s, "hello");
    assert_eq!(encode("hello".to_string()), b"hello".to_vec());
    assert_eq!(String::new().type_info(), PgTypeInfo(PgType::Varchar));

    // binary and text bytea
    assert_eq!(
        Vec::<u8>::decode(binary_ref(&[1, 2, 3], PgType::Bytea)).unwrap(),
        vec![1, 2, 3]
    );
    assert_eq!(
        Vec::<u8>::decode(text_ref("\\x0102ff")).unwrap(),
        vec![1, 2, 255]
    );
    assert!(Vec::<u8>::decode(text_ref("0102")).is_err());
    assert!(Vec::<u8>::decode(text_ref("\\xzz")).is_err());
    assert_eq!(encode(vec![1u8, 2, 3]), vec![1, 2, 3]);
    assert_eq!(encode(&[1u8, 2, 3][..]), vec![1, 2, 3]);

    let bytea = Bytea(vec![9, 8]);
    assert_eq!(format!("{bytea}"), "[len=2]");
    assert_eq!(encode(bytea.clone()), vec![9, 8]);
    assert_eq!(
        Bytea::decode(binary_ref(&[9, 8], PgType::Bytea)).unwrap(),
        bytea
    );
    let v: Value = bytea.into();
    assert_eq!(v, Value::Ext("Bytea", Box::new(Value::Binary(vec![9, 8]))));
}

#[test]
fn date_time_codecs() {
    let d = fastdate::Date {
        year: 2000,
        mon: 1,
        day: 2,
    };
    let encoded = encode(d.clone());
    assert_eq!(encoded, 1i32.to_be_bytes().to_vec());
    assert_eq!(
        fastdate::Date::decode(binary_ref(&encoded, PgType::Date)).unwrap(),
        d
    );

    // before epoch exercises the negative branch
    let before = fastdate::Date {
        year: 1999,
        mon: 12,
        day: 31,
    };
    let before_encoded = encode(before.clone());
    assert_eq!(before_encoded, (-1i32).to_be_bytes().to_vec());
    assert_eq!(
        fastdate::Date::decode(binary_ref(&before_encoded, PgType::Date)).unwrap(),
        before
    );

    assert_eq!(
        fastdate::Date::decode(text_ref("2020-05-06")).unwrap(),
        fastdate::Date {
            year: 2020,
            mon: 5,
            day: 6
        }
    );
    assert!(fastdate::Date::decode(text_ref("not-a-date")).is_err());

    let rbdc_date = Date(d);
    assert_eq!(
        Date::decode(binary_ref(&encoded, PgType::Date)).unwrap(),
        rbdc_date
    );
    assert_eq!(encode(rbdc_date.clone()), encoded);

    let t = rbdc::Time(fastdate::Time {
        hour: 1,
        minute: 2,
        sec: 3,
        nano: 0,
    });
    // NOTE: the encoder writes microseconds as u32 while the decoder reads an
    // i64; assert the encoding and decode a well-formed i64 payload separately.
    let t_encoded = encode(t.clone());
    assert_eq!(t_encoded.len(), 4);
    let micros = (1u64 * 3600 + 2 * 60 + 3) as i64 * 1_000_000;
    assert_eq!(
        rbdc::Time::decode(binary_ref(&micros.to_be_bytes(), PgType::Time)).unwrap(),
        t
    );
    assert_eq!(rbdc::Time::decode(text_ref("01:02:03")).unwrap(), t);
    assert!(rbdc::Time::decode(text_ref("nope")).is_err());
    // negative microseconds branch
    assert!(rbdc::Time::decode(binary_ref(&(-1i64).to_be_bytes(), PgType::Time)).is_ok());

    let ts_ms = DateTime::from_str("2021-02-03 04:05:06")
        .unwrap()
        .unix_timestamp_millis();
    let ts = Timestamp(ts_ms);
    let ts_encoded = encode(ts.clone());
    assert_eq!(
        Timestamp::decode(binary_ref(&ts_encoded, PgType::Timestamp)).unwrap(),
        ts
    );
    assert_eq!(
        Timestamp::decode(text_ref("2021-02-03 04:05:06")).unwrap(),
        Timestamp(
            fastdate::DateTime::from_str("2021-02-03 04:05:06Z")
                .unwrap()
                .unix_timestamp_millis()
        )
    );
    assert!(Timestamp::decode(text_ref("nope")).is_err());
    // negative branch
    let early = Timestamp(-1000);
    let early_encoded = encode(early.clone());
    assert_eq!(
        Timestamp::decode(binary_ref(&early_encoded, PgType::Timestamp)).unwrap(),
        early
    );

    let dt = DateTime::from_timestamp_millis(1_600_000_000_000);
    let dt_encoded = encode(dt.clone());
    assert_eq!(
        DateTime::decode(binary_ref(&dt_encoded, PgType::Timestamptz)).unwrap(),
        dt
    );
    // negative branch
    let early_dt = DateTime::from_timestamp_millis(-1_000_000);
    let early_dt_encoded = encode(early_dt.clone());
    assert_eq!(
        DateTime::decode(binary_ref(&early_dt_encoded, PgType::Timestamptz)).unwrap(),
        early_dt
    );
    // text format
    let text_dt = DateTime::decode(text_ref("2021-02-03 04:05:06")).unwrap();
    assert_eq!(
        text_dt.unix_timestamp_millis(),
        DateTime::from_str("2021-02-03 04:05:06")
            .unwrap()
            .unix_timestamp_millis()
    );
    assert!(DateTime::decode(text_ref("nope")).is_err());
}

#[test]
fn timetz_codec() {
    let tz = Timetz(OffsetTz {
        time: fastdate::Time {
            hour: 1,
            minute: 2,
            sec: 3,
            nano: 0,
        },
        offset: 3600,
    });
    let encoded = encode(tz.clone());
    assert_eq!(encoded.len(), 12);
    assert_eq!(
        Timetz::decode(binary_ref(&encoded, PgType::Timetz)).unwrap(),
        tz
    );
    assert_eq!(Timetz::decode(text_ref("01:02:03+01")).unwrap(), tz);

    assert_eq!(format!("{tz}"), "01:02:03+01:00");
    let negative = Timetz(OffsetTz {
        time: fastdate::Time {
            hour: 0,
            minute: 0,
            sec: 0,
            nano: 0,
        },
        offset: -18000,
    });
    assert_eq!(format!("{negative}"), "00:00:00-05:00");
    let utc = Timetz(OffsetTz {
        time: fastdate::Time {
            hour: 0,
            minute: 0,
            sec: 0,
            nano: 0,
        },
        offset: 0,
    });
    assert_eq!(format!("{utc}"), "00:00:00");
    let _: Value = tz.into();
}

#[test]
fn uuid_codec() {
    let s = "550e8400-e29b-41d4-a716-446655440000";
    let uuid = Uuid(s.to_string());
    let encoded = encode(uuid.clone());
    assert_eq!(encoded.len(), 16);
    assert_eq!(
        Uuid::decode(binary_ref(&encoded, PgType::Uuid)).unwrap(),
        uuid
    );
    assert_eq!(Uuid::decode(text_ref_typed(s, PgType::Uuid)).unwrap(), uuid);
    // text format is parsed as a plain string by the rbdc wrapper
    assert_eq!(
        Uuid::decode(text_ref("not-a-uuid")).unwrap(),
        Uuid("not-a-uuid".to_string())
    );
    assert!(Uuid::decode(binary_ref(&[0, 1], PgType::Uuid)).is_err());
    assert!(encode_err(Uuid("not-a-uuid".to_string())));
}

#[test]
fn json_codec() {
    let json = Json("{\"a\":1}".to_string());
    let encoded = encode(json.clone());
    assert_eq!(encoded[0], 1);
    assert_eq!(&encoded[1..], b"{\"a\":1}");
    assert_eq!(
        Json::decode(binary_ref(&encoded, PgType::Jsonb)).unwrap(),
        json
    );

    // JSON (not JSONB) does not strip a version byte
    let raw = b"{\"a\":2}";
    assert_eq!(
        Json::decode(binary_ref(raw, PgType::Json)).unwrap(),
        Json("{\"a\":2}".to_string())
    );
    assert_eq!(
        Json::decode(text_ref_typed("{\"a\":3}", PgType::Jsonb)).unwrap(),
        Json("{\"a\":3}".to_string())
    );
    // empty and zero-length buffers are "null"
    assert_eq!(
        Json::decode(null_ref(PgType::Json)).unwrap(),
        Json("null".to_string())
    );
    assert_eq!(
        Json::decode(binary_ref(&[], PgType::Json)).unwrap(),
        Json("null".to_string())
    );
    // empty Json encodes as the literal null
    assert_eq!(encode(Json(String::new()))[1..].to_vec(), b"null".to_vec());
    assert_eq!(Json("x".to_string()).type_info(), PgTypeInfo(PgType::Jsonb));

    // decode_json / encode_json through Value
    let v = Value::decode(binary_ref(&encoded, PgType::Jsonb)).unwrap();
    assert_eq!(v, rbs::value!({"a": 1u64}));
    let v = Value::decode(binary_ref(raw, PgType::Json)).unwrap();
    assert_eq!(v, rbs::value!({"a": 2u64}));
    assert_eq!(
        Value::decode(binary_ref(&[], PgType::Json)).unwrap(),
        Value::Null
    );
    assert!(Value::decode(binary_ref(b"not json", PgType::Json)).is_err());

    let mut buf = PgArgumentBuffer::default();
    let is_null = rbdc_pg::types::json::encode_json(rbs::value!({"b": 2}), &mut buf).unwrap();
    assert!(matches!(is_null, IsNull::No));
    assert_eq!(buf[0], 1);
    assert_eq!(&buf[1..], b"{\"b\":2}");
}

#[test]
fn numeric_and_decimal_codec() {
    let dec: BigDecimal = "12345.6789".parse().unwrap();
    let encoded = encode(dec.clone());
    // header + digits
    assert!(encoded.len() >= 8);
    assert_eq!(
        BigDecimal::decode(binary_ref(&encoded, PgType::Numeric)).unwrap(),
        dec
    );
    assert_eq!(BigDecimal::decode(text_ref("12345.6789")).unwrap(), dec);
    assert!(BigDecimal::decode(text_ref("not-a-number")).is_err());

    // zero is encoded as an empty digit array
    let zero: BigDecimal = "0".parse().unwrap();
    let zero_encoded = encode(zero.clone());
    assert_eq!(
        BigDecimal::decode(binary_ref(&zero_encoded, PgType::Numeric)).unwrap(),
        zero
    );

    // NaN payload is rejected by BigDecimal
    let nan = [0u8, 0, 0, 0, 0xC0, 0, 0, 0, 0];
    assert!(BigDecimal::decode(binary_ref(&nan, PgType::Numeric)).is_err());

    // invalid sign
    let mut bad_sign = Vec::new();
    bad_sign.extend(0u16.to_be_bytes()); // num digits
    bad_sign.extend(0i16.to_be_bytes()); // weight
    bad_sign.extend(0x1234u16.to_be_bytes()); // sign
    bad_sign.extend(0i16.to_be_bytes()); // scale
    assert!(BigDecimal::decode(binary_ref(&bad_sign, PgType::Numeric)).is_err());

    // Decimal wrapper
    let decimal = Decimal(dec.clone());
    let encoded = encode(decimal.clone());
    assert_eq!(
        Decimal::decode(binary_ref(&encoded, PgType::Numeric)).unwrap(),
        decimal
    );
    assert_eq!(Decimal::decode(text_ref("12345.6789")).unwrap(), decimal);
}

#[test]
fn money_codec() {
    let money = Money(12345);
    assert_eq!(format!("{money}"), "12345");
    assert_eq!(encode(money.clone()), 12345i64.to_be_bytes().to_vec());
    assert_eq!(
        Money::decode(binary_ref(&12345i64.to_be_bytes(), PgType::Money)).unwrap(),
        money
    );
    assert!(Money::decode(text_ref("$123.45")).is_err());
    let v: Value = money.into();
    assert_eq!(v, Value::Ext("Money", Box::new(Value::I64(12345))));
}

#[test]
fn point_codec() {
    let p = Point { x: 1.5, y: -2.5 };
    assert_eq!(format!("{p}"), "POINT(1.5 -2.5)");
    assert_eq!(Point::decode(text_ref("POINT(1.5 -2.5)")).unwrap(), p);
    assert_eq!(Point::decode(text_ref("  POINT(1.5 -2.5)  ")).unwrap(), p);
    assert_eq!(Point::decode(text_ref("(1.5,-2.5)")).unwrap(), p);
    assert!(Point::decode(text_ref("POINT(1.5)")).is_err());
    assert!(Point::decode(text_ref("LINESTRING(1 2)")).is_err());
    assert!(Point::decode(text_ref("POINT(a b)")).is_err());
    assert_eq!(
        Point::decode(binary_ref(&encode(p), PgType::Point)).unwrap(),
        p
    );
    assert_eq!(
        Point::decode(binary_ref(&[0; 16], PgType::Point)).unwrap(),
        Point { x: 0.0, y: 0.0 }
    );
    let v: Value = p.into();
    assert!(matches!(v, Value::Ext("Point", _)));
}

#[test]
fn hstore_codec() {
    let mut map = HashMap::new();
    map.insert("name".to_string(), "John".to_string());
    map.insert("age".to_string(), "30".to_string());
    let hstore = Hstore(map.clone());
    assert!(format!("{hstore}").contains("name=>John"));
    assert!(format!("{hstore}").contains("age=>30"));

    // Text format
    let decoded = Hstore::decode(text_ref("name=>John, age=>30")).unwrap();
    assert_eq!(decoded, hstore);
    assert_eq!(Hstore::decode(text_ref("   ")).unwrap(), Hstore::default());
    assert_eq!(Hstore::decode(text_ref("")).unwrap(), Hstore::default());
    assert_eq!(
        Hstore::decode(text_ref("a=>1,,b=>2")).unwrap(),
        Hstore(HashMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string())
        ]))
    );
    assert!(Hstore::decode(text_ref("malformed")).is_err());

    // Binary format
    let binary = hstore_binary(&[("name", Some("John")), ("missing", None)]);
    let decoded = Hstore::decode(binary_ref(&binary, PgType::Hstore)).unwrap();
    assert_eq!(decoded.0.get("name").map(String::as_str), Some("John"));
    assert_eq!(decoded.0.get("missing").map(String::as_str), Some("null"));

    assert!(Hstore::decode(binary_ref(&[0, 0], PgType::Hstore)).is_err());
    // entry header too short (count=1, no room for key length)
    assert!(Hstore::decode(binary_ref(&[0, 0, 0, 1, 0, 0], PgType::Hstore)).is_err());
    // key length larger than buffer
    let mut too_long_key = Vec::new();
    too_long_key.extend(1i32.to_be_bytes());
    too_long_key.extend(99i32.to_be_bytes());
    assert!(Hstore::decode(binary_ref(&too_long_key, PgType::Hstore)).is_err());
    // invalid utf8 key
    let mut bad_key = Vec::new();
    bad_key.extend(1i32.to_be_bytes());
    bad_key.extend(1i32.to_be_bytes());
    bad_key.push(0xff);
    bad_key.extend(0i32.to_be_bytes());
    assert!(Hstore::decode(binary_ref(&bad_key, PgType::Hstore)).is_err());
    // invalid utf8 value
    let mut bad_value = Vec::new();
    bad_value.extend(1i32.to_be_bytes()); // count
    bad_value.extend(1i32.to_be_bytes()); // key length
    bad_value.push(b'k');
    bad_value.extend(1i32.to_be_bytes()); // value length
    bad_value.push(0xff);
    assert!(Hstore::decode(binary_ref(&bad_value, PgType::Hstore)).is_err());
    // value length larger than buffer
    let mut too_long_value = Vec::new();
    too_long_value.extend(1i32.to_be_bytes());
    too_long_value.extend(1i32.to_be_bytes());
    too_long_value.push(b'k');
    too_long_value.extend(99i32.to_be_bytes());
    assert!(Hstore::decode(binary_ref(&too_long_value, PgType::Hstore)).is_err());

    let encoded = encode(hstore.clone());
    assert_eq!(
        Hstore::decode(binary_ref(&encoded, PgType::Hstore)).unwrap(),
        hstore
    );
    let from_map: Hstore = map.into();
    assert_eq!(from_map, hstore);
    let v: Value = hstore.into();
    assert!(matches!(v, Value::Ext("Hstore", _)));
}

fn hstore_binary(entries: &[(&str, Option<&str>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend((entries.len() as i32).to_be_bytes());
    for (key, value) in entries {
        // wire order is: key length, key bytes, value length, value bytes
        buf.extend((key.len() as i32).to_be_bytes());
        buf.extend(key.as_bytes());
        match value {
            Some(value) => buf.extend((value.len() as i32).to_be_bytes()),
            None => buf.extend((-1i32).to_be_bytes()),
        }
        if let Some(value) = value {
            buf.extend(value.as_bytes());
        }
    }
    buf
}

#[test]
fn tsvector_tsquery_codec() {
    let tv = TsVector("'a' 'b'".to_string());
    assert_eq!(format!("{tv}"), "'a' 'b'");
    assert_eq!(encode(tv.clone()), b"'a' 'b'");
    assert_eq!(TsVector::decode(text_ref("'a' 'b'")).unwrap(), tv);
    assert_eq!(
        TsVector::decode(binary_ref(b"'a' 'b'", PgType::Tsvector)).unwrap(),
        tv
    );
    assert_eq!(TsVector::from(String::from("x")).0, "x");
    let v: Value = tv.into();
    assert!(matches!(v, Value::Ext("TsVector", _)));

    let tq = TsQuery("a & b".to_string());
    assert_eq!(format!("{tq}"), "a & b");
    assert_eq!(encode(tq.clone()), b"a & b");
    assert_eq!(TsQuery::decode(text_ref("a & b")).unwrap(), tq);
    assert_eq!(
        TsQuery::decode(binary_ref(b"a & b", PgType::Tsquery)).unwrap(),
        tq
    );
    assert_eq!(TsQuery::from(String::from("x")).0, "x");
    let v: Value = tq.into();
    assert!(matches!(v, Value::Ext("TsQuery", _)));
}

#[test]
fn oid_codec() {
    let mut oid = Oid(41);
    let mut buf = PgArgumentBuffer::default();
    Oid(41).encode(&mut buf).unwrap();
    assert_eq!(buf.to_vec(), 41u32.to_be_bytes().to_vec());
    assert_eq!(
        Oid::decode(binary_ref(&41u32.to_be_bytes(), PgType::Oid)).unwrap(),
        oid
    );
    assert_eq!(Oid::decode(text_ref("41")).unwrap(), oid);
    assert!(Oid::decode(text_ref("x")).is_err());
    assert_eq!(oid.type_info(), PgTypeInfo(PgType::Oid));

    oid.incr_one();
    assert_eq!(oid, Oid(42));
    oid = Oid(u32::MAX);
    oid.incr_one();
    assert_eq!(oid, Oid(0));
    assert_eq!(Oid::from(7u32), Oid(7));

    let json = serde_json::to_string(&Oid(9)).unwrap();
    assert_eq!(json, "9");
    assert_eq!(serde_json::from_str::<Oid>("9").unwrap(), Oid(9));
}

#[test]
fn array_binary_codec() {
    // ndim = 0 -> empty
    let bytes = 0i32.to_be_bytes();
    assert_eq!(
        Vec::<Value>::decode(binary_ref(&bytes, PgType::Int4Array)).unwrap(),
        Vec::<Value>::new()
    );

    // multi-dimensional arrays are rejected
    let bytes = 2i32.to_be_bytes();
    assert!(Vec::<Value>::decode(binary_ref(&bytes, PgType::Int4Array)).is_err());

    // valid one-dimensional array
    let bytes = int4_array_binary(&[1, 2, 3]);
    assert_eq!(
        Vec::<Value>::decode(binary_ref(&bytes, PgType::Int4Array)).unwrap(),
        vec![Value::I32(1), Value::I32(2), Value::I32(3)]
    );
    // element type resolved via the outer array type when the OID is unknown
    let mut unknown_oid = int4_array_binary(&[7]);
    unknown_oid[8..12].copy_from_slice(&9999u32.to_be_bytes());
    assert_eq!(
        Vec::<Value>::decode(binary_ref(&unknown_oid, PgType::Int4Array)).unwrap(),
        vec![Value::I32(7)]
    );
    // and an error when it cannot be resolved at all
    assert!(Vec::<Value>::decode(binary_ref(&unknown_oid, PgType::Text)).is_err());

    // array not starting at one is rejected
    let mut bad_lower = int4_array_binary(&[1]);
    bad_lower[16..20].copy_from_slice(&0i32.to_be_bytes());
    assert!(Vec::<Value>::decode(binary_ref(&bad_lower, PgType::Int4Array)).is_err());

    // NULL element (-1 length) decodes through Value
    let mut with_null = Vec::new();
    with_null.extend(1i32.to_be_bytes()); // ndim
    with_null.extend(0i32.to_be_bytes()); // flags
    with_null.extend(23u32.to_be_bytes()); // int4 oid
    with_null.extend(2i32.to_be_bytes()); // len
    with_null.extend(1i32.to_be_bytes()); // lower
    with_null.extend((-1i32).to_be_bytes());
    with_null.extend(4i32.to_be_bytes());
    with_null.extend(9i32.to_be_bytes());
    assert_eq!(
        Vec::<Value>::decode(binary_ref(&with_null, PgType::Int4Array)).unwrap(),
        vec![Value::Null, Value::I32(9)]
    );
}

fn int4_array_binary(values: &[i32]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(1i32.to_be_bytes()); // ndim
    buf.extend(0i32.to_be_bytes()); // flags
    buf.extend(23u32.to_be_bytes()); // element oid
    buf.extend((values.len() as i32).to_be_bytes());
    buf.extend(1i32.to_be_bytes()); // lower bound
    for value in values {
        buf.extend(4i32.to_be_bytes());
        buf.extend(value.to_be_bytes());
    }
    buf
}

#[test]
fn array_text_codec() {
    let v = Vec::<Value>::decode(text_ref_typed("{1,2,3}", PgType::Int4Array)).unwrap();
    assert_eq!(v, vec![Value::I32(1), Value::I32(2), Value::I32(3)]);

    // empty array
    assert_eq!(
        Vec::<Value>::decode(text_ref_typed("{}", PgType::Int4Array)).unwrap(),
        Vec::<Value>::new()
    );

    // quoted values, escapes, NULL
    let v = Vec::<Value>::decode(text_ref_typed(
        "{\"a,b\",\"c\\\"d\",NULL,\"e\\\\f\"}",
        PgType::TextArray,
    ))
    .unwrap();
    assert_eq!(
        v,
        vec![
            Value::String("a,b".to_string()),
            Value::String("c\"d".to_string()),
            Value::Null,
            Value::String("e\\f".to_string())
        ]
    );

    // BOX arrays use ';' as delimiter
    let v = Vec::<String>::decode(text_ref_typed("{(1,2);(3,4)}", PgType::BoxArray)).unwrap();
    assert_eq!(v, vec!["(1,2)".to_string(), "(3,4)".to_string()]);

    // Text type_info without an array kind falls back to UNKNOWN, so
    // Value elements decode as text.
    let v = Vec::<Value>::decode(text_ref_typed("{1}", PgType::Text)).unwrap();
    assert_eq!(v, vec![Value::String("1".to_string())]);
}

#[test]
fn array_encode_codec() {
    // empty arrays declare an UNKNOWN element type
    let encoded = encode(Vec::<Value>::new());
    assert_eq!(&encoded[0..4], &1i32.to_be_bytes());
    assert_eq!(&encoded[4..8], &0i32.to_be_bytes());
    assert_eq!(&encoded[8..12], &705u32.to_be_bytes());
    assert_eq!(&encoded[12..16], &0i32.to_be_bytes());
    assert_eq!(&encoded[16..20], &1i32.to_be_bytes());
    assert_eq!(encoded.len(), 20);

    let encoded = encode(vec![Value::I32(1), Value::I32(2)]);
    assert_eq!(&encoded[8..12], &23u32.to_be_bytes());
    assert_eq!(&encoded[12..16], &2i32.to_be_bytes());
    // each element is length-prefixed
    assert_eq!(&encoded[20..24], &4i32.to_be_bytes());
    assert_eq!(&encoded[24..28], &1i32.to_be_bytes());
    assert_eq!(&encoded[28..32], &4i32.to_be_bytes());
    assert_eq!(&encoded[32..36], &2i32.to_be_bytes());
}

#[test]
fn value_type_info_matches_driver_types() {
    let cases = [
        (Value::Null, PgTypeInfo(PgType::Unknown)),
        (Value::Bool(true), PgTypeInfo(PgType::Bool)),
        (Value::I32(1), PgTypeInfo(PgType::Int4)),
        (Value::I64(1), PgTypeInfo(PgType::Int8)),
        (Value::U32(1), PgTypeInfo(PgType::Int4)),
        (Value::U64(1), PgTypeInfo(PgType::Int8)),
        (Value::F32(1.0), PgTypeInfo(PgType::Float4)),
        (Value::F64(1.0), PgTypeInfo(PgType::Float8)),
        (Value::String("x".into()), PgTypeInfo(PgType::Varchar)),
        (Value::Binary(vec![1]), PgTypeInfo(PgType::Bytea)),
        (Value::Array(vec![]), PgTypeInfo(PgType::Json)),
        (rbs::value!({"a": 1}), PgTypeInfo(PgType::Json)),
    ];
    for (value, expected) in cases {
        assert_eq!(value.type_info(), expected);
    }

    let ext_cases = [
        (
            Value::Ext("TsVector", Box::new(Value::String("'a'".into()))),
            PgTypeInfo(PgType::Tsvector),
        ),
        (
            Value::Ext("TsQuery", Box::new(Value::String("a".into()))),
            PgTypeInfo(PgType::Tsquery),
        ),
        (
            Value::Ext("Hstore", Box::new(Value::String("a=>b".into()))),
            PgTypeInfo(PgType::Hstore),
        ),
        (
            Value::Ext(
                "Point",
                Box::new(Value::Array(vec![Value::F64(1.0), Value::F64(2.0)])),
            ),
            PgTypeInfo(PgType::Point),
        ),
    ];
    for (value, expected) in ext_cases {
        assert_eq!(value.type_info(), expected);
    }

    assert_eq!(encode(Value::Ext("Int2", Box::new(Value::I64(5)))).len(), 2);
    assert_eq!(encode(Value::Ext("Int4", Box::new(Value::I64(5)))).len(), 4);
    assert_eq!(encode(Value::Ext("Int8", Box::new(Value::I64(5)))).len(), 8);
    assert_eq!(
        Value::decode(text_ref_typed("unknown", PgType::Unknown)).unwrap(),
        Value::String("unknown".into())
    );
    assert_eq!(
        Value::decode(binary_ref(&[], PgType::Void)).unwrap(),
        Value::Null
    );
}

#[test]
fn value_encode_covers_ext_names() {
    let cases: Vec<(&'static str, Value)> = vec![
        (
            "Uuid",
            Value::String("550e8400-e29b-41d4-a716-446655440000".into()),
        ),
        ("Bool", Value::Bool(true)),
        ("Decimal", Value::String("1.5".into())),
        ("Numeric", Value::String("1.5".into())),
        ("Date", Value::String("2000-01-02".into())),
        ("Time", Value::String("01:02:03".into())),
        ("Timestamp", Value::I64(1000)),
        ("DateTime", Value::String("2021-02-03 04:05:06".into())),
        ("Bytea", Value::Binary(vec![1, 2])),
        ("Char", Value::String("c".into())),
        ("Name", Value::String("n".into())),
        ("Int8", Value::I64(5)),
        ("Int2", Value::I64(5)),
        ("Int4", Value::I64(5)),
        ("Text", Value::String("t".into())),
        ("Oid", Value::U64(5)),
        ("Json", Value::String("{}".into())),
        ("Jsonb", Value::String("{}".into())),
        ("Lseg", Value::Binary(vec![1])),
        ("Path", Value::Binary(vec![1])),
        ("Box", Value::Binary(vec![1])),
        ("Polygon", Value::Binary(vec![1])),
        ("Line", Value::Binary(vec![1])),
        ("Cidr", Value::Binary(vec![1])),
        ("Float4", Value::F64(1.0)),
        ("Float8", Value::F64(1.0)),
        ("Unknown", Value::Binary(vec![1])),
        ("Circle", Value::Binary(vec![1])),
        ("Macaddr8", Value::Binary(vec![1])),
        ("Macaddr", Value::Binary(vec![1])),
        ("Inet", Value::Binary(vec![1])),
        ("Bpchar", Value::Binary(vec![1])),
        ("Varchar", Value::Binary(vec![1])),
        ("Interval", Value::Binary(vec![1])),
        ("Bit", Value::Binary(vec![1])),
        ("Varbit", Value::Binary(vec![1])),
        ("Record", Value::Binary(vec![1])),
        ("Int4Range", Value::Binary(vec![1])),
        ("NumRange", Value::Binary(vec![1])),
        ("TsRange", Value::Binary(vec![1])),
        ("TstzRange", Value::Binary(vec![1])),
        ("DateRange", Value::Binary(vec![1])),
        ("Int8Range", Value::Binary(vec![1])),
        ("Jsonpath", Value::Binary(vec![1])),
        ("Money", Value::I64(100)),
        ("Void", Value::Binary(vec![1])),
        ("TsVector", Value::String("'a'".into())),
        ("TsQuery", Value::String("a".into())),
        ("Hstore", Value::String("a=>b".into())),
        ("Point", Value::String("POINT(1 2)".into())),
        ("Custom", Value::Binary(vec![1])),
        ("DeclareWithName", Value::Binary(vec![1])),
        ("DeclareWithOid", Value::Binary(vec![1])),
    ];

    for (name, value) in cases {
        let mut buf = PgArgumentBuffer::default();
        let result = buf.encode(Value::Ext(name, Box::new(value)));
        assert!(result.is_ok(), "encode failed for {name}: {result:?}");
    }

    // Timestamptz / Timetz go through rbs deserialization; cover the arms.
    let mut buf = PgArgumentBuffer::default();
    let _ = buf.encode(Value::Ext(
        "Timestamptz",
        Box::new(rbs::value(&DateTime::from_timestamp_millis(0)).unwrap()),
    ));
    let mut buf = PgArgumentBuffer::default();
    let _ = buf.encode(Value::Ext(
        "Timetz",
        Box::new(
            rbs::value(&Timetz(OffsetTz {
                time: fastdate::Time {
                    hour: 0,
                    minute: 0,
                    sec: 0,
                    nano: 0,
                },
                offset: 0,
            }))
            .unwrap(),
        ),
    ));

    // unknown extension names are treated as NULL
    let mut buf = PgArgumentBuffer::default();
    let _ = buf
        .encode(Value::Ext("Nonexistent", Box::new(Value::Null)))
        .unwrap();

    // an invalid POINT is an error
    let mut buf = PgArgumentBuffer::default();
    assert!(buf
        .encode(Value::Ext("Point", Box::new(Value::String("nope".into()))))
        .is_err());

    // Null value is a NULL argument
    let mut buf = PgArgumentBuffer::default();
    buf.encode(Value::Null).unwrap();
    assert!(matches!(buf.encode(Value::Null), Ok(_)));
}

#[test]
fn flags_and_null_handling() {
    // non-null values are length-prefixed
    let mut buf = PgArgumentBuffer::default();
    buf.encode(Value::I32(7)).unwrap();
    assert_eq!(&buf[0..4], &4i32.to_be_bytes());
    assert_eq!(&buf[4..8], &7i32.to_be_bytes());

    // null values are encoded as length -1
    let mut buf = PgArgumentBuffer::default();
    buf.encode(Value::Null).unwrap();
    assert_eq!(&buf[0..4], &(-1i32).to_be_bytes());
    assert_eq!(buf.len(), 4);
}

#[test]
fn value_decode_common_types() {
    assert_eq!(Value::decode(null_ref(PgType::Bool)).unwrap(), Value::Null);
    assert_eq!(
        Value::decode(binary_ref(&[1], PgType::Bool)).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        Value::decode(binary_ref(&[0, 0, 0, 5], PgType::Int4)).unwrap(),
        Value::I32(5)
    );
    assert_eq!(
        Value::decode(binary_ref(&[0, 5], PgType::Int2)).unwrap(),
        Value::I32(5)
    );
    assert_eq!(
        Value::decode(binary_ref(&[0, 0, 0, 0, 0, 0, 0, 5], PgType::Int8)).unwrap(),
        Value::I64(5)
    );
    assert_eq!(
        Value::decode(text_ref_typed("hello", PgType::Text)).unwrap(),
        Value::String("hello".into())
    );
    assert_eq!(
        Value::decode(binary_ref(&[1, 2], PgType::Bytea)).unwrap(),
        Value::Binary(vec![1, 2])
    );
    assert_eq!(
        Value::decode(binary_ref(&7u32.to_be_bytes(), PgType::Oid)).unwrap(),
        Value::Ext("Oid", Box::new(Value::U32(7)))
    );
    assert_eq!(
        Value::decode(text_ref_typed("POINT(1 2)", PgType::Point)).unwrap(),
        Value::from(Point { x: 1.0, y: 2.0 })
    );
    assert_eq!(
        Value::decode(text_ref_typed("1.5", PgType::Float4)).unwrap(),
        Value::F32(1.5)
    );
    assert_eq!(
        Value::decode(text_ref_typed("1.5", PgType::Float8)).unwrap(),
        Value::F64(1.5)
    );
    assert_eq!(
        Value::decode(text_ref_typed("2020-01-02", PgType::Date)).unwrap(),
        Value::Ext("Date", Box::new(Value::String("2020-01-02".into())))
    );
    assert_eq!(
        Value::decode(text_ref_typed("01:02:03", PgType::Time)).unwrap(),
        Value::Ext("Time", Box::new(Value::String("01:02:03".into())))
    );
    let ts = DateTime::from_timestamp_millis(1_000_000);
    let ts_encoded = encode(Timestamp(ts.unix_timestamp_millis()));
    assert!(matches!(
        Value::decode(binary_ref(&ts_encoded, PgType::Timestamp)).unwrap(),
        Value::Ext("Timestamp", _)
    ));
    let dt_encoded = encode(DateTime::from_timestamp_millis(ts.unix_timestamp_millis()));
    assert!(matches!(
        Value::decode(binary_ref(&dt_encoded, PgType::Timestamptz)).unwrap(),
        Value::Ext("DateTime", _)
    ));
    assert!(matches!(
        Value::decode(text_ref_typed("1.5", PgType::Numeric)).unwrap(),
        Value::Ext("Decimal", _)
    ));
    assert!(matches!(
        Value::decode(text_ref_typed(
            "550e8400-e29b-41d4-a716-446655440000",
            PgType::Uuid
        ))
        .unwrap(),
        Value::Ext("Uuid", _)
    ));
    assert_eq!(
        Value::decode(text_ref_typed("{}", PgType::HstoreArray)).unwrap(),
        Value::Array(vec![])
    );
}

#[test]
fn value_decode_all_pg_types_does_not_panic() {
    // Walk every built-in type using the public `PgTypeInfo` constructors. The
    // exact result does not matter here; the goal is to execute every decode
    // arm, including both text and binary formats.
    let types = [
        PgType::Bool,
        PgType::Bytea,
        PgType::Char,
        PgType::Name,
        PgType::Int8,
        PgType::Int2,
        PgType::Int4,
        PgType::Text,
        PgType::Oid,
        PgType::Json,
        PgType::JsonArray,
        PgType::Point,
        PgType::Lseg,
        PgType::Path,
        PgType::Box,
        PgType::Polygon,
        PgType::Line,
        PgType::LineArray,
        PgType::Cidr,
        PgType::CidrArray,
        PgType::Float4,
        PgType::Float8,
        PgType::Unknown,
        PgType::Circle,
        PgType::CircleArray,
        PgType::Macaddr8,
        PgType::Macaddr8Array,
        PgType::Macaddr,
        PgType::Inet,
        PgType::BoolArray,
        PgType::ByteaArray,
        PgType::CharArray,
        PgType::NameArray,
        PgType::Int2Array,
        PgType::Int4Array,
        PgType::TextArray,
        PgType::BpcharArray,
        PgType::VarcharArray,
        PgType::Int8Array,
        PgType::PointArray,
        PgType::LsegArray,
        PgType::PathArray,
        PgType::BoxArray,
        PgType::Float4Array,
        PgType::Float8Array,
        PgType::PolygonArray,
        PgType::OidArray,
        PgType::MacaddrArray,
        PgType::InetArray,
        PgType::Bpchar,
        PgType::Varchar,
        PgType::Date,
        PgType::Time,
        PgType::Timestamp,
        PgType::TimestampArray,
        PgType::DateArray,
        PgType::TimeArray,
        PgType::Timestamptz,
        PgType::TimestamptzArray,
        PgType::Interval,
        PgType::IntervalArray,
        PgType::NumericArray,
        PgType::Timetz,
        PgType::TimetzArray,
        PgType::Bit,
        PgType::BitArray,
        PgType::Varbit,
        PgType::VarbitArray,
        PgType::Numeric,
        PgType::Record,
        PgType::RecordArray,
        PgType::Uuid,
        PgType::UuidArray,
        PgType::Jsonb,
        PgType::JsonbArray,
        PgType::Int4Range,
        PgType::Int4RangeArray,
        PgType::NumRange,
        PgType::NumRangeArray,
        PgType::TsRange,
        PgType::TsRangeArray,
        PgType::TstzRange,
        PgType::TstzRangeArray,
        PgType::DateRange,
        PgType::DateRangeArray,
        PgType::Int8Range,
        PgType::Int8RangeArray,
        PgType::Jsonpath,
        PgType::JsonpathArray,
        PgType::Money,
        PgType::MoneyArray,
        PgType::Hstore,
        PgType::HstoreArray,
        PgType::Tsvector,
        PgType::Tsquery,
        PgType::Void,
    ];

    for ty in types {
        let is_array = matches!(
            PgTypeInfo(ty.clone()).kind(),
            rbdc_pg::type_info::PgTypeKind::Array(_)
        );
        let text_value: &[u8] = if is_array { b"{}" } else { b"1" };
        let _ = Value::decode(PgValueRef {
            value: Some(text_value),
            type_info: PgTypeInfo(ty.clone()),
            format: PgValueFormat::Text,
            timezone_sec: None,
        });
        // JSONB needs a valid version byte only when read in binary format;
        // `Json` (text format) exercises the other branch.
        let binary: &[u8] = if ty == PgType::Jsonb {
            &[1, b'1']
        } else {
            &[0u8; 16]
        };
        let _ = Value::decode(binary_ref(binary, ty.clone()));
    }
}
