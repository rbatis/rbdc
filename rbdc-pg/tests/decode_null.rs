use rbdc_pg::type_info::{PgType, PgTypeInfo};
use rbdc_pg::types::decode::Decode;
use rbdc_pg::value::{PgValueFormat, PgValueRef};
use rbs::Value;

/// Verify that common PG types return Value::Null when the underlying value is None.
#[test]
fn test_decode_null_for_types() {
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
        PgType::Float4,
        PgType::Float8,
        PgType::Unknown,
        PgType::Bpchar,
        PgType::Varchar,
        PgType::Date,
        PgType::Time,
        PgType::Timestamp,
        PgType::Timestamptz,
        PgType::Uuid,
        PgType::Jsonb,
        PgType::Numeric,
        PgType::Void,
    ];
    for ty in types {
        let val = PgValueRef {
            value: None,
            type_info: PgTypeInfo(ty.clone()),
            format: PgValueFormat::Text,
            timezone_sec: None,
        };
        let result = <Value as Decode>::decode(val).unwrap();
        assert_eq!(result, Value::Null, "expected Null for PgType::{ty:?}");
    }
}
