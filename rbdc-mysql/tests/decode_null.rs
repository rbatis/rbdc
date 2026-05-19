use rbdc_mysql::protocol::text::ColumnType;
use rbdc_mysql::result_set::MySqlTypeInfo;
use rbdc_mysql::types::Decode;
use rbdc_mysql::value::{MySqlValueFormat, MySqlValueRef};
use rbdc_mysql::options::MySqlConnectOptions;
use rbs::Value;
use std::sync::Arc;

/// Verify that every column type returns Value::Null when the underlying value is None.
#[test]
fn test_decode_null_for_all_types() {
    let option = Arc::new(MySqlConnectOptions::new());
    let types = [
        ColumnType::Decimal,
        ColumnType::Tiny,
        ColumnType::Short,
        ColumnType::Long,
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::Null,
        ColumnType::Timestamp,
        ColumnType::LongLong,
        ColumnType::Int24,
        ColumnType::Date,
        ColumnType::Time,
        ColumnType::Datetime,
        ColumnType::Year,
        ColumnType::VarChar,
        ColumnType::Bit,
        ColumnType::Json,
        ColumnType::NewDecimal,
        ColumnType::Enum,
        ColumnType::Set,
        ColumnType::TinyBlob,
        ColumnType::MediumBlob,
        ColumnType::LongBlob,
        ColumnType::Blob,
        ColumnType::VarString,
        ColumnType::String,
        ColumnType::Geometry,
    ];
    for ty in types {
        let val = MySqlValueRef {
            value: None,
            type_info: MySqlTypeInfo::from_type(ty),
            format: MySqlValueFormat::Text,
            option: option.clone(),
        };
        let result = <Value as Decode>::decode(val).unwrap();
        assert_eq!(result, Value::Null, "expected Null for ColumnType::{ty:?}");
    }
}

/// Sanity check: non-NULL VarChar still decodes correctly.
#[test]
fn test_decode_non_null_varchar() {
    let option = Arc::new(MySqlConnectOptions::new());
    let val = MySqlValueRef {
        value: Some(b"hello".as_slice()),
        type_info: MySqlTypeInfo::from_type(ColumnType::VarChar),
        format: MySqlValueFormat::Text,
        option,
    };
    let result = <Value as Decode>::decode(val).unwrap();
    assert_eq!(result, Value::String("hello".to_string()));
}
