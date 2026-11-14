//! Offline coverage for rows, metadata, statements/arguments and type info.

use std::collections::HashMap;
use std::sync::Arc;

use rbdc::db::{MetaData, Row as DbRow};
use rbdc::ext::ustr::UStr;
use rbdc_mysql::meta_data::MysqlMetaData;
use rbdc_mysql::options::MySqlConnectOptions;
use rbdc_mysql::protocol::text::ColumnType;
use rbdc_mysql::protocol::Row as ProtoRow;
use rbdc_mysql::result_set::{MySqlColumn, MySqlTypeInfo};
use rbdc_mysql::row::MySqlRow;
use rbdc_mysql::stmt::{MySqlArguments, MySqlStatement, MySqlStatementMetadata};
use rbdc_mysql::value::MySqlValueFormat;
use rbs::Value;

fn column(ordinal: usize, name: &str, ty: ColumnType) -> MySqlColumn {
    MySqlColumn {
        ordinal,
        name: UStr::new(name),
        type_info: MySqlTypeInfo::from_type(ty),
    }
}

// ---------------------------------------------------------------------------
// MySqlRow
// ---------------------------------------------------------------------------

#[test]
fn test_row_try_get_and_get() {
    let columns = Arc::new(vec![
        column(0, "id", ColumnType::Long),
        column(1, "name", ColumnType::VarChar),
    ]);
    let mut names = HashMap::new();
    names.insert(
        UStr::new("id"),
        (0usize, MySqlTypeInfo::from_type(ColumnType::Long)),
    );
    names.insert(
        UStr::new("name"),
        (1usize, MySqlTypeInfo::from_type(ColumnType::VarChar)),
    );

    let mut row = MySqlRow {
        row: ProtoRow {
            storage: vec![Some(b"123".to_vec()), None],
            values: vec![Some(0..3), None],
        },
        format: MySqlValueFormat::Text,
        columns: columns.clone(),
        column_names: Arc::new(names),
        option: Arc::new(MySqlConnectOptions::new()),
    };

    assert_eq!(row.columns().len(), 2);

    let value = row.try_get(0).unwrap();
    assert_eq!(value.value, Some(&b"123"[..]));
    assert_eq!(value.format, MySqlValueFormat::Text);
    assert_eq!(value.type_info.r#type, ColumnType::Long);

    assert_eq!(DbRow::get(&mut row, 0).unwrap(), Value::I64(123));
    // A NULL cell decodes to Value::Null.
    assert_eq!(DbRow::get(&mut row, 1).unwrap(), Value::Null);

    let meta = row.meta_data();
    assert_eq!(meta.column_len(), 2);
    assert_eq!(meta.column_name(0), "id");
    assert_eq!(meta.column_name(1), "name");
    assert_eq!(meta.column_name(99), "");
    assert_eq!(meta.column_type(0), "Long");
    assert_eq!(meta.column_type(1), "VarChar");
    assert_eq!(meta.column_type(99), "");
}

// ---------------------------------------------------------------------------
// MysqlMetaData
// ---------------------------------------------------------------------------

#[test]
fn test_metadata_direct() {
    let mut names = HashMap::new();
    names.insert(UStr::new("a"), (0usize, MySqlTypeInfo::from_type(ColumnType::Tiny)));
    names.insert(UStr::new("b"), (1usize, MySqlTypeInfo::from_type(ColumnType::Json)));
    let meta = MysqlMetaData {
        inner: Arc::new(names),
    };
    assert_eq!(meta.column_len(), 2);
    assert_eq!(meta.column_name(1), "b");
    assert_eq!(meta.column_type(0), "Tiny");
    // The Debug impl prints the inner map.
    assert!(!format!("{meta:?}").is_empty());
}

// ---------------------------------------------------------------------------
// Statements and arguments
// ---------------------------------------------------------------------------

#[test]
fn test_arguments_add_and_null_bitmap() {
    let mut args = MySqlArguments::default();
    assert_eq!(args.len(), 0);

    args.add(Value::Null).unwrap();
    assert_eq!(args.len(), 1);
    assert_eq!(args.null_bitmap, vec![0b0000_0001]);

    args.add(Value::I32(5)).unwrap();
    assert_eq!(args.len(), 2);
    assert_eq!(args.types[1].r#type, ColumnType::Long);

    // Add enough arguments to grow the null bitmap across byte boundaries.
    for _ in 0..10 {
        args.add(Value::Null).unwrap();
    }
    assert_eq!(args.len(), 12);
    assert_eq!(args.null_bitmap.len(), 2);
    // Bits 0 and 2..=7 are NULL in the first byte; bits 0..=3 in the second.
    assert_eq!(args.null_bitmap[0], 0b1111_1101);
    assert_eq!(args.null_bitmap[1], 0b0000_1111);
}

#[test]
fn test_arguments_from_args_and_encoding_errors() {
    let args = MySqlArguments::from_args(vec![
        Value::String("a".into()),
        Value::Binary(vec![1, 2]),
        Value::Bool(false),
    ])
    .unwrap();
    assert_eq!(args.len(), 3);

    // Encoding propagates errors from value encoding.
    let mut args = MySqlArguments::default();
    let err = args.add(Value::Ext(
        "Date",
        Box::new(Value::String("not-a-date".into())),
    ));
    assert!(err.is_err());
}

#[test]
fn test_statement_and_metadata_defaults() {
    let metadata = MySqlStatementMetadata::default();
    let statement = MySqlStatement {
        sql: "SELECT 1".to_string(),
        metadata,
    };
    assert_eq!(statement.sql, "SELECT 1");
    assert_eq!(format!("{:?}", statement.metadata), "MySqlStatementMetadata { columns: [], column_names: {}, parameters: 0 }");
}

// ---------------------------------------------------------------------------
// MySqlTypeInfo helpers
// ---------------------------------------------------------------------------

#[test]
fn test_type_info_helpers() {
    let binary = MySqlTypeInfo::binary(ColumnType::Blob);
    assert_eq!(binary.r#type, ColumnType::Blob);
    assert_eq!(binary.char_set, 63);

    let null = MySqlTypeInfo::null();
    assert_eq!(null.r#type, ColumnType::Null);
    assert_eq!(null.char_set, 63);

    let e = MySqlTypeInfo::__enum();
    assert_eq!(e.r#type, ColumnType::Enum);
    assert_eq!(e.char_set, 63);

    let t = MySqlTypeInfo::from_type(ColumnType::LongLong);
    assert_eq!(t.r#type, ColumnType::LongLong);

    let gate_cases = [
        (ColumnType::Date, Some("time")),
        (ColumnType::Time, Some("time")),
        (ColumnType::Timestamp, Some("time")),
        (ColumnType::DateTime, Some("time")),
        (ColumnType::Json, Some("json")),
        (ColumnType::NewDecimal, Some("bigdecimal")),
        (ColumnType::Long, None),
    ];
    for (ty, expected) in gate_cases {
        assert_eq!(MySqlTypeInfo::from_type(ty).__type_feature_gate(), expected);
    }
}

#[test]
fn test_type_info_and_column_serde_roundtrip() {
    let info = MySqlTypeInfo::from_type(ColumnType::VarChar);
    let json = serde_json::to_string(&info).unwrap();
    let back: MySqlTypeInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(back.r#type, ColumnType::VarChar);
    assert_eq!(back.char_set, info.char_set);

    let col = column(3, "c", ColumnType::Json);
    let json = serde_json::to_string(&col).unwrap();
    let back: MySqlColumn = serde_json::from_str(&json).unwrap();
    assert_eq!(back.ordinal, 3);
    assert_eq!(&*back.name, "c");
    assert_eq!(back.type_info.r#type, ColumnType::Json);
    assert!(format!("{col:?}").contains("Json"));
}
