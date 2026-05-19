use rbdc_mssql::decode::Decode;
use rbs::Value;
use tiberius::ColumnData;

/// Verify that every ColumnData None variant produces Value::Null.
#[test]
fn test_decode_null_for_column_data_types() {
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::U8(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::I16(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::I32(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::I64(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::F32(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::F64(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Bit(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::String(None)).unwrap(),
        Value::Null
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Binary(None)).unwrap(),
        Value::Null
    );
}
