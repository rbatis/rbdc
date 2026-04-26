//! Value conversions between rbs::Value and DuckDB.

use crate::types::DuckDbParam;
use rbs::Value;

/// Convert rbs::Value to DuckDB parameter (takes ownership)
pub fn value_to_param(v: Value) -> DuckDbParam {
    match v {
        Value::Null => Box::new(Option::<i32>::None),
        Value::Bool(b) => Box::new(b),
        Value::I32(n) => Box::new(n),
        Value::I64(n) => Box::new(n),
        Value::U32(n) => Box::new(n as i64),
        Value::U64(n) => Box::new(n as i64),
        Value::F32(f) => Box::new(f as f64),
        Value::F64(f) => Box::new(f),
        Value::String(s) => Box::new(s),
        Value::Binary(b) => Box::new(b),
        Value::Array(arr) => Box::new(serde_json::to_string(&arr).unwrap_or_default()),
        Value::Map(m) => Box::new(serde_json::to_string(&m).unwrap_or_default()),
        Value::Ext(type_name, box_value) => match type_name {
            "Date" => {
                Box::new(box_value.into_string().unwrap_or_default())
            }
            "Time" => {
                Box::new(box_value.into_string().unwrap_or_default())
            }
            "DateTime" | "Datetime" => {
                Box::new(box_value.into_string().unwrap_or_default())
            }
            "Timestamp" => {
                // Timestamp can be i64 or string
                if let Some(n) = (*box_value).as_i64() {
                    Box::new(n)
                } else {
                    Box::new(box_value.into_string().unwrap_or_default())
                }
            }
            "Decimal" => {
                Box::new(box_value.into_string().unwrap_or_default())
            }
            "Json" => {
                Box::new(box_value.into_string().unwrap_or_default())
            }
            "Uuid" => {
                Box::new(box_value.into_string().unwrap_or_default())
            }
            _ => Box::new(serde_json::to_string(&Value::Ext(type_name, box_value)).unwrap_or_default()),
        },
    }
}

/// Extract row values from DuckDB row to rbs::Value
pub fn extract_row_values(row: &duckdb::Row) -> Vec<Value> {
    let mut values = Vec::new();
    // DuckDB doesn't provide column count from row directly
    // We try up to 64 columns which is enough for any practical use
    for i in 0..64 {
        // Try f64 first for DOUBLE/REAL (avoid integer truncation of floats)
        if let Ok(v) = row.get::<usize, f64>(i) {
            values.push(Value::F64(v));
            continue;
        }
        // Try i64 for INTEGER/BIGINT
        if let Ok(v) = row.get::<usize, i64>(i) {
            values.push(Value::I64(v));
            continue;
        }
        // Try i32 for smaller integers
        if let Ok(v) = row.get::<usize, i32>(i) {
            values.push(Value::I32(v));
            continue;
        }
        // Try String (includes VARCHAR, but also dates/times as strings)
        if let Ok(v) = row.get::<usize, String>(i) {
            values.push(Value::String(v));
            continue;
        }
        // Try bool
        if let Ok(v) = row.get::<usize, bool>(i) {
            values.push(Value::Bool(v));
            continue;
        }
        // Try Vec<u8> for BLOB
        if let Ok(v) = row.get::<usize, Vec<u8>>(i) {
            values.push(Value::Binary(v));
            continue;
        }
        // Try i16
        if let Ok(v) = row.get::<usize, i16>(i) {
            values.push(Value::I32(v as i32));
            continue;
        }
        // Try i8
        if let Ok(v) = row.get::<usize, i8>(i) {
            values.push(Value::I32(v as i32));
            continue;
        }
        // If we get here, all types failed for this column index
        // This means we've gone past the last column
        break;
    }
    values
}
