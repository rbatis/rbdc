//! Value conversions between rbs::Value and DuckDB.

use crate::types::DuckDbParam;
use rbs::Value;

/// Convert rbs::Value to DuckDB parameter
pub fn value_to_param(v: &Value) -> DuckDbParam {
    match v {
        Value::Null => Box::new(Option::<i32>::None),
        Value::Bool(b) => Box::new(*b),
        Value::I32(n) => Box::new(*n),
        Value::I64(n) => Box::new(*n),
        Value::U32(n) => Box::new(*n as i64),
        Value::U64(n) => Box::new(*n as i64),
        Value::F32(f) => Box::new(*f as f64),
        Value::F64(f) => Box::new(*f),
        Value::String(s) => Box::new(s.clone()),
        Value::Binary(b) => Box::new(b.clone()),
        Value::Array(_) | Value::Map(_) | Value::Ext(_, _) => {
            Box::new(serde_json::to_string(v).unwrap_or_default())
        }
    }
}

/// Extract row values from DuckDB row to rbs::Value
pub fn extract_row_values(row: &duckdb::Row) -> Vec<Value> {
    let mut values = Vec::new();
    let mut i: usize = 0;
    loop {
        // Try i64
        if let Ok(v) = row.get::<usize, i64>(i) {
            values.push(Value::I64(v));
            i += 1;
            continue;
        }
        // Try String
        if let Ok(v) = row.get::<usize, String>(i) {
            values.push(Value::String(v));
            i += 1;
            continue;
        }
        // Try f64
        if let Ok(v) = row.get::<usize, f64>(i) {
            values.push(Value::F64(v));
            i += 1;
            continue;
        }
        // Try bool
        if let Ok(v) = row.get::<usize, bool>(i) {
            values.push(Value::Bool(v));
            i += 1;
            continue;
        }
        // Try Vec<u8>
        if let Ok(v) = row.get::<usize, Vec<u8>>(i) {
            values.push(Value::Binary(v));
            i += 1;
            continue;
        }
        // If all fail, break
        break;
    }
    values
}
