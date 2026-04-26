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
        Value::Array(arr) => Box::new(serde_json::to_string(arr).unwrap_or_default()),
        Value::Map(_) => Box::new(serde_json::to_string(v).unwrap_or_default()),
        Value::Ext(type_name, box_value) => match *type_name {
            "Date" => {
                if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            "Time" => {
                if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            "DateTime" | "Datetime" => {
                if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            "Timestamp" => {
                // DuckDB expects timestamp as string in format "YYYY-MM-DD HH:MM:SS[.f]"
                if let Value::I64(n) = &**box_value {
                    Box::new(*n)
                } else if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            "Decimal" => {
                if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            "Json" => {
                if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            "Uuid" => {
                if let Value::String(s) = &**box_value {
                    Box::new(s.clone())
                } else {
                    Box::new(v_to_string(v))
                }
            }
            _ => Box::new(serde_json::to_string(v).unwrap_or_default()),
        },
    }
}

/// Extract row values from DuckDB row to rbs::Value
pub fn extract_row_values(row: &duckdb::Row) -> Vec<Value> {
    let mut values = Vec::new();
    // DuckDB doesn't provide column count from row directly
    // We try up to 64 columns which is enough for any practical use
    for i in 0..64 {
        // DuckDB type precedence: NULL, INTEGER, VARCHAR, REAL, BLOB
        // Try in order that avoids truncation: NULL first, then exact types, then strings

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

/// Convert rbs::Value to String
fn v_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::I64(n) => n.to_string(),
        Value::I32(n) => n.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}
