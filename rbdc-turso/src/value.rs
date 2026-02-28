//! Value type for the Turso adapter.
//!
//! Wraps `libsql::Value` with type metadata and provides conversions
//! to/from `rbs::Value` matching the SQLite adapter's public behavior.

use rbs::Value;

/// Data type classification for Turso values.
///
/// Thin wrapper around `libsql::ValueType` that adds the display/name
/// methods needed by `MetaData::column_type()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TursoDataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

impl TursoDataType {
    /// Canonical SQL type name, matching SQLite adapter conventions.
    pub fn name(&self) -> &'static str {
        match self {
            TursoDataType::Null => "NULL",
            TursoDataType::Integer => "INTEGER",
            TursoDataType::Real => "REAL",
            TursoDataType::Text => "TEXT",
            TursoDataType::Blob => "BLOB",
        }
    }
}

impl From<libsql::ValueType> for TursoDataType {
    fn from(vt: libsql::ValueType) -> Self {
        match vt {
            libsql::ValueType::Null => TursoDataType::Null,
            libsql::ValueType::Integer => TursoDataType::Integer,
            libsql::ValueType::Real => TursoDataType::Real,
            libsql::ValueType::Text => TursoDataType::Text,
            libsql::ValueType::Blob => TursoDataType::Blob,
        }
    }
}

impl From<&libsql::Value> for TursoDataType {
    fn from(v: &libsql::Value) -> Self {
        match v {
            libsql::Value::Null => TursoDataType::Null,
            libsql::Value::Integer(_) => TursoDataType::Integer,
            libsql::Value::Real(_) => TursoDataType::Real,
            libsql::Value::Text(_) => TursoDataType::Text,
            libsql::Value::Blob(_) => TursoDataType::Blob,
        }
    }
}

impl std::fmt::Display for TursoDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A Turso value with associated type metadata.
#[derive(Debug, Clone)]
pub struct TursoValue {
    pub(crate) inner: libsql::Value,
    pub(crate) data_type: TursoDataType,
}

impl TursoValue {
    /// Create from a `libsql::Value`, inferring type from the value itself.
    pub fn new(value: libsql::Value) -> Self {
        let data_type = TursoDataType::from(&value);
        Self {
            inner: value,
            data_type,
        }
    }

    /// Create with an explicit data type (e.g. from column metadata).
    pub fn with_type(value: libsql::Value, data_type: TursoDataType) -> Self {
        Self {
            inner: value,
            data_type,
        }
    }

    pub fn data_type(&self) -> TursoDataType {
        self.data_type
    }

    pub fn is_null(&self) -> bool {
        matches!(self.inner, libsql::Value::Null)
    }

    pub fn as_integer(&self) -> Option<i64> {
        match &self.inner {
            libsql::Value::Integer(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_real(&self) -> Option<f64> {
        match &self.inner {
            libsql::Value::Real(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match &self.inner {
            libsql::Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match &self.inner {
            libsql::Value::Blob(b) => Some(b.as_slice()),
            _ => None,
        }
    }
}

/// Convert a `TursoValue` to `rbs::Value`.
///
/// Matches the SQLite adapter's `Decode for Value` behavior:
/// - Null → `Value::Null`
/// - Integer → `Value::I64`
/// - Real → `Value::F64`
/// - Text → `Value::String` (or deserialized JSON for JSON-shaped strings)
/// - Blob → `Value::Binary`
pub fn turso_value_to_rbs(tv: &TursoValue) -> Value {
    match &tv.inner {
        libsql::Value::Null => Value::Null,
        libsql::Value::Integer(n) => Value::I64(*n),
        libsql::Value::Real(f) => Value::F64(*f),
        libsql::Value::Text(s) => {
            if is_json_string(s) {
                if let Ok(v) = serde_json::from_str::<Value>(s) {
                    v
                } else {
                    Value::String(s.clone())
                }
            } else {
                Value::String(s.clone())
            }
        }
        libsql::Value::Blob(b) => Value::Binary(b.clone()),
    }
}

/// Convert `libsql::Value` directly to `rbs::Value` (convenience wrapper).
pub fn libsql_to_value(v: libsql::Value) -> Value {
    turso_value_to_rbs(&TursoValue::new(v))
}

/// Convert `rbs::Value` to `libsql::Value` for parameter binding.
///
/// Matches the SQLite adapter's `Encode for Value` behavior.
pub fn value_to_libsql(v: &Value) -> Result<libsql::Value, rbdc::Error> {
    match v {
        Value::Null => Ok(libsql::Value::Null),
        Value::Bool(b) => Ok(libsql::Value::Integer(if *b { 1 } else { 0 })),
        Value::I32(n) => Ok(libsql::Value::Integer(*n as i64)),
        Value::I64(n) => Ok(libsql::Value::Integer(*n)),
        Value::U32(n) => Ok(libsql::Value::Integer(*n as i64)),
        Value::U64(n) => Ok(libsql::Value::Integer(*n as i64)),
        Value::F32(f) => Ok(libsql::Value::Real(*f as f64)),
        Value::F64(f) => Ok(libsql::Value::Real(*f)),
        Value::String(s) => Ok(libsql::Value::Text(s.clone())),
        Value::Binary(b) => Ok(libsql::Value::Blob(b.clone())),
        Value::Ext(type_tag, val) => match &**type_tag {
            "Date" | "DateTime" | "Time" | "Decimal" | "Uuid" => Ok(libsql::Value::Text(
                val.as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| val.to_string()),
            )),
            "Timestamp" => Ok(libsql::Value::Integer(val.as_i64().unwrap_or_default())),
            "Json" => match val.as_ref() {
                Value::Binary(b) => Ok(libsql::Value::Blob(b.clone())),
                _ => Ok(libsql::Value::Blob(val.to_string().into_bytes())),
            },
            _ => match val.as_ref() {
                Value::String(s) => Ok(libsql::Value::Text(s.clone())),
                Value::I64(n) => Ok(libsql::Value::Integer(*n)),
                Value::U64(n) => Ok(libsql::Value::Integer(*n as i64)),
                Value::F64(f) => Ok(libsql::Value::Real(*f)),
                _ => Ok(libsql::Value::Text(val.to_string())),
            },
        },
        Value::Array(_) | Value::Map(_) => Ok(libsql::Value::Text(
            serde_json::to_string(v).unwrap_or_default(),
        )),
    }
}

/// Check if a string looks like JSON (null, object, or array).
///
/// Same heuristic as the SQLite adapter's `is_json_string`.
pub fn is_json_string(s: &str) -> bool {
    s == "null"
        || (s.starts_with('{') && s.ends_with('}'))
        || (s.starts_with('[') && s.ends_with(']'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_roundtrip() {
        let tv = TursoValue::new(libsql::Value::Null);
        assert!(tv.is_null());
        assert_eq!(tv.data_type(), TursoDataType::Null);
        assert_eq!(turso_value_to_rbs(&tv), Value::Null);
    }

    #[test]
    fn test_integer_roundtrip() {
        let tv = TursoValue::new(libsql::Value::Integer(42));
        assert_eq!(tv.data_type(), TursoDataType::Integer);
        assert_eq!(turso_value_to_rbs(&tv), Value::I64(42));
    }

    #[test]
    fn test_i64_extremes() {
        assert_eq!(
            turso_value_to_rbs(&TursoValue::new(libsql::Value::Integer(i64::MAX))),
            Value::I64(i64::MAX)
        );
        assert_eq!(
            turso_value_to_rbs(&TursoValue::new(libsql::Value::Integer(i64::MIN))),
            Value::I64(i64::MIN)
        );
    }

    #[test]
    fn test_real_roundtrip() {
        let tv = TursoValue::new(libsql::Value::Real(3.14));
        assert_eq!(tv.data_type(), TursoDataType::Real);
        assert_eq!(turso_value_to_rbs(&tv), Value::F64(3.14));
    }

    #[test]
    fn test_text_plain() {
        let tv = TursoValue::new(libsql::Value::Text("hello".into()));
        assert_eq!(turso_value_to_rbs(&tv), Value::String("hello".into()));
    }

    #[test]
    fn test_text_json_object() {
        let tv = TursoValue::new(libsql::Value::Text(r#"{"key":"value"}"#.into()));
        assert!(matches!(turso_value_to_rbs(&tv), Value::Map(_)));
    }

    #[test]
    fn test_text_json_array() {
        let tv = TursoValue::new(libsql::Value::Text("[1,2,3]".into()));
        assert!(matches!(turso_value_to_rbs(&tv), Value::Array(_)));
    }

    #[test]
    fn test_text_json_null() {
        let tv = TursoValue::new(libsql::Value::Text("null".into()));
        assert_eq!(turso_value_to_rbs(&tv), Value::Null);
    }

    #[test]
    fn test_blob_roundtrip() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let tv = TursoValue::new(libsql::Value::Blob(data.clone()));
        assert_eq!(turso_value_to_rbs(&tv), Value::Binary(data));
    }

    #[test]
    fn test_data_type_from_value_type() {
        assert_eq!(
            TursoDataType::from(libsql::ValueType::Integer),
            TursoDataType::Integer
        );
        assert_eq!(
            TursoDataType::from(libsql::ValueType::Real),
            TursoDataType::Real
        );
        assert_eq!(
            TursoDataType::from(libsql::ValueType::Text),
            TursoDataType::Text
        );
        assert_eq!(
            TursoDataType::from(libsql::ValueType::Blob),
            TursoDataType::Blob
        );
        assert_eq!(
            TursoDataType::from(libsql::ValueType::Null),
            TursoDataType::Null
        );
    }

    #[test]
    fn test_value_to_libsql_basics() {
        assert!(matches!(
            value_to_libsql(&Value::Null).unwrap(),
            libsql::Value::Null
        ));
        assert!(matches!(
            value_to_libsql(&Value::Bool(true)).unwrap(),
            libsql::Value::Integer(1)
        ));
        assert!(matches!(
            value_to_libsql(&Value::Bool(false)).unwrap(),
            libsql::Value::Integer(0)
        ));
        assert!(matches!(
            value_to_libsql(&Value::I64(100)).unwrap(),
            libsql::Value::Integer(100)
        ));
    }

    #[test]
    fn test_is_json_string_checks() {
        assert!(is_json_string("null"));
        assert!(is_json_string(r#"{"a":1}"#));
        assert!(is_json_string("[1,2]"));
        assert!(!is_json_string("hello"));
        assert!(!is_json_string(""));
    }
}
