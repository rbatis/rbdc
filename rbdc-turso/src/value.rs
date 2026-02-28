//! Value type for the Turso adapter.
//!
//! Provides `TursoValue` wrapping the underlying database value with
//! type metadata, and conversion functions to/from `rbs::Value`.
//!
//! Full encode/decode implementation is delivered in WP03.

use rbs::Value;

/// Data type classification for Turso values, corresponding to the
/// five SQLite storage classes: NULL, INTEGER, REAL, TEXT, BLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TursoDataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

impl TursoDataType {
    /// Canonical SQL type name.
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

impl std::fmt::Display for TursoDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A Turso value with associated type metadata.
///
/// Wraps the underlying database value and tracks the data type
/// (either inferred from the value itself or from column metadata).
#[derive(Debug, Clone)]
pub struct TursoValue {
    pub(crate) inner: libsql::Value,
    pub(crate) data_type: TursoDataType,
}

impl TursoValue {
    /// Create from a `libsql::Value`, inferring type.
    pub fn new(value: libsql::Value) -> Self {
        let data_type = match &value {
            libsql::Value::Null => TursoDataType::Null,
            libsql::Value::Integer(_) => TursoDataType::Integer,
            libsql::Value::Real(_) => TursoDataType::Real,
            libsql::Value::Text(_) => TursoDataType::Text,
            libsql::Value::Blob(_) => TursoDataType::Blob,
        };
        Self {
            inner: value,
            data_type,
        }
    }

    /// Returns the data type.
    pub fn data_type(&self) -> TursoDataType {
        self.data_type
    }

    /// Returns `true` if this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self.inner, libsql::Value::Null)
    }
}

/// Convert a `libsql::Value` to an `rbs::Value`.
///
/// Basic conversion without JSON detection. Full parity conversion
/// is delivered in WP03.
pub fn libsql_to_value(v: libsql::Value) -> Value {
    match v {
        libsql::Value::Null => Value::Null,
        libsql::Value::Integer(n) => Value::I64(n),
        libsql::Value::Real(f) => Value::F64(f),
        libsql::Value::Text(s) => Value::String(s),
        libsql::Value::Blob(b) => Value::Binary(b),
    }
}

/// Convert an `rbs::Value` parameter to a `libsql::Value` for binding.
///
/// Basic conversion. Full parity encoding is delivered in WP03.
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
        _ => Ok(libsql::Value::Text(v.to_string())),
    }
}
