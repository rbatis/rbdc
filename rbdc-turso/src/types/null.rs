//! Null type handling for Turso values.
//!
//! Null values in libsql map directly to `libsql::Value::Null` and
//! convert to `rbs::Value::Null`, matching SQLite adapter behavior.

use rbs::Value;

/// Convert a null `libsql::Value` to `rbs::Value`.
///
/// Always returns `Value::Null`. This exists for completeness and
/// to make the type conversion dispatch explicit.
#[inline]
pub fn decode_null() -> Value {
    Value::Null
}

/// Encode `Value::Null` to `libsql::Value::Null`.
#[inline]
pub fn encode_null() -> libsql::Value {
    libsql::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_null() {
        assert_eq!(decode_null(), Value::Null);
    }

    #[test]
    fn test_encode_null() {
        assert!(matches!(encode_null(), libsql::Value::Null));
    }
}
