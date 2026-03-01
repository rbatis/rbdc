//! Boolean type handling for Turso values.
//!
//! SQLite/libsql stores booleans as INTEGER (0 = false, 1 = true).
//! The SQLite adapter encodes booleans as `SqliteArgumentValue::Int(i32::from(b))`
//! and decodes via `value.int() != 0`.
//!
//! For Turso, we use `libsql::Value::Integer` with the same 0/1 convention.

use rbs::Value;

/// Decode a boolean from a libsql integer value.
///
/// Follows the SQLite adapter convention: any non-zero integer is `true`.
#[inline]
pub fn decode_bool(n: i64) -> Value {
    Value::Bool(n != 0)
}

/// Encode a boolean to a libsql integer value.
///
/// `true` → `Integer(1)`, `false` → `Integer(0)`.
#[inline]
pub fn encode_bool(b: bool) -> libsql::Value {
    libsql::Value::Integer(if b { 1 } else { 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_true() {
        assert_eq!(decode_bool(1), Value::Bool(true));
    }

    #[test]
    fn test_decode_false() {
        assert_eq!(decode_bool(0), Value::Bool(false));
    }

    #[test]
    fn test_decode_nonzero_is_true() {
        assert_eq!(decode_bool(42), Value::Bool(true));
        assert_eq!(decode_bool(-1), Value::Bool(true));
    }

    #[test]
    fn test_encode_true() {
        assert!(matches!(encode_bool(true), libsql::Value::Integer(1)));
    }

    #[test]
    fn test_encode_false() {
        assert!(matches!(encode_bool(false), libsql::Value::Integer(0)));
    }
}
