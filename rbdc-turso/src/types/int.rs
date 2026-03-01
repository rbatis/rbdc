//! Integer type handling for Turso values.
//!
//! All integer types (i8, i16, i32, i64, u8, u16, u32, u64) map to
//! `libsql::Value::Integer(i64)` on the wire, matching SQLite's storage.
//!
//! The SQLite adapter decodes integers as `Value::I64` (from DataType::Int
//! and DataType::Int64 both). We maintain this behavior.

use rbs::Value;

/// Decode an integer from a libsql integer value.
///
/// Returns `Value::I64`, matching the SQLite adapter's decode behavior
/// where both `DataType::Int` and `DataType::Int64` produce `Value::I64`.
#[inline]
pub fn decode_integer(n: i64) -> Value {
    Value::I64(n)
}

/// Encode an i32 to a libsql integer value.
#[inline]
pub fn encode_i32(n: i32) -> libsql::Value {
    libsql::Value::Integer(n as i64)
}

/// Encode an i64 to a libsql integer value.
#[inline]
pub fn encode_i64(n: i64) -> libsql::Value {
    libsql::Value::Integer(n)
}

/// Encode a u32 to a libsql integer value.
#[inline]
pub fn encode_u32(n: u32) -> libsql::Value {
    libsql::Value::Integer(n as i64)
}

/// Encode a u64 to a libsql integer value.
///
/// Note: values above `i64::MAX` will be truncated. This matches the
/// SQLite adapter behavior where `(v as i64).encode(args)` is used.
#[inline]
pub fn encode_u64(n: u64) -> libsql::Value {
    libsql::Value::Integer(n as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_positive() {
        assert_eq!(decode_integer(42), Value::I64(42));
    }

    #[test]
    fn test_decode_negative() {
        assert_eq!(decode_integer(-1), Value::I64(-1));
    }

    #[test]
    fn test_decode_zero() {
        assert_eq!(decode_integer(0), Value::I64(0));
    }

    #[test]
    fn test_decode_i64_max() {
        assert_eq!(decode_integer(i64::MAX), Value::I64(i64::MAX));
    }

    #[test]
    fn test_decode_i64_min() {
        assert_eq!(decode_integer(i64::MIN), Value::I64(i64::MIN));
    }

    #[test]
    fn test_encode_i32() {
        assert!(matches!(encode_i32(42), libsql::Value::Integer(42)));
    }

    #[test]
    fn test_encode_i64() {
        assert!(matches!(encode_i64(i64::MAX), libsql::Value::Integer(n) if n == i64::MAX));
    }

    #[test]
    fn test_encode_u32() {
        assert!(matches!(encode_u32(100), libsql::Value::Integer(100)));
    }

    #[test]
    fn test_encode_u64() {
        assert!(matches!(encode_u64(200), libsql::Value::Integer(200)));
    }
}
