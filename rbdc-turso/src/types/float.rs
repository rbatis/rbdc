//! Float type handling for Turso values.
//!
//! Both f32 and f64 map to `libsql::Value::Real(f64)` on the wire,
//! matching SQLite's REAL storage class. The SQLite adapter decodes
//! floats as `Value::F64`; we maintain this behavior.

use rbs::Value;

/// Decode a float from a libsql real value.
///
/// Returns `Value::F64`, matching the SQLite adapter's decode behavior.
#[inline]
pub fn decode_real(f: f64) -> Value {
    Value::F64(f)
}

/// Encode an f32 to a libsql real value.
///
/// Widened to f64, matching SQLite's storage.
#[inline]
pub fn encode_f32(f: f32) -> libsql::Value {
    libsql::Value::Real(f as f64)
}

/// Encode an f64 to a libsql real value.
#[inline]
pub fn encode_f64(f: f64) -> libsql::Value {
    libsql::Value::Real(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_real() {
        assert_eq!(decode_real(3.14), Value::F64(3.14));
    }

    #[test]
    fn test_decode_zero() {
        assert_eq!(decode_real(0.0), Value::F64(0.0));
    }

    #[test]
    fn test_decode_negative() {
        assert_eq!(decode_real(-1.5), Value::F64(-1.5));
    }

    #[test]
    fn test_encode_f32() {
        if let libsql::Value::Real(f) = encode_f32(1.5f32) {
            assert!((f - 1.5).abs() < f64::EPSILON);
        } else {
            panic!("expected Real");
        }
    }

    #[test]
    fn test_encode_f64() {
        if let libsql::Value::Real(f) = encode_f64(2.718281828) {
            assert!((f - 2.718281828).abs() < f64::EPSILON);
        } else {
            panic!("expected Real");
        }
    }
}
