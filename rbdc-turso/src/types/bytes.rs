//! Blob/Bytes type handling for Turso values.
//!
//! Blob values in libsql map to `libsql::Value::Blob(Vec<u8>)` and are
//! decoded to `rbs::Value::Binary(Vec<u8>)`, matching the SQLite adapter.

use rbs::Value;

/// Decode a blob from a libsql blob value.
///
/// Returns `Value::Binary`, matching the SQLite adapter's decode behavior.
#[inline]
pub fn decode_blob(bytes: Vec<u8>) -> Value {
    Value::Binary(bytes)
}

/// Encode bytes to a libsql blob value.
#[inline]
pub fn encode_blob(bytes: Vec<u8>) -> libsql::Value {
    libsql::Value::Blob(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_blob() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(decode_blob(data.clone()), Value::Binary(data));
    }

    #[test]
    fn test_decode_empty_blob() {
        assert_eq!(decode_blob(vec![]), Value::Binary(vec![]));
    }

    #[test]
    fn test_encode_blob() {
        let data = vec![1, 2, 3];
        assert!(matches!(
            encode_blob(data.clone()),
            libsql::Value::Blob(b) if b == data
        ));
    }
}
