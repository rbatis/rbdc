//! String/Text type handling for Turso values.
//!
//! Text values in libsql map to `libsql::Value::Text(String)` and are
//! decoded to `rbs::Value::String`, with special handling for JSON-shaped
//! strings (matching the SQLite adapter's behavior).

use crate::value::is_json_string;
use rbs::Value;

/// Decode a text value from a libsql text string.
///
/// If the string looks like JSON (object, array, or "null"), it is parsed
/// as JSON and returned as the corresponding `rbs::Value` variant. Otherwise,
/// it is returned as `Value::String`.
///
/// This matches the SQLite adapter's `Decode for Value` behavior for
/// `DataType::Text`.
pub fn decode_text(s: &str) -> Value {
    if is_json_string(s) {
        if let Ok(v) = serde_json::from_str::<Value>(s) {
            return v;
        }
    }
    Value::String(s.to_string())
}

/// Encode a string to a libsql text value.
#[inline]
pub fn encode_text(s: String) -> libsql::Value {
    libsql::Value::Text(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_plain_text() {
        assert_eq!(decode_text("hello"), Value::String("hello".to_string()));
    }

    #[test]
    fn test_decode_empty_string() {
        assert_eq!(decode_text(""), Value::String(String::new()));
    }

    #[test]
    fn test_decode_json_object() {
        let result = decode_text(r#"{"key":"value"}"#);
        assert!(matches!(result, Value::Map(_)));
    }

    #[test]
    fn test_decode_json_array() {
        let result = decode_text("[1,2,3]");
        assert!(matches!(result, Value::Array(_)));
    }

    #[test]
    fn test_decode_json_null() {
        let result = decode_text("null");
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_decode_invalid_json_stays_string() {
        // Starts with { but is not valid JSON
        let result = decode_text("{not valid json}");
        assert_eq!(result, Value::String("{not valid json}".to_string()));
    }

    #[test]
    fn test_encode_text() {
        assert!(matches!(
            encode_text("hello".to_string()),
            libsql::Value::Text(s) if s == "hello"
        ));
    }
}
