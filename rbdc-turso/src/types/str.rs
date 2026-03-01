//! String/Text type handling for Turso values.
//!
//! Text values in libsql map to `libsql::Value::Text(String)` and are
//! decoded to `rbs::Value::String`, with special handling for JSON-shaped
//! strings (matching the SQLite adapter's behavior).

use crate::value::is_json_string;
use rbs::Value;

/// Decode a text value from a libsql text string.
///
/// When `json_detect` is `true`, strings that look like JSON (object,
/// array, or "null") are parsed and returned as structured `Value` types.
/// When `false`, all text is returned as `Value::String`.
pub fn decode_text(s: &str, json_detect: bool) -> Value {
    if json_detect && is_json_string(s) {
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
        assert_eq!(
            decode_text("hello", false),
            Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_decode_empty_string() {
        assert_eq!(decode_text("", false), Value::String(String::new()));
    }

    #[test]
    fn test_decode_json_disabled() {
        // With json_detect=false, JSON-shaped text stays as String
        assert_eq!(
            decode_text(r#"{"key":"value"}"#, false),
            Value::String(r#"{"key":"value"}"#.to_string())
        );
        assert_eq!(
            decode_text("[1,2,3]", false),
            Value::String("[1,2,3]".to_string())
        );
        assert_eq!(
            decode_text("null", false),
            Value::String("null".to_string())
        );
    }

    #[test]
    fn test_decode_json_object_when_enabled() {
        let result = decode_text(r#"{"key":"value"}"#, true);
        assert!(matches!(result, Value::Map(_)));
    }

    #[test]
    fn test_decode_json_array_when_enabled() {
        let result = decode_text("[1,2,3]", true);
        assert!(matches!(result, Value::Array(_)));
    }

    #[test]
    fn test_decode_json_null_when_enabled() {
        let result = decode_text("null", true);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_decode_invalid_json_stays_string() {
        // Starts with { but is not valid JSON — falls back to String even with json_detect
        let result = decode_text("{not valid json}", true);
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
