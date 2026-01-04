use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValue, PgValueFormat};
use rbdc::Error;
use rbs::Value;

/// PostgreSQL TSVECTOR type for full-text search
///
/// TSVECTOR stores a sorted list of distinct lexemes with their positions and weights.
/// In most cases, you'll use this as TEXT and let PostgreSQL handle the full-text search.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TsVector(pub String);

impl From<String> for TsVector {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TsVector {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<TsVector> for Value {
    fn from(arg: TsVector) -> Self {
        rbs::Value::Ext("tsvector", Box::new(rbs::Value::String(arg.0)))
    }
}

impl std::fmt::Display for TsVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Decode for TsVector {
    fn decode(value: PgValue) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                // TSVECTOR binary format is complex
                // For now, treat it as TEXT and return the raw bytes as a string
                // Applications should use TEXT format or PostgreSQL functions
                let bytes = value.as_bytes()?;
                Self(String::from_utf8_lossy(bytes).to_string())
            }
            PgValueFormat::Text => {
                let s = value.as_str()?;
                Self(s.to_string())
            }
        })
    }
}

impl Encode for TsVector {
    fn encode(self, _buf: &mut crate::arguments::PgArgumentBuffer) -> Result<IsNull, Error> {
        // TSVECTOR encoding is complex
        // Applications should use PostgreSQL's to_tsvector() function instead
        Err(Error::from(
            "TsVector encoding not supported. Use PostgreSQL's to_tsvector() function in your query instead."
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decode::Decode;
    use crate::value::{PgValue, PgValueFormat};

    #[test]
    fn test_display() {
        let tsvector = TsVector("'a' 'b' 'c'".to_string());
        assert_eq!(format!("{}", tsvector), "'a' 'b' 'c'");
    }

    #[test]
    fn test_from_string() {
        let tsvector = TsVector::from("hello world");
        assert_eq!(tsvector.0, "hello world");
    }

    #[test]
    fn test_decode_text() {
        let s = "'hello' 'world':1,3";
        let result: TsVector = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::UNKNOWN,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(result.0, "'hello' 'world':1,3");
    }

    #[test]
    fn test_from_value() {
        let tsvector = TsVector("test".to_string());
        let value: rbs::Value = tsvector.into();
        match value {
            rbs::Value::Ext(type_name, boxed) => {
                assert_eq!(type_name, "tsvector");
                if let rbs::Value::String(s) = *boxed {
                    assert_eq!(s, "test");
                } else {
                    panic!("Expected String value");
                }
            }
            _ => panic!("Expected Ext variant"),
        }
    }
}
