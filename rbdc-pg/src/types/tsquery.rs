use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValue, PgValueFormat};
use rbdc::Error;
use rbs::Value;

/// PostgreSQL TSQUERY type for full-text search queries
///
/// TSQUERY stores a lexeme search query for full-text search.
/// In most cases, you'll use this as TEXT and let PostgreSQL handle the full-text search.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TsQuery(pub String);

impl From<String> for TsQuery {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TsQuery {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<TsQuery> for Value {
    fn from(arg: TsQuery) -> Self {
        rbs::Value::Ext("tsquery", Box::new(rbs::Value::String(arg.0)))
    }
}

impl std::fmt::Display for TsQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Decode for TsQuery {
    fn decode(value: PgValue) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                // TSQUERY binary format is complex
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

impl Encode for TsQuery {
    fn encode(self, _buf: &mut crate::arguments::PgArgumentBuffer) -> Result<IsNull, Error> {
        // TSQUERY encoding is complex
        // Applications should use PostgreSQL's to_tsquery() or plainto_tsquery() functions instead
        Err(Error::from(
            "TsQuery encoding not supported. Use PostgreSQL's to_tsquery() function in your query instead."
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
        let tsquery = TsQuery("hello & world".to_string());
        assert_eq!(format!("{}", tsquery), "hello & world");
    }

    #[test]
    fn test_from_str() {
        let tsquery = TsQuery::from("search & query");
        assert_eq!(tsquery.0, "search & query");
    }

    #[test]
    fn test_decode_text() {
        let s = "hello & world";
        let result: TsQuery = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::UNKNOWN,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(result.0, "hello & world");
    }

    #[test]
    fn test_from_value() {
        let tsquery = TsQuery("test".to_string());
        let value: rbs::Value = tsquery.into();
        match value {
            rbs::Value::Ext(type_name, boxed) => {
                assert_eq!(type_name, "tsquery");
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
