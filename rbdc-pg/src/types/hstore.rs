use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValue, PgValueFormat};
use rbdc::Error;
use rbs::Value;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// PostgreSQL HStore type for key-value pairs
///
/// HStore is a PostgreSQL extension module that implements the hstore data type
/// for storing sets of key/value pairs within a single PostgreSQL value.
///
/// # Examples
///
/// ```ignore
/// // Create an hstore from a HashMap
/// let mut map = HashMap::new();
/// map.insert("name".to_string(), "John".to_string());
/// map.insert("age".to_string(), "30".to_string());
/// let hstore = Hstore(map);
///
/// // Text format representation: "name=>John, age=>30"
/// ```
///
/// This implementation supports both TEXT and BINARY formats:
/// - TEXT: "key1=>value1, key2=>value2"
/// - BINARY: 32-bit header + count + entries
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Hstore(pub HashMap<String, String>);

impl Default for Hstore {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl Display for Hstore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let pairs: Vec<String> = self.0
            .iter()
            .map(|(k, v)| format!("{}=>{}", k, v))
            .collect();
        write!(f, "{}", pairs.join(", "))
    }
}

impl From<HashMap<String, String>> for Hstore {
    fn from(map: HashMap<String, String>) -> Self {
        Self(map)
    }
}

impl From<Hstore> for Value {
    fn from(arg: Hstore) -> Self {
        // Store as string representation: "key1=>value1, key2=>value2"
        let s = format!("{}", arg);
        Value::Ext("hstore", Box::new(Value::String(s)))
    }
}

impl Decode for Hstore {
    fn decode(value: PgValue) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                // Binary format:
                // 4 bytes: number of entries (int32)
                // For each entry:
                //   4 bytes: key length
                //   key bytes
                //   4 bytes: value length (-1 for NULL)
                //   value bytes (if not NULL)
                let bytes = value.as_bytes()?;
                if bytes.len() < 4 {
                    return Err(Error::from("HSTORE binary data too short"));
                }

                let mut buf = &bytes[..];
                use byteorder::{BigEndian, ReadBytesExt};

                let count = buf.read_i32::<BigEndian>()? as usize;
                let mut map = HashMap::new();

                for _ in 0..count {
                    if buf.len() < 8 {
                        return Err(Error::from("HSTORE binary entry too short"));
                    }

                    let key_len = buf.read_i32::<BigEndian>()? as usize;
                    let val_len = buf.read_i32::<BigEndian>()? as i32;

                    if buf.len() < key_len {
                        return Err(Error::from("HSTORE binary key too short"));
                    }

                    let key = String::from_utf8(buf[..key_len].to_vec())
                        .map_err(|e| Error::from(format!("Invalid HSTORE key: {}", e)))?;
                    buf = &buf[key_len..];

                    if val_len < 0 {
                        // NULL value
                        map.insert(key, "null".to_string());
                    } else {
                        let val_len = val_len as usize;
                        if buf.len() < val_len {
                            return Err(Error::from("HSTORE binary value too short"));
                        }

                        let val = String::from_utf8(buf[..val_len].to_vec())
                            .map_err(|e| Error::from(format!("Invalid HSTORE value: {}", e)))?;
                        buf = &buf[val_len..];

                        map.insert(key, val);
                    }
                }

                Self(map)
            }
            PgValueFormat::Text => {
                // Text format: "key1=>value1, key2=>value2"
                let s = value.as_str()?.trim();
                if s.is_empty() {
                    return Ok(Self(HashMap::new()));
                }

                let mut map = HashMap::new();
                // Parse pairs separated by comma
                for pair in s.split(',') {
                    let pair = pair.trim();
                    if pair.is_empty() {
                        continue;
                    }

                    // Find the => separator
                    if let Some(pos) = pair.find("=>") {
                        let key = pair[..pos].trim().to_string();
                        let value = pair[pos + 2..].trim().to_string();
                        map.insert(key, value);
                    } else {
                        return Err(Error::from(format!(
                            "Invalid HSTORE format: '{}'. Expected 'key=>value'",
                            pair
                        )));
                    }
                }

                Self(map)
            }
        })
    }
}

impl Encode for Hstore {
    fn encode(self, _buf: &mut crate::arguments::PgArgumentBuffer) -> Result<IsNull, Error> {
        // HSTORE encoding is complex
        // Applications should use hstore(text) or hstore(text, text) in their query
        Err(Error::from(
            "HStore encoding not supported. Use hstore(text) or hstore(text, text) in your query instead."
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
        let mut map = HashMap::new();
        map.insert("name".to_string(), "John".to_string());
        map.insert("age".to_string(), "30".to_string());
        let hstore = Hstore(map);
        let display = format!("{}", hstore);
        // HashMap iteration order is not guaranteed, so just check both parts are present
        assert!(display.contains("name=>John"));
        assert!(display.contains("age=>30"));
    }

    #[test]
    fn test_default() {
        let hstore = Hstore::default();
        assert_eq!(hstore.0.len(), 0);
    }

    #[test]
    fn test_from_hashmap() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), "value".to_string());
        let hstore: Hstore = map.into();
        assert_eq!(hstore.0.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_decode_text_empty() {
        let hstore: Hstore = Decode::decode(PgValue {
            value: Some(b"".to_vec()),
            type_info: crate::type_info::PgTypeInfo::HSTORE,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(hstore.0.len(), 0);
    }

    #[test]
    fn test_decode_text_single() {
        let s = "name=>John";
        let hstore: Hstore = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::HSTORE,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(hstore.0.get("name"), Some(&"John".to_string()));
    }

    #[test]
    fn test_decode_text_multiple() {
        let s = "name=>John, age=>30, city=>NYC";
        let hstore: Hstore = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::HSTORE,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(hstore.0.len(), 3);
        assert_eq!(hstore.0.get("name"), Some(&"John".to_string()));
        assert_eq!(hstore.0.get("age"), Some(&"30".to_string()));
        assert_eq!(hstore.0.get("city"), Some(&"NYC".to_string()));
    }

    #[test]
    fn test_from_value() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), "value".to_string());
        let hstore = Hstore(map);
        let value: Value = hstore.into();
        match value {
            Value::Ext(type_name, boxed) => {
                assert_eq!(type_name, "hstore");
                if let Value::String(s) = *boxed {
                    assert!(s.contains("key"));
                    assert!(s.contains("value"));
                } else {
                    panic!("Expected String");
                }
            }
            _ => panic!("Expected Ext variant"),
        }
    }

    #[test]
    fn test_equality() {
        let mut map1 = HashMap::new();
        map1.insert("key".to_string(), "value".to_string());
        let h1 = Hstore(map1.clone());

        let h2 = Hstore(map1);

        assert_eq!(h1, h2);
    }
}
