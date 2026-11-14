use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValueFormat, PgValueRef};
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
        let pairs: Vec<String> = self
            .0
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
        Value::Ext("Hstore", Box::new(Value::String(arg.to_string())))
    }
}

impl Decode for Hstore {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
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
                    if buf.len() < 4 {
                        return Err(Error::from("HSTORE binary entry too short"));
                    }

                    let key_len = buf.read_i32::<BigEndian>()? as usize;

                    if buf.len() < key_len {
                        return Err(Error::from("HSTORE binary key too short"));
                    }

                    let key = String::from_utf8(buf[..key_len].to_vec())
                        .map_err(|e| Error::from(format!("Invalid HSTORE key: {}", e)))?;
                    buf = &buf[key_len..];

                    if buf.len() < 4 {
                        return Err(Error::from("HSTORE binary value header too short"));
                    }
                    let val_len = buf.read_i32::<BigEndian>()?;

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
    fn encode(self, buf: &mut crate::arguments::PgArgumentBuffer) -> Result<IsNull, Error> {
        buf.extend_from_slice(&(self.0.len() as i32).to_be_bytes());
        for (key, value) in self.0 {
            buf.extend_from_slice(&(key.len() as i32).to_be_bytes());
            buf.extend_from_slice(key.as_bytes());
            if value == "null" {
                buf.extend_from_slice(&(-1_i32).to_be_bytes());
            } else {
                buf.extend_from_slice(&(value.len() as i32).to_be_bytes());
                buf.extend_from_slice(value.as_bytes());
            }
        }
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip() {
        let value = Hstore(HashMap::from([
            ("name".to_string(), "John".to_string()),
            ("age".to_string(), "30".to_string()),
        ]));
        let encoded = rbs::to_value(&value).unwrap();
        let decoded: Hstore = rbs::from_value(encoded).unwrap();
        assert_eq!(decoded, value);
    }
}
