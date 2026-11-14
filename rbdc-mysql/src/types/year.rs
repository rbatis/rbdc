use crate::types::{Decode, Encode};
use crate::value::{MySqlValueFormat, MySqlValueRef};
use rbdc::Error;
use std::fmt::{Debug, Display, Formatter};

#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
#[serde(rename = "Year")]
pub struct Year(pub u16);

impl Display for Year {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Year {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Year({})", self.0)
    }
}

impl Encode for Year {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        // Bind YEAR values as a fixed-width SHORT parameter. MySQL result
        // columns still use the one-byte YEAR representation.
        buf.extend_from_slice(&self.0.to_le_bytes());
        Ok(2)
    }
}

impl Decode for Year {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, Error> {
        Ok(Self({
            match value.format() {
                MySqlValueFormat::Text => value.as_str()?.parse().unwrap_or_default(),
                MySqlValueFormat::Binary => {
                    let buf = value.as_bytes()?;
                    if buf.is_empty() {
                        return Err(Error::from("YEAR binary value must not be empty"));
                    }
                    if buf.len() < 2 {
                        return Err(Error::from("YEAR binary value must be 2 bytes"));
                    }
                    u16::from_le_bytes([buf[0], buf[1]])
                }
            }
        }))
    }
}
