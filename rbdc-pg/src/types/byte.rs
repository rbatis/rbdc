use crate::arguments::PgArgumentBuffer;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValue, PgValueFormat};
use rbdc::Error;
use rbs::Value;
use std::fmt::{Display, Formatter};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename = "Bytea")]
pub struct Bytea(pub Vec<u8>);

impl Display for Bytea {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[len={}]", self.0.len())
    }
}

impl Encode for Bytea {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        Encode::encode(self.0, buf)
    }
}

impl Decode for Bytea {
    fn decode(value: PgValue) -> Result<Self, Error> {
        // note: in the TEXT encoding, a value of "0" here is encoded as an empty string
        Ok(Self(Vec::<u8>::decode(value)?))
    }
}

impl From<Bytea> for Value {
    fn from(arg: Bytea) -> Self {
        Value::Ext("Bytea", Box::new(Value::Binary(arg.0)))
    }
}

impl Encode for &[u8] {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        buf.extend_from_slice(self);
        Ok(IsNull::No)
    }
}

impl Encode for Vec<u8> {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        buf.extend(self);
        Ok(IsNull::No)
    }
}

impl Decode for Vec<u8> {
    fn decode(value: PgValue) -> Result<Self, Error> {
        match value.format() {
            PgValueFormat::Binary => value.into_bytes(),
            PgValueFormat::Text => {
                hex::decode(text_hex_decode_input(&value)?).map_err(|e| Error::from(e.to_string()))
            }
        }
    }
}

fn text_hex_decode_input(value: &PgValue) -> Result<&[u8], Error> {
    // BYTEA is formatted as \x followed by hex characters
    value
        .as_bytes()?
        .strip_prefix(b"\\x")
        .ok_or("text does not start with \\x")
        .map_err(Into::into)
}
