use crate::arguments::PgArgumentBuffer;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValueRef, PgValueFormat};
use byteorder::{BigEndian, ByteOrder};
use rbdc::Error;

impl Decode for f64 {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => BigEndian::read_f64(value.as_bytes()?),
            PgValueFormat::Text => value.as_str()?.parse()?,
        })
    }
}

impl Decode for f32 {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                let bytes = value.as_bytes()?;
                if bytes.len() == 8 {
                    BigEndian::read_f64(bytes) as f32
                } else if bytes.len() == 4 {
                    BigEndian::read_f32(bytes)
                } else {
                    return Err(Error::from("error f32 bytes len"));
                }
            }
            PgValueFormat::Text => value.as_str()?.parse()?,
        })
    }
}

impl Encode for f64 {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        buf.extend(&self.to_be_bytes());

        Ok(IsNull::No)
    }
}

impl Encode for f32 {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        buf.extend(&self.to_be_bytes());

        Ok(IsNull::No)
    }
}