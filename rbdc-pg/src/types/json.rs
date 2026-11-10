use crate::arguments::PgArgumentBuffer;
use crate::type_info::PgTypeInfo;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::types::TypeInfo;
use crate::value::{PgValueFormat, PgValueRef};
use rbdc::json::Json;
use rbdc::Error;
use rbs::Value;
use std::io::Write;

impl Encode for Json {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        let mut bytes = self.0.into_bytes();
        if bytes.is_empty() {
            bytes = "null".to_string().into_bytes();
        }
        // we have a tiny amount of dynamic behavior depending if we are resolved to be JSON
        // instead of JSONB
        buf.patch(|buf, ty: &PgTypeInfo| {
            if *ty == PgTypeInfo::JSON || *ty == PgTypeInfo::JSON_ARRAY {
                buf[0] = b' ';
            }
        });

        // JSONB version (as of 2020-03-20)
        buf.push(1);

        // the JSON data written to the buffer is the same regardless of parameter type
        buf.write_all(&bytes)?;

        Ok(IsNull::No)
    }
}

impl Decode for Json {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        let fmt = value.format();
        let type_info = value.type_info;
        let buf = value.value.unwrap_or_default();
        if buf.len() == 0 {
            return Ok(Json {
                0: "null".to_string(),
            });
        }
        if fmt == PgValueFormat::Binary && type_info == PgTypeInfo::JSONB {
            assert_eq!(
                buf[0], 1,
                "unsupported JSONB format version {}; please open an issue",
                buf[0]
            );
            Ok(Self {
                0: String::from_utf8_lossy(&buf[1..]).into_owned(),
            })
        } else {
            Ok(Self {
                0: String::from_utf8_lossy(&buf).into_owned(),
            })
        }
    }
}

pub fn decode_json(value: PgValueRef) -> Result<Value, Error> {
    let fmt = value.format();
    let type_info = value.type_info;
    let buf = value.value.unwrap_or_default();
    if buf.len() == 0 {
        return Ok(Value::Null);
    }
    if fmt == PgValueFormat::Binary && type_info == PgTypeInfo::JSONB {
        assert_eq!(
            buf[0], 1,
            "unsupported JSONB format version {}; please open an issue",
            buf[0]
        );
        Ok(serde_json::from_str(&String::from_utf8_lossy(&buf[1..]))
            .map_err(|e| Error::from(e.to_string()))?)
    } else {
        Ok(serde_json::from_str(&String::from_utf8_lossy(&buf))
            .map_err(|e| Error::from(e.to_string()))?)
    }
}

pub fn encode_json(v: Value, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
    // we have a tiny amount of dynamic behavior depending if we are resolved to be JSON
    // instead of JSONB
    buf.patch(|buf, ty: &PgTypeInfo| {
        if *ty == PgTypeInfo::JSON || *ty == PgTypeInfo::JSON_ARRAY {
            buf[0] = b' ';
        }
    });

    // JSONB version (as of 2020-03-20)
    buf.push(1);

    // the JSON data written to the buffer is the same regardless of parameter type
    buf.write_all(&v.to_string().into_bytes())?;

    Ok(IsNull::No)
}

impl TypeInfo for Json {
    fn type_info(&self) -> PgTypeInfo {
        PgTypeInfo::JSONB
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    fn jsonb_binary(bytes: &[u8]) -> PgValueRef<'_> {
        PgValueRef {
            value: Some(bytes),
            type_info: PgTypeInfo::JSONB,
            format: PgValueFormat::Binary,
            timezone_sec: None,
        }
    }

    #[test]
    #[should_panic(expected = "unsupported JSONB format version")]
    fn json_decode_rejects_unknown_jsonb_version() {
        let _ = Json::decode(jsonb_binary(&[2, b'n', b'u', b'l', b'l']));
    }

    #[test]
    #[should_panic(expected = "unsupported JSONB format version")]
    fn decode_json_rejects_unknown_jsonb_version() {
        let _ = decode_json(jsonb_binary(&[2, b'n', b'u', b'l', b'l']));
    }

    #[test]
    fn decode_json_round_trips_valid_payloads() {
        let value = decode_json(jsonb_binary(&[1, b'1'])).unwrap();
        assert_eq!(value, Value::from(1u64));
        assert_eq!(decode_json(jsonb_binary(&[])).unwrap(), Value::Null);
        assert!(decode_json(jsonb_binary(&[1, b'{'])).is_err());
    }
}
