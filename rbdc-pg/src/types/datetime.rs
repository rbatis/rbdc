use crate::arguments::PgArgumentBuffer;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValueFormat, PgValueRef};
use byteorder::{BigEndian, ReadBytesExt};
use rbdc::datetime::DateTime;
use rbdc::Error;
use std::io::Cursor;
use std::str::FromStr;
use std::time::Duration;

/// Encode to Timestamptz
impl Encode for DateTime {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        let mut millis = self.unix_timestamp_millis();
        // Add session timezone offset to compensate for PostgreSQL's timezone conversion
        if let Some(tz_sec) = buf.timezone_sec {
            millis = millis + Duration::from_secs(tz_sec as u64).as_millis() as i64;
        }
        let epoch = fastdate::DateTime::from(fastdate::Date {
            day: 1,
            mon: 1,
            year: 2000,
        });
        let dt = fastdate::DateTime::from_timestamp_millis(millis);
        let micros;
        if dt >= epoch {
            micros = (dt - epoch).as_micros() as i64;
        } else {
            micros = (epoch - dt).as_micros() as i64 * -1;
        }
        micros.encode(buf)
    }
}

impl Decode for DateTime {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                let mut buf = Cursor::new(value.as_bytes()?);
                let us = buf.read_i64::<BigEndian>()?;
                let epoch = fastdate::DateTime::from(fastdate::Date {
                    day: 1,
                    mon: 1,
                    year: 2000,
                });
                let v = {
                    if us < 0 {
                        epoch - std::time::Duration::from_micros(-us as u64)
                    } else {
                        epoch + std::time::Duration::from_micros(us as u64)
                    }
                };
                let mut dt = DateTime(fastdate::DateTime::from_timestamp_millis(
                    v.unix_timestamp_millis(),
                ));
                // Apply session timezone offset if available
                if let Some(tz_sec) = value.timezone_sec {
                    dt = dt.set_offset(tz_sec);
                }
                dt
            }
            PgValueFormat::Text => {
                let s = value.as_str()?;
                let date =
                    fastdate::DateTime::from_str(s).map_err(|e| Error::from(e.to_string()))?;
                DateTime(date)
            }
        })
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::type_info::{PgType, PgTypeInfo};

    #[test]
    fn encode_applies_session_timezone() {
        let dt = DateTime(fastdate::DateTime::from_timestamp_millis(1_600_000_000_000));

        let mut without = PgArgumentBuffer::default();
        dt.clone().encode(&mut without).unwrap();

        let mut with = PgArgumentBuffer::default();
        with.timezone_sec = Some(28800);
        dt.clone().encode(&mut with).unwrap();

        let a = i64::from_be_bytes(without[..8].try_into().unwrap());
        let b = i64::from_be_bytes(with[..8].try_into().unwrap());
        assert_eq!(b - a, 28_800 * 1_000_000);
    }

    #[test]
    fn decode_binary_applies_timezone_offset() {
        let dt = DateTime(fastdate::DateTime::from_timestamp_millis(1_600_000_000_000));
        let mut buf = PgArgumentBuffer::default();
        dt.clone().encode(&mut buf).unwrap();

        let value = PgValueRef {
            value: Some(&buf[..]),
            type_info: PgTypeInfo(PgType::Timestamptz),
            format: PgValueFormat::Binary,
            timezone_sec: Some(3600),
        };
        let decoded = DateTime::decode(value).unwrap();
        // set_offset changes the display offset, not the instant
        assert_eq!(
            decoded.unix_timestamp_millis(),
            dt.unix_timestamp_millis()
        );

        // negative microseconds branch
        let negative = (-1_000_000i64).to_be_bytes();
        let value = PgValueRef {
            value: Some(&negative),
            type_info: PgTypeInfo(PgType::Timestamptz),
            format: PgValueFormat::Binary,
            timezone_sec: None,
        };
        assert!(DateTime::decode(value).is_ok());
    }

    #[test]
    fn decode_text_and_errors() {
        let value = PgValueRef {
            value: Some(b"2021-02-03 04:05:06"),
            type_info: PgTypeInfo(PgType::Timestamptz),
            format: PgValueFormat::Text,
            timezone_sec: None,
        };
        assert_eq!(
            DateTime::decode(value).unwrap().unix_timestamp_millis(),
            fastdate::DateTime::from_str("2021-02-03 04:05:06")
                .unwrap()
                .unix_timestamp_millis()
        );

        let value = PgValueRef {
            value: Some(b"nope"),
            type_info: PgTypeInfo(PgType::Timestamptz),
            format: PgValueFormat::Text,
            timezone_sec: None,
        };
        assert!(DateTime::decode(value).is_err());
    }
}
