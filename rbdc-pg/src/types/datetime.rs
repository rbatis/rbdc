use crate::arguments::PgArgumentBuffer;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValue, PgValueFormat};
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
    fn decode(value: PgValue) -> Result<Self, Error> {
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
