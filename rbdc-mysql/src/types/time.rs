use crate::types::{Decode, Encode};
use crate::value::{MySqlValue, MySqlValueFormat};
use bytes::Buf;
use rbdc::types::time::Time;
use rbdc::Error;
use std::str::FromStr;

impl Encode for Time {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        self.0.encode(buf)
    }
}

impl Decode for Time {
    fn decode(value: MySqlValue) -> Result<Self, Error> {
        Ok(Time(fastdate::Time::decode(value)?))
    }
}

impl Encode for fastdate::Time {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        let len = time_encoded_len(&self);
        buf.push(len);

        // sign byte: Time is never negative
        buf.push(0);

        // Number of days in the interval; always 0 for time-of-day values.
        // https://mariadb.com/kb/en/resultset-row/#teimstamp-binary-encoding
        buf.extend_from_slice(&[0_u8; 4]);

        encode_time(&self, len > 8, buf);

        Ok(len as usize)
    }
}

impl Decode for fastdate::Time {
    fn decode(value: MySqlValue) -> Result<Self, Error> {
        Ok(match value.format() {
            MySqlValueFormat::Text => {
                fastdate::Time::from_str(value.as_str()?).map_err(|e| Error::from(e.to_string()))?
            }
            MySqlValueFormat::Binary => {
                let buf = value.as_bytes()?;
                let len = buf[0];
                if len > 4 {
                    decode_time(&buf[5..])
                } else {
                    fastdate::Time {
                        nano: 0,
                        sec: 0,
                        minute: 0,
                        hour: 0,
                    }
                }
            }
        })
    }
}

pub fn decode_time(mut buf: &[u8]) -> fastdate::Time {
    let hour = buf.get_u8();
    let minute = buf.get_u8();
    let seconds = buf.get_u8();
    let micros = if !buf.is_empty() {
        // microseconds : int<EOF>
        buf.get_uint_le(buf.len())
    } else {
        0
    };
    // NaiveTime::from_hms_micro(hour as u32, minute as u32, seconds as u32, micros as u32)
    fastdate::Time {
        nano: micros as u32 * 1000,
        sec: seconds,
        minute: minute,
        hour,
    }
}

fn encode_time(time: &fastdate::Time, include_micros: bool, buf: &mut Vec<u8>) {
    buf.push(time.get_hour());
    buf.push(time.get_minute());
    buf.push(time.get_sec());

    if include_micros {
        let micro = time.get_nano() / 1000;
        buf.extend(&micro.to_le_bytes());
    }
}

#[inline(always)]
fn time_encoded_len(time: &fastdate::Time) -> u8 {
    if time.get_nano() == 0 {
        // if micro_seconds is 0, length is 8 and micro_seconds is not sent
        8
    } else {
        // otherwise length is 12
        12
    }
}
