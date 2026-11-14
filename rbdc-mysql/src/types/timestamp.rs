use std::str::FromStr;

use crate::types::date::decode_date_buf;
use crate::types::time::decode_time;
use crate::types::{Decode, Encode};
use crate::value::{MySqlValueFormat, MySqlValueRef};
use rbdc::timestamp::Timestamp;
use rbdc::Error;

impl Encode for Timestamp {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        let datetime = fastdate::DateTime::from_timestamp_millis(self.0 as i64);
        let payload_len = date_time_size_hint(
            datetime.hour(),
            datetime.minute(),
            datetime.sec(),
            datetime.nano(),
        );
        let start = buf.len();
        buf.push(payload_len as u8);
        buf.extend_from_slice(&(datetime.year() as u16).to_le_bytes());
        buf.push(datetime.mon() as u8);
        buf.push(datetime.day() as u8);
        if payload_len > 4 {
            buf.push(datetime.hour() as u8);
            buf.push(datetime.minute() as u8);
            buf.push(datetime.sec() as u8);
        }
        if payload_len > 7 {
            buf.extend_from_slice(&(datetime.nano() / 1000).to_le_bytes());
        }
        Ok(buf.len() - start)
    }
}

impl Decode for Timestamp {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, Error> {
        Ok(match value.format() {
            MySqlValueFormat::Text => Self(
                fastdate::DateTime::from_str(value.as_str()?)
                    .map_err(|e| Error::from(e.to_string()))?
                    .unix_timestamp_millis(),
            ),
            MySqlValueFormat::Binary => {
                let buf = value.as_bytes()?;
                let len = buf[0];
                let date = decode_date_buf(&buf[1..])?;
                let time = if len > 4 {
                    decode_time(&buf[5..])
                } else {
                    fastdate::Time {
                        nano: 0,
                        sec: 0,
                        minute: 0,
                        hour: 0,
                    }
                };
                Self(fastdate::DateTime::from((date, time)).unix_timestamp_millis())
            }
        })
    }
}

fn date_time_size_hint(hour: u8, min: u8, sec: u8, nano: u32) -> usize {
    // to save space the packet can be compressed:
    match (hour, min, sec, nano) {
        // if hour, minutes, seconds and micro_seconds are all 0,
        // length is 4 and no other field is sent
        (0, 0, 0, 0) => 4,

        // if micro_seconds is 0, length is 7
        // and micro_seconds is not sent
        (_, _, _, 0) => 7,

        // otherwise length is 11
        (_, _, _, _) => 11,
    }
}
