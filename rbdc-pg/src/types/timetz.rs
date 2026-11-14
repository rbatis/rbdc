use crate::arguments::PgArgumentBuffer;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValueFormat, PgValueRef};
use byteorder::{BigEndian, ReadBytesExt};
use fastdate::time1::UtcOffset;
use rbdc::Error;
use rbs::Value;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::str::FromStr;
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename = "Timez")]
pub struct Timetz(pub OffsetTz);

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename = "OffsetTz")]
pub struct OffsetTz {
    pub time: fastdate::Time,
    pub offset: i32,
}

impl Display for OffsetTz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.time, f)?;
        let offset = UtcOffset::from_whole_seconds(self.offset).unwrap();
        let (h, m, _) = offset.as_hms();
        if h != 0 || m != 0 {
            if self.offset >= 0 {
                f.write_str("+")?;
            } else {
                f.write_str("-")?;
            }
            f.write_str(&format!("{:02}", h.abs()))?;
            f.write_str(":")?;
            f.write_str(&format!("{:02}", m.abs()))?;
        }
        Ok(())
    }
}

impl Display for Timetz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Timetz {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let split = s
            .char_indices()
            .skip(1)
            .find(|(_, c)| *c == '+' || *c == '-')
            .map(|(i, _)| i)
            .ok_or_else(|| Error::from(format!("Invalid TIMETZ value: {s}. Expected time+offset")))?;
        let (time, raw_offset) = s.split_at(split);
        let time = fastdate::Time::from_str(time)
            .map_err(|e| Error::from(format!("Invalid TIMETZ time: {e}")))?;
        let offset = parse_offset(raw_offset)?;
        Ok(Self(OffsetTz { time, offset }))
    }
}

impl From<Timetz> for Value {
    fn from(arg: Timetz) -> Self {
        rbs::value!(arg)
    }
}

impl Decode for Timetz {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        match value.format() {
            PgValueFormat::Binary => {
                let mut buf = Cursor::new(value.as_bytes()?);

                // TIME is encoded as the microseconds since midnight
                let microseconds = buf.read_i64::<BigEndian>()?;

                // OFFSET is encoded as seconds from UTC
                let seconds = buf.read_i32::<BigEndian>()?;

                Ok(Self(OffsetTz {
                    time: fastdate::Time::from(Duration::from_micros(microseconds as u64)),
                    offset: seconds,
                }))
            }
            PgValueFormat::Text => Timetz::from_str(value.as_str()?),
        }
    }
}

fn parse_offset(raw: &str) -> Result<i32, Error> {
    let (sign, digits) = match raw.as_bytes().first() {
        Some(b'+') => (1, &raw[1..]),
        Some(b'-') => (-1, &raw[1..]),
        _ => {
            return Err(Error::from(format!(
                "Invalid TIMETZ offset: {raw}. Expected +HH[:MM[:SS]] or -HH[:MM[:SS]]"
            )));
        }
    };
    let mut parts = digits.split(':');
    let hours: i32 = parts
        .next()
        .unwrap_or_default()
        .parse()
        .map_err(|e| Error::from(format!("Invalid TIMETZ offset hour: {e}")))?;
    let minutes: i32 = parts
        .next()
        .unwrap_or("0")
        .parse()
        .map_err(|e| Error::from(format!("Invalid TIMETZ offset minute: {e}")))?;
    let seconds: i32 = parts
        .next()
        .unwrap_or("0")
        .parse()
        .map_err(|e| Error::from(format!("Invalid TIMETZ offset second: {e}")))?;
    if parts.next().is_some() || hours > 23 || minutes > 59 || seconds > 59 {
        return Err(Error::from(format!("Invalid TIMETZ offset: {raw}")));
    }
    Ok(sign * (hours * 3600 + minutes * 60 + seconds))
}

impl Encode for Timetz {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        let _ = (Duration::from(self.0.time).as_micros() as i64).encode(buf)?;
        let _ = self.0.offset.encode(buf)?;
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod test {
    use crate::types::timetz::OffsetTz;
    use fastdate::{offset_sec, Time};

    #[test]
    fn test_display() {
        let o = OffsetTz {
            time: Time {
                nano: 0,
                sec: 0,
                minute: 0,
                hour: 0,
            },
            offset: offset_sec(),
        };

        println!("{}", o);
    }

    #[test]
    fn test_from_str_with_offset() {
        let timetz: super::Timetz = "12:30:45+08:00".parse().unwrap();
        assert_eq!(timetz.0.offset, 8 * 3600);
        assert_eq!(timetz.to_string(), "12:30:45+08:00");
    }
}
