use crate::protocol::text::ColumnType;
use crate::value::{MySqlValueFormat, MySqlValueRef};
use byteorder::{ByteOrder, LittleEndian};
use bytes::Buf;
use fastdate::Date;
use rbdc::Error;

pub(crate) fn uint_decode(value: MySqlValueRef<'_>) -> Result<u64, Error> {
    if value.type_info.r#type == ColumnType::Bit {
        // NOTE: Regardless of the value format, there is raw binary data here
        let buf = value.as_bytes()?;
        let mut value: u64 = 0;
        for b in buf {
            value = (*b as u64) | (value << 8);
        }
        return Ok(value);
    }

    Ok(match value.format() {
        MySqlValueFormat::Text => value.as_str()?.parse().unwrap_or_default(),

        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;
            LittleEndian::read_uint(buf, buf.len())
        }
    })
}

pub(crate) fn int_decode(value: MySqlValueRef<'_>) -> Result<i64, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Text => value.as_str()?.parse().unwrap_or_default(),
        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;
            LittleEndian::read_int(buf, buf.len())
        }
    })
}

pub(crate) fn f32_decode(value: MySqlValueRef<'_>) -> Result<f32, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;

            if buf.len() == 8 {
                // MySQL can return 8-byte DOUBLE values for a FLOAT
                // We take and truncate to f32 as that's the same behavior as *in* MySQL
                LittleEndian::read_f64(buf) as f32
            } else {
                LittleEndian::read_f32(buf)
            }
        }

        MySqlValueFormat::Text => value.as_str()?.parse().unwrap_or_default(),
    })
}

pub(crate) fn f64_decode(value: MySqlValueRef<'_>) -> Result<f64, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Binary => LittleEndian::read_f64(value.as_bytes()?),
        MySqlValueFormat::Text => value.as_str()?.parse().unwrap_or_default(),
    })
}

pub(crate) fn decode_timestamp(value: MySqlValueRef<'_>) -> Result<String, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Text => {
            let mut v = value.as_str()?.to_string();
            if !v.ends_with("Z") {
                v.push_str("Z");
            }
            v
        }
        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;
            let len = buf[0];
            let date = decode_date_buf(&buf[1..])?.to_string();
            let dt = if len > 4 {
                decode_time_buf(len - 4, &buf[5..])?
            } else {
                "00:00:00".to_string()
            };
            date + " " + &dt + "Z"
        }
    })
}

pub(crate) fn decode_year(value: MySqlValueRef<'_>) -> Result<String, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Text => value.as_str()?.to_string(),
        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;
            let date = decode_year_buf(buf)?;
            date
        }
    })
}

pub(crate) fn decode_date(value: MySqlValueRef<'_>) -> Result<String, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Text => value.as_str()?.to_string(),
        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;
            let date = decode_date_buf(&buf[1..])?.to_string();
            date
        }
    })
}

pub(crate) fn decode_time(value: MySqlValueRef<'_>) -> Result<String, Error> {
    Ok(match value.format() {
        MySqlValueFormat::Text => value.as_str()?.to_string(),
        MySqlValueFormat::Binary => {
            let buf = value.as_bytes()?;
            let len = buf[0];
            let dt = if len > 4 {
                decode_time_buf(len - 4, &buf[5..])?
            } else {
                "00:00:00".to_string()
            };
            dt
        }
    })
}

pub(crate) fn decode_date_buf(buf: &[u8]) -> Result<Date, Error> {
    if buf.is_empty() {
        // zero buffer means a zero date (null)
        return Ok(Date {
            year: 0000,
            mon: 00,
            day: 00,
        });
    }
    Ok(Date {
        year: LittleEndian::read_u16(buf) as i32,
        mon: buf[2] as u8,
        day: buf[3] as u8,
    })
}

pub(crate) fn decode_year_buf(buf: &[u8]) -> Result<String, Error> {
    if buf.is_empty() {
        // zero buffer means a zero date (null)
        return Ok("".to_string());
    }
    if buf.len() < 2 {
        return Err(Error::from("YEAR binary value must be 2 bytes"));
    }
    let year = LittleEndian::read_u16(buf) as i32;
    Ok(format!("{year:0>4}"))
}

pub(crate) fn decode_time_buf(_: u8, mut buf: &[u8]) -> Result<String, Error> {
    let hour = buf.get_u8();
    let minute = buf.get_u8();
    let seconds = buf.get_u8();
    let milliseconds = if buf.len() >= 4 {
        buf.get_u32_le()
    } else {
        0 // 如果没有足够的字节表示毫秒值，将其设置为默认值
    };
    Ok(format!(
        "{:0>2}:{:0>2}:{:0>2}.{:0>6}",
        hour, minute, seconds, milliseconds
    ))
}

pub(crate) fn decode_bool(value: MySqlValueRef<'_>) -> Result<bool, Error> {
    Ok(int_decode(value)? != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_set::MySqlTypeInfo;
    use crate::value::MySqlValueFormat;
    use std::sync::Arc;

    fn value<'a>(
        data: Option<&'a [u8]>,
        ty: ColumnType,
        format: MySqlValueFormat,
    ) -> MySqlValueRef<'a> {
        MySqlValueRef {
            value: data,
            type_info: MySqlTypeInfo::from_type(ty),
            format,
            option: Arc::new(crate::options::MySqlConnectOptions::new()),
        }
    }

    #[test]
    fn test_decode_bool_text() {
        assert!(decode_bool(value(Some(b"1"), ColumnType::Tiny, MySqlValueFormat::Text)).unwrap());
        assert!(!decode_bool(value(Some(b"0"), ColumnType::Tiny, MySqlValueFormat::Text)).unwrap());
        // Unparsable values fall back to zero, not an error.
        assert!(
            !decode_bool(value(Some(b"nope"), ColumnType::Tiny, MySqlValueFormat::Text)).unwrap()
        );
    }

    #[test]
    fn test_decode_bool_binary() {
        assert!(decode_bool(value(Some(&[1]), ColumnType::Tiny, MySqlValueFormat::Binary)).unwrap());
        assert!(
            !decode_bool(value(Some(&[0]), ColumnType::Tiny, MySqlValueFormat::Binary)).unwrap()
        );
    }

    #[test]
    fn test_decode_bool_missing_value_errors() {
        assert!(decode_bool(value(None, ColumnType::Tiny, MySqlValueFormat::Text)).is_err());
    }

    #[test]
    fn test_decode_time_buf_with_and_without_millis() {
        assert_eq!(
            decode_time_buf(8, &[1, 2, 3]).unwrap(),
            "01:02:03.000000"
        );
        let mut buf = vec![1, 2, 3];
        buf.extend_from_slice(&456_u32.to_le_bytes());
        assert_eq!(decode_time_buf(11, &buf).unwrap(), "01:02:03.000456");
    }

    #[test]
    fn test_decode_year_buf() {
        assert_eq!(decode_year_buf(&[]).unwrap(), "");
        assert_eq!(decode_year_buf(&[0xE7, 0x07]).unwrap(), "2023");
        assert!(decode_year_buf(&[123]).is_err());
    }

    #[test]
    fn test_decode_date_buf_empty_is_zero_date() {
        let d = decode_date_buf(&[]).unwrap();
        assert_eq!((d.year, d.mon, d.day), (0, 0, 0));
        let d = decode_date_buf(&[0xE7, 0x07, 5, 6]).unwrap();
        assert_eq!((d.year, d.mon, d.day), (2023, 5, 6));
    }
}

#[cfg(test)]
mod uint_tests {
    use super::*;
    use crate::result_set::MySqlTypeInfo;
    use crate::value::MySqlValueFormat;
    use std::sync::Arc;

    fn value<'a>(
        data: Option<&'a [u8]>,
        ty: ColumnType,
        format: MySqlValueFormat,
    ) -> MySqlValueRef<'a> {
        MySqlValueRef {
            value: data,
            type_info: MySqlTypeInfo::from_type(ty),
            format,
            option: Arc::new(crate::options::MySqlConnectOptions::new()),
        }
    }

    #[test]
    fn test_uint_decode_text_and_binary_formats() {
        assert_eq!(
            uint_decode(value(Some(b"42"), ColumnType::Long, MySqlValueFormat::Text)).unwrap(),
            42
        );
        assert_eq!(
            uint_decode(value(
                Some(&0x0102u16.to_le_bytes()),
                ColumnType::Long,
                MySqlValueFormat::Binary
            ))
            .unwrap(),
            0x0102
        );
        // Bit columns are always raw binary, big-endian.
        assert_eq!(
            uint_decode(value(Some(&[0x01, 0x02]), ColumnType::Bit, MySqlValueFormat::Text)).unwrap(),
            0x0102
        );
        assert!(uint_decode(value(None, ColumnType::Long, MySqlValueFormat::Text)).is_err());
    }
}
