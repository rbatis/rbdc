use crate::io::MySqlBufMutExt;
use crate::protocol::text::ColumnType;
use crate::result_set::MySqlTypeInfo;
use crate::types::decode::{
    decode_date, decode_time, decode_timestamp, decode_year, f32_decode, f64_decode, int_decode,
    uint_decode,
};
use crate::types::enums::Enum;
use crate::types::json::{decode_json, encode_json};
use crate::types::set::Set;
use crate::types::year::Year;
use crate::types::{Decode, Encode, TypeInfo};
use crate::value::MySqlValueRef;
use rbdc::date::Date;
use rbdc::datetime::DateTime;
use rbdc::decimal::Decimal;
use rbdc::timestamp::Timestamp;
use rbdc::types::time::Time;
use rbdc::uuid::Uuid;
use rbdc::{Error, Json};
use rbs::Value;
use std::str::FromStr;

impl TypeInfo for Value {
    fn type_info(&self) -> MySqlTypeInfo {
        match self {
            Value::Null => MySqlTypeInfo::null(),
            Value::Bool(_) => MySqlTypeInfo::from_type(ColumnType::Tiny),
            Value::I32(_) => MySqlTypeInfo::from_type(ColumnType::Long),
            Value::I64(_) => MySqlTypeInfo::from_type(ColumnType::LongLong),
            Value::U32(_) => MySqlTypeInfo::unsigned(ColumnType::Long),
            Value::U64(_) => MySqlTypeInfo::unsigned(ColumnType::LongLong),
            Value::F32(_) => MySqlTypeInfo::from_type(ColumnType::Float),
            Value::F64(_) => MySqlTypeInfo::from_type(ColumnType::Double),
            Value::String(_) => MySqlTypeInfo::from_type(ColumnType::VarChar),
            Value::Binary(_) => MySqlTypeInfo::from_type(ColumnType::Blob),
            Value::Array(_) => MySqlTypeInfo::from_type(ColumnType::Json),
            Value::Map(_) => MySqlTypeInfo::from_type(ColumnType::Json),
            Value::Ext(ext_type, _) => {
                match *ext_type {
                    "Uuid" => MySqlTypeInfo::from_type(ColumnType::VarChar),
                    //decimal = 12345678
                    "Decimal" => MySqlTypeInfo::from_type(ColumnType::NewDecimal),
                    //year = "1993"
                    "Year" => MySqlTypeInfo::from_type(ColumnType::Short),
                    //Date = "1993-02-06"
                    "Date" => MySqlTypeInfo::from_type(ColumnType::Date),
                    //RFC3339NanoTime = "15:04:05.999999999"
                    "Time" => MySqlTypeInfo::from_type(ColumnType::Time),
                    //RFC3339 = "2006-01-02 15:04:05.999999"
                    "Timestamp" => {
                        //datetime=5byte
                        MySqlTypeInfo::from_type(ColumnType::Timestamp)
                    }
                    "DateTime" => MySqlTypeInfo::from_type(ColumnType::DateTime),
                    "Json" => MySqlTypeInfo::from_type(ColumnType::Json),
                    // Connector/C binds ENUM/SET through its string path.
                    "Enum" | "Set" => MySqlTypeInfo::from_type(ColumnType::VarChar),
                    "Geometry" => MySqlTypeInfo::from_type(ColumnType::Blob),
                    // Connector/C does not expose a bindable BIT parameter type;
                    // bind the raw bytes as a binary string and let MySQL convert it.
                    "Bit" => MySqlTypeInfo::from_type(ColumnType::Blob),
                    "Blob" | "TinyBlob" | "MediumBlob" | "LongBlob" => {
                        MySqlTypeInfo::from_type(ColumnType::Blob)
                    }
                    _ => MySqlTypeInfo::from_type(ColumnType::VarChar),
                }
            }
        }
    }
}

impl Encode for Value {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        match self {
            Value::Null => Ok(0),
            Value::Bool(v) => {
                buf.extend(&(v as i8).to_le_bytes());
                Ok(1)
            }
            Value::I32(v) => {
                buf.extend(v.to_le_bytes());
                Ok(4)
            }
            Value::I64(v) => {
                buf.extend(v.to_le_bytes());
                Ok(8)
            }
            Value::U32(v) => {
                buf.extend(v.to_le_bytes());
                Ok(4)
            }
            Value::U64(v) => {
                buf.extend(v.to_le_bytes());
                Ok(8)
            }
            Value::F32(v) => {
                let len = &v.to_le_bytes();
                buf.extend(len);
                Ok(len.len())
            }
            Value::F64(v) => {
                let len = &v.to_le_bytes();
                buf.extend(len);
                Ok(len.len())
            }
            Value::String(v) => {
                buf.put_str_lenenc(&v);
                Ok(0)
            }
            Value::Binary(v) => {
                buf.put_bytes_lenenc(v);
                Ok(0)
            }
            Value::Array(v) => encode_json(Value::Array(v), buf),
            Value::Map(v) => encode_json(Value::Map(v), buf),
            Value::Ext(ext_type, v) => {
                match ext_type {
                    "Uuid" => {
                        let text = v
                            .into_string()
                            .ok_or_else(|| Error::from("invalid Uuid value"))?;
                        Uuid(text).encode(buf)
                    }
                    //decimal = 12345678
                    "Decimal" => {
                        let text = v
                            .into_string()
                            .ok_or_else(|| Error::from("invalid Decimal value"))?;
                        Decimal::from_str(&text)
                            .map_err(|e| Error::from(e.to_string()))?
                            .encode(buf)
                    }
                    //year = "1993"
                    "Year" => Year(parse_year(&v)?).encode(buf),
                    //Date = "1993-02-06"
                    "Date" => Date(
                        fastdate::Date::from_str(&v.into_string().unwrap_or_default())
                            .map_err(|e| Error::from(e.to_string()))?,
                    )
                    .encode(buf),
                    //RFC3339NanoTime = "15:04:05.999999999"
                    "Time" => Time(
                        fastdate::Time::from_str(&v.into_string().unwrap_or_default())
                            .map_err(|e| Error::from(e.to_string()))?,
                    )
                    .encode(buf),
                    //RFC3339 = "2006-01-02 15:04:05.999999"
                    "Timestamp" => Timestamp(parse_i64(&v, "Timestamp")?).encode(buf),
                    "DateTime" => DateTime(
                        fastdate::DateTime::from_str(
                            &v.into_string()
                                .ok_or_else(|| Error::from("invalid DateTime value"))?,
                        )
                        .map_err(|e| Error::from(e.to_string()))?,
                    )
                    .encode(buf),
                    "Json" => {
                        let json_str = match *v {
                            Value::String(text) => text,
                            Value::Binary(bytes) => String::from_utf8(bytes).map_err(|e| {
                                Error::from(format!("invalid Json UTF-8: {e}"))
                            })?,
                            other => other.to_string(),
                        };
                        Json(json_str).encode(buf)
                    }
                    "Enum" => Enum(
                        v.into_string()
                            .ok_or_else(|| Error::from("invalid Enum value"))?,
                    )
                    .encode(buf),
                    "Set" => Set(
                        v.into_string()
                            .ok_or_else(|| Error::from("invalid Set value"))?,
                    )
                    .encode(buf),
                    "Geometry" => {
                        let bytes = match *v {
                            Value::Binary(bytes) => bytes,
                            _ => return Err(Error::from("invalid Geometry value")),
                        };
                        buf.put_bytes_lenenc(bytes);
                        Ok(0)
                    }
                    "Bit" => encode_bit(v, buf),
                    _ => {
                        let bytes = v.into_bytes().unwrap_or_default();
                        buf.put_bytes_lenenc(bytes);
                        Ok(0)
                    }
                }
            }
        }
    }
}

fn parse_year(value: &Value) -> Result<u16, Error> {
    match value {
        Value::I32(v) => u16::try_from(*v)
            .map_err(|_| Error::from(format!("invalid Year value: {v}"))),
        Value::I64(v) => u16::try_from(*v)
            .map_err(|_| Error::from(format!("invalid Year value: {v}"))),
        Value::U32(v) => u16::try_from(*v)
            .map_err(|_| Error::from(format!("invalid Year value: {v}"))),
        Value::U64(v) => u16::try_from(*v)
            .map_err(|_| Error::from(format!("invalid Year value: {v}"))),
        Value::String(v) => v
            .parse::<u16>()
            .map_err(|e| Error::from(format!("invalid Year value {v:?}: {e}"))),
        other => Err(Error::from(format!("invalid Year value: {other:?}"))),
    }
}

fn parse_i64(value: &Value, type_name: &str) -> Result<i64, Error> {
    match value {
        Value::I32(v) => Ok(*v as i64),
        Value::I64(v) => Ok(*v),
        Value::U32(v) => Ok(*v as i64),
        Value::U64(v) => i64::try_from(*v)
            .map_err(|_| Error::from(format!("{type_name} value {v} exceeds i64 range"))),
        Value::String(v) => v
            .parse::<i64>()
            .map_err(|e| Error::from(format!("invalid {type_name} value {v:?}: {e}"))),
        other => Err(Error::from(format!("invalid {type_name} value: {other:?}"))),
    }
}

fn encode_bit(value: Box<Value>, buf: &mut Vec<u8>) -> Result<usize, Error> {
    let bits = match *value {
        Value::U64(v) => v,
        Value::U32(v) => v as u64,
        Value::I64(v) => u64::try_from(v)
            .map_err(|_| Error::from("invalid negative Bit value"))?,
        Value::I32(v) => u64::try_from(v)
            .map_err(|_| Error::from("invalid negative Bit value"))?,
        Value::String(v) => v
            .parse::<u64>()
            .map_err(|e| Error::from(format!("invalid Bit value {v:?}: {e}")))?,
        Value::Binary(bytes) => {
            buf.put_bytes_lenenc(bytes);
            return Ok(0);
        }
        other => return Err(Error::from(format!("invalid Bit value: {other:?}"))),
    };
    let bytes = bits.to_be_bytes();
    let first = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);
    buf.put_bytes_lenenc(bytes[first..].to_vec());
    Ok(0)
}

impl Decode for Value {
    fn decode(v: MySqlValueRef<'_>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        if v.value.is_none() {
            return Ok(Value::Null);
        }

        let type_info = v.type_info.r#type;
        let unsigned = v.type_info.is_unsigned();
        Ok(match type_info {
            ColumnType::Tiny if unsigned => Value::U32(uint_decode(v).unwrap_or_default() as u32),
            ColumnType::Tiny => Value::I32(int_decode(v).unwrap_or_default() as i32),
            ColumnType::Short if unsigned => Value::U32(uint_decode(v).unwrap_or_default() as u32),
            ColumnType::Short => Value::I32(int_decode(v).unwrap_or_default() as i32),
            ColumnType::Long if unsigned => Value::U64(uint_decode(v).unwrap_or_default()),
            ColumnType::Long => Value::I64(int_decode(v).unwrap_or_default()),
            ColumnType::Float => Value::F32(f32_decode(v).unwrap_or_default()),
            ColumnType::Double => Value::F64(f64_decode(v).unwrap_or_default()),
            ColumnType::Null => Value::Null,
            ColumnType::LongLong if unsigned => Value::U64(uint_decode(v).unwrap_or_default()),
            ColumnType::LongLong => Value::I64(int_decode(v).unwrap_or_default()),
            ColumnType::Int24 if unsigned => Value::U32(uint_decode(v).unwrap_or_default() as u32),
            ColumnType::Int24 => Value::I32(int_decode(v).unwrap_or_default() as i32),
            ColumnType::VarChar => Value::String(v.as_str().unwrap_or_default().to_string()),
            ColumnType::Bit => Value::U64(uint_decode(v).unwrap_or_default()),
            ColumnType::TinyBlob => Value::Binary(v.as_bytes().unwrap_or_default().to_vec()),
            ColumnType::MediumBlob => Value::Binary(v.as_bytes().unwrap_or_default().to_vec()),
            ColumnType::LongBlob => Value::Binary(v.as_bytes().unwrap_or_default().to_vec()),
            ColumnType::Blob => Value::Binary(v.as_bytes().unwrap_or_default().to_vec()),
            ColumnType::VarString => Value::String(v.as_str().unwrap_or_default().to_string()),
            ColumnType::String => Value::String(v.as_str().unwrap_or_default().to_string()),
            ColumnType::Timestamp => Value::Ext(
                "Timestamp",
                Box::new(Value::U64({
                    let s = decode_timestamp(v).unwrap_or_default();
                    let date =
                        fastdate::DateTime::from_str(&s).map_err(|e| Error::from(e.to_string()))?;
                    date.unix_timestamp_millis() as u64
                })),
            ),
            ColumnType::Decimal => Value::Ext(
                "Decimal",
                Box::new(Value::String(v.as_str().unwrap_or("0").to_string())),
            ),
            ColumnType::Date => Value::Ext(
                "Date",
                Box::new(Value::String(decode_date(v).unwrap_or_default())),
            ),
            ColumnType::Time => Value::Ext(
                "Time",
                Box::new(Value::String(decode_time(v).unwrap_or_default())),
            ),
            ColumnType::DateTime => Value::Ext(
                "DateTime",
                Box::new(Value::String(DateTime::decode(v)?.to_string())),
            ),
            ColumnType::Year => Value::Ext(
                "Year",
                Box::new(Value::String(decode_year(v)?)),
            ),
            ColumnType::Json => decode_json(v)?,
            ColumnType::NewDecimal => Value::Ext(
                "Decimal",
                Box::new(Value::String(v.as_str().unwrap_or("0").to_string())),
            ),
            ColumnType::Enum => Value::Ext(
                "Enum",
                Box::new(Value::String(v.as_str().unwrap_or("").to_string())),
            ),
            ColumnType::Set => Value::Ext(
                "Set",
                Box::new(Value::String(v.as_str().unwrap_or("").to_string())),
            ),
            //bytes ,see https://dev.mysql.com/doc/internals/en/x-protocol-messages-messages.html
            ColumnType::Geometry => Value::Ext(
                "Geometry",
                Box::new(Value::Binary(v.as_bytes().unwrap_or_default().to_vec())),
            ),
        })
    }
}
