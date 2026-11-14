use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::type_info::DataType;
use crate::{SqliteArgumentValue, SqliteValueRef};
use rbdc::Error;
use rbs::Value;

impl Decode for Value {
    fn decode(value: SqliteValueRef) -> Result<Self, Error>
    where
        Self: Sized,
    {
        if value.type_info_opt().is_none() {
            return Ok(Value::Null);
        }
        match value.type_info().0 {
            DataType::Null => Ok(Value::Null),
            DataType::Int => Ok(Value::I64(i64::decode(value)?)),
            DataType::Float => Ok(Value::F64(f64::decode(value)?)),
            DataType::Text => {
                let s = value.text()?;
                if is_json_string(s) {
                    if let Ok(v) = serde_json::from_str::<Value>(&s) {
                        Ok(v)
                    } else {
                        Ok(Value::String(s.to_string()))
                    }
                } else {
                    Ok(Value::String(s.to_string()))
                }
            }
            DataType::Blob => Ok(Value::Binary(Vec::<u8>::decode(value)?)),
            DataType::Numeric => Ok(Value::String(String::decode(value)?)),
            DataType::Bool => Ok(Value::Bool(bool::decode(value)?)),
            DataType::Int64 => Ok(Value::I64(i64::decode(value)?)),
            DataType::Date => Ok(Value::Ext(
                "Date",
                Box::new(Value::String(String::decode(value)?)),
            )),
            DataType::Time => Ok(Value::Ext(
                "Time",
                Box::new(Value::String(String::decode(value)?)),
            )),
            DataType::DateTime => Ok(Value::Ext(
                "DateTime",
                Box::new(Value::String(String::decode(value)?)),
            )),
        }
    }
}

impl Encode for Value {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        match self {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::I32(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::I64(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::U32(v) => {
                (v as i64).encode(args)?;
                Ok(IsNull::No)
            }
            Value::U64(v) => {
                let value: i64 = v.try_into().map_err(|_| {
                    Error::from(format!(
                        "u64 value {v} exceeds SQLite's signed INTEGER range"
                    ))
                })?;
                value.encode(args)?;
                Ok(IsNull::No)
            }
            Value::F32(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::F64(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::String(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::Binary(v) => {
                v.encode(args)?;
                Ok(IsNull::No)
            }
            Value::Array(v) => {
                //json
                Value::Array(v).to_string().encode(args)?;
                Ok(IsNull::No)
            }
            Value::Map(v) => {
                //json
                Value::Map(v).to_string().encode(args)?;
                Ok(IsNull::No)
            }
            Value::Ext(t, v) => match t {
                "Date" | "DateTime" | "Time" | "Decimal" | "Uuid" => {
                    v.into_string()
                        .ok_or_else(|| Error::from(format!("invalid {t} value")))?
                        .encode(args)?;
                    Ok(IsNull::No)
                }
                "Timestamp" => {
                    v.as_i64()
                        .ok_or_else(|| Error::from("invalid Timestamp value"))?
                        .encode(args)?;
                    Ok(IsNull::No)
                }
                "Json" => {
                    let text = match *v {
                        Value::String(text) => text,
                        Value::Binary(bytes) => String::from_utf8(bytes)
                            .map_err(|e| Error::from(format!("invalid Json UTF-8: {e}")))?,
                        other => other.to_string(),
                    };
                    text.encode(args)?;
                    Ok(IsNull::No)
                }
                other => Err(Error::from(format!(
                    "unsupported SQLite extension value type: {other}"
                ))),
            },
        }
    }
}

//if is json null/map/array
pub fn is_json_string(js: &str) -> bool {
    if js == "null"
        || js.starts_with("{") && js.ends_with("}")
        || js.starts_with("[") && js.ends_with("]")
    {
        true
    } else {
        false
    }
}
