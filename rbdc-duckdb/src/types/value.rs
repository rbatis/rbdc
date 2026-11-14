//! Type conversions between rbs::Value and DuckDB.

use rbdc::Error;
use rbs::Value;

pub trait Encode {
    fn encode(self, args: &mut Vec<DuckDbArgumentValue>) -> Result<IsNull, Error>;
}

pub enum IsNull {
    Yes,
    No,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum DuckDbType {
    Null,
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    UTinyInt,
    USmallInt,
    UInt,
    UBigInt,
    Float,
    Double,
    Decimal,
    Timestamp,
    TimestampS,
    TimestampMs,
    TimestampNs,
    TimestampTz,
    Date,
    Time,
    TimeTz,
    Interval,
    HugeInt,
    UHugeInt,
    Varchar,
    Blob,
    Enum,
    List,
    Struct,
    Map,
    Array,
    Union,
    UUID,
    Bit,
    SQLNull,
}

impl<'de> serde::Deserialize<'de> for DuckDbType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <Value as serde::Deserialize>::deserialize(deserializer)?;
        let name = match value {
            Value::String(name) => name,
            Value::Ext(_, inner) => inner.into_string().unwrap_or_default(),
            other => other.to_string(),
        };
        Ok(match name.as_str() {
            "Null" => DuckDbType::Null,
            "Boolean" => DuckDbType::Boolean,
            "TinyInt" => DuckDbType::TinyInt,
            "SmallInt" => DuckDbType::SmallInt,
            "Int" => DuckDbType::Int,
            "BigInt" => DuckDbType::BigInt,
            "UTinyInt" => DuckDbType::UTinyInt,
            "USmallInt" => DuckDbType::USmallInt,
            "UInt" => DuckDbType::UInt,
            "UBigInt" => DuckDbType::UBigInt,
            "Float" => DuckDbType::Float,
            "Double" => DuckDbType::Double,
            "Decimal" => DuckDbType::Decimal,
            "Timestamp" => DuckDbType::Timestamp,
            "TimestampS" => DuckDbType::TimestampS,
            "TimestampMs" => DuckDbType::TimestampMs,
            "TimestampNs" => DuckDbType::TimestampNs,
            "TimestampTz" => DuckDbType::TimestampTz,
            "Date" => DuckDbType::Date,
            "Time" => DuckDbType::Time,
            "TimeTz" => DuckDbType::TimeTz,
            "Interval" => DuckDbType::Interval,
            "HugeInt" => DuckDbType::HugeInt,
            "UHugeInt" => DuckDbType::UHugeInt,
            "Varchar" => DuckDbType::Varchar,
            "Blob" => DuckDbType::Blob,
            "Enum" => DuckDbType::Enum,
            "List" => DuckDbType::List,
            "Struct" => DuckDbType::Struct,
            "Map" => DuckDbType::Map,
            "Array" => DuckDbType::Array,
            "Union" => DuckDbType::Union,
            "UUID" => DuckDbType::UUID,
            "Bit" => DuckDbType::Bit,
            "SQLNull" => DuckDbType::SQLNull,
            _ => return Err(serde::de::Error::unknown_variant(&name, &[])),
        })
    }
}

impl DuckDbType {
    pub fn from_duckdb_type(ty: libduckdb_sys::DUCKDB_TYPE) -> Self {
        match ty {
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_INVALID => DuckDbType::Null,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => DuckDbType::Boolean,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => DuckDbType::TinyInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => DuckDbType::SmallInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => DuckDbType::Int,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => DuckDbType::BigInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => DuckDbType::UTinyInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => DuckDbType::USmallInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => DuckDbType::UInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => DuckDbType::UBigInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => DuckDbType::Float,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => DuckDbType::Double,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => DuckDbType::Decimal,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => DuckDbType::Timestamp,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => DuckDbType::TimestampS,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => DuckDbType::TimestampMs,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => DuckDbType::TimestampNs,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => DuckDbType::TimestampTz,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_DATE => DuckDbType::Date,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIME => DuckDbType::Time,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => DuckDbType::TimeTz,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => DuckDbType::Interval,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => DuckDbType::HugeInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => DuckDbType::UHugeInt,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => DuckDbType::Varchar,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_BLOB => DuckDbType::Blob,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_ENUM => DuckDbType::Enum,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_LIST => DuckDbType::List,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => DuckDbType::Struct,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_MAP => DuckDbType::Map,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => DuckDbType::Array,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UNION => DuckDbType::Union,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UUID => DuckDbType::UUID,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_BIT => DuckDbType::Bit,
            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => DuckDbType::SQLNull,
            _ => DuckDbType::Varchar,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DuckDbArgumentValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    HugeInt(i128),
    UHugeInt(u128),
    Float(f32),
    Double(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Encode for Value {
    fn encode(self, args: &mut Vec<DuckDbArgumentValue>) -> Result<IsNull, Error> {
        match self {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(v) => {
                args.push(DuckDbArgumentValue::Boolean(v));
                Ok(IsNull::No)
            }
            Value::I32(v) => {
                args.push(DuckDbArgumentValue::Int(v));
                Ok(IsNull::No)
            }
            Value::I64(v) => {
                args.push(DuckDbArgumentValue::Int64(v));
                Ok(IsNull::No)
            }
            Value::U32(v) => {
                args.push(DuckDbArgumentValue::UInt32(v));
                Ok(IsNull::No)
            }
            Value::U64(v) => {
                args.push(DuckDbArgumentValue::UInt64(v));
                Ok(IsNull::No)
            }
            Value::F32(v) => {
                args.push(DuckDbArgumentValue::Float(v));
                Ok(IsNull::No)
            }
            Value::F64(v) => {
                args.push(DuckDbArgumentValue::Double(v));
                Ok(IsNull::No)
            }
            Value::String(v) => {
                args.push(DuckDbArgumentValue::Text(v));
                Ok(IsNull::No)
            }
            Value::Binary(v) => {
                args.push(DuckDbArgumentValue::Blob(v));
                Ok(IsNull::No)
            }
            Value::Array(v) => {
                args.push(DuckDbArgumentValue::Text(
                    serde_json::to_string(&v).unwrap_or_default(),
                ));
                Ok(IsNull::No)
            }
            Value::Map(v) => {
                args.push(DuckDbArgumentValue::Text(
                    serde_json::to_string(&v).unwrap_or_default(),
                ));
                Ok(IsNull::No)
            }
            Value::Ext(t, v) => match t {
                "Date" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "DateTime" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "Time" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "Timestamp" => {
                    if let Some(n) = v.as_i64() {
                        args.push(DuckDbArgumentValue::Int64(n));
                    } else {
                        args.push(DuckDbArgumentValue::Text(
                            v.into_string().unwrap_or_default(),
                        ));
                    }
                    Ok(IsNull::No)
                }
                "Decimal" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "Json" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "Uuid" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "Bit" | "Enum" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "TimestampTz" => {
                    args.push(DuckDbArgumentValue::Text(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "HugeInt" => {
                    let n = v
                        .into_string()
                        .unwrap_or_default()
                        .parse()
                        .map_err(|e| Error::from(format!("invalid HugeInt value: {e}")))?;
                    args.push(DuckDbArgumentValue::HugeInt(n));
                    Ok(IsNull::No)
                }
                "UHugeInt" => {
                    let n = v
                        .into_string()
                        .unwrap_or_default()
                        .parse()
                        .map_err(|e| Error::from(format!("invalid UHugeInt value: {e}")))?;
                    args.push(DuckDbArgumentValue::UHugeInt(n));
                    Ok(IsNull::No)
                }
                other => Err(Error::from(format!(
                    "unsupported DuckDB extension value type: {other}"
                ))),
            },
        }
    }
}

fn ty_to_type_name(ty: &DuckDbType) -> &'static str {
    match ty {
        DuckDbType::Date => "Date",
        DuckDbType::Time => "Time",
        DuckDbType::TimeTz => "Time",
        DuckDbType::Timestamp => "Timestamp",
        DuckDbType::TimestampS => "Timestamp",
        DuckDbType::TimestampMs => "Timestamp",
        DuckDbType::TimestampNs => "Timestamp",
        DuckDbType::TimestampTz => "DateTime",
        _ => "String",
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DuckDbTypeInfo(pub DuckDbType);

/// Convert rbs::Value to DuckDB argument value
pub fn value_to_param(v: Value) -> DuckDbArgumentValue {
    match v {
        Value::Null => DuckDbArgumentValue::Null,
        Value::Bool(b) => DuckDbArgumentValue::Boolean(b),
        Value::I32(n) => DuckDbArgumentValue::Int(n),
        Value::I64(n) => DuckDbArgumentValue::Int64(n),
        Value::U32(n) => DuckDbArgumentValue::UInt32(n),
        Value::U64(n) => DuckDbArgumentValue::UInt64(n),
        Value::F32(f) => DuckDbArgumentValue::Float(f),
        Value::F64(f) => DuckDbArgumentValue::Double(f),
        Value::String(s) => DuckDbArgumentValue::Text(s),
        Value::Binary(b) => DuckDbArgumentValue::Blob(b),
        Value::Array(arr) => {
            DuckDbArgumentValue::Text(serde_json::to_string(&arr).unwrap_or_default())
        }
        Value::Map(m) => DuckDbArgumentValue::Text(serde_json::to_string(&m).unwrap_or_default()),
        Value::Ext(type_name, box_value) => {
            let s = box_value.into_string().unwrap_or_default();
            match type_name {
                "Date" => DuckDbArgumentValue::Text(s),
                "Time" => DuckDbArgumentValue::Text(s),
                "DateTime" => DuckDbArgumentValue::Text(s),
                "Timestamp" => {
                    if let Ok(n) = s.parse::<i64>() {
                        DuckDbArgumentValue::Int64(n)
                    } else {
                        DuckDbArgumentValue::Text(s)
                    }
                }
                "Decimal" => DuckDbArgumentValue::Text(s),
                "Json" => DuckDbArgumentValue::Text(s),
                "Uuid" => DuckDbArgumentValue::Text(s),
                "Bit" | "Enum" | "TimestampTz" => DuckDbArgumentValue::Text(s),
                "HugeInt" => s
                    .parse()
                    .map(DuckDbArgumentValue::HugeInt)
                    .unwrap_or(DuckDbArgumentValue::Text(s)),
                "UHugeInt" => s
                    .parse()
                    .map(DuckDbArgumentValue::UHugeInt)
                    .unwrap_or(DuckDbArgumentValue::Text(s)),
                _ => DuckDbArgumentValue::Text(s),
            }
        }
    }
}

/// Extract row values from DuckDB result using FFI
fn value_varchar(
    result: &mut libduckdb_sys::duckdb_result,
    col_idx: usize,
    row_idx: usize,
) -> Option<String> {
    let ptr =
        unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
    if ptr.is_null() {
        return None;
    }
    let value = unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
    Some(value)
}

fn value_blob(
    result: &mut libduckdb_sys::duckdb_result,
    col_idx: usize,
    row_idx: usize,
) -> Option<Vec<u8>> {
    let blob = unsafe { libduckdb_sys::duckdb_value_blob(result, col_idx as u64, row_idx as u64) };
    if blob.data.is_null() {
        return None;
    }
    let bytes =
        unsafe { std::slice::from_raw_parts(blob.data as *const u8, blob.size as usize) }.to_vec();
    unsafe { libduckdb_sys::duckdb_free(blob.data) };
    Some(bytes)
}

pub fn extract_row_values(
    result: &mut libduckdb_sys::duckdb_result,
    row_idx: usize,
    col_count: usize,
) -> Vec<Value> {
    let mut values = Vec::new();
    for col_idx in 0..col_count {
        let col_type = unsafe { libduckdb_sys::duckdb_column_type(result, col_idx as u64) };
        let ty = DuckDbType::from_duckdb_type(col_type);

        // Check if null
        let is_null =
            unsafe { libduckdb_sys::duckdb_value_is_null(result, col_idx as u64, row_idx as u64) };
        if is_null {
            values.push(Value::Null);
            continue;
        }

        match ty {
            DuckDbType::TinyInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const i8;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v as i32));
            }
            DuckDbType::SmallInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const i16;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v as i32));
            }
            DuckDbType::Int => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const i32;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v));
            }
            DuckDbType::BigInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const i64;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I64(v));
            }
            DuckDbType::UTinyInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const u8;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::U32(v as u32));
            }
            DuckDbType::USmallInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const u16;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::U32(v as u32));
            }
            DuckDbType::UInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const u32;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::U64(v as u64));
            }
            DuckDbType::UBigInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const u64;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::U64(v));
            }
            DuckDbType::Float => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const f32;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::F32(v));
            }
            DuckDbType::Double => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const f64;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::F64(v));
            }
            DuckDbType::Boolean => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const i8;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::Bool(v != 0));
            }
            DuckDbType::Varchar => match value_varchar(result, col_idx, row_idx) {
                None => values.push(Value::Null),
                Some(s) => {
                    if is_json_string(&s) {
                        if let Ok(v) = serde_json::from_str::<Value>(&s) {
                            values.push(v);
                            continue;
                        }
                    }
                    if let Ok(n) = s.parse::<i64>() {
                        values.push(Value::I64(n));
                    } else if let Ok(n) = s.parse::<f64>() {
                        values.push(Value::F64(n));
                    } else {
                        values.push(Value::String(s));
                    }
                }
            },
            DuckDbType::Enum => values.push(
                value_varchar(result, col_idx, row_idx)
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            DuckDbType::UUID => values.push(
                value_varchar(result, col_idx, row_idx)
                    .map(|s| Value::Ext("Uuid", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null),
            ),
            DuckDbType::Bit => values.push(
                value_varchar(result, col_idx, row_idx)
                    .map(|s| Value::Ext("Bit", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null),
            ),
            DuckDbType::Blob => {
                values.push(
                    value_blob(result, col_idx, row_idx)
                        .map(Value::Binary)
                        .unwrap_or(Value::Null),
                );
            }
            DuckDbType::Date
            | DuckDbType::Time
            | DuckDbType::TimeTz
            | DuckDbType::Timestamp
            | DuckDbType::TimestampS
            | DuckDbType::TimestampMs
            | DuckDbType::TimestampNs
            | DuckDbType::TimestampTz => {
                values.push(
                    value_varchar(result, col_idx, row_idx)
                        .map(|s| Value::Ext(ty_to_type_name(&ty), Box::new(Value::String(s))))
                        .unwrap_or(Value::Null),
                );
            }
            DuckDbType::Decimal => {
                let ptr = unsafe {
                    libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64)
                };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    values.push(Value::Ext("Decimal", Box::new(Value::String(s))));
                }
            }
            DuckDbType::Interval => {
                let ptr = unsafe {
                    libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64)
                };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    values.push(Value::Ext("Interval", Box::new(Value::String(s))));
                }
            }
            DuckDbType::HugeInt | DuckDbType::UHugeInt => {
                let ptr = unsafe {
                    libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64)
                };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    let tag = match ty {
                        DuckDbType::HugeInt => "HugeInt",
                        DuckDbType::UHugeInt => "UHugeInt",
                        _ => "String",
                    };
                    if tag == "String" {
                        values.push(Value::String(s));
                    } else {
                        values.push(Value::Ext(tag, Box::new(Value::String(s))));
                    }
                }
            }
            DuckDbType::List | DuckDbType::Array | DuckDbType::Struct | DuckDbType::Map => {
                let ptr = unsafe {
                    libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64)
                };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    if let Ok(v) = serde_json::from_str::<Value>(&s) {
                        values.push(v);
                    } else {
                        values.push(Value::String(s));
                    }
                }
            }
            DuckDbType::Union => {
                let ptr = unsafe {
                    libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64)
                };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    values.push(Value::String(s));
                }
            }
            DuckDbType::Null | DuckDbType::SQLNull => {
                values.push(Value::Null);
            }
        }
    }
    values
}

//if is json null/map/array
pub fn is_json_string(js: &str) -> bool {
    js == "null"
        || (js.starts_with('{') && js.ends_with('}'))
        || (js.starts_with('[') && js.ends_with(']'))
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn ty_to_type_name_covers_every_mapped_variant() {
        assert_eq!(ty_to_type_name(&DuckDbType::Date), "Date");
        assert_eq!(ty_to_type_name(&DuckDbType::Time), "Time");
        assert_eq!(ty_to_type_name(&DuckDbType::TimeTz), "Time");
        assert_eq!(ty_to_type_name(&DuckDbType::Timestamp), "Timestamp");
        assert_eq!(ty_to_type_name(&DuckDbType::TimestampS), "Timestamp");
        assert_eq!(ty_to_type_name(&DuckDbType::TimestampMs), "Timestamp");
        assert_eq!(ty_to_type_name(&DuckDbType::TimestampNs), "Timestamp");
        // Everything else falls back to a plain string type name.
        assert_eq!(ty_to_type_name(&DuckDbType::Varchar), "String");
        assert_eq!(ty_to_type_name(&DuckDbType::Int), "String");
    }

    #[test]
    fn type_info_serde_roundtrip() {
        let info = DuckDbTypeInfo(DuckDbType::TimestampTz);
        let value = rbs::to_value(&info).unwrap();
        let decoded: DuckDbTypeInfo = rbs::from_value(value).unwrap();
        assert_eq!(decoded.0, DuckDbType::TimestampTz);
    }
}
