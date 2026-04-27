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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Text(String),
    Blob(Vec<u8>),
    Double(f64),
    Int(i32),
    Int64(i64),
}

impl Encode for Value {
    fn encode(self, args: &mut Vec<DuckDbArgumentValue>) -> Result<IsNull, Error> {
        match self {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(v) => {
                args.push(DuckDbArgumentValue::Int(if v { 1 } else { 0 }));
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
                args.push(DuckDbArgumentValue::Int(v as i32));
                Ok(IsNull::No)
            }
            Value::U64(v) => {
                args.push(DuckDbArgumentValue::Int64(v as i64));
                Ok(IsNull::No)
            }
            Value::F32(v) => {
                args.push(DuckDbArgumentValue::Double(v as f64));
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
                args.push(DuckDbArgumentValue::Text(serde_json::to_string(&v).unwrap_or_default()));
                Ok(IsNull::No)
            }
            Value::Map(v) => {
                args.push(DuckDbArgumentValue::Text(serde_json::to_string(&v).unwrap_or_default()));
                Ok(IsNull::No)
            }
            Value::Ext(t, v) => match &*t {
                "Date" => {
                    args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    Ok(IsNull::No)
                }
                "DateTime" => {
                    args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    Ok(IsNull::No)
                }
                "Time" => {
                    args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    Ok(IsNull::No)
                }
                "Timestamp" => {
                    if let Some(n) = v.as_i64() {
                        args.push(DuckDbArgumentValue::Int64(n));
                    } else {
                        args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    }
                    Ok(IsNull::No)
                }
                "Decimal" => {
                    args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    Ok(IsNull::No)
                }
                "Json" => {
                    args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    Ok(IsNull::No)
                }
                "Uuid" => {
                    args.push(DuckDbArgumentValue::Text(v.into_string().unwrap_or_default()));
                    Ok(IsNull::No)
                }
                _ => Ok(IsNull::Yes),
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
        _ => "String",
    }
}

#[derive(Debug, Clone)]
pub struct DuckDbTypeInfo(pub DuckDbType);

/// Convert rbs::Value to DuckDB argument value
pub fn value_to_param(v: Value) -> DuckDbArgumentValue {
    match v {
        Value::Null => DuckDbArgumentValue::Null,
        Value::Bool(b) => DuckDbArgumentValue::Int(if b { 1 } else { 0 }),
        Value::I32(n) => DuckDbArgumentValue::Int(n),
        Value::I64(n) => DuckDbArgumentValue::Int64(n),
        Value::U32(n) => DuckDbArgumentValue::Int(n as i32),
        Value::U64(n) => DuckDbArgumentValue::Int64(n as i64),
        Value::F32(f) => DuckDbArgumentValue::Double(f as f64),
        Value::F64(f) => DuckDbArgumentValue::Double(f),
        Value::String(s) => DuckDbArgumentValue::Text(s),
        Value::Binary(b) => DuckDbArgumentValue::Blob(b),
        Value::Array(arr) => DuckDbArgumentValue::Text(serde_json::to_string(&arr).unwrap_or_default()),
        Value::Map(m) => DuckDbArgumentValue::Text(serde_json::to_string(&m).unwrap_or_default()),
        Value::Ext(type_name, box_value) => {
            let s = box_value.into_string().unwrap_or_default();
            match &*type_name {
                "Date" => DuckDbArgumentValue::Text(s),
                "Time" => DuckDbArgumentValue::Text(s),
                "DateTime" | "Datetime" => DuckDbArgumentValue::Text(s),
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
                _ => DuckDbArgumentValue::Text(s),
            }
        }
    }
}

/// Extract row values from DuckDB result using FFI
pub fn extract_row_values(result: &mut libduckdb_sys::duckdb_result, row_idx: usize, col_count: usize) -> Vec<Value> {
    let mut values = Vec::new();
    for col_idx in 0..col_count {
        let col_type = unsafe { libduckdb_sys::duckdb_column_type(result, col_idx as u64) };
        let ty = DuckDbType::from_duckdb_type(col_type);

        // Check if null
        let is_null = unsafe { libduckdb_sys::duckdb_value_is_null(result, col_idx as u64, row_idx as u64) };
        if is_null {
            values.push(Value::Null);
            continue;
        }

        match ty {
            DuckDbType::TinyInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const i8;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v as i32));
            }
            DuckDbType::SmallInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const i16;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v as i32));
            }
            DuckDbType::Int => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const i32;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v));
            }
            DuckDbType::BigInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const i64;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I64(v));
            }
            DuckDbType::UTinyInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const u8;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v as i32));
            }
            DuckDbType::USmallInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const u16;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I32(v as i32));
            }
            DuckDbType::UInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const u32;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I64(v as i64));
            }
            DuckDbType::UBigInt => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const u64;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::I64(v as i64));
            }
            DuckDbType::Float => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const f32;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::F64(v as f64));
            }
            DuckDbType::Double => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const f64;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::F64(v));
            }
            DuckDbType::Boolean => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) } as *const i8;
                let v = unsafe { *ptr.add(row_idx) };
                values.push(Value::Bool(v != 0));
            }
            DuckDbType::Varchar | DuckDbType::Enum | DuckDbType::UUID | DuckDbType::Bit => {
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };

                    // Try to parse as JSON first
                    if is_json_string(&s) {
                        if let Ok(v) = serde_json::from_str::<Value>(&s) {
                            values.push(v);
                            continue;
                        }
                    }

                    // Try to parse as number
                    if let Ok(n) = s.parse::<i64>() {
                        values.push(Value::I64(n));
                    } else if let Ok(n) = s.parse::<f64>() {
                        values.push(Value::F64(n));
                    } else {
                        values.push(Value::String(s));
                    }
                }
            }
            DuckDbType::Blob => {
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
                if ptr.is_null() {
                    values.push(Value::Binary(vec![]));
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    values.push(Value::Binary(s.into_bytes()));
                }
            }
            DuckDbType::Date | DuckDbType::Time | DuckDbType::TimeTz | DuckDbType::Timestamp
            | DuckDbType::TimestampS | DuckDbType::TimestampMs | DuckDbType::TimestampNs => {
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    values.push(Value::Ext(ty_to_type_name(&ty), Box::new(Value::String(s))));
                }
            }
            DuckDbType::Decimal => {
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
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
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
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
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
                if ptr.is_null() {
                    values.push(Value::Null);
                } else {
                    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    let s = c_str.to_string_lossy().into_owned();
                    unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
                    values.push(Value::String(s));
                }
            }
            DuckDbType::List | DuckDbType::Array | DuckDbType::Struct | DuckDbType::Map => {
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
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
                let ptr = unsafe { libduckdb_sys::duckdb_value_varchar(result, col_idx as u64, row_idx as u64) };
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