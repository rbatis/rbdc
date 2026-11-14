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
    Uuid(u128),
    Bit(Vec<u8>),
    DateTime(String),
    Enum(String),
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
                    args.push(DuckDbArgumentValue::DateTime(
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
                    let text = v.into_string().unwrap_or_default();
                    args.push(DuckDbArgumentValue::Uuid(parse_uuid(&text)?));
                    Ok(IsNull::No)
                }
                "Bit" => {
                    let text = v.into_string().unwrap_or_default();
                    args.push(DuckDbArgumentValue::Bit(parse_bit_string(&text)?));
                    Ok(IsNull::No)
                }
                "Enum" => {
                    args.push(DuckDbArgumentValue::Enum(
                        v.into_string().unwrap_or_default(),
                    ));
                    Ok(IsNull::No)
                }
                "TimestampTz" => {
                    args.push(DuckDbArgumentValue::DateTime(
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

fn parse_uuid(text: &str) -> Result<u128, Error> {
    let compact: String = text.chars().filter(|c| *c != '-').collect();
    if compact.len() != 32 {
        return Err(Error::from(format!("invalid UUID value: {text}")));
    }
    u128::from_str_radix(&compact, 16)
        .map_err(|e| Error::from(format!("invalid UUID value {text:?}: {e}")))
}

fn parse_bit_string(text: &str) -> Result<Vec<u8>, Error> {
    if text.is_empty() || !text.bytes().all(|b| b == b'0' || b == b'1') {
        return Err(Error::from(format!("invalid BIT value: {text}")));
    }

    let padding = (8 - text.len() % 8) % 8;
    let first_len = text.len() % 8;
    let first_len = if first_len == 0 { 8 } else { first_len };
    let mut data = Vec::with_capacity(1 + text.len().div_ceil(8));
    data.push(padding as u8);

    let bytes = text.as_bytes();
    let (first, rest) = bytes.split_at(first_len);
    let mut first_byte = 0u8;
    for bit in first {
        first_byte = (first_byte << 1) | u8::from(*bit == b'1');
    }
    if padding > 0 {
        first_byte |= 0xFF << (8 - padding);
    }
    data.push(first_byte);
    for chunk in rest.chunks(8) {
        let mut byte = 0u8;
        for bit in chunk {
            byte = (byte << 1) | u8::from(*bit == b'1');
        }
        data.push(byte);
    }
    Ok(data)
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
                "DateTime" | "TimestampTz" => DuckDbArgumentValue::DateTime(s),
                "Timestamp" => {
                    if let Ok(n) = s.parse::<i64>() {
                        DuckDbArgumentValue::Int64(n)
                    } else {
                        DuckDbArgumentValue::Text(s)
                    }
                }
                "Decimal" => DuckDbArgumentValue::Text(s),
                "Json" => DuckDbArgumentValue::Text(s),
                "Uuid" => parse_uuid(&s)
                    .map(DuckDbArgumentValue::Uuid)
                    .unwrap_or(DuckDbArgumentValue::Text(s)),
                "Bit" => parse_bit_string(&s)
                    .map(DuckDbArgumentValue::Bit)
                    .unwrap_or(DuckDbArgumentValue::Text(s)),
                "Enum" => DuckDbArgumentValue::Enum(s),
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

fn hugeint_to_uhugeint(value: libduckdb_sys::duckdb_hugeint) -> libduckdb_sys::duckdb_uhugeint {
    libduckdb_sys::duckdb_uhugeint {
        lower: value.lower,
        upper: (value.upper as u64).wrapping_add(i64::MAX as u64 + 1),
    }
}

fn read_string_t(
    data: *mut std::ffi::c_void,
    row_idx: usize,
) -> Option<(Vec<u8>, libduckdb_sys::duckdb_string_t)> {
    if data.is_null() {
        return None;
    }
    let mut value = unsafe { *(data as *const libduckdb_sys::duckdb_string_t).add(row_idx) };
    let len = unsafe { libduckdb_sys::duckdb_string_t_length(value) } as usize;
    let ptr = unsafe { libduckdb_sys::duckdb_string_t_data(&mut value) } as *const u8;
    if ptr.is_null() {
        return None;
    }
    Some((unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec(), value))
}

fn bit_bytes_to_string(bytes: &[u8]) -> String {
    if bytes.len() < 2 {
        return String::new();
    }
    let padding = bytes[0] as usize;
    if padding > 7 {
        return String::new();
    }
    let bit_count = (bytes.len() - 1) * 8 - padding;
    let mut result = String::with_capacity(bit_count);
    for bit in padding..8 {
        result.push(if bytes[1] & (1 << (7 - bit)) != 0 {
            '1'
        } else {
            '0'
        });
    }
    for byte in &bytes[2..] {
        for bit in (0..8).rev() {
            result.push(if byte & (1 << bit) != 0 { '1' } else { '0' });
        }
    }
    result
}

fn result_vector(
    result: &mut libduckdb_sys::duckdb_result,
    row_idx: usize,
    col_idx: usize,
) -> Option<(
    libduckdb_sys::duckdb_data_chunk,
    libduckdb_sys::duckdb_vector,
    usize,
)> {
    let vector_size = unsafe { libduckdb_sys::duckdb_vector_size() } as usize;
    if vector_size == 0 {
        return None;
    }
    let chunk_idx = row_idx / vector_size;
    let row_in_chunk = row_idx % vector_size;
    let chunk = unsafe { libduckdb_sys::duckdb_result_get_chunk(*result, chunk_idx as u64) };
    if chunk.is_null() {
        return None;
    }
    let vector = unsafe { libduckdb_sys::duckdb_data_chunk_get_vector(chunk, col_idx as u64) };
    if vector.is_null() {
        let mut chunk = chunk;
        unsafe { libduckdb_sys::duckdb_destroy_data_chunk(&mut chunk) };
        return None;
    }
    Some((chunk, vector, row_in_chunk))
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

fn value_to_string(mut value: libduckdb_sys::duckdb_value) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let ptr = unsafe { libduckdb_sys::duckdb_value_to_string(value) };
    let text = if ptr.is_null() {
        None
    } else {
        let text = unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { libduckdb_sys::duckdb_free(ptr as *mut std::ffi::c_void) };
        Some(text)
    };
    unsafe { libduckdb_sys::duckdb_destroy_value(&mut value) };
    text
}

fn uuid_value_to_string(value: libduckdb_sys::duckdb_uhugeint) -> Option<String> {
    let value = unsafe { libduckdb_sys::duckdb_create_uuid(value) };
    value_to_string(value).map(|value| normalize_sql_literal(&value))
}

fn timestamp_tz_value_to_string(value: libduckdb_sys::duckdb_timestamp) -> Option<String> {
    let value = unsafe { libduckdb_sys::duckdb_create_timestamp_tz(value) };
    value_to_string(value).map(|value| normalize_sql_literal(&value))
}

fn enum_value_to_string(
    logical_type: libduckdb_sys::duckdb_logical_type,
    index: u64,
) -> Option<String> {
    let value = unsafe { libduckdb_sys::duckdb_create_enum_value(logical_type, index) };
    value_to_string(value).map(|value| normalize_sql_literal(&value))
}

fn normalize_sql_literal(value: &str) -> String {
    let value = value.trim();
    if let Some(rest) = value.strip_prefix('\'') {
        if let Some(end) = rest.rfind('\'') {
            return rest[..end].replace("''", "'");
        }
    }
    value.to_string()
}

fn sql_string(value: libduckdb_sys::duckdb_value) -> Option<String> {
    value_to_string(value).map(|value| normalize_sql_literal(&value))
}

fn i64_to_hugeint(value: i64) -> libduckdb_sys::duckdb_hugeint {
    libduckdb_sys::duckdb_hugeint {
        lower: value as u64,
        upper: if value < 0 { -1 } else { 0 },
    }
}

fn extract_row_values_chunk(
    result: &mut libduckdb_sys::duckdb_result,
    row_idx: usize,
    col_count: usize,
) -> Option<Vec<Value>> {
    let vector_size = unsafe { libduckdb_sys::duckdb_vector_size() } as usize;
    if vector_size == 0 {
        return None;
    }
    let mut chunk = unsafe {
        libduckdb_sys::duckdb_result_get_chunk(*result, (row_idx / vector_size) as u64)
    };
    if chunk.is_null() {
        return None;
    }
    let local_row = row_idx % vector_size;
    let mut values = Vec::with_capacity(col_count);

    for col_idx in 0..col_count {
        let vector = unsafe {
            libduckdb_sys::duckdb_data_chunk_get_vector(chunk, col_idx as u64)
        };
        if vector.is_null() {
            values.push(Value::Null);
            continue;
        }
        let mut logical_type = unsafe { libduckdb_sys::duckdb_vector_get_column_type(vector) };
        let ty = DuckDbType::from_duckdb_type(unsafe {
            libduckdb_sys::duckdb_get_type_id(logical_type)
        });
        let validity = unsafe { libduckdb_sys::duckdb_vector_get_validity(vector) };
        if !validity.is_null()
            && !unsafe {
                libduckdb_sys::duckdb_validity_row_is_valid(validity, local_row as u64)
            }
        {
            unsafe { libduckdb_sys::duckdb_destroy_logical_type(&mut logical_type) };
            values.push(Value::Null);
            continue;
        }

        let data = unsafe { libduckdb_sys::duckdb_vector_get_data(vector) };
        let value = match ty {
            DuckDbType::TinyInt => Value::I32(unsafe {
                *(data as *const i8).add(local_row) as i32
            }),
            DuckDbType::SmallInt => Value::I32(unsafe {
                *(data as *const i16).add(local_row) as i32
            }),
            DuckDbType::Int => Value::I32(unsafe { *(data as *const i32).add(local_row) }),
            DuckDbType::BigInt => Value::I64(unsafe { *(data as *const i64).add(local_row) }),
            DuckDbType::UTinyInt => Value::U32(unsafe {
                *(data as *const u8).add(local_row) as u32
            }),
            DuckDbType::USmallInt => Value::U32(unsafe {
                *(data as *const u16).add(local_row) as u32
            }),
            DuckDbType::UInt => Value::U64(unsafe {
                *(data as *const u32).add(local_row) as u64
            }),
            DuckDbType::UBigInt => Value::U64(unsafe { *(data as *const u64).add(local_row) }),
            DuckDbType::Float => Value::F32(unsafe { *(data as *const f32).add(local_row) }),
            DuckDbType::Double => Value::F64(unsafe { *(data as *const f64).add(local_row) }),
            DuckDbType::Boolean => Value::Bool(unsafe {
                *(data as *const bool).add(local_row)
            }),
            DuckDbType::Varchar => read_string_t(data, local_row)
                .and_then(|(bytes, _)| String::from_utf8(bytes).ok())
                .map(varchar_to_value)
                .unwrap_or(Value::Null),
            DuckDbType::Blob => read_string_t(data, local_row)
                .map(|(bytes, _)| Value::Binary(bytes))
                .unwrap_or(Value::Null),
            DuckDbType::Bit => read_string_t(data, local_row)
                .map(|(bytes, _)| {
                    Value::Ext("Bit", Box::new(Value::String(bit_bytes_to_string(&bytes))))
                })
                .unwrap_or(Value::Null),
            DuckDbType::UUID => {
                let uuid = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_hugeint).add(local_row),
                    )
                };
                uuid_value_to_string(hugeint_to_uhugeint(uuid))
                    .map(|s| Value::Ext("Uuid", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::Enum => {
                let internal = unsafe { libduckdb_sys::duckdb_enum_internal_type(logical_type) };
                let index = unsafe {
                    match internal {
                        libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                            Some(*(data as *const u8).add(local_row) as u64)
                        }
                        libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                            Some(*(data as *const u16).add(local_row) as u64)
                        }
                        libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
                            Some(*(data as *const u32).add(local_row) as u64)
                        }
                        libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                            Some(*(data as *const u64).add(local_row))
                        }
                        _ => None,
                    }
                };
                index
                    .and_then(|index| enum_value_to_string(logical_type, index))
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            }
            DuckDbType::Date => {
                let date = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_date).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_date(date) })
                    .map(|s| Value::Ext("Date", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::Time | DuckDbType::TimeTz => {
                let time = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_time).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_time(time) })
                    .map(|s| Value::Ext("Time", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::Timestamp
            | DuckDbType::TimestampS
            | DuckDbType::TimestampMs
            | DuckDbType::TimestampNs => {
                let timestamp = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_timestamp).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_timestamp(timestamp) })
                    .map(|s| Value::Ext("Timestamp", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::TimestampTz => {
                let timestamp = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_timestamp).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_timestamp_tz(timestamp) })
                    .map(|s| Value::Ext("DateTime", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::Interval => {
                let interval = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_interval).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_interval(interval) })
                    .map(|s| Value::Ext("Interval", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::Decimal => {
                let width = unsafe { libduckdb_sys::duckdb_decimal_width(logical_type) };
                let scale = unsafe { libduckdb_sys::duckdb_decimal_scale(logical_type) };
                let value = match width {
                    0..=4 => i64_to_hugeint(unsafe {
                        *(data as *const i16).add(local_row) as i64
                    }),
                    5..=9 => i64_to_hugeint(unsafe {
                        *(data as *const i32).add(local_row) as i64
                    }),
                    10..=18 => i64_to_hugeint(unsafe {
                        *(data as *const i64).add(local_row)
                    }),
                    _ => unsafe {
                        std::ptr::read_unaligned(
                            (data as *const libduckdb_sys::duckdb_hugeint).add(local_row),
                        )
                    },
                };
                sql_string(unsafe {
                    libduckdb_sys::duckdb_create_decimal(libduckdb_sys::duckdb_decimal {
                        width,
                        scale,
                        value,
                    })
                })
                .map(|s| Value::Ext("Decimal", Box::new(Value::String(s))))
                .unwrap_or(Value::Null)
            }
            DuckDbType::HugeInt => {
                let value = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_hugeint).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_hugeint(value) })
                    .map(|s| Value::Ext("HugeInt", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            DuckDbType::UHugeInt => {
                let value = unsafe {
                    std::ptr::read_unaligned(
                        (data as *const libduckdb_sys::duckdb_uhugeint).add(local_row),
                    )
                };
                sql_string(unsafe { libduckdb_sys::duckdb_create_uhugeint(value) })
                    .map(|s| Value::Ext("UHugeInt", Box::new(Value::String(s))))
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        };
        unsafe { libduckdb_sys::duckdb_destroy_logical_type(&mut logical_type) };
        values.push(value);
    }

    unsafe { libduckdb_sys::duckdb_destroy_data_chunk(&mut chunk) };
    Some(values)
}
pub fn extract_row_values(
    result: &mut libduckdb_sys::duckdb_result,
    row_idx: usize,
    col_count: usize,
) -> Vec<Value> {
    if let Some(values) = extract_row_values_chunk(result, row_idx, col_count) {
        return values;
    }
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
            DuckDbType::Enum => {
                let mut logical_type =
                    unsafe { libduckdb_sys::duckdb_column_logical_type(result, col_idx as u64) };
                let data = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) };
                let internal = unsafe { libduckdb_sys::duckdb_enum_internal_type(logical_type) };
                let index = if data.is_null() {
                    None
                } else {
                    unsafe {
                        match internal {
                            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                                Some(*(data as *const u8).add(row_idx) as u64)
                            }
                            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                                Some(*(data as *const u16).add(row_idx) as u64)
                            }
                            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
                                Some(*(data as *const u32).add(row_idx) as u64)
                            }
                            libduckdb_sys::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                                Some(*(data as *const u64).add(row_idx))
                            }
                            _ => None,
                        }
                    }
                };
                let value = index
                    .and_then(|index| enum_value_to_string(logical_type, index))
                    .map(Value::String)
                    .unwrap_or(Value::Null);
                unsafe { libduckdb_sys::duckdb_destroy_logical_type(&mut logical_type) };
                values.push(value);
            }
            DuckDbType::UUID => {
                values.push(if let Some((mut chunk, vector, local_row)) =
                    result_vector(result, row_idx, col_idx)
                {
                    let data = unsafe { libduckdb_sys::duckdb_vector_get_data(vector) }
                        as *const libduckdb_sys::duckdb_hugeint;
                    let value = if data.is_null() {
                        Value::Null
                    } else {
                        let uuid = unsafe { std::ptr::read_unaligned(data.add(local_row)) };
                        uuid_value_to_string(hugeint_to_uhugeint(uuid))
                            .map(|s| Value::Ext("Uuid", Box::new(Value::String(s))))
                            .unwrap_or(Value::Null)
                    };
                    unsafe { libduckdb_sys::duckdb_destroy_data_chunk(&mut chunk) };
                    value
                } else {
                    Value::Null
                });
            }
            DuckDbType::Bit => {
                values.push(if let Some((mut chunk, vector, local_row)) =
                    result_vector(result, row_idx, col_idx)
                {
                    let data = unsafe { libduckdb_sys::duckdb_vector_get_data(vector) };
                    let value = read_string_t(data, local_row)
                        .map(|(bytes, _)| {
                            Value::Ext("Bit", Box::new(Value::String(bit_bytes_to_string(&bytes))))
                        })
                        .unwrap_or(Value::Null);
                    unsafe { libduckdb_sys::duckdb_destroy_data_chunk(&mut chunk) };
                    value
                } else {
                    Value::Null
                });
            }
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
            | DuckDbType::TimestampNs => {
                values.push(
                    value_varchar(result, col_idx, row_idx)
                        .map(|s| Value::Ext(ty_to_type_name(&ty), Box::new(Value::String(s))))
                        .unwrap_or(Value::Null),
                );
            }
            DuckDbType::TimestampTz => {
                let ptr = unsafe { libduckdb_sys::duckdb_column_data(result, col_idx as u64) }
                    as *const libduckdb_sys::duckdb_timestamp;
                values.push(if ptr.is_null() {
                    Value::Null
                } else {
                    let timestamp = unsafe { std::ptr::read_unaligned(ptr.add(row_idx)) };
                    timestamp_tz_value_to_string(timestamp)
                        .map(|s| Value::Ext("DateTime", Box::new(Value::String(s))))
                        .unwrap_or(Value::Null)
                });
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

fn varchar_to_value(text: String) -> Value {
    if is_json_string(&text) {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            return value;
        }
    }
    if let Ok(value) = text.parse::<i64>() {
        Value::I64(value)
    } else if let Ok(value) = text.parse::<f64>() {
        Value::F64(value)
    } else {
        Value::String(text)
    }
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

    #[test]
    fn uuid_value_uses_expected_byte_order() {
        let uuid = 0x550e8400_e29b_41d4_a716_4466_5544_0000_u128;
        let value = libduckdb_sys::duckdb_uhugeint {
            lower: uuid as u64,
            upper: (uuid >> 64) as u64,
        };
        assert_eq!(
            uuid_value_to_string(value).unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn bit_encoding_matches_duckdb_padding_format() {
        assert_eq!(parse_bit_string("101").unwrap(), vec![5, 0xFD]);
        assert_eq!(parse_bit_string("10101010").unwrap(), vec![0, 0xAA]);
        assert_eq!(
            parse_bit_string("10101010101").unwrap(),
            vec![5, 0xFD, 0x55]
        );
        assert_eq!(bit_bytes_to_string(&[5, 0xFD]), "101");
        assert_eq!(bit_bytes_to_string(&[0, 0xAA]), "10101010");
        assert_eq!(bit_bytes_to_string(&[5, 0xFD, 0x55]), "10101010101");
    }
}
