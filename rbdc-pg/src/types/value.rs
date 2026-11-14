use crate::arguments::PgArgumentBuffer;
use crate::type_info::PgType;
use crate::type_info::PgTypeInfo;
use crate::type_info::PgTypeKind;
use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::types::json::{decode_json, encode_json};
use crate::types::money::Money;
use crate::types::timestamptz::Timestamptz;
use crate::types::timetz::Timetz;
use crate::types::Oid;
use crate::types::TypeInfo;
use crate::value::{PgValueFormat, PgValueRef};
use rbdc::date::Date;
use rbdc::datetime::DateTime;
use rbdc::decimal::Decimal;
use rbdc::json::Json;
use rbdc::timestamp::Timestamp;
use rbdc::types::time::Time;
use rbdc::uuid::Uuid;
use rbdc::Error;
use rbs::Value;
use std::str::FromStr;

impl TypeInfo for Value {
    fn type_info(&self) -> PgTypeInfo {
        match self {
            Value::Null => PgTypeInfo::UNKNOWN,
            Value::Bool(_) => PgTypeInfo::BOOL,
            Value::I32(_) => PgTypeInfo::INT4,
            Value::I64(_) => PgTypeInfo::INT8,
            Value::U32(_) => PgTypeInfo::INT4,
            Value::U64(_) => PgTypeInfo::INT8,
            Value::F32(_) => PgTypeInfo::FLOAT4,
            Value::F64(_) => PgTypeInfo::FLOAT8,
            Value::String(_) => PgTypeInfo::VARCHAR,
            Value::Binary(_) => PgTypeInfo::BYTEA,
            Value::Array(_) => PgTypeInfo::JSON,
            Value::Map(_) => PgTypeInfo::JSON,
            Value::Ext(type_name, _) => {
                match *type_name {
                    "Uuid" => PgTypeInfo::UUID,
                    //decimal = 12345678
                    "Decimal" => PgTypeInfo::NUMERIC,
                    //Date = "1993-02-06"
                    "Date" => PgTypeInfo::DATE,
                    //RFC3339NanoTime = "15:04:05.999999999"
                    "Time" => PgTypeInfo::TIME,
                    //RFC3339 = "2006-01-02 15:04:05.999999"
                    "Timestamp" => PgTypeInfo::TIMESTAMP,
                    "DateTime" => PgTypeInfo::TIMESTAMPTZ,
                    "Bool" => PgTypeInfo::BOOL,
                    "Bytea" => PgTypeInfo::BYTEA,
                    "Char" => PgTypeInfo::CHAR,
                    "Name" => PgTypeInfo::NAME,
                    "Int8" => PgTypeInfo::INT8,
                    "Int2" => PgTypeInfo::INT2,
                    "Int4" => PgTypeInfo::INT4,
                    "Text" => PgTypeInfo::TEXT,
                    "Oid" => PgTypeInfo::OID,
                    "Json" => PgTypeInfo::JSON,
                    "Point" | "point" => PgTypeInfo::POINT,
                    "TsVector" | "tsvector" => PgTypeInfo::TSVECTOR,
                    "TsQuery" | "tsquery" => PgTypeInfo::TSQUERY,
                    "Hstore" | "hstore" => PgTypeInfo::with_name("hstore"),
                    "Lseg" => PgTypeInfo::LSEG,
                    "Path" => PgTypeInfo::PATH,
                    "Box" => PgTypeInfo::BOX,
                    "Polygon" => PgTypeInfo::POLYGON,
                    "Line" => PgTypeInfo::LINE,
                    "Cidr" => PgTypeInfo::CIDR,
                    "Float4" => PgTypeInfo::FLOAT4,
                    "Float8" => PgTypeInfo::FLOAT8,
                    "Unknown" => PgTypeInfo::UNKNOWN,
                    "Circle" => PgTypeInfo::CIRCLE,
                    "Macaddr8" => PgTypeInfo::MACADDR8,
                    "Macaddr" => PgTypeInfo::MACADDR,
                    "Inet" => PgTypeInfo::INET,
                    "Bpchar" => PgTypeInfo::BPCHAR,
                    "Varchar" => PgTypeInfo::VARCHAR,
                    "Timestamptz" => PgTypeInfo::TIMESTAMPTZ,
                    "Interval" => PgTypeInfo::INTERVAL,
                    "Timetz" => PgTypeInfo::TIMETZ,
                    "Bit" => PgTypeInfo::BIT,
                    "Varbit" => PgTypeInfo::VARBIT,
                    "Numeric" => PgTypeInfo::NUMERIC,
                    "Record" => PgTypeInfo::RECORD,
                    "Jsonb" => PgTypeInfo::JSONB,
                    "Int4Range" => PgTypeInfo::INT4_RANGE,
                    "NumRange" => PgTypeInfo::NUM_RANGE,
                    "TsRange" => PgTypeInfo::TS_RANGE,
                    "TstzRange" => PgTypeInfo::TSTZ_RANGE,
                    "DateRange" => PgTypeInfo::DATE_RANGE,
                    "Int8Range" => PgTypeInfo::INT8_RANGE,
                    "Jsonpath" => PgTypeInfo::JSONPATH,
                    "Money" => PgTypeInfo::MONEY,
                    "Void" => PgTypeInfo::VOID,
                    "Custom" => PgTypeInfo::UNKNOWN,
                    "DeclareWithName" => PgTypeInfo::UNKNOWN,
                    "DeclareWithOid" => PgTypeInfo::UNKNOWN,
                    _ => PgTypeInfo::UNKNOWN,
                }
            }
        }
    }
}

impl Decode for Value {
    fn decode(arg: PgValueRef<'_>) -> Result<Self, Error> {
        if arg.value.is_none() {
            return Ok(Value::Null);
        }
        Ok(match arg.type_info().0 {
            PgType::Bool => Value::Bool(Decode::decode(arg)?),
            PgType::Bytea => Value::Binary(Vec::<u8>::decode(arg)?), //Vec<u8> is Bytea
            PgType::Char => Value::String(Decode::decode(arg)?),
            PgType::Name => Value::String(Decode::decode(arg)?),
            PgType::Int8 => Value::I64(Decode::decode(arg)?),
            PgType::Int2 => Value::I32({
                let i16: i16 = Decode::decode(arg)?;
                i16 as i32
            }),
            PgType::Int4 => Value::I32(Decode::decode(arg)?),
            PgType::Text => Value::String(Decode::decode(arg)?),
            PgType::Oid => Value::Ext("Oid", Box::new(Value::U32(Decode::decode(arg)?))),
            PgType::Json => decode_json(arg)?,
            PgType::Point => {
                use crate::types::point::Point;
                Point::decode(arg)?.into()
            }
            PgType::Lseg => Value::Ext(
                "Lseg",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Path => Value::Ext(
                "Path",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Box => Value::Ext(
                "Box",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Polygon => Value::Ext(
                "Polygon",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Line => Value::Ext(
                "Line",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Cidr => Value::Ext(
                "Cidr",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),

            PgType::Float4 => Value::F32(Decode::decode(arg)?),
            PgType::Float8 => Value::F64(Decode::decode(arg)?),
            PgType::Unknown => Value::String(match arg.format() {
                PgValueFormat::Text => arg.as_str()?.to_string(),
                PgValueFormat::Binary => String::from_utf8_lossy(arg.as_bytes()?).into_owned(),
            }),
            PgType::Circle => Value::Ext(
                "Circle",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Macaddr8 => Value::Ext(
                "Macaddr8",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Macaddr => Value::Ext(
                "Macaddr",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Inet => Value::Ext(
                "Inet",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Bpchar | PgType::Varchar => Value::String(Decode::decode(arg)?),
            PgType::Date => {
                let v: Date = Decode::decode(arg)?;
                v
            }
            .into(),
            PgType::Time => {
                let v: Time = Decode::decode(arg)?;
                v
            }
            .into(),
            PgType::Timestamp => {
                let v: Timestamp = Decode::decode(arg)?;
                v
            }
            .into(),
            PgType::Timestamptz => Timestamptz::decode(arg)?.into(),
            PgType::Interval => Value::Ext(
                "Interval",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Timetz => Timetz::decode(arg)?.into(),
            PgType::Bit => Value::Ext(
                "Bit",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Varbit => Value::Ext(
                "Varbit",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Numeric => Decimal::decode(arg)?.into(),
            PgType::Record => Value::Ext(
                "Record",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Uuid => Uuid::decode(arg)?.into(),
            PgType::Jsonb => decode_json(arg)?,
            PgType::Tsvector => match arg.format() {
                PgValueFormat::Text => {
                    use crate::types::tsvector::TsVector;
                    TsVector::decode(arg)?.into()
                }
                PgValueFormat::Binary => Value::Ext(
                    "TsVector",
                    Box::new(Value::Binary(arg.as_bytes()?.to_vec())),
                ),
            },
            PgType::Tsquery => match arg.format() {
                PgValueFormat::Text => {
                    use crate::types::tsquery::TsQuery;
                    TsQuery::decode(arg)?.into()
                }
                PgValueFormat::Binary => {
                    Value::Ext("TsQuery", Box::new(Value::Binary(arg.as_bytes()?.to_vec())))
                }
            },
            PgType::Hstore => {
                use crate::types::hstore::Hstore;
                Hstore::decode(arg)?.into()
            }
            PgType::HstoreArray => Value::Array(Decode::decode(arg)?),
            PgType::Int4Range => Value::Ext(
                "Int4Range",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::NumRange => Value::Ext(
                "NumRange",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::TsRange => Value::Ext(
                "TsRange",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::TstzRange => Value::Ext(
                "TstzRange",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::DateRange => Value::Ext(
                "DateRange",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Int8Range => Value::Ext(
                "Int8Range",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Jsonpath => Value::Ext(
                "Jsonpath",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::Money => Money::decode(arg)?.into(),
            PgType::Void => Value::Null,
            PgType::Custom(ref pg_custom_type) => match &pg_custom_type.kind {
                PgTypeKind::Enum(_) => Value::String(Decode::decode(arg)?),
                PgTypeKind::Array(_) => Value::Array(Decode::decode(arg)?),
                _ if &*pg_custom_type.name == "hstore" => {
                    use crate::types::hstore::Hstore;
                    Hstore::decode(arg)?.into()
                }
                _ => Value::Ext(
                    "Custom",
                    Box::new(Value::Binary({
                        match arg.format() {
                            PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                            PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                        }
                    })),
                ),
            },
            PgType::DeclareWithName(_) => Value::Ext(
                "DeclareWithName",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::DeclareWithOid(_) => Value::Ext(
                "DeclareWithOid",
                Box::new(Value::Binary({
                    match arg.format() {
                        PgValueFormat::Binary => arg.as_bytes()?.to_owned(),
                        PgValueFormat::Text => arg.as_str()?.as_bytes().to_vec(),
                    }
                })),
            ),
            PgType::JsonArray => Value::Array(Decode::decode(arg)?),
            PgType::LineArray => Value::Array(Decode::decode(arg)?),
            PgType::CidrArray => Value::Array(Decode::decode(arg)?),
            PgType::CircleArray => Value::Array(Decode::decode(arg)?),
            PgType::Macaddr8Array => Value::Array(Decode::decode(arg)?),
            PgType::BoolArray => Value::Array(Decode::decode(arg)?),
            PgType::ByteaArray => Value::Array(Decode::decode(arg)?),
            PgType::CharArray => Value::Array(Decode::decode(arg)?),
            PgType::NameArray => Value::Array(Decode::decode(arg)?),
            PgType::Int2Array => Value::Array(Decode::decode(arg)?),
            PgType::Int4Array => Value::Array(Decode::decode(arg)?),
            PgType::TextArray => Value::Array(Decode::decode(arg)?),
            PgType::BpcharArray => Value::Array(Decode::decode(arg)?),
            PgType::VarcharArray => Value::Array(Decode::decode(arg)?),
            PgType::Int8Array => Value::Array(Decode::decode(arg)?),
            PgType::PointArray => Value::Array(Decode::decode(arg)?),
            PgType::LsegArray => Value::Array(Decode::decode(arg)?),
            PgType::PathArray => Value::Array(Decode::decode(arg)?),
            PgType::BoxArray => Value::Array(Decode::decode(arg)?),
            PgType::Float4Array => Value::Array(Decode::decode(arg)?),
            PgType::Float8Array => Value::Array(Decode::decode(arg)?),
            PgType::PolygonArray => Value::Array(Decode::decode(arg)?),
            PgType::OidArray => Value::Array(Decode::decode(arg)?),
            PgType::MacaddrArray => Value::Array(Decode::decode(arg)?),
            PgType::InetArray => Value::Array(Decode::decode(arg)?),
            PgType::TimestampArray => Value::Array(Decode::decode(arg)?),
            PgType::DateArray => Value::Array(Decode::decode(arg)?),
            PgType::TimeArray => Value::Array(Decode::decode(arg)?),
            PgType::TimestamptzArray => Value::Array(Decode::decode(arg)?),
            PgType::IntervalArray => Value::Array(Decode::decode(arg)?),
            PgType::NumericArray => Value::Array(Decode::decode(arg)?),
            PgType::TimetzArray => Value::Array(Decode::decode(arg)?),
            PgType::BitArray => Value::Array(Decode::decode(arg)?),
            PgType::VarbitArray => Value::Array(Decode::decode(arg)?),
            PgType::RecordArray => Value::Array(Decode::decode(arg)?),
            PgType::UuidArray => Value::Array(Decode::decode(arg)?),
            PgType::JsonbArray => Value::Array(Decode::decode(arg)?),
            PgType::Int4RangeArray => Value::Array(Decode::decode(arg)?),
            PgType::NumRangeArray => Value::Array(Decode::decode(arg)?),
            PgType::TsRangeArray => Value::Array(Decode::decode(arg)?),
            PgType::TstzRangeArray => Value::Array(Decode::decode(arg)?),
            PgType::DateRangeArray => Value::Array(Decode::decode(arg)?),
            PgType::Int8RangeArray => Value::Array(Decode::decode(arg)?),
            PgType::JsonpathArray => Value::Array(Decode::decode(arg)?),
            PgType::MoneyArray => Value::Array(Decode::decode(arg)?),
        })
    }
}

impl Encode for Value {
    fn encode(self, buf: &mut PgArgumentBuffer) -> Result<IsNull, Error> {
        Ok(match self {
            Value::Null => IsNull::Yes,
            Value::Bool(v) => v.encode(buf)?,
            Value::I32(v) => v.encode(buf)?,
            Value::I64(v) => v.encode(buf)?,
            Value::U32(v) => v.encode(buf)?,
            Value::U64(v) => v.encode(buf)?,
            Value::F32(v) => v.encode(buf)?,
            Value::F64(v) => v.encode(buf)?,
            Value::String(v) => {
                //default -> string
                v.encode(buf)?
            }
            Value::Binary(v) => v.encode(buf)?,
            Value::Array(v) => encode_json(Value::Array(v), buf)?,
            Value::Map(v) => encode_json(Value::Map(v), buf)?,
            Value::Ext(type_name, v) => {
                match type_name {
                    "Uuid" => Uuid(v.into_string().unwrap_or_default()).encode(buf)?,
                    //decimal = 12345678
                    "Decimal" => Decimal::from_str(v.as_str().unwrap_or_default())
                        .unwrap_or_default()
                        .encode(buf)?,
                    //Date = "1993-02-06"
                    "Date" => Date(
                        fastdate::Date::from_str(&v.into_string().unwrap_or_default())
                            .map_err(|e| Error::from(e.to_string()))?,
                    )
                    .encode(buf)?,
                    //RFC3339NanoTime = "15:04:05.999999999"
                    "Time" => Time(
                        fastdate::Time::from_str(&v.into_string().unwrap_or_default())
                            .map_err(|e| Error::from(e.to_string()))?,
                    )
                    .encode(buf)?,
                    //RFC3339 = "2006-01-02 15:04:05.999999"
                    "Timestamp" => Timestamp(v.as_i64().unwrap_or_default()).encode(buf)?,
                    "DateTime" => DateTime(
                        fastdate::DateTime::from_str(&v.into_string().unwrap_or_default())
                            .map_err(|e| Error::from(e.to_string()))?,
                    )
                    .encode(buf)?,
                    "Bytea" => v.as_slice().unwrap_or_default().encode(buf)?,
                    "Char" => v.into_string().unwrap_or_default().encode(buf)?,
                    "Name" => v.into_string().unwrap_or_default().encode(buf)?,
                    "Int8" => v.as_i64().unwrap_or_default().encode(buf)?,
                    "Int2" => (v.as_i64().unwrap_or_default() as i16).encode(buf)?,
                    "Int4" => (v.as_i64().unwrap_or_default() as i32).encode(buf)?,
                    "Text" => v.into_string().unwrap_or_default().encode(buf)?,
                    "Oid" => Oid::from(v.as_u64().unwrap_or_default() as u32).encode(buf)?,
                    "Json" => Json(v.into_string().unwrap_or_default()).encode(buf)?,
                    "Lseg" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Path" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Box" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Polygon" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Line" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Cidr" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Float4" => (v.as_f64().unwrap_or_default() as f32).encode(buf)?,
                    "Float8" => v.as_f64().unwrap_or_default().encode(buf)?,
                    "Unknown" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Circle" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Macaddr8" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Macaddr" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Inet" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Bpchar" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Varchar" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Timestamptz" => {
                        let tz: Timestamptz = rbs::from_value(Value::Ext(type_name, v))?;
                        tz.encode(buf)?
                    }
                    "Interval" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Timetz" => {
                        Timetz(rbs::from_value(*v).map_err(|e| Error::from(e.to_string()))?)
                            .encode(buf)?
                    }
                    "Bit" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Varbit" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Numeric" => Decimal::from_str(v.as_str().unwrap_or_default())
                        .unwrap_or_default()
                        .encode(buf)?,
                    "Record" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Jsonb" => Json(v.into_string().unwrap_or_default()).encode(buf)?,
                    "Int4Range" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "NumRange" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "TsRange" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "TstzRange" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "DateRange" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Int8Range" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Jsonpath" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "Money" => Money(v.as_i64().unwrap_or_default()).encode(buf)?,
                    "Void" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "TsVector" | "tsvector" => {
                        use crate::types::tsvector::TsVector;
                        let text = match *v {
                            Value::String(text) => text,
                            Value::Binary(bytes) => String::from_utf8(bytes)
                                .map_err(|e| Error::from(format!("invalid TsVector text: {e}")))?,
                            _ => return Err(Error::from("Invalid TsVector value")),
                        };
                        TsVector(text).encode(buf)?
                    }
                    "TsQuery" | "tsquery" => {
                        use crate::types::tsquery::TsQuery;
                        let text = match *v {
                            Value::String(text) => text,
                            Value::Binary(bytes) => String::from_utf8(bytes)
                                .map_err(|e| Error::from(format!("invalid TsQuery text: {e}")))?,
                            _ => return Err(Error::from("Invalid TsQuery value")),
                        };
                        TsQuery(text).encode(buf)?
                    }
                    "Hstore" | "hstore" => {
                        use crate::types::hstore::Hstore;
                        match *v {
                            Value::Binary(bytes) => bytes.encode(buf)?,
                            Value::Map(values) => {
                                let map = values
                                    .into_iter()
                                    .map(|(key, value)| {
                                        (
                                            key.into_string().unwrap_or_default(),
                                            value.into_string().unwrap_or_default(),
                                        )
                                    })
                                    .collect();
                                Hstore(map).encode(buf)?
                            }
                            Value::String(s) => {
                                let mut map = std::collections::HashMap::new();
                                for pair in s.split(',') {
                                    let pair = pair.trim();
                                    if let Some(pos) = pair.find("=>") {
                                        let key = pair[..pos].trim().to_string();
                                        let value = pair[pos + 2..].trim().to_string();
                                        map.insert(key, value);
                                    }
                                }
                                Hstore(map).encode(buf)?
                            }
                            _ => return Err(Error::from("Invalid HSTORE value")),
                        }
                    }
                    "Point" | "point" => {
                        use crate::types::point::Point;
                        match *v {
                            Value::Array(values) if values.len() == 2 => Point {
                                x: values[0].as_f64().unwrap_or_default(),
                                y: values[1].as_f64().unwrap_or_default(),
                            }
                            .encode(buf)?,
                            Value::String(s) => Point::from_str(&s)
                                .map_err(|e| Error::from(e.to_string()))?
                                .encode(buf)?,
                            _ => return Err(Error::from("Invalid POINT value")),
                        }
                    }
                    "Custom" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "DeclareWithName" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    "DeclareWithOid" => v.into_bytes().unwrap_or_default().encode(buf)?,
                    _ => IsNull::Yes,
                }
            }
        })
    }
}
