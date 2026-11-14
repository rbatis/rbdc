use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::type_info::DataType;
use crate::types::Type;
use crate::{SqliteArgumentValue, SqliteTypeInfo, SqliteValueRef};
use rbdc::error::Error;

impl Type for u8 {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Int)
    }
}

impl Encode for u8 {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Int(self as i32));

        Ok(IsNull::No)
    }
}

impl Decode for u8 {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(value.to_owned().int().try_into()?)
    }
}

impl Type for u16 {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Int)
    }
}

impl Encode for u16 {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Int(self as i32));

        Ok(IsNull::No)
    }
}

impl Decode for u16 {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(value.to_owned().int().try_into()?)
    }
}

impl Type for u32 {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Int64)
    }
}

impl Encode for u32 {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Int64(self as i64));

        Ok(IsNull::No)
    }
}

impl Decode for u32 {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(value.to_owned().int64().try_into()?)
    }
}

impl Type for u64 {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Int64)
    }
}

impl Encode for u64 {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        let value: i64 = self.try_into()?;
        args.push(SqliteArgumentValue::Int64(value));

        Ok(IsNull::No)
    }
}

impl Decode for u64 {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(value.to_owned().int64().try_into()?)
    }
}
