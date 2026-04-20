use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::type_info::DataType;
use crate::types::Type;
use crate::{SqliteArgumentValue, SqliteTypeInfo, SqliteValueRef};
use rbdc::error::Error;

impl Type for f32 {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Float)
    }
}

impl Encode for f32 {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Double(self.into()));

        Ok(IsNull::No)
    }
}

impl Decode for f32 {
    fn decode(value: SqliteValueRef) -> Result<f32, Error> {
        Ok(value.to_owned().double() as f32)
    }
}

impl Type for f64 {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Float)
    }
}

impl Encode for f64 {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Double(self));

        Ok(IsNull::No)
    }
}

impl Decode for f64 {
    fn decode(value: SqliteValueRef) -> Result<f64, Error> {
        Ok(value.to_owned().double())
    }
}
