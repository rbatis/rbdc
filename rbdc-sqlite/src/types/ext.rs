use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::type_info::DataType;
use crate::types::Type;
use crate::{SqliteArgumentValue, SqliteTypeInfo, SqliteValueRef};
use rbdc::date::Date;
use rbdc::datetime::DateTime;
use rbdc::decimal::Decimal;
use rbdc::json::Json;
use rbdc::timestamp::Timestamp;
use rbdc::types::time::Time;
use rbdc::uuid::Uuid;
use rbdc::Error;
use std::str::FromStr;

impl Type for Date {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Date)
    }
}

impl Encode for Date {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Text(self.0.to_string()));
        Ok(IsNull::No)
    }
}

impl Decode for Date {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self(
            fastdate::Date::from_str(value.to_owned().text()?)
                .map_err(|e| Error::from(e.to_string()))?,
        ))
    }
}

impl Type for DateTime {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::DateTime)
    }
}

impl Encode for DateTime {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Text(self.0.to_string()));
        Ok(IsNull::No)
    }
}

impl Decode for DateTime {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self(
            fastdate::DateTime::from_str(value.to_owned().text()?)
                .map_err(|e| Error::from(e.to_string()))?,
        ))
    }
}

impl Type for Time {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Time)
    }
}

impl Encode for Time {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Text(self.0.to_string()));
        Ok(IsNull::No)
    }
}

impl Decode for Time {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self(
            fastdate::Time::from_str(value.to_owned().text()?)
                .map_err(|e| Error::from(e.to_string()))?,
        ))
    }
}

impl Type for Timestamp {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Int64)
    }
}

impl Encode for Timestamp {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Int64(self.0));
        Ok(IsNull::No)
    }
}

impl Decode for Timestamp {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self(value.to_owned().int64()))
    }
}

impl Type for Decimal {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Numeric)
    }
}

impl Encode for Decimal {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Text(self.0.to_string()));
        Ok(IsNull::No)
    }
}

impl Decode for Decimal {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self::from_str(value.to_owned().text()?)?)
    }
}

impl Type for Json {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Text)
    }
}

impl Encode for Json {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Text(self.0));
        Ok(IsNull::No)
    }
}

impl Decode for Json {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self(value.to_owned().text()?.to_string()))
    }
}

impl Type for Uuid {
    fn type_info(&self) -> SqliteTypeInfo {
        SqliteTypeInfo(DataType::Text)
    }
}

impl Encode for Uuid {
    fn encode(self, args: &mut Vec<SqliteArgumentValue>) -> Result<IsNull, Error> {
        args.push(SqliteArgumentValue::Text(self.0));
        Ok(IsNull::No)
    }
}

impl Decode for Uuid {
    fn decode(value: SqliteValueRef) -> Result<Self, Error> {
        Ok(Self(value.to_owned().text()?.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_type_info_is_stable() {
        assert_eq!(Date::default().type_info().name(), "DATE");
        assert_eq!(DateTime::default().type_info().name(), "DATETIME");
        assert_eq!(Time::default().type_info().name(), "TIME");
        assert_eq!(Timestamp::default().type_info().name(), "INTEGER");
        assert_eq!(Decimal::default().type_info().name(), "NUMERIC");
        assert_eq!(Json::default().type_info().name(), "TEXT");
        assert_eq!(Uuid::default().type_info().name(), "TEXT");
    }
}
