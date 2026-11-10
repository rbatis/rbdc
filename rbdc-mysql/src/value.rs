use crate::options::MySqlConnectOptions;
use crate::protocol::text::ColumnType;
use crate::result_set::MySqlTypeInfo;
use rbdc::Error;
use std::borrow::Cow;
use std::str::from_utf8;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum MySqlValueFormat {
    Text,
    Binary,
}

/// Implementation of [`Value`] for MySQL.
#[derive(Clone)]
pub struct MySqlValue {
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) type_info: MySqlTypeInfo,
    pub(crate) format: MySqlValueFormat,
    pub(crate) option: Arc<MySqlConnectOptions>,
}

/// Implementation of [`ValueRef`] for MySQL.
#[derive(Clone)]
pub struct MySqlValueRef<'r> {
    pub value: Option<&'r [u8]>,
    pub type_info: MySqlTypeInfo,
    pub format: MySqlValueFormat,
    pub option: Arc<MySqlConnectOptions>,
}

impl MySqlValue {
    pub fn format(&self) -> MySqlValueFormat {
        self.format
    }

    pub fn as_bytes(&self) -> Result<&[u8], Error> {
        match &self.value {
            Some(v) => Ok(v),
            None => Err(Error::protocol("UnexpectedNull")),
        }
    }

    pub fn as_str(&self) -> Result<&str, Error> {
        Ok(from_utf8(self.as_bytes()?)?)
    }
}

impl<'r> MySqlValueRef<'r> {
    pub(crate) fn format(&self) -> MySqlValueFormat {
        self.format
    }

    pub(crate) fn as_bytes(&self) -> Result<&'r [u8], Error> {
        match &self.value {
            Some(v) => Ok(v),
            None => Err(Error::protocol("UnexpectedNull")),
        }
    }

    pub(crate) fn as_str(&self) -> Result<&'r str, Error> {
        Ok(from_utf8(self.as_bytes()?)?)
    }
}

impl MySqlValue {
    fn as_ref(&self) -> MySqlValueRef<'_> {
        MySqlValueRef {
            value: self.value.as_deref(),
            type_info: self.type_info.clone(),
            format: self.format,
            option: self.option.clone(),
        }
    }

    pub fn type_info(&self) -> Cow<'_, MySqlTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    pub fn is_null(&self) -> bool {
        is_null(self.value.as_deref(), &self.type_info)
    }
}

fn is_null(value: Option<&[u8]>, ty: &MySqlTypeInfo) -> bool {
    if let Some(value) = value {
        // zero dates and date times should be treated the same as NULL
        if matches!(
            ty.r#type,
            ColumnType::Date | ColumnType::Timestamp | ColumnType::Datetime
        ) && value.get(0) == Some(&0)
        {
            return true;
        }
    }

    value.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::text::ColumnType;

    fn owned(
        value: Option<Vec<u8>>,
        ty: ColumnType,
        format: MySqlValueFormat,
    ) -> MySqlValue {
        MySqlValue {
            value,
            type_info: MySqlTypeInfo::from_type(ty),
            format,
            option: Arc::new(MySqlConnectOptions::new()),
        }
    }

    #[test]
    fn test_format_and_bytes() {
        let v = owned(Some(b"abc".to_vec()), ColumnType::VarChar, MySqlValueFormat::Text);
        assert_eq!(v.format(), MySqlValueFormat::Text);
        assert_eq!(v.as_bytes().unwrap(), b"abc");
        assert_eq!(v.as_str().unwrap(), "abc");
        assert_eq!(v.type_info().r#type, ColumnType::VarChar);
        assert!(!v.is_null());
    }

    #[test]
    fn test_missing_value_errors() {
        let v = owned(None, ColumnType::VarChar, MySqlValueFormat::Text);
        assert!(v.as_bytes().is_err());
        assert!(v.as_str().is_err());
        assert!(v.is_null());
    }

    #[test]
    fn test_invalid_utf8_errors() {
        let v = owned(Some(vec![0xff]), ColumnType::VarChar, MySqlValueFormat::Binary);
        assert!(v.as_str().is_err());
    }

    #[test]
    fn test_zero_dates_are_null() {
        for ty in [
            ColumnType::Date,
            ColumnType::Timestamp,
            ColumnType::Datetime,
        ] {
            let v = owned(Some(vec![0, 0, 0, 0]), ty, MySqlValueFormat::Binary);
            assert!(v.is_null(), "zero value for {ty:?} should be null");

            let v = owned(Some(vec![4, 0xE7, 0x07, 1, 1]), ty, MySqlValueFormat::Binary);
            assert!(!v.is_null());
        }
        // Non-temporal values are never treated as zero-date nulls.
        let v = owned(Some(vec![0]), ColumnType::Tiny, MySqlValueFormat::Binary);
        assert!(!v.is_null());
    }

    #[test]
    fn test_as_ref_roundtrip() {
        let v = owned(Some(vec![1, 2]), ColumnType::Bit, MySqlValueFormat::Binary);
        let r = v.as_ref();
        assert_eq!(r.value, Some(&[1u8, 2][..]));
        assert_eq!(r.format(), MySqlValueFormat::Binary);
        assert_eq!(r.type_info.r#type, ColumnType::Bit);
        assert_eq!(r.option.port, 3306);
        assert_eq!(r.as_bytes().unwrap(), &[1, 2]);
        assert!(r.as_str().is_ok());
        let borrowed = MySqlValueRef {
            value: Some(b"str"),
            type_info: MySqlTypeInfo::from_type(ColumnType::VarChar),
            format: MySqlValueFormat::Text,
            option: Arc::new(MySqlConnectOptions::new()),
        };
        assert_eq!(borrowed.as_str().unwrap(), "str");
        assert_eq!(borrowed.as_bytes().unwrap(), b"str");

        let invalid = MySqlValueRef {
            value: Some(&[0xff]),
            type_info: MySqlTypeInfo::from_type(ColumnType::VarChar),
            format: MySqlValueFormat::Binary,
            option: Arc::new(MySqlConnectOptions::new()),
        };
        assert!(invalid.as_str().is_err());

        let missing = MySqlValueRef {
            value: None,
            type_info: MySqlTypeInfo::from_type(ColumnType::VarChar),
            format: MySqlValueFormat::Text,
            option: Arc::new(MySqlConnectOptions::new()),
        };
        assert!(missing.as_bytes().is_err());
        assert!(missing.as_str().is_err());
    }
}
