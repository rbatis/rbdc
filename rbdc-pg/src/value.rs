use crate::type_info::PgTypeInfo;
use bytes::Buf;
use rbdc::Error;
use std::borrow::Cow;
use std::str::from_utf8;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PgValueFormat {
    Text = 0,
    Binary = 1,
}

/// Implementation of [`ValueRef`] for PostgreSQL.
#[derive(Clone)]
pub struct PgValueRef<'r> {
    pub value: Option<&'r [u8]>,
    pub type_info: PgTypeInfo,
    pub format: PgValueFormat,
    /// Session timezone offset in seconds from UTC, used for TIMESTAMPTZ decoding
    pub timezone_sec: Option<i32>,
}

/// Implementation of [`Value`] for PostgreSQL.
#[derive(Clone)]
pub struct PgValue {
    pub value: Option<Vec<u8>>,
    pub type_info: PgTypeInfo,
    pub format: PgValueFormat,
    /// Session timezone offset in seconds from UTC, used for TIMESTAMPTZ decoding
    pub timezone_sec: Option<i32>,
}

impl<'r> PgValueRef<'r> {
    pub(crate) fn get(
        buf: &mut &'r [u8],
        format: PgValueFormat,
        ty: PgTypeInfo,
        timezone_sec: Option<i32>,
    ) -> Self {
        let mut element_len = buf.get_i32();

        let element_val = if element_len == -1 {
            element_len = 0;
            None
        } else {
            Some(&buf[..(element_len as usize)])
        };

        buf.advance(element_len as usize);

        PgValueRef {
            value: element_val,
            type_info: ty,
            format,
            timezone_sec,
        }
    }

    pub fn format(&self) -> PgValueFormat {
        self.format
    }

    pub fn as_bytes(&self) -> Result<&'r [u8], Error> {
        match &self.value {
            Some(v) => Ok(v),
            None => Err(Error::from("UnexpectedNullError")),
        }
    }

    pub fn as_str(&self) -> Result<&'r str, Error> {
        Ok(from_utf8(self.as_bytes()?)?)
    }
}

impl PgValue {
    pub fn get(
        buf: &mut &[u8],
        format: PgValueFormat,
        ty: PgTypeInfo,
        timezone_sec: Option<i32>,
    ) -> Self {
        let mut element_len = buf.get_i32();

        let element_val = if element_len == -1 {
            element_len = 0;
            None
        } else {
            Some(buf[..(element_len as usize)].to_vec())
        };

        buf.advance(element_len as usize);

        PgValue {
            value: element_val,
            type_info: ty,
            format,
            timezone_sec,
        }
    }

    #[inline]
    pub fn as_ref(&self) -> PgValueRef<'_> {
        PgValueRef {
            value: self.value.as_deref(),
            type_info: self.type_info.clone(),
            format: self.format,
            timezone_sec: self.timezone_sec,
        }
    }

    pub fn type_info(&self) -> Cow<'_, PgTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    pub fn is_null(&self) -> bool {
        self.value.is_none()
    }

    pub fn format(&self) -> PgValueFormat {
        self.format
    }
    pub fn as_str(&self) -> Result<&str, Error> {
        Ok(from_utf8(self.as_bytes()?)?)
    }

    pub fn as_bytes(&self) -> Result<&[u8], Error> {
        match &self.value {
            Some(v) => Ok(v),
            None => Err(Error::from("UnexpectedNullError")),
        }
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Error> {
        match self.value {
            Some(v) => Ok(v),
            None => Err(Error::from("UnexpectedNullError")),
        }
    }
}

impl<'r> PgValueRef<'r> {
    pub fn to_owned(&self) -> PgValue {
        let value = match self.value {
            Some(value) => Some(value.to_vec()),
            _ => None,
        };
        PgValue {
            value: value,
            format: self.format,
            type_info: self.type_info.clone(),
            timezone_sec: self.timezone_sec,
        }
    }

    pub fn type_info(&self) -> Cow<'_, PgTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    pub fn is_null(&self) -> bool {
        self.value.is_none()
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::type_info::PgType;

    fn typed() -> PgTypeInfo {
        PgTypeInfo(PgType::Int4)
    }

    #[test]
    fn pg_value_ref_get_reads_length_prefixed_values() {
        let mut buf: &[u8] = &[0, 0, 0, 2, 0xab, 0xcd];
        let value = PgValueRef::get(&mut buf, PgValueFormat::Binary, typed(), Some(60));
        assert_eq!(value.value, Some(&[0xab, 0xcd][..]));
        assert_eq!(value.format(), PgValueFormat::Binary);
        assert_eq!(value.timezone_sec, Some(60));
        assert_eq!(value.type_info().as_ref(), &typed());
        assert!(!value.is_null());
        assert_eq!(value.as_bytes().unwrap(), &[0xab, 0xcd]);
        assert!(buf.is_empty());

        // NULL value encoded as -1
        let mut buf: &[u8] = &[0xff, 0xff, 0xff, 0xff];
        let value = PgValueRef::get(&mut buf, PgValueFormat::Text, typed(), None);
        assert!(value.is_null());
        assert!(value.as_bytes().is_err());
        assert!(value.as_str().is_err());

        let owned = value.to_owned();
        assert!(owned.is_null());
        assert_eq!(owned.format(), PgValueFormat::Text);
        assert_eq!(owned.type_info().as_ref(), &typed());
    }

    #[test]
    fn pg_value_ref_as_str_and_invalid_utf8() {
        let value = PgValueRef {
            value: Some(b"hello"),
            type_info: typed(),
            format: PgValueFormat::Text,
            timezone_sec: None,
        };
        assert_eq!(value.as_str().unwrap(), "hello");

        let invalid = PgValueRef {
            value: Some(&[0xff]),
            type_info: typed(),
            format: PgValueFormat::Text,
            timezone_sec: None,
        };
        assert!(invalid.as_str().is_err());
    }

    #[test]
    fn pg_value_accessors() {
        let mut buf: &[u8] = &[0, 0, 0, 3, b'a', b'b', b'c'];
        let value = PgValue::get(&mut buf, PgValueFormat::Text, typed(), Some(1));
        assert_eq!(value.as_str().unwrap(), "abc");
        assert_eq!(value.as_bytes().unwrap(), b"abc");
        assert!(!value.is_null());
        assert_eq!(value.format(), PgValueFormat::Text);
        assert_eq!(value.type_info().as_ref(), &typed());
        assert_eq!(value.as_ref().timezone_sec, Some(1));
        assert!(buf.is_empty());

        let cloned = value.clone();
        assert_eq!(cloned.into_bytes().unwrap(), b"abc".to_vec());

        // NULL values
        let mut buf: &[u8] = &[0xff, 0xff, 0xff, 0xff];
        let value = PgValue::get(&mut buf, PgValueFormat::Binary, typed(), None);
        assert!(value.is_null());
        assert!(value.as_str().is_err());
        assert!(value.as_bytes().is_err());
        assert!(value.clone().into_bytes().is_err());
        assert!(value.as_ref().is_null());
    }
}
