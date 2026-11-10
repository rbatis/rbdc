use std::error::Error as StdError;
use std::ffi::CStr;
use std::fmt::{self, Display, Formatter};
use std::os::raw::c_int;
use std::{borrow::Cow, str::from_utf8_unchecked};

use libsqlite3_sys::{sqlite3, sqlite3_errmsg, sqlite3_extended_errcode};

// Error Codes And Messages
// https://www.sqlite.org/c3ref/errcode.html

#[derive(Debug)]
pub struct SqliteError {
    code: c_int,
    message: String,
}

impl SqliteError {
    pub(crate) fn new(handle: *mut sqlite3) -> Self {
        // returns the extended result code even when extended result codes are disabled
        let code: c_int = unsafe { sqlite3_extended_errcode(handle) };

        // return English-language text that describes the error
        let message = unsafe {
            let msg = sqlite3_errmsg(handle);
            debug_assert!(!msg.is_null());

            from_utf8_unchecked(CStr::from_ptr(msg).to_bytes())
        };

        Self {
            code,
            message: message.to_owned(),
        }
    }
}

impl Display for SqliteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(&self.message)
    }
}

impl StdError for SqliteError {}

impl SqliteError {
    /// The extended result code.
    #[inline]
    pub fn code(&self) -> Option<Cow<'_, str>> {
        Some(format!("{}", self.code).into())
    }

    #[inline]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[doc(hidden)]
    pub fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }

    #[doc(hidden)]
    pub fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }

    #[doc(hidden)]
    pub fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }
}

impl From<SqliteError> for rbdc::Error {
    fn from(e: SqliteError) -> Self {
        Self::from(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libsqlite3_sys::{sqlite3_close, sqlite3_exec, sqlite3_open, SQLITE_OK};
    use std::ffi::CString;
    use std::ptr;

    fn make_error() -> SqliteError {
        let filename = CString::new(":memory:").unwrap();
        let sql = CString::new("THIS IS NOT SQL").unwrap();
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(filename.as_ptr(), &mut db), SQLITE_OK);
            assert_ne!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );
            let err = SqliteError::new(db);
            sqlite3_close(db);
            err
        }
    }

    #[test]
    fn test_error_accessors() {
        let err = make_error();
        assert!(!err.message().is_empty());
        assert!(err.code().is_some());
        assert_eq!(err.to_string(), err.message());
        assert!(!format!("{:?}", err).is_empty());

        let as_error = err.as_error();
        assert_eq!(as_error.to_string(), err.message());

        let mut err = make_error();
        let as_error_mut = err.as_error_mut();
        assert!(!as_error_mut.to_string().is_empty());

        let boxed: Box<dyn StdError + Send + Sync + 'static> = Box::new(make_error()).into_error();
        assert!(!boxed.to_string().is_empty());

        let converted: rbdc::Error = make_error().into();
        assert!(!converted.to_string().is_empty());

        // `source` is not implemented, so the default is used.
        let err = make_error();
        assert!(StdError::source(&err).is_none());
    }
}
