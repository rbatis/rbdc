use std::ptr::NonNull;
use std::slice::from_raw_parts;
use std::str::from_utf8;
use std::sync::Arc;

use libsqlite3_sys::{
    sqlite3_value, sqlite3_value_blob, sqlite3_value_bytes, sqlite3_value_double,
    sqlite3_value_dup, sqlite3_value_free, sqlite3_value_int, sqlite3_value_int64,
    sqlite3_value_type, SQLITE_NULL,
};

use crate::type_info::DataType;
use crate::SqliteTypeInfo;
use rbdc::error::Error;
use std::borrow::Cow;

enum SqliteValueData<'r> {
    Value(&'r SqliteValue),
}

pub struct SqliteValueRef<'r>(SqliteValueData<'r>);

impl<'r> SqliteValueRef<'r> {
    pub fn value(value: &'r SqliteValue) -> Self {
        Self(SqliteValueData::Value(value))
    }

    pub(super) fn int(&self) -> i32 {
        match self.0 {
            SqliteValueData::Value(v) => v.int(),
        }
    }

    pub(super) fn int64(&self) -> i64 {
        match self.0 {
            SqliteValueData::Value(v) => v.int64(),
        }
    }

    pub(super) fn double(&self) -> f64 {
        match self.0 {
            SqliteValueData::Value(v) => v.double(),
        }
    }

    pub(super) fn blob(&self) -> &'r [u8] {
        match self.0 {
            SqliteValueData::Value(v) => v.blob(),
        }
    }

    pub(super) fn text(&self) -> Result<&'r str, Error> {
        match self.0 {
            SqliteValueData::Value(v) => v.text(),
        }
    }
}

impl<'r> SqliteValueRef<'r> {
    pub fn to_owned(&self) -> SqliteValue {
        match self.0 {
            SqliteValueData::Value(v) => v.clone(),
        }
    }

    pub fn type_info(&self) -> Cow<'_, SqliteTypeInfo> {
        match self.0 {
            SqliteValueData::Value(v) => v.type_info(),
        }
    }

    pub fn type_info_opt(&self) -> Option<SqliteTypeInfo> {
        match self.0 {
            SqliteValueData::Value(v) => v.type_info_opt(),
        }
    }

    pub fn is_null(&self) -> bool {
        match self.0 {
            SqliteValueData::Value(v) => v.is_null(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteValue {
    pub(crate) handle: Arc<ValueHandle>,
    pub(crate) type_info: SqliteTypeInfo,
}

#[derive(Debug)]
pub(crate) struct ValueHandle(NonNull<sqlite3_value>);

// SAFE: only protected value objects are stored in SqliteValue
unsafe impl Send for ValueHandle {}
unsafe impl Sync for ValueHandle {}

impl SqliteValue {
    pub unsafe fn new(value: *mut sqlite3_value, type_info: SqliteTypeInfo) -> Self {
        debug_assert!(!value.is_null());

        Self {
            type_info,
            handle: Arc::new(ValueHandle(NonNull::new_unchecked(sqlite3_value_dup(
                value,
            )))),
        }
    }

    pub fn type_info_opt(&self) -> Option<SqliteTypeInfo> {
        let dt = DataType::from_code(unsafe { sqlite3_value_type(self.handle.0.as_ptr()) });
        if let DataType::Null = dt {
            None
        } else {
            Some(SqliteTypeInfo(dt))
        }
    }

    pub fn int(&self) -> i32 {
        unsafe { sqlite3_value_int(self.handle.0.as_ptr()) }
    }

    pub fn int64(&self) -> i64 {
        unsafe { sqlite3_value_int64(self.handle.0.as_ptr()) }
    }

    pub fn double(&self) -> f64 {
        unsafe { sqlite3_value_double(self.handle.0.as_ptr()) }
    }

    pub fn blob(&self) -> &[u8] {
        let len = unsafe { sqlite3_value_bytes(self.handle.0.as_ptr()) } as usize;

        if len == 0 {
            // empty blobs are NULL so just return an empty slice
            return &[];
        }

        let ptr = unsafe { sqlite3_value_blob(self.handle.0.as_ptr()) } as *const u8;
        debug_assert!(!ptr.is_null());

        unsafe { from_raw_parts(ptr, len) }
    }

    pub fn text(&self) -> Result<&str, Error> {
        Ok(from_utf8(self.blob())?)
    }
}

impl SqliteValue {
    pub fn as_ref(&self) -> SqliteValueRef<'_> {
        SqliteValueRef::value(self)
    }

    pub fn type_info(&self) -> Cow<'_, SqliteTypeInfo> {
        self.type_info_opt()
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(&self.type_info))
    }

    pub fn is_null(&self) -> bool {
        unsafe { sqlite3_value_type(self.handle.0.as_ptr()) == SQLITE_NULL }
    }
}

impl Drop for ValueHandle {
    fn drop(&mut self) {
        unsafe {
            sqlite3_value_free(self.0.as_ptr());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libsqlite3_sys::{
        sqlite3, sqlite3_close, sqlite3_column_value, sqlite3_finalize, sqlite3_open,
        sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt, SQLITE_OK, SQLITE_ROW,
    };
    use std::ffi::CString;
    use std::ptr;

    fn value_for(sql: &str) -> SqliteValue {
        let filename = CString::new(":memory:").unwrap();
        let query = CString::new(sql).unwrap();
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(filename.as_ptr(), &mut db), SQLITE_OK);
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, query.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let raw = sqlite3_column_value(stmt, 0);
            let value = SqliteValue::new(raw, SqliteTypeInfo::null());
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            value
        }
    }

    #[test]
    fn test_value_ref_accessors() {
        let integer = value_for("SELECT 7");
        let r = integer.as_ref();
        assert_eq!(r.int(), 7);
        assert_eq!(r.int64(), 7);
        assert_eq!(r.double(), 7.0);
        assert!(!r.is_null());
        assert!(r.type_info_opt().is_some());
        assert_eq!(r.type_info().name(), "INTEGER");
        assert_eq!(r.to_owned().int(), 7);

        let text = value_for("SELECT 'abc'");
        assert_eq!(text.as_ref().text().unwrap(), "abc");
        assert_eq!(text.as_ref().blob(), b"abc");

        let blob = value_for("SELECT x'FF'");
        assert_eq!(blob.as_ref().blob(), &[0xFF]);
        assert!(blob.as_ref().text().is_err());

        let null = value_for("SELECT NULL");
        let r = null.as_ref();
        assert!(r.is_null());
        assert!(r.type_info_opt().is_none());
        // NULL values fall back to the type information stored at creation.
        assert!(r.type_info().is_null());
        assert_eq!(r.blob(), &[] as &[u8]);
        assert_eq!(r.text().unwrap(), "");
        assert_eq!(r.int(), 0);
        assert_eq!(r.int64(), 0);
        assert_eq!(r.double(), 0.0);
        assert_eq!(null.type_info().name(), "NULL");
        assert_eq!(null.type_info_opt(), None);
    }
}
