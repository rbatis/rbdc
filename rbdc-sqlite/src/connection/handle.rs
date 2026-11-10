use std::ffi::CString;
use std::ptr;
use std::ptr::NonNull;

use libsqlite3_sys::{sqlite3, sqlite3_close, sqlite3_exec, sqlite3_last_insert_rowid, SQLITE_OK};
use rbdc::err_protocol;
use rbdc::error::Error;

use crate::SqliteError;

/// Managed handle to the raw SQLite3 database handle.
/// The database handle will be closed when this is dropped and no `ConnectionHandleRef`s exist.
#[derive(Debug)]
pub(crate) struct ConnectionHandle(NonNull<sqlite3>);

/// A wrapper around `ConnectionHandle` which *does not* finalize the handle on-drop.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionHandleRaw(NonNull<sqlite3>);

// A SQLite3 handle is safe to send between threads, provided not more than
// one is accessing it at the same time. This is upheld as long as [SQLITE_CONFIG_MULTITHREAD] is
// enabled and [SQLITE_THREADSAFE] was enabled when sqlite was compiled. We refuse to work
// if these conditions are not upheld.

// <https://www.sqlite.org/c3ref/threadsafe.html>

// <https://www.sqlite.org/c3ref/c_config_covering_index_scan.html#sqliteconfigmultithread>

unsafe impl Send for ConnectionHandle {}

// SAFETY: this type does nothing but provide access to the DB handle pointer.
unsafe impl Send for ConnectionHandleRaw {}

impl ConnectionHandle {
    #[inline]
    pub(super) unsafe fn new(ptr: *mut sqlite3) -> Self {
        Self(NonNull::new_unchecked(ptr))
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut sqlite3 {
        self.0.as_ptr()
    }

    pub(crate) fn as_non_null_ptr(&self) -> NonNull<sqlite3> {
        self.0
    }

    #[inline]
    pub(crate) fn to_raw(&self) -> ConnectionHandleRaw {
        ConnectionHandleRaw(self.0)
    }

    pub(crate) fn last_insert_rowid(&mut self) -> i64 {
        // SAFETY: we have exclusive access to the database handle
        unsafe { sqlite3_last_insert_rowid(self.as_ptr()) }
    }

    pub(crate) fn exec(&mut self, query: impl Into<String>) -> Result<(), Error> {
        let query = query.into();
        let query = CString::new(query).map_err(|_| err_protocol!("query contains nul bytes"))?;

        // SAFETY: we have exclusive access to the database handle
        unsafe {
            let status = sqlite3_exec(
                self.as_ptr(),
                query.as_ptr(),
                // callback if we wanted result rows
                None,
                // callback data
                ptr::null_mut(),
                // out-pointer for the error message, we just use `SqliteError::new()`
                ptr::null_mut(),
            );

            if status == SQLITE_OK {
                Ok(())
            } else {
                Err(SqliteError::new(self.as_ptr()).into())
            }
        }
    }
}

impl ConnectionHandleRaw {
    pub(crate) fn as_ptr(&self) -> *mut sqlite3 {
        self.0.as_ptr()
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        unsafe {
            // https://sqlite.org/c3ref/close.html
            let status = sqlite3_close(self.0.as_ptr());
            if status != SQLITE_OK {
                // this should *only* happen due to an internal bug in SQLite where we left
                // SQLite handles open. We log the error instead of panicking because
                // panicking in Drop is unsafe (can cause二次panic).
                log::error!(
                    "Failed to close SQLite connection: {}",
                    SqliteError::new(self.0.as_ptr())
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libsqlite3_sys::sqlite3_open;
    use std::ffi::CString;

    fn open_memory() -> ConnectionHandle {
        let filename = CString::new(":memory:").unwrap();
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(filename.as_ptr(), &mut db), SQLITE_OK);
            ConnectionHandle::new(db)
        }
    }

    #[test]
    fn test_exec_success_and_error() {
        let mut handle = open_memory();
        handle.exec("CREATE TABLE t (id INTEGER)").unwrap();
        handle.exec(String::from("INSERT INTO t VALUES (1)")).unwrap();
        assert_eq!(handle.last_insert_rowid(), 1);

        let err = handle.exec("NOT SQL").unwrap_err();
        assert!(!err.to_string().is_empty());

        // Queries containing nul bytes are rejected before reaching SQLite.
        let err = handle.exec("SELECT 1\0;").unwrap_err();
        assert!(err.to_string().contains("nul bytes"));
    }
}
