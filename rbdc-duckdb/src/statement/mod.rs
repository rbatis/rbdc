//! Statement handling for DuckDB - follows SQLite pattern

use rbdc::Error;
use std::ffi::{CStr, CString};
use std::ptr::NonNull;
use std::sync::Arc;

/// Wrapper for DuckDB prepared statement handle, shared via Arc for multiple references.
/// When cloned, all clones share the same underlying statement. Drop calls
/// duckdb_destroy_prepare only when the last Arc reference is dropped.
#[derive(Debug)]
pub struct DuckDbStatementHandle(Arc<NonNull<libduckdb_sys::_duckdb_prepared_statement>>);

unsafe impl Send for DuckDbStatementHandle {}

impl DuckDbStatementHandle {
    pub(crate) fn new(ptr: *mut libduckdb_sys::_duckdb_prepared_statement) -> Self {
        Self(Arc::new(unsafe { NonNull::new_unchecked(ptr) }))
    }

    pub(crate) fn as_ptr(&self) -> libduckdb_sys::duckdb_prepared_statement {
        self.0.as_ptr() as libduckdb_sys::duckdb_prepared_statement
    }

    #[inline]
    pub(crate) fn clear_bindings(&self) {
        unsafe {
            libduckdb_sys::duckdb_clear_bindings(self.0.as_ptr() as libduckdb_sys::duckdb_prepared_statement);
        }
    }
}

impl Clone for DuckDbStatementHandle {
    fn clone(&self) -> Self {
        DuckDbStatementHandle(Arc::clone(&self.0))
    }
}

impl Drop for DuckDbStatementHandle {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.0.as_ptr() as libduckdb_sys::duckdb_prepared_statement;
            libduckdb_sys::duckdb_destroy_prepare(&mut ptr);
        }
    }
}

/// A DuckDB statement that can be executed multiple times
#[derive(Debug, Clone)]
pub struct DuckDbStatement {
    pub(crate) sql: String,
}

impl DuckDbStatement {
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

/// Virtual statement that wraps a prepared statement and manages its lifecycle
#[derive(Debug)]
pub struct VirtualStatement {
    /// The prepared statement handle (Arc-based for shared ownership)
    handle: Option<DuckDbStatementHandle>,
    /// Whether this statement is persistent (cached)
    persistent: bool,
    /// The SQL query
    query: String,
}

impl Clone for VirtualStatement {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            persistent: self.persistent,
            query: self.query.clone(),
        }
    }
}

impl VirtualStatement {
    pub(crate) fn new(query: &str, persistent: bool) -> Self {
        Self {
            handle: None,
            persistent,
            query: query.to_string(),
        }
    }

    /// Prepare the statement
    pub(crate) fn prepare(
        &mut self,
        conn: libduckdb_sys::duckdb_connection,
    ) -> Result<&mut DuckDbStatementHandle, Error> {
        if let Some(ref mut handle) = self.handle {
            return Ok(handle);
        }

        let sql_cstr = CString::new(self.query.as_str()).map_err(|_| "Invalid SQL string")?;

        let mut stmt: libduckdb_sys::duckdb_prepared_statement = std::ptr::null_mut();
        let r = unsafe {
            libduckdb_sys::duckdb_prepare(conn, sql_cstr.as_ptr(), &mut stmt)
        };

        if r != libduckdb_sys::DuckDBSuccess {
            let err_str = if stmt.is_null() {
                "prepare failed: statement is null".to_string()
            } else {
                let err_ptr = unsafe { libduckdb_sys::duckdb_prepare_error(stmt) };
                let msg = if err_ptr.is_null() {
                    "prepare failed: unknown error".to_string()
                } else {
                    unsafe { CStr::from_ptr(err_ptr) }
                        .to_string_lossy()
                        .into_owned()
                };
                unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                msg
            };
            return Err(Error::from(err_str));
        }

        if let Some(ptr) = NonNull::new(stmt) {
            self.handle = Some(DuckDbStatementHandle::new(ptr.as_ptr() as *mut _));
            Ok(self.handle.as_mut().unwrap())
        } else {
            Err(Error::from("prepare returned null statement"))
        }
    }

    pub(crate) fn reset(&mut self) {
        if let Some(ref handle) = self.handle {
            handle.clear_bindings();
        }
    }

    pub(crate) fn handle_mut(&mut self) -> Option<&mut DuckDbStatementHandle> {
        self.handle.as_mut()
    }
}

impl Drop for VirtualStatement {
    fn drop(&mut self) {
        // DuckDbStatementHandle's Drop will call duckdb_destroy_prepare
    }
}
