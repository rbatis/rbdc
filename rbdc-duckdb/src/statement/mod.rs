//! Statement handling for DuckDB - follows SQLite pattern

use rbdc::Error;
use std::ffi::{CStr, CString};
use std::ptr::NonNull;
use std::sync::Arc;

/// Owns a raw prepared statement and destroys it exactly once, when the last
/// `Arc` reference to it is dropped.
#[derive(Debug)]
struct PreparedStatementPtr(NonNull<libduckdb_sys::_duckdb_prepared_statement>);

impl Drop for PreparedStatementPtr {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.0.as_ptr() as libduckdb_sys::duckdb_prepared_statement;
            libduckdb_sys::duckdb_destroy_prepare(&mut ptr);
        }
    }
}

/// Wrapper for a DuckDB prepared statement handle, shared via `Arc` for
/// multiple references. Clones share the same underlying statement; the
/// statement is destroyed when the last reference is dropped.
#[derive(Debug)]
pub struct DuckDbStatementHandle(Arc<PreparedStatementPtr>);

unsafe impl Send for DuckDbStatementHandle {}

impl DuckDbStatementHandle {
    pub(crate) fn new(ptr: *mut libduckdb_sys::_duckdb_prepared_statement) -> Self {
        // Safety: DuckDbStatementHandle impls Send (the raw ptr is only accessed
        // from the worker thread, under a mutex). The Arc is used to share the
        // handle among cloned VirtualStatements within the same thread.
        #[allow(clippy::arc_with_non_send_sync)]
        let inner = Arc::new(PreparedStatementPtr(unsafe {
            NonNull::new_unchecked(ptr)
        }));
        Self(inner)
    }

    pub(crate) fn as_ptr(&self) -> libduckdb_sys::duckdb_prepared_statement {
        self.0.0.as_ptr() as libduckdb_sys::duckdb_prepared_statement
    }

    #[inline]
    pub(crate) fn clear_bindings(&self) {
        unsafe {
            libduckdb_sys::duckdb_clear_bindings(
                self.0.0.as_ptr() as libduckdb_sys::duckdb_prepared_statement,
            );
        }
    }
}

impl Clone for DuckDbStatementHandle {
    fn clone(&self) -> Self {
        DuckDbStatementHandle(Arc::clone(&self.0))
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

/// RAII guard to ensure duckdb_destroy_extracted is called.
struct ExtractedGuard(libduckdb_sys::duckdb_extracted_statements);

impl Drop for ExtractedGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { libduckdb_sys::duckdb_destroy_extracted(&mut self.0); }
        }
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

    /// Prepare the statement using DuckDB's extracted-statements API.
    /// This mirrors duckdb-rs's approach: use duckdb_extract_statements to
    /// split the SQL, execute intermediate statements (if any), and prepare
    /// the final statement via duckdb_prepare_extracted_statement.
    pub(crate) fn prepare(
        &mut self,
        conn: libduckdb_sys::duckdb_connection,
    ) -> Result<&mut DuckDbStatementHandle, Error> {
        if let Some(ref mut handle) = self.handle {
            return Ok(handle);
        }

        let sql_cstr = CString::new(self.query.as_str()).map_err(|_| Error::from("Invalid SQL string"))?;

        // Step 1: Extract all statements from the SQL string
        let mut extracted: libduckdb_sys::duckdb_extracted_statements = std::ptr::null_mut();
        let num_stmts = unsafe {
            libduckdb_sys::duckdb_extract_statements(conn, sql_cstr.as_ptr(), &mut extracted)
        };

        if num_stmts == 0 {
            // Extract failed - get error message
            let err_str = if extracted.is_null() {
                "extract statements failed".to_string()
            } else {
                let msg = get_extract_error(extracted);
                unsafe { libduckdb_sys::duckdb_destroy_extracted(&mut extracted) };
                msg
            };
            return Err(Error::from(err_str));
        }

        // RAII: clean up extracted when we're done
        let _guard = ExtractedGuard(extracted);

        // Step 2: Execute all intermediate statements (for multi-statement SQL)
        for i in 0..(num_stmts - 1) {
            let mut stmt: libduckdb_sys::duckdb_prepared_statement = std::ptr::null_mut();
            let r = unsafe {
                libduckdb_sys::duckdb_prepare_extracted_statement(conn, extracted, i, &mut stmt)
            };
            if r != libduckdb_sys::DuckDBSuccess {
                let err_str = get_prepare_error(stmt);
                return Err(Error::from(err_str));
            }

            let mut result: libduckdb_sys::duckdb_result = unsafe { std::mem::zeroed() };
            let rc = unsafe { libduckdb_sys::duckdb_execute_prepared(stmt, &mut result) };
            let error = if rc != libduckdb_sys::DuckDBSuccess {
                let err_ptr = unsafe { libduckdb_sys::duckdb_result_error(&mut result) };
                let msg = if err_ptr.is_null() {
                    "intermediate statement failed".to_string()
                } else {
                    unsafe { CStr::from_ptr(err_ptr) }.to_string_lossy().into_owned()
                };
                Some(msg)
            } else {
                None
            };

            // Always destroy both resources
            unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
            unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };

            if let Some(err) = error {
                return Err(Error::from(err));
            }
        }

        // Step 3: Prepare the final (or only) statement
        let mut stmt: libduckdb_sys::duckdb_prepared_statement = std::ptr::null_mut();
        let r = unsafe {
            libduckdb_sys::duckdb_prepare_extracted_statement(conn, extracted, num_stmts - 1, &mut stmt)
        };

        if r != libduckdb_sys::DuckDBSuccess {
            let err_str = get_prepare_error(stmt);
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

/// Get error message from a failed duckdb_prepare or duckdb_prepare_extracted_statement.
/// The `stmt` may be non-null even on error (DuckDB quirk). If non-null, we must
/// destroy it via duckdb_destroy_prepare before returning.
fn get_prepare_error(mut stmt: libduckdb_sys::duckdb_prepared_statement) -> String {
    if stmt.is_null() {
        return "prepare failed: statement is null".to_string();
    }
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
}

/// Get error message from a failed duckdb_extract_statements.
fn get_extract_error(extracted: libduckdb_sys::duckdb_extracted_statements) -> String {
    let err_ptr = unsafe { libduckdb_sys::duckdb_extract_statements_error(extracted) };
    if err_ptr.is_null() {
        "extract statements failed: unknown error".to_string()
    } else {
        unsafe { CStr::from_ptr(err_ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn statement_sql_clone_and_debug() {
        let stmt = DuckDbStatement {
            sql: "SELECT 1".to_string(),
        };
        assert_eq!(stmt.sql(), "SELECT 1");
        let cloned = stmt.clone();
        assert_eq!(cloned.sql(), "SELECT 1");
        assert!(format!("{cloned:?}").contains("DuckDbStatement"));
    }

    #[test]
    fn virtual_statement_without_handle() {
        let mut v = VirtualStatement::new("SELECT 2", true);
        assert!(v.handle_mut().is_none());
        v.reset(); // no handle: must be a no-op

        let cloned = v.clone();
        assert!(cloned.persistent);
        assert_eq!(cloned.query, "SELECT 2");
        assert!(format!("{cloned:?}").contains("VirtualStatement"));
    }

    #[test]
    fn error_helpers_handle_null_handles() {
        assert_eq!(
            get_prepare_error(std::ptr::null_mut()),
            "prepare failed: statement is null"
        );
        assert_eq!(
            get_extract_error(std::ptr::null_mut()),
            "extract statements failed: unknown error"
        );
    }

    struct TestConnection {
        db: libduckdb_sys::duckdb_database,
        con: libduckdb_sys::duckdb_connection,
    }

    impl TestConnection {
        fn new() -> Self {
            unsafe {
                let mut db = std::ptr::null_mut();
                assert_eq!(
                    libduckdb_sys::duckdb_open(std::ptr::null(), &mut db),
                    libduckdb_sys::DuckDBSuccess
                );
                let mut con = std::ptr::null_mut();
                assert_eq!(
                    libduckdb_sys::duckdb_connect(db, &mut con),
                    libduckdb_sys::DuckDBSuccess
                );
                Self { db, con }
            }
        }
    }

    impl Drop for TestConnection {
        fn drop(&mut self) {
            unsafe {
                libduckdb_sys::duckdb_disconnect(&mut self.con);
                libduckdb_sys::duckdb_close(&mut self.db);
            }
        }
    }

    #[test]
    fn prepare_reuses_and_clones_handle() {
        let conn = TestConnection::new();
        let mut stmt = VirtualStatement::new("SELECT 1", true);

        {
            let handle = stmt.prepare(conn.con).unwrap();
            let raw = handle.as_ptr();
            // Cloning shares ownership of the prepared statement; dropping the
            // clone must not destroy it while the original is alive.
            let cloned = handle.clone();
            assert_eq!(cloned.as_ptr(), raw);
            cloned.clear_bindings();
        }

        // A second prepare returns the already prepared handle rather than
        // preparing the query again.
        {
            let handle = stmt.prepare(conn.con).unwrap();
            assert!(!handle.as_ptr().is_null());

            // The statement is still usable after the clone above was dropped.
            let mut result: libduckdb_sys::duckdb_result = unsafe { std::mem::zeroed() };
            let rc =
                unsafe { libduckdb_sys::duckdb_execute_prepared(handle.as_ptr(), &mut result) };
            assert_eq!(rc, libduckdb_sys::DuckDBSuccess);
            unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
        }

        assert!(stmt.handle_mut().is_some());
        stmt.reset();
        drop(stmt);
    }

    #[test]
    fn prepare_empty_and_invalid_sql_errors() {
        let conn = TestConnection::new();

        let mut empty = VirtualStatement::new("   ", false);
        let err = empty.prepare(conn.con).unwrap_err();
        assert!(!err.to_string().is_empty());

        let mut invalid = VirtualStatement::new("NOT SQL", false);
        let err = invalid.prepare(conn.con).unwrap_err();
        assert!(!err.to_string().is_empty());
    }
}
