/// Wrapper for DuckDB database and connection handles.
/// Implements Drop to ensure resources are properly released.
#[derive(Debug)]
pub(crate) struct DuckDbConnectionHandle {
    pub db: libduckdb_sys::duckdb_database,
    pub con: libduckdb_sys::duckdb_connection,
}

impl DuckDbConnectionHandle {
    pub fn new(db: libduckdb_sys::duckdb_database, con: libduckdb_sys::duckdb_connection) -> Self {
        Self { db, con }
    }
}

impl Drop for DuckDbConnectionHandle {
    fn drop(&mut self) {
        unsafe {
            if !self.con.is_null() {
                libduckdb_sys::duckdb_disconnect(&mut self.con);
            }
            if !self.db.is_null() {
                libduckdb_sys::duckdb_close(&mut self.db);
            }
        }
    }
}

unsafe impl Send for DuckDbConnectionHandle {}
