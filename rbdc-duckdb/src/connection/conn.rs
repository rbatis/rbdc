use std::ops::Deref;

/// Wrapper for duckdb_database to implement Send + Sync
#[derive(Debug)]
pub struct DuckDbDatabase(pub libduckdb_sys::duckdb_database);

// SAFETY: DuckDB database pointer can be shared across threads when using instance cache
unsafe impl Send for DuckDbDatabase {}
unsafe impl Sync for DuckDbDatabase {}

impl Deref for DuckDbDatabase {
    type Target = libduckdb_sys::duckdb_database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for DuckDbDatabase {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                libduckdb_sys::duckdb_close(&mut self.0);
            }
        }
    }
}

/// Wrapper for DuckDB database and connection handles.
/// Implements Drop to ensure resources are properly released.
#[derive(Debug)]
pub(crate) struct DuckDbConnectionHandle {
    // db 是可选的，因为共享数据库不应被单个连接关闭
    pub db: Option<libduckdb_sys::duckdb_database>,
    pub con: libduckdb_sys::duckdb_connection,
}

impl DuckDbConnectionHandle {
    #[allow(unused)]
    // 用于独立模式（db 和 connection 都归自己管理）
    pub fn new(db: libduckdb_sys::duckdb_database, con: libduckdb_sys::duckdb_connection) -> Self {
        Self { db: Some(db), con }
    }

    // 用于共享模式（只管理 connection，db 归全局管理）
    pub fn new_shared_db(_db: libduckdb_sys::duckdb_database, con: libduckdb_sys::duckdb_connection) -> Self {
        Self { db: None, con }
    }
}

impl Drop for DuckDbConnectionHandle {
    fn drop(&mut self) {
        unsafe {
            if !self.con.is_null() {
                libduckdb_sys::duckdb_disconnect(&mut self.con);
            }
            // 只关闭 db（如果是 Some），共享的 db 由全局管理器管理，不在这里关闭
            if let Some(mut db) = self.db.take() {
                libduckdb_sys::duckdb_close(&mut db);
            }
        }
    }
}

unsafe impl Send for DuckDbConnectionHandle {}
