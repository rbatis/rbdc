/// Wrapper for DuckDB database and connection handles.
/// Implements Drop to ensure resources are properly released.
#[derive(Debug)]
pub(crate) struct DuckDbConnectionHandle {
    // db 改为 Option，因为共享数据库不应被关闭
    #[allow(unused)]
    pub db: Option<libduckdb_sys::duckdb_database>,
    pub con: libduckdb_sys::duckdb_connection,
}

impl DuckDbConnectionHandle {
    #[allow(unused)]
    // 原有的构造函数（用于独立模式）
    pub fn new(db: libduckdb_sys::duckdb_database, con: libduckdb_sys::duckdb_connection) -> Self {
        Self { db: Some(db), con }
    }

    // 用于共享模式，不负责关闭 db
    pub fn new_without_db(con: libduckdb_sys::duckdb_connection) -> Self {
        Self { db: None, con }
    }
}

impl Drop for DuckDbConnectionHandle {
    fn drop(&mut self) {
        unsafe {
            if !self.con.is_null() {
                libduckdb_sys::duckdb_disconnect(&mut self.con);
            }
            // 注意：不关闭 db，因为它由全局缓存管理
            // 全局 db 会在程序退出时由 OnceLock 自动管理
        }
    }
}

unsafe impl Send for DuckDbConnectionHandle {}
