use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crossfire::{spsc, AsyncTx};
use futures_channel::oneshot;
use parking_lot::Mutex as ParkingMutex;
use rbdc::common::StatementCache;
use rbdc::error::Error;

use crate::connection::conn::{DuckDbConnectionHandle, DuckDbDatabase};
use crate::types::Encode;
use crate::DuckDbRow;
use rbs::Value;

/// Connection state containing the handle and cached statements
pub(crate) struct DuckDbConnectionState {
    pub(crate) handle: DuckDbConnectionHandle,
    /// LRU cached prepared statements
    pub(crate) statements: StatementCache<libduckdb_sys::duckdb_prepared_statement>,
    /// Whether statement caching is enabled (false when cache size is 0)
    pub(crate) cache_enabled: bool,
}

impl Drop for DuckDbConnectionState {
    fn drop(&mut self) {
        // Must properly destroy all DuckDB prepared statements before dropping
        // StatementCache::clear() does not call destructors, so we use remove_lru
        while let Some(mut stmt) = self.statements.remove_lru() {
            unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
        }
    }
}

/// Shared state between worker and connection
pub(crate) struct DuckDbWorkerSharedState {
    pub(crate) cached_statements_size: AtomicUsize,
    pub(crate) conn: ParkingMutex<DuckDbConnectionState>,
}

pub(crate) struct DuckDbWorker {
    pub command_tx: AsyncTx<crossfire::spsc::Array<Command>>,
    pub(crate) row_channel_size: usize,
    #[allow(unused)]
    pub(crate) shared: Arc<DuckDbWorkerSharedState>,
}

unsafe impl Send for DuckDbWorker {}
unsafe impl Sync for DuckDbWorker {}

impl Drop for DuckDbWorker {
    fn drop(&mut self) {
        // 发送 Shutdown 命令通知 worker 线程关闭
        let (tx, _rx) = oneshot::channel();
        let _ = self.command_tx.try_send(Command::Shutdown { tx });
    }
}

#[allow(unused)]
pub(crate) enum Command {
    ExecRows {
        sql: Box<str>,
        params: Vec<Value>,
        tx: crossfire::Tx<crossfire::spsc::Array<Result<DuckDbRow, Error>>>,
    },
    Exec {
        sql: Box<str>,
        params: Vec<Value>,
        tx: oneshot::Sender<Result<u64, Error>>,
    },
    Ping {
        tx: oneshot::Sender<()>,
    },
    Shutdown {
        tx: oneshot::Sender<()>,
    },
    ClearCache {
        tx: oneshot::Sender<()>,
    },
}

impl DuckDbWorker {
    pub(crate) async fn establish(
        path: String,
        thread_name: String,
        command_channel_size: usize,
        row_channel_size: usize,
        statement_cache_size: usize,
        shared_database: Arc<Mutex<Option<DuckDbDatabase>>>,
    ) -> Result<DuckDbWorker, Error> {
        let (establish_tx, establish_rx) = oneshot::channel();

        thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                let (command_tx, command_rx) = spsc::bounded_async_blocking(command_channel_size);

                // 确保父目录存在（仅对文件数据库）
                if path != ":memory:" && !path.is_empty() {
                    if let Some(parent) = std::path::Path::new(&path).parent() {
                        if !parent.as_os_str().is_empty() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                    }
                }

                // 获取或创建共享的数据库实例
                log::debug!("DuckDB getting/creating shared database: path={}", path);
                let db_raw = {
                    let mut guard = shared_database.lock().unwrap();
                    if let Some(ref db) = *guard {
                        db.0
                    } else {
                        // 创建新的数据库实例
                        let db_path: *const std::os::raw::c_char = if path == ":memory:" || path.is_empty() {
                            ptr::null_mut()
                        } else {
                            std::ffi::CString::new(&*path)
                                .map(|s| s.into_raw())
                                .unwrap_or(ptr::null_mut())
                        };

                        let mut db: libduckdb_sys::duckdb_database = ptr::null_mut();
                        let r = unsafe { libduckdb_sys::duckdb_open(db_path, &mut db) };

                        // 释放 CString
                        if !db_path.is_null() && path != ":memory:" && !path.is_empty() {
                            drop(unsafe { std::ffi::CString::from_raw(db_path as *mut std::os::raw::c_char) });
                        }

                        if r != libduckdb_sys::DuckDBSuccess {
                            let _ = establish_tx.send(Err(Error::from("duckdb_open failed")));
                            return;
                        }

                        *guard = Some(DuckDbDatabase(db));
                        db
                    }
                };
                log::debug!("DuckDB database obtained: path={}", path);

                // 为这个 Worker 创建独立的连接
                let mut con: libduckdb_sys::duckdb_connection = ptr::null_mut();
                let r = unsafe { libduckdb_sys::duckdb_connect(db_raw, &mut con) };
                if r != libduckdb_sys::DuckDBSuccess {
                    let _ = establish_tx.send(Err(Error::from("duckdb_connect failed")));
                    return;
                }
                log::debug!("DuckDB connection created for thread: {}", thread_name);

                let handle = DuckDbConnectionHandle::new_shared_db(db_raw, con);

                // Create LRU cache - use at least 1 even if caching is disabled (StatementCache requires non-zero)
                let cache_capacity = statement_cache_size.max(1);
                let statements = StatementCache::new(cache_capacity);

                let shared = Arc::new(DuckDbWorkerSharedState {
                    cached_statements_size: AtomicUsize::new(0),
                    conn: ParkingMutex::new(DuckDbConnectionState {
                        handle,
                        statements,
                        cache_enabled: statement_cache_size > 0,
                    }),
                });

                // Lock connection for the worker thread
                let mut conn_guard = shared.conn.lock();

                let worker = Self {
                    command_tx,
                    row_channel_size,
                    shared: Arc::clone(&shared),
                };

                if establish_tx.send(Ok(worker)).is_err() {
                    return;
                }

                loop {
                    let cmd = match command_rx.recv() {
                        Ok(cmd) => cmd,
                        Err(_) => break,
                    };

                    match cmd {
                        Command::ExecRows { sql, params, tx } => {
                            let sql_str = (*sql).to_string();
                            let cache_enabled = conn_guard.cache_enabled;

                            // Get or prepare statement (only if caching is enabled)
                            let mut stmt: libduckdb_sys::duckdb_prepared_statement = if cache_enabled {
                                if let Some(cached_stmt) = conn_guard.statements.get_mut(&sql_str) {
                                    // Clear previous bindings
                                    unsafe { libduckdb_sys::duckdb_clear_bindings(*cached_stmt) };
                                    *cached_stmt
                                } else {
                                    // Need to prepare new statement
                                    let mut new_stmt: libduckdb_sys::duckdb_prepared_statement = ptr::null_mut();
                                    let sql_cstr = CString::new(&*sql).unwrap();
                                    let r = unsafe {
                                        libduckdb_sys::duckdb_prepare(conn_guard.handle.con, sql_cstr.as_ptr(), &mut new_stmt)
                                    };
                                    if r != libduckdb_sys::DuckDBSuccess {
                                        let err_msg = if new_stmt.is_null() {
                                            "prepare failed: statement is null".to_string()
                                        } else {
                                            let err_ptr = unsafe { libduckdb_sys::duckdb_prepare_error(new_stmt) };
                                            if err_ptr.is_null() {
                                                "prepare failed: unknown error".to_string()
                                            } else {
                                                let err_str = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                                                format!("prepare failed: {}", err_str)
                                            }
                                        };
                                        unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut new_stmt) };
                                        tx.send(Err(Error::from(err_msg))).ok();
                                        continue;
                                    }
                                    // Insert into LRU cache - StatementCache::insert returns evicted item if any
                                    if let Some(mut evicted_stmt) = conn_guard.statements.insert(&sql_str, new_stmt) {
                                        // Destroy the evicted statement to prevent resource leak
                                        unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut evicted_stmt) };
                                    }
                                    shared.cached_statements_size.store(conn_guard.statements.len(), Ordering::Release);
                                    new_stmt
                                }
                            } else {
                                // Caching disabled - always prepare fresh
                                let mut new_stmt: libduckdb_sys::duckdb_prepared_statement = ptr::null_mut();
                                let sql_cstr = CString::new(&*sql).unwrap();
                                let r = unsafe {
                                    libduckdb_sys::duckdb_prepare(conn_guard.handle.con, sql_cstr.as_ptr(), &mut new_stmt)
                                };
                                if r != libduckdb_sys::DuckDBSuccess {
                                    let err_msg = if new_stmt.is_null() {
                                        "prepare failed: statement is null".to_string()
                                    } else {
                                        let err_ptr = unsafe { libduckdb_sys::duckdb_prepare_error(new_stmt) };
                                        if err_ptr.is_null() {
                                            "prepare failed: unknown error".to_string()
                                        } else {
                                            let err_str = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                                            format!("prepare failed: {}", err_str)
                                        }
                                    };
                                    unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut new_stmt) };
                                    tx.send(Err(Error::from(err_msg))).ok();
                                    continue;
                                }
                                new_stmt
                            };

                            // Bind parameters
                            let mut args = Vec::new();
                            for (i, param) in params.into_iter().enumerate() {
                                let idx = (i + 1) as u64;
                                let _ = param.encode(&mut args);
                                if let Some(arg) = args.last() {
                                    match arg {
                                        crate::types::DuckDbArgumentValue::Null => {
                                            unsafe { libduckdb_sys::duckdb_bind_null(stmt, idx) };
                                        }
                                        crate::types::DuckDbArgumentValue::Int(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_int32(stmt, idx, *v) };
                                        }
                                        crate::types::DuckDbArgumentValue::Int64(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_int64(stmt, idx, *v) };
                                        }
                                        crate::types::DuckDbArgumentValue::Double(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_double(stmt, idx, *v) };
                                        }
                                        crate::types::DuckDbArgumentValue::Text(v) => {
                                            let cstr = CString::new(v.as_str()).unwrap();
                                            unsafe { libduckdb_sys::duckdb_bind_varchar(stmt, idx, cstr.as_ptr()) };
                                        }
                                        crate::types::DuckDbArgumentValue::Blob(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_blob(stmt, idx, v.as_ptr() as *const std::ffi::c_void, v.len() as u64) };
                                        }
                                    }
                                }
                            }

                            // Execute
                            let mut result: libduckdb_sys::duckdb_result = unsafe { std::mem::zeroed() };
                            let r = unsafe { libduckdb_sys::duckdb_execute_prepared(stmt, &mut result) };

                            if r == libduckdb_sys::DuckDBSuccess {
                                let row_count = unsafe { libduckdb_sys::duckdb_row_count(&mut result) };
                                let col_count = unsafe { libduckdb_sys::duckdb_column_count(&mut result) };

                                let mut col_names = Vec::new();
                                for col_idx in 0..col_count as usize {
                                    let name_ptr = unsafe { libduckdb_sys::duckdb_column_name(&mut result, col_idx as u64) };
                                    if !name_ptr.is_null() {
                                        let name = unsafe { CStr::from_ptr(name_ptr).to_string_lossy().into_owned() };
                                        col_names.push(name);
                                    }
                                }

                                for row_idx in 0..row_count as usize {
                                    let values = crate::types::extract_row_values(&mut result, row_idx, col_count as usize);
                                    let duckdb_row = DuckDbRow::new(values, col_count as usize, col_names.clone());
                                    tx.send(Ok(duckdb_row)).ok();
                                }

                                unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
                                // Do NOT call duckdb_destroy_prepare here if cached:
                                // - If caching enabled: statement is owned by conn_guard.statements (the cache)
                                // - If caching disabled: statement is leaked! We need to destroy it.
                                if !cache_enabled {
                                    unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                                }
                            } else {
                                let err_ptr = unsafe { libduckdb_sys::duckdb_result_error(&mut result) };
                                if !err_ptr.is_null() {
                                    let err_str = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                                    tx.send(Err(Error::from(err_str))).ok();
                                } else {
                                    tx.send(Err(Error::from("execute failed"))).ok();
                                }
                                unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
                                // Also need to destroy the statement if caching is disabled
                                if !cache_enabled {
                                    unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                                }
                            }
                        }
                        Command::Exec { sql, params, tx } => {
                            let sql_str = (*sql).to_string();

                            log::debug!("DuckDB Exec: sql_str length={}, sql={}", sql_str.len(), sql_str);

                            // Always prepare a new statement to avoid caching issues with DuckDB
                            let mut new_stmt: libduckdb_sys::duckdb_prepared_statement = ptr::null_mut();
                            let sql_cstr = CString::new(&*sql).unwrap();
                            let r = unsafe {
                                libduckdb_sys::duckdb_prepare(conn_guard.handle.con, sql_cstr.as_ptr(), &mut new_stmt)
                            };
                            if r != libduckdb_sys::DuckDBSuccess {
                                let err_msg = if new_stmt.is_null() {
                                    "prepare failed: statement is null".to_string()
                                } else {
                                    let err_ptr = unsafe { libduckdb_sys::duckdb_prepare_error(new_stmt) };
                                    if err_ptr.is_null() {
                                        "prepare failed: unknown error".to_string()
                                    } else {
                                        let err_str = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                                        format!("prepare failed: {}", err_str)
                                    }
                                };
                                unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut new_stmt) };
                                tx.send(Err(Error::from(err_msg))).ok();
                                continue;
                            }

                            let mut stmt = new_stmt;

                            // Bind parameters
                            let mut args = Vec::new();
                            for (i, param) in params.into_iter().enumerate() {
                                let idx = (i + 1) as u64;
                                let _ = param.encode(&mut args);
                                if let Some(arg) = args.last() {
                                    match arg {
                                        crate::types::DuckDbArgumentValue::Null => {
                                            unsafe { libduckdb_sys::duckdb_bind_null(stmt, idx) };
                                        }
                                        crate::types::DuckDbArgumentValue::Int(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_int32(stmt, idx, *v) };
                                        }
                                        crate::types::DuckDbArgumentValue::Int64(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_int64(stmt, idx, *v) };
                                        }
                                        crate::types::DuckDbArgumentValue::Double(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_double(stmt, idx, *v) };
                                        }
                                        crate::types::DuckDbArgumentValue::Text(v) => {
                                            let cstr = CString::new(v.as_str()).unwrap();
                                            unsafe { libduckdb_sys::duckdb_bind_varchar(stmt, idx, cstr.as_ptr()) };
                                        }
                                        crate::types::DuckDbArgumentValue::Blob(v) => {
                                            unsafe { libduckdb_sys::duckdb_bind_blob(stmt, idx, v.as_ptr() as *const std::ffi::c_void, v.len() as u64) };
                                        }
                                    }
                                }
                            }

                            // Execute
                            let mut result: libduckdb_sys::duckdb_result = unsafe { std::mem::zeroed() };
                            let r = unsafe { libduckdb_sys::duckdb_execute_prepared(stmt, &mut result) };

                            if r == libduckdb_sys::DuckDBSuccess {
                                let rows_changed = unsafe { libduckdb_sys::duckdb_rows_changed(&mut result) };
                                tx.send(Ok(rows_changed as u64)).ok();
                            } else {
                                let err_ptr = unsafe { libduckdb_sys::duckdb_result_error(&mut result) };
                                if !err_ptr.is_null() {
                                    let err_str = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                                    tx.send(Err(Error::from(err_str))).ok();
                                } else {
                                    tx.send(Err(Error::from("execute failed"))).ok();
                                }
                            }

                            unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
                            unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                        }
                        Command::Ping { tx } => {
                            let sql_cstr = CString::new("SELECT 1").unwrap();
                            let r = unsafe { libduckdb_sys::duckdb_query(conn_guard.handle.con, sql_cstr.as_ptr(), ptr::null_mut()) };
                            if r == libduckdb_sys::DuckDBSuccess {
                                tx.send(()).ok();
                            }
                        }
                        Command::ClearCache { tx } => {
                            while let Some(mut stmt) = conn_guard.statements.remove_lru() {
                                unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                            }
                            shared.cached_statements_size.store(0, Ordering::Release);
                            tx.send(()).ok();
                        }
                        Command::Shutdown { tx } => {
                            // 只销毁当前连接和语句，不关闭共享的 db
                            drop(conn_guard);
                            drop(shared);
                            let _ = tx.send(());
                            return;
                        }
                    }
                }
            })?;

        let result = establish_rx.await;
        match result {
            Ok(worker) => worker,
            Err(_) => return Err(Error::from("WorkerCrashed")),
        }
    }

    pub(crate) async fn exec_rows(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<crossfire::AsyncRx<crossfire::spsc::Array<Result<DuckDbRow, Error>>>, Error> {
        let (tx, rx) = spsc::bounded_blocking_async(self.row_channel_size);

        self.command_tx
            .send(Command::ExecRows {
                sql: sql.into(),
                params,
                tx,
            })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        Ok(rx)
    }

    pub(crate) async fn exec(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<u64, Error> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(Command::Exec {
                sql: sql.into(),
                params,
                tx,
            })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        rx.await.map_err(|_| Error::from("WorkerCrashed"))?
    }

    pub(crate) async fn ping(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(Command::Ping { tx })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        rx.await.map_err(|_| Error::from("WorkerCrashed"))
    }

    #[allow(unused)]
    pub(crate) async fn clear_cache(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(Command::ClearCache { tx })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        rx.await.map_err(|_| Error::from("WorkerCrashed"))
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(Command::Shutdown { tx })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        rx.await.map_err(|_| Error::from("WorkerCrashed"))
    }

    #[allow(unused)]
    pub fn cached_statements_size(&self) -> usize {
        self.shared.cached_statements_size.load(Ordering::Acquire)
    }
}
