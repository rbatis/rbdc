use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crossfire::{spsc, AsyncTx};
use either::Either;
use futures_channel::oneshot;
use parking_lot::Mutex as ParkingMutex;
use rbdc::error::Error;

use crate::connection::conn::{DuckDbConnectionHandle, DuckDbDatabase};
use crate::connection::DuckDbConnectionState;
use crate::DuckDbRow;
use rbs::Value;

/// Shared state between worker and connection
pub(crate) struct DuckDbWorkerSharedState {
    pub(crate) cached_statements_size: AtomicUsize,
    pub(crate) conn: Arc<ParkingMutex<DuckDbConnectionState>>,
}

pub(crate) struct DuckDbWorker {
    pub command_tx: AsyncTx<crossfire::spsc::Array<Command>>,
    pub(crate) row_channel_size: usize,
    pub(crate) shared: Arc<DuckDbWorkerSharedState>,
}

unsafe impl Send for DuckDbWorker {}
unsafe impl Sync for DuckDbWorker {}

impl Drop for DuckDbWorker {
    fn drop(&mut self) {
        let (tx, _rx) = oneshot::channel();
        let _ = self.command_tx.try_send(Command::Shutdown { tx });
    }
}

pub(crate) enum Command {
    /// Execute a statement, streaming rows or affected count
    Execute {
        query: Box<str>,
        params: Vec<Value>,
        tx: crossfire::Tx<crossfire::spsc::Array<Result<Either<DuckDbRow, u64>, Error>>>,
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
        shared_database: Arc<ParkingMutex<Option<DuckDbDatabase>>>,
    ) -> Result<DuckDbWorker, Error> {
        let (establish_tx, establish_rx) = oneshot::channel();

        thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                let (command_tx, command_rx) = spsc::bounded_async_blocking(command_channel_size);

                if path != ":memory:" && !path.is_empty() {
                    if let Some(parent) = std::path::Path::new(&path).parent() {
                        if !parent.as_os_str().is_empty() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                    }
                }

                let db_raw = {
                    let mut guard = shared_database.lock();
                    if let Some(ref db) = *guard {
                        db.0
                    } else {
                        let db_path: *const std::os::raw::c_char = if path == ":memory:" || path.is_empty() {
                            ptr::null_mut()
                        } else {
                            CString::new(&*path)
                                .map(|s| s.into_raw())
                                .unwrap_or(ptr::null_mut())
                        };

                        let mut db: libduckdb_sys::duckdb_database = ptr::null_mut();
                        let r = unsafe { libduckdb_sys::duckdb_open(db_path, &mut db) };

                        if !db_path.is_null() && path != ":memory:" && !path.is_empty() {
                            drop(unsafe { CString::from_raw(db_path as *mut std::os::raw::c_char) });
                        }
                        if r != libduckdb_sys::DuckDBSuccess {
                            let _ = establish_tx.send(Err(Error::from("duckdb_open failed")));
                            return;
                        }

                        *guard = Some(DuckDbDatabase(db));
                        db
                    }
                };

                let mut con: libduckdb_sys::duckdb_connection = ptr::null_mut();
                let r = unsafe { libduckdb_sys::duckdb_connect(db_raw, &mut con) };
                if r != libduckdb_sys::DuckDBSuccess {
                    let _ = establish_tx.send(Err(Error::from("duckdb_connect failed")));
                    return;
                }
                let handle = DuckDbConnectionHandle::new_shared_db(db_raw, con);
                let shared = Arc::new(DuckDbWorkerSharedState {
                    cached_statements_size: AtomicUsize::new(0),
                    conn: Arc::new(ParkingMutex::new(DuckDbConnectionState {
                        handle,
                        statements: crate::connection::DuckDbStatements::new(statement_cache_size),
                    })),
                });
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
                        Command::Execute { query, params, tx } => {
                            let mut guard = shared.conn.lock();
                            execute::iter(&mut guard, &query, params, tx, &shared.cached_statements_size);
                        }
                        Command::Ping { tx } => {
                            let conn_guard = shared.conn.lock();
                            let sql_cstr = CString::new("SELECT 1").unwrap_or_default();
                            let r = unsafe {
                                libduckdb_sys::duckdb_query(conn_guard.handle.con, sql_cstr.as_ptr(), ptr::null_mut())
                            };
                            if r == libduckdb_sys::DuckDBSuccess {
                                tx.send(()).ok();
                            }
                        }
                        Command::ClearCache { tx } => {
                            let mut conn_guard = shared.conn.lock();
                            conn_guard.statements.clear();
                            shared.cached_statements_size.store(0, Ordering::Release);
                            tx.send(()).ok();
                        }
                        Command::Shutdown { tx } => {
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
            Err(_) => Err(Error::from("WorkerCrashed")),
        }
    }

    pub(crate) async fn exec_rows(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<crossfire::AsyncRx<crossfire::spsc::Array<Result<Either<DuckDbRow, u64>, Error>>>, Error> {
        let (tx, rx) = spsc::bounded_blocking_async(self.row_channel_size);

        self.command_tx
            .send(Command::Execute {
                query: sql.into(),
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
        let (tx, rx) = spsc::bounded_blocking_async(self.row_channel_size);

        self.command_tx
            .send(Command::Execute {
                query: sql.into(),
                params,
                tx,
            })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        // Collect results
        let mut rows_affected: u64 = 0;
        loop {
            match rx.recv().await {
                Ok(Ok(Either::Left(_))) => {}
                Ok(Ok(Either::Right(count))) => {
                    rows_affected = count;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => break,
            }
        }

        Ok(rows_affected)
    }

    pub(crate) async fn ping(&mut self) -> Result<(), Error> {
        self.oneshot_cmd(|tx| Command::Ping { tx }).await
    }

    pub(crate) async fn oneshot_cmd<F, T>(&mut self, command: F) -> Result<T, Error>
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(command(tx))
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        rx.await.map_err(|_| Error::from("WorkerCrashed"))
    }

    #[allow(unused)]
    pub(crate) async fn clear_cache(&mut self) -> Result<(), Error> {
        self.oneshot_cmd(|tx| Command::ClearCache { tx }).await
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

mod execute {
    use super::*;
    use crate::types::DuckDbArgumentValue;

    /// Execute a prepared statement, streaming rows or affected count through the channel.
    ///
    /// Architecture (following rbdc-sqlite pattern):
    /// 1. Get or prepare statement via `statements.prepare()` (handles cache lifecycle)
    /// 2. Bind parameters using direct Value matching (no accumulating Vec)
    /// 3. Execute via DuckDB FFI
    /// 4. On success: extract all rows, destroy result ONCE, send via channel
    /// 5. On error: destroy result, remove corrupted statement from cache
    /// 6. Reset statement bindings for next use
    /// 7. Update cached_statements_size atomic BEFORE sending final result (avoids races with readers)
    pub(crate) fn iter(
        conn: &mut DuckDbConnectionState,
        query: &str,
        params: Vec<Value>,
        tx: crossfire::Tx<crossfire::spsc::Array<Result<Either<DuckDbRow, u64>, Error>>>,
        cached_statements_size: &AtomicUsize,
    ) {
        // Step 1: Get or prepare statement (handles caching internally)
        let stmt_ptr = match conn.statements.prepare(query, conn.handle.con) {
            Ok(ptr) => ptr,
            Err(e) => {
                // Cache unchanged (failed prepare never cached), but sync atomic before error send
                cached_statements_size.store(conn.statements.len(), Ordering::Release);
                let _ = tx.send(Err(e));
                return;
            }
        };
        // Sync cache size atomic AFTER prepare (which may have inserted into cache)
        cached_statements_size.store(conn.statements.len(), Ordering::Release);

        // Step 2: Bind parameters
        bind_params(stmt_ptr, &params);

        // Step 3: Execute
        let mut result: libduckdb_sys::duckdb_result = unsafe { std::mem::zeroed() };
        let r = unsafe { libduckdb_sys::duckdb_execute_prepared(stmt_ptr, &mut result) };

        if r != libduckdb_sys::DuckDBSuccess {
            let err_ptr = unsafe { libduckdb_sys::duckdb_result_error(&mut result) };
            let err_str = if !err_ptr.is_null() {
                unsafe { CStr::from_ptr(err_ptr) }
                    .to_string_lossy()
                    .into_owned()
            } else {
                "execute failed".to_string()
            };
            unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
            // Remove corrupted statement from cache so it gets re-prepared next time
            conn.statements.remove(query);
            cached_statements_size.store(conn.statements.len(), Ordering::Release);
            let _ = tx.send(Err(Error::from(err_str)));
            return;
        }

        // Step 4: Process results
        let row_count = unsafe { libduckdb_sys::duckdb_row_count(&mut result) };
        let col_count = unsafe { libduckdb_sys::duckdb_column_count(&mut result) };

        // DuckDB prepared statements report row_count/col_count differently than duckdb_query.
        // For INSERT via prepared statement, row_count=1 and col_count=1 but it's DML.
        // Always use duckdb_rows_changed to distinguish: DML returns > 0, SELECT returns 0.
        let rows_changed = unsafe { libduckdb_sys::duckdb_rows_changed(&mut result) };

        if rows_changed == 0 && col_count > 0 && row_count > 0 {
            // SELECT-like query: stream row data
            let mut col_names = Vec::with_capacity(col_count as usize);
            for col_idx in 0..col_count as usize {
                let name_ptr = unsafe { libduckdb_sys::duckdb_column_name(&mut result, col_idx as u64) };
                if !name_ptr.is_null() {
                    let name = unsafe { CStr::from_ptr(name_ptr) }
                        .to_string_lossy()
                        .into_owned();
                    col_names.push(name);
                }
            }

            for row_idx in 0..row_count as usize {
                let values = crate::types::extract_row_values(&mut result, row_idx, col_count as usize);
                let duckdb_row = DuckDbRow::new(values, col_count as usize, col_names.clone());
                if tx.send(Ok(Either::Left(duckdb_row))).is_err() {
                    break;
                }
            }
        }

        // Always send rows_changed (for exec() caller which only reads Either::Right)
        let _ = tx.send(Ok(Either::Right(rows_changed as u64)));

        // Step 5: Destroy result AFTER all rows are extracted (critical fix:
        // previously this was inside the row loop, causing use-after-free for multi-row results)
        unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
    }

    /// Bind parameters to a prepared statement by directly matching on rbs::Value variants.
    ///
    /// This avoids the previous pattern of accumulating all encoded values into a Vec
    /// then using `.last()` for each parameter. Common types (Null, Bool, Int, Float,
    /// String, Binary) are handled directly; complex types (Array, Map, Ext) fall back
    /// to the Encode trait's conversion.
    fn bind_params(stmt: libduckdb_sys::duckdb_prepared_statement, params: &[Value]) {
        for (i, param) in params.iter().enumerate() {
            let idx = (i + 1) as u64;

            match param {
                Value::Null => unsafe { libduckdb_sys::duckdb_bind_null(stmt, idx); },
                Value::Bool(v) => unsafe { libduckdb_sys::duckdb_bind_int32(stmt, idx, *v as i32); },
                Value::I32(v) => unsafe { libduckdb_sys::duckdb_bind_int32(stmt, idx, *v); },
                Value::U32(v) => unsafe { libduckdb_sys::duckdb_bind_int32(stmt, idx, *v as i32); },
                Value::I64(v) => unsafe { libduckdb_sys::duckdb_bind_int64(stmt, idx, *v); },
                Value::U64(v) => unsafe { libduckdb_sys::duckdb_bind_int64(stmt, idx, *v as i64); },
                Value::F32(v) => unsafe { libduckdb_sys::duckdb_bind_double(stmt, idx, *v as f64); },
                Value::F64(v) => unsafe { libduckdb_sys::duckdb_bind_double(stmt, idx, *v); },
                Value::String(v) => {
                    // CString::new copies the string data; DuckDB binds copy immediately
                    if let Ok(cstr) = CString::new(v.as_str()) {
                        unsafe { libduckdb_sys::duckdb_bind_varchar(stmt, idx, cstr.as_ptr()); }
                    }
                }
                Value::Binary(v) => {
                    unsafe {
                        libduckdb_sys::duckdb_bind_blob(
                            stmt,
                            idx,
                            v.as_ptr() as *const std::ffi::c_void,
                            v.len() as u64,
                        );
                    }
                }
                // Array, Map, Ext types: convert via Encode trait
                _ => {
                    use crate::types::Encode;
                    let mut args = Vec::new();
                    if param.clone().encode(&mut args).is_ok() {
                        if let Some(arg) = args.last() {
                            bind_duckdb_arg(stmt, idx, arg);
                        }
                    }
                }
            }
        }
    }

    /// Bind a single DuckDbArgumentValue to a prepared statement parameter index.
    fn bind_duckdb_arg(
        stmt: libduckdb_sys::duckdb_prepared_statement,
        idx: u64,
        arg: &DuckDbArgumentValue,
    ) {
        match arg {
            DuckDbArgumentValue::Null => unsafe { libduckdb_sys::duckdb_bind_null(stmt, idx); },
            DuckDbArgumentValue::Int(v) => unsafe { libduckdb_sys::duckdb_bind_int32(stmt, idx, *v); },
            DuckDbArgumentValue::Int64(v) => unsafe { libduckdb_sys::duckdb_bind_int64(stmt, idx, *v); },
            DuckDbArgumentValue::Double(v) => unsafe { libduckdb_sys::duckdb_bind_double(stmt, idx, *v); },
            DuckDbArgumentValue::Text(v) => {
                if let Ok(cstr) = CString::new(v.as_str()) {
                    unsafe { libduckdb_sys::duckdb_bind_varchar(stmt, idx, cstr.as_ptr()); }
                }
            }
            DuckDbArgumentValue::Blob(v) => {
                unsafe {
                    libduckdb_sys::duckdb_bind_blob(
                        stmt,
                        idx,
                        v.as_ptr() as *const std::ffi::c_void,
                        v.len() as u64,
                    );
                }
            }
        }
    }
}
