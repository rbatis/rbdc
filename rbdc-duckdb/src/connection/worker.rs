use crossfire::{spsc, AsyncTx};
use futures_channel::oneshot;
use rbdc::error::Error;
use std::ffi::{CStr, CString};
use std::ptr;
use std::thread;

use crate::connection::conn::DuckDbConnectionHandle;
use crate::types::Encode;
use crate::DuckDbRow;
use rbs::Value;

pub(crate) struct DuckDbWorker {
    pub command_tx: AsyncTx<crossfire::spsc::Array<Command>>,
}

unsafe impl Send for DuckDbWorker {}
unsafe impl Sync for DuckDbWorker {}

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
}

impl DuckDbWorker {
    pub(crate) async fn establish(_path: String) -> Result<Self, Error> {
        let (establish_tx, establish_rx) = oneshot::channel();

        thread::Builder::new()
            .name("rbdc-duckdb".to_string())
            .spawn(move || {
                let (command_tx, command_rx) = spsc::bounded_async_blocking(16);

                // Open database using raw FFI
                let mut db: libduckdb_sys::duckdb_database = ptr::null_mut();
                let mut con: libduckdb_sys::duckdb_connection = ptr::null_mut();

                let r = unsafe { libduckdb_sys::duckdb_open(ptr::null_mut(), &mut db) };
                if r != libduckdb_sys::DuckDBSuccess {
                    let _ = establish_tx.send(Err(Error::from("duckdb_open failed")));
                    return;
                }

                let r = unsafe { libduckdb_sys::duckdb_connect(db, &mut con) };
                if r != libduckdb_sys::DuckDBSuccess {
                    let _ = establish_tx.send(Err(Error::from("duckdb_connect failed")));
                    return;
                }

                // Create RAII handle that will clean up on drop
                let handle = DuckDbConnectionHandle::new(db, con);

                if establish_tx.send(Ok(Self { command_tx })).is_err() {
                    return;
                }

                loop {
                    let cmd = match command_rx.recv() {
                        Ok(cmd) => cmd,
                        Err(_) => break,
                    };

                    match cmd {
                        Command::ExecRows { sql, params, tx } => {
                            // Prepare statement
                            let mut stmt: libduckdb_sys::duckdb_prepared_statement = ptr::null_mut();
                            let sql_cstr = CString::new(&*sql).unwrap();
                            let r = unsafe {
                                libduckdb_sys::duckdb_prepare(handle.con, sql_cstr.as_ptr(), &mut stmt)
                            };
                            if r != libduckdb_sys::DuckDBSuccess {
                                tx.send(Err(Error::from("prepare failed"))).ok();
                                continue;
                            }

                            // Bind parameters using Encode trait
                            let mut args = Vec::new();
                            for (i, param) in params.into_iter().enumerate() {
                                let idx = (i + 1) as u64;
                                let _ = param.encode(&mut args);
                                // Actually bind using duckdb_bind functions based on type
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

                                // Get column names
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
                            } else {
                                let err_ptr = unsafe { libduckdb_sys::duckdb_result_error(&mut result) };
                                if !err_ptr.is_null() {
                                    let err_str = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                                    tx.send(Err(Error::from(err_str))).ok();
                                } else {
                                    tx.send(Err(Error::from("execute failed"))).ok();
                                }
                            }

                            unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                        }
                        Command::Exec { sql, params, tx } => {
                            // Prepare statement
                            let mut stmt: libduckdb_sys::duckdb_prepared_statement = ptr::null_mut();
                            let sql_cstr = CString::new(&*sql).unwrap();
                            let r = unsafe {
                                libduckdb_sys::duckdb_prepare(handle.con, sql_cstr.as_ptr(), &mut stmt)
                            };
                            if r != libduckdb_sys::DuckDBSuccess {
                                tx.send(Err(Error::from("prepare failed"))).ok();
                                continue;
                            }

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
                                tx.send(Err(Error::from("execute failed"))).ok();
                            }

                            unsafe { libduckdb_sys::duckdb_destroy_result(&mut result) };
                            unsafe { libduckdb_sys::duckdb_destroy_prepare(&mut stmt) };
                        }
                        Command::Ping { tx } => {
                            let sql_cstr = CString::new("SELECT 1").unwrap();
                            let r = unsafe { libduckdb_sys::duckdb_query(handle.con, sql_cstr.as_ptr(), ptr::null_mut()) };
                            if r == libduckdb_sys::DuckDBSuccess {
                                tx.send(()).ok();
                            }
                        }
                        Command::Shutdown { tx } => {
                            // Drop the handle explicitly before sending response
                            // This will call duckdb_disconnect and duckdb_close via Drop impl
                            drop(handle);
                            let _ = tx.send(());
                            return;
                        }
                    }
                }
                // When loop exits (break), handle goes out of scope and Drop impl
                // will call duckdb_disconnect and duckdb_close automatically
            })?;

        establish_rx
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?
    }

    pub(crate) async fn exec_rows(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<crossfire::AsyncRx<crossfire::spsc::Array<Result<DuckDbRow, Error>>>, Error> {
        let (tx, rx) = spsc::bounded_blocking_async(16);

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

    pub(crate) async fn shutdown(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(Command::Shutdown { tx })
            .await
            .map_err(|_| Error::from("WorkerCrashed"))?;

        rx.await.map_err(|_| Error::from("WorkerCrashed"))
    }
}