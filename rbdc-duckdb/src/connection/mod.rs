mod establish;

pub use establish::DuckDbConnection;
pub use establish::DuckDbConn;

use crate::error::DuckDbError;
use crate::types::{extract_row_values, value_to_param, DuckDbParam};
use crate::DuckDbRow;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::Error;
use rbs::Value;
use tokio::task;

impl Connection for DuckDbConnection {
    fn exec_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Box<dyn Row>, Error>>, Error>> {
        let sql = sql.to_owned();
        let params = params;
        let conn = self.conn.clone();
        Box::pin(async move {
            let stream = task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| Error::from(e.to_string()))?;
                let mut stmt = conn.prepare(&sql).map_err(DuckDbError::from)?;

                let rows: Vec<Box<dyn Row>> = if params.is_empty() {
                    let mut rows_iter = stmt.query([]).map_err(DuckDbError::from)?;
                    let mut rows = Vec::new();
                    loop {
                        match rows_iter.next() {
                            Ok(Some(row)) => {
                                let values = extract_row_values(&row);
                                rows.push(Box::new(DuckDbRow::new(values)) as Box<dyn Row>);
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                    rows
                } else {
                    let duckdb_params: Vec<DuckDbParam> = params.iter().map(|p| value_to_param(p)).collect();
                    let mut rows_iter = stmt.query(duckdb::params_from_iter(duckdb_params)).map_err(DuckDbError::from)?;
                    let mut rows = Vec::new();
                    loop {
                        match rows_iter.next() {
                            Ok(Some(row)) => {
                                let values = extract_row_values(&row);
                                rows.push(Box::new(DuckDbRow::new(values)) as Box<dyn Row>);
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                    rows
                };

                Ok::<_, Error>(futures_util::stream::iter(rows.into_iter().map(Ok::<_, Error>)))
            })
            .await
            .map_err(|e| Error::from(e.to_string()))??;

            Ok(stream.boxed() as BoxStream<'_, Result<Box<dyn Row>, Error>>)
        })
    }

    fn exec(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<ExecResult, Error>> {
        let sql = sql.to_owned();
        let params = params;
        let conn = self.conn.clone();
        Box::pin(async move {
            let result = task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| Error::from(e.to_string()))?;
                let affected = if params.is_empty() {
                    conn.execute(&sql, []).map_err(DuckDbError::from)?
                } else {
                    let duckdb_params: Vec<DuckDbParam> = params.iter().map(|p| value_to_param(p)).collect();
                    conn.execute(&sql, duckdb::params_from_iter(duckdb_params)).map_err(DuckDbError::from)?
                };
                Ok(ExecResult {
                    rows_affected: affected as u64,
                    last_insert_id: Value::Null,
                })
            })
            .await
            .map_err(|e| Error::from(e.to_string()))?;

            result
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        let conn = self.conn.clone();
        Box::pin(async move {
            task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| Error::from(e.to_string()))?;
                conn.execute("SELECT 1", []).map_err(DuckDbError::from)?;
                Ok::<_, Error>(())
            })
            .await
            .map_err(|e| Error::from(e.to_string()))?
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            Ok(())
        })
    }
}
