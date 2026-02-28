use crate::error::TursoError;
use crate::row::TursoRow;
use futures_core::future::BoxFuture;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::error::Error;
use rbs::Value;

/// A connection to a Turso database via the native async libsql API.
///
/// This connection is established at startup/initialization time and remains
/// bound to the configured backend for its entire lifetime. If the Turso
/// backend becomes unavailable, operations will return errors rather than
/// falling back to any other backend.
pub struct TursoConnection {
    #[allow(dead_code)]
    pub(crate) db: libsql::Database,
    pub(crate) conn: libsql::Connection,
}

// libsql::Connection is Send+Sync
unsafe impl Send for TursoConnection {}
unsafe impl Sync for TursoConnection {}

impl std::fmt::Debug for TursoConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoConnection").finish()
    }
}

/// Convert an `rbs::Value` parameter to a `libsql::Value` for binding.
fn value_to_libsql(v: &Value) -> Result<libsql::Value, Error> {
    match v {
        Value::Null => Ok(libsql::Value::Null),
        Value::Bool(b) => Ok(libsql::Value::Integer(if *b { 1 } else { 0 })),
        Value::I32(n) => Ok(libsql::Value::Integer(*n as i64)),
        Value::I64(n) => Ok(libsql::Value::Integer(*n)),
        Value::U32(n) => Ok(libsql::Value::Integer(*n as i64)),
        Value::U64(n) => Ok(libsql::Value::Integer(*n as i64)),
        Value::F32(f) => Ok(libsql::Value::Real(*f as f64)),
        Value::F64(f) => Ok(libsql::Value::Real(*f)),
        Value::String(s) => Ok(libsql::Value::Text(s.clone())),
        Value::Binary(b) => Ok(libsql::Value::Blob(b.clone())),
        Value::Ext(type_tag, val) => {
            // Handle rbdc extension types by converting to string representation
            match &**type_tag {
                // Decimal, Uuid, and date/time types are stored as text
                _ => match val.as_ref() {
                    Value::String(s) => Ok(libsql::Value::Text(s.clone())),
                    Value::I64(n) => Ok(libsql::Value::Integer(*n)),
                    Value::U64(n) => Ok(libsql::Value::Integer(*n as i64)),
                    Value::F64(f) => Ok(libsql::Value::Real(*f)),
                    _ => Ok(libsql::Value::Text(val.to_string())),
                },
            }
        }
        Value::Array(_) | Value::Map(_) => {
            // Serialize complex types as JSON text
            Ok(libsql::Value::Text(
                serde_json::to_string(v).unwrap_or_default(),
            ))
        }
    }
}

/// Convert a `libsql::Value` to an `rbs::Value`.
fn libsql_to_value(v: libsql::Value) -> Value {
    match v {
        libsql::Value::Null => Value::Null,
        libsql::Value::Integer(n) => Value::I64(n),
        libsql::Value::Real(f) => Value::F64(f),
        libsql::Value::Text(s) => Value::String(s),
        libsql::Value::Blob(b) => Value::Binary(b),
    }
}

impl Connection for TursoConnection {
    fn get_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<Vec<Box<dyn Row>>, Error>> {
        let sql = sql.to_owned();
        Box::pin(async move {
            let libsql_params: Vec<libsql::Value> = params
                .iter()
                .map(value_to_libsql)
                .collect::<Result<Vec<_>, _>>()?;

            let mut rows_result = self
                .conn
                .query(&sql, libsql_params)
                .await
                .map_err(|e| TursoError::from(e))?;

            let column_count = rows_result.column_count() as usize;
            let mut column_names: Vec<String> = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let name = rows_result
                    .column_name(i as i32)
                    .unwrap_or_default()
                    .to_string();
                column_names.push(name);
            }
            let column_names = std::sync::Arc::new(column_names);

            let mut data: Vec<Box<dyn Row>> = Vec::new();
            while let Some(row) = rows_result.next().await.map_err(|e| TursoError::from(e))? {
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let v = row
                        .get_value(i as i32)
                        .map_err(|e| TursoError::from(e))?;
                    values.push(libsql_to_value(v));
                }
                data.push(Box::new(TursoRow {
                    values,
                    column_names: column_names.clone(),
                }));
            }
            Ok(data)
        })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<ExecResult, Error>> {
        let sql = sql.to_owned();
        Box::pin(async move {
            let libsql_params: Vec<libsql::Value> = params
                .iter()
                .map(value_to_libsql)
                .collect::<Result<Vec<_>, _>>()?;

            let rows_affected = self
                .conn
                .execute(&sql, libsql_params)
                .await
                .map_err(|e| TursoError::from(e))?;

            // libsql execute returns the number of rows changed.
            // For last_insert_id we query last_insert_rowid().
            let last_id = self.conn.last_insert_rowid();

            Ok(ExecResult {
                rows_affected,
                last_insert_id: Value::U64(last_id as u64),
            })
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            // Execute a simple query to verify the connection is alive.
            // If the Turso backend is unavailable, this will fail (no fallback).
            // Use query() instead of execute() because SELECT returns rows.
            let mut rows = self
                .conn
                .query("SELECT 1", ())
                .await
                .map_err(|e| TursoError::from(e))?;
            // Consume the single result row
            let _ = rows.next().await;
            Ok(())
        })
    }
}
