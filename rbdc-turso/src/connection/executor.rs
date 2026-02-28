//! Executor logic for the Turso connection.
//!
//! Query execution pipeline converting between `libsql` result types
//! and the rbdc trait types (Row, ExecResult).

use crate::column::TursoColumn;
use crate::connection::TursoConnection;
use crate::error::TursoError;
use crate::query_result::TursoQueryResult;
use crate::row::TursoRow;
use crate::value::{value_to_libsql, TursoDataType, TursoValue};
use rbdc::db::{ExecResult, Row};
use rbdc::error::Error;
use rbs::Value;
use std::sync::Arc;

impl TursoConnection {
    /// Execute a SELECT-style query and return rows.
    pub(crate) async fn execute_query(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<Box<dyn Row>>, Error> {
        let libsql_params: Vec<libsql::Value> = params
            .iter()
            .map(value_to_libsql)
            .collect::<Result<Vec<_>, _>>()?;

        let mut rows_result = self
            .conn
            .query(sql, libsql_params)
            .await
            .map_err(|e| {
                log::warn!("turso: query failed: {}", e);
                TursoError::from(e)
            })?;

        let column_count = rows_result.column_count() as usize;

        // Build column metadata. column_type() returns libsql::ValueType
        // directly -- no string parsing needed.
        let mut columns: Vec<TursoColumn> = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let name = rows_result
                .column_name(i as i32)
                .unwrap_or_default()
                .to_string();
            let type_info = rows_result
                .column_type(i as i32)
                .map(TursoDataType::from)
                .unwrap_or(TursoDataType::Null);
            columns.push(TursoColumn::new(name, i, type_info));
        }
        let columns = Arc::new(columns);

        let mut data: Vec<Box<dyn Row>> = Vec::new();
        while let Some(row) = rows_result.next().await.map_err(TursoError::from)? {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let v = row.get_value(i as i32).map_err(TursoError::from)?;
                // Use actual value type (more precise for dynamic typing).
                // Fall back to declared column type for nulls.
                let data_type = match &v {
                    libsql::Value::Null => columns[i].type_info,
                    other => TursoDataType::from(other),
                };
                values.push(TursoValue::with_type(v, data_type));
            }
            data.push(Box::new(TursoRow {
                values,
                columns: columns.clone(),
            }));
        }
        Ok(data)
    }

    /// Execute a non-SELECT statement and return the result.
    pub(crate) async fn execute_exec(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<ExecResult, Error> {
        let libsql_params: Vec<libsql::Value> = params
            .iter()
            .map(value_to_libsql)
            .collect::<Result<Vec<_>, _>>()?;

        let rows_affected = self
            .conn
            .execute(sql, libsql_params)
            .await
            .map_err(|e| {
                log::warn!("turso: exec failed: {}", e);
                TursoError::from(e)
            })?;

        let last_id = self.conn.last_insert_rowid();

        let result = TursoQueryResult::new(rows_affected, last_id);
        Ok(result.into())
    }
}
