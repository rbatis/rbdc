//! Connection module for the Turso/libSQL adapter.
//!
//! Provides the `TursoConnection` type implementing `rbdc::db::Connection`.
//! Query execution and value conversion logic is delegated to the `executor`
//! submodule and the `value` module respectively.

pub mod executor;

use crate::error::TursoError;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::error::Error;
use rbdc::try_stream;
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
    /// Whether to attempt JSON detection on TEXT values.
    pub(crate) json_detect: bool,
}

impl std::fmt::Debug for TursoConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoConnection").finish()
    }
}

impl Connection for TursoConnection {
    fn exec_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Box<dyn Row>, Error>>, Error>> {
        let sql = sql.to_owned();
        Box::pin(async move {
            let rows = self.execute_query(&sql, params).await?;
            let stream = try_stream! {
                for row in rows {
                    r#yield!(row);
                }
                Ok(())
            }
            .boxed();
            Ok(stream as BoxStream<'_, Result<Box<dyn Row>, Error>>)
        })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<ExecResult, Error>> {
        let sql = sql.to_owned();
        Box::pin(async move { self.execute_exec(&sql, params).await })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut rows = self.conn.query("SELECT 1", ()).await.map_err(|e| {
                log::warn!("turso: ping failed — backend may be unavailable: {}", e);
                TursoError::from(e)
            })?;
            let _ = rows.next().await.map_err(|e| {
                log::warn!("turso: ping failed while consuming probe result row: {}", e);
                TursoError::from(e)
            })?;
            Ok(())
        })
    }
}
