pub mod conn;
pub mod establish;
pub mod worker;

pub use conn::DuckDbDatabase;
pub use establish::DuckDbConnection;

use crate::statement::VirtualStatement;
use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use rbdc::common::StatementCache;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::error::Error;
use rbs::Value;

/// Connection state containing the handle and cached statements
///
/// IMPORTANT: `statements` MUST be declared before `handle` so that Rust drops
/// prepared statements (duckdb_destroy_prepare) BEFORE disconnecting
/// (duckdb_disconnect). If handle drops first, we'd call duckdb_destroy_prepare
/// on a dead connection, which may leak DuckDB internal resources.
pub(crate) struct DuckDbConnectionState {
    pub(crate) statements: DuckDbStatements,
    pub(crate) handle: crate::connection::conn::DuckDbConnectionHandle,
}

/// Statements manager following rbdc-sqlite architecture.
///
/// Manages an LRU cache of prepared statements plus a temp slot for non-cached statements.
/// Cache insertion happens only after successful prepare - failed prepares are never cached.
///
/// ## Negative caching (memory leak prevention)
///
/// When `duckdb_prepare` (or `duckdb_prepare_extracted_statement`) fails for a SQL string,
/// DuckDB may not fully release all associated memory even after `duckdb_destroy_prepare`.
/// Repeated prepare/destroy cycles for the same failing SQL cause unbounded memory growth.
///
/// To prevent this, `failed_stmt` tracks SQL strings whose prepare failed. On subsequent
/// calls with the same SQL, the cached error is returned immediately — no DuckDB API call
/// is made. The failed cache is cleared on any successful prepare, on `clear_cache()`,
/// or on `remove()`.
pub(crate) struct DuckDbStatements {
    /// LRU cached prepared statements for persistent queries
    cached: StatementCache<VirtualStatement>,
    /// Temporary statement slot (used when cache disabled, or as staging before cache insert)
    temp: Option<VirtualStatement>,
    /// Whether caching is enabled (set to false when statement_cache_size is 0)
    cache_enabled: bool,
    /// Negative cache: SQL strings whose prepare failed. Prevents repeated prepare/destroy
    /// cycles that leak memory in DuckDB for statements that cannot succeed (e.g. INSERT
    /// into non-existent table).
    failed_stmt: std::collections::HashSet<String>,
}

impl DuckDbStatements {
    fn new(cache_capacity: usize) -> Self {
        Self {
            cached: StatementCache::new(cache_capacity.max(1)),
            temp: None,
            cache_enabled: cache_capacity > 0,
            failed_stmt: std::collections::HashSet::new(),
        }
    }

    /// Get or prepare a statement, managing cache lifecycle.
    ///
    /// - If cached: returns the cached raw pointer (resets bindings first).
    /// - If in failed_stmt cache: returns cached error immediately (avoids repeated
    ///   DuckDB prepare/destroy cycle that leaks memory).
    /// - If not cached: creates a new VirtualStatement, prepares it via DuckDB, inserts into
    ///   cache (if enabled), and returns the raw pointer.
    pub(crate) fn prepare(
        &mut self,
        query: &str,
        conn: libduckdb_sys::duckdb_connection,
    ) -> Result<libduckdb_sys::duckdb_prepared_statement, Error> {
        // Fast path: cache hit
        if self.cache_enabled && self.cached.contains_key(query) {
            let stmt = self.cached.get_mut(query).unwrap();
            stmt.reset();
            return Ok(stmt.handle_mut().unwrap().as_ptr());
        }

        // Negative cache: this SQL previously failed to prepare; return cached error
        // to avoid repeated DuckDB prepare/destroy cycle that leaks memory.
        if self.failed_stmt.contains(query) {
            return Err(Error::from(format!(
                "Statement previously failed to prepare (negative cache): {}",
                query
            )));
        }

        // Prepare a new statement
        let mut vstmt = VirtualStatement::new(query, self.cache_enabled);
        let prepare_result = vstmt.prepare(conn);
        match prepare_result {
            Ok(_) => {
                // Any successful prepare means the schema may have changed; clear
                // the negative cache so previously-failed SQL can be retried.
                self.failed_stmt.clear();
                let ptr = vstmt.handle_mut().unwrap().as_ptr();

                // Insert into cache (or temp if caching disabled)
                if self.cache_enabled {
                    self.cached.insert(query, vstmt);
                } else {
                    self.temp = Some(vstmt);
                }

                Ok(ptr)
            }
            Err(e) => {
                // Prepare failed: record in negative cache to avoid repeated prepare/destroy
                self.failed_stmt.insert(query.to_string());
                Err(e)
            }
        }
    }

    /// Remove a statement from the cache (used when execution fails and the handle is corrupted).
    pub(crate) fn remove(&mut self, query: &str) {
        if self.cache_enabled {
            self.cached.remove(query);
        }
        self.temp = None;
        // Also clear negative cache so the statement can be re-prepared
        self.failed_stmt.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.cached.len()
    }

    pub(crate) fn clear(&mut self) {
        self.cached.clear();
        self.temp = None;
        self.failed_stmt.clear();
    }
}

impl Drop for DuckDbConnectionState {
    fn drop(&mut self) {
        // Finalize all prepared statements before DuckDbConnectionHandle drops (disconnects)
        self.statements.clear();
    }
}

impl Connection for DuckDbConnection {
    fn exec_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Box<dyn Row>, Error>>, Error>> {
        let sql = sql.to_owned();
        let params = params;

        Box::pin(async move {
            let rx = self.worker.exec_rows(sql, params).await?;

            let stream = futures_util::stream::unfold(rx, |rx| async move {
                match rx.recv().await {
                    Ok(Ok(Either::Left(row))) => Some((Ok(Box::new(row) as Box<dyn Row>), rx)),
                    Ok(Ok(Either::Right(_))) => None,
                    Ok(Err(e)) => Some((Err(e), rx)),
                    Err(_) => None,
                }
            });

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

        Box::pin(async move {
            let affected = self.worker.exec(sql, params).await?;
            Ok(ExecResult {
                rows_affected: affected,
                last_insert_id: Value::Null,
            })
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.worker.ping().await?;
            Ok(())
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.worker.shutdown().await?;
            Ok(())
        })
    }
}
