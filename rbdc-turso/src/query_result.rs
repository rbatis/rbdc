//! Query result type for the Turso adapter.
//!
//! Wraps execution results (rows affected, last insert rowid).
//! Full implementation with `Extend` and `ExecResult` conversion
//! is delivered in WP03.

use rbdc::db::ExecResult;
use rbs::Value;

/// Result of executing a non-SELECT statement via Turso.
#[derive(Debug, Default, Clone)]
pub struct TursoQueryResult {
    /// Number of rows changed by the statement.
    pub(crate) changes: u64,
    /// Last inserted rowid.
    pub(crate) last_insert_rowid: i64,
}

impl TursoQueryResult {
    /// Create a new query result.
    pub fn new(changes: u64, last_insert_rowid: i64) -> Self {
        Self {
            changes,
            last_insert_rowid,
        }
    }

    /// Returns the number of rows affected.
    pub fn rows_affected(&self) -> u64 {
        self.changes
    }

    /// Returns the last inserted rowid.
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }

    /// Convert to `rbdc::db::ExecResult`.
    pub fn to_exec_result(&self) -> ExecResult {
        ExecResult {
            rows_affected: self.changes,
            last_insert_id: Value::U64(self.last_insert_rowid as u64),
        }
    }
}

impl From<TursoQueryResult> for ExecResult {
    fn from(r: TursoQueryResult) -> Self {
        r.to_exec_result()
    }
}
