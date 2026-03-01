//! Query result type for the Turso/libSQL adapter.
//!
//! Wraps execution results (rows affected, last insert rowid) in a type
//! analogous to `rbdc-sqlite`'s `SqliteQueryResult`, with explicit
//! semantics for rows-affected and last-insert-id.

use rbdc::db::ExecResult;
use rbs::Value;

/// Result of executing a non-SELECT statement via Turso/libSQL.
///
/// Stores the number of rows affected and the last inserted rowid,
/// matching the SQLite adapter's `SqliteQueryResult` contract.
#[derive(Debug, Default, Clone)]
pub struct TursoQueryResult {
    /// Number of rows changed by the statement.
    pub(crate) changes: u64,
    /// Last inserted rowid (from `last_insert_rowid()`).
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

    /// Convert to the `rbdc::db::ExecResult` type used by the `Connection` trait.
    ///
    /// The `last_insert_id` is stored as `Value::I64` to faithfully preserve
    /// the signed rowid semantics of SQLite/libSQL (rowids are i64).
    pub fn to_exec_result(&self) -> ExecResult {
        ExecResult {
            rows_affected: self.changes,
            last_insert_id: Value::I64(self.last_insert_rowid),
        }
    }
}

impl From<TursoQueryResult> for ExecResult {
    fn from(r: TursoQueryResult) -> Self {
        r.to_exec_result()
    }
}

/// Extend a `TursoQueryResult` with results from additional statements.
///
/// Accumulates changes and takes the last rowid, matching the
/// `SqliteQueryResult::Extend` behavior.
impl Extend<TursoQueryResult> for TursoQueryResult {
    fn extend<T: IntoIterator<Item = TursoQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.changes += elem.changes;
            self.last_insert_rowid = elem.last_insert_rowid;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let r = TursoQueryResult::new(5, 42);
        assert_eq!(r.rows_affected(), 5);
        assert_eq!(r.last_insert_rowid(), 42);
    }

    #[test]
    fn test_default() {
        let r = TursoQueryResult::default();
        assert_eq!(r.rows_affected(), 0);
        assert_eq!(r.last_insert_rowid(), 0);
    }

    #[test]
    fn test_to_exec_result() {
        let r = TursoQueryResult::new(3, 10);
        let exec = r.to_exec_result();
        assert_eq!(exec.rows_affected, 3);
        assert_eq!(exec.last_insert_id, Value::I64(10));
    }

    #[test]
    fn test_into_exec_result() {
        let r = TursoQueryResult::new(1, 7);
        let exec: ExecResult = r.into();
        assert_eq!(exec.rows_affected, 1);
        assert_eq!(exec.last_insert_id, Value::I64(7));
    }

    #[test]
    fn test_extend() {
        let mut r = TursoQueryResult::new(2, 5);
        r.extend(vec![
            TursoQueryResult::new(3, 10),
            TursoQueryResult::new(1, 15),
        ]);
        assert_eq!(r.rows_affected(), 6); // 2 + 3 + 1
        assert_eq!(r.last_insert_rowid(), 15); // last one wins
    }
}
