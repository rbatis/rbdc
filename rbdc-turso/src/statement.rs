//! Statement abstraction for the Turso adapter.
//!
//! Lightweight metadata wrapper around SQL text and column info.
//! Unlike the SQLite adapter which maintains compiled statement handles,
//! Turso handles compilation internally via `Connection::query/execute`.
//! Parameter counting comes from `libsql::Statement::parameter_count()`
//! when needed, not from string parsing.

use crate::column::TursoColumn;
use std::sync::Arc;

/// A statement descriptor for the Turso adapter.
///
/// Holds SQL text and column metadata. The actual compiled statement
/// lives inside the Turso connection; this is a metadata-only handle
/// used for structural parity with `SqliteStatement`.
#[derive(Debug, Clone)]
pub struct TursoStatement {
    /// The SQL text.
    pub(crate) sql: String,
    /// Column metadata (populated after execution).
    pub(crate) columns: Arc<Vec<TursoColumn>>,
}

impl TursoStatement {
    /// Create a new statement from SQL text.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            columns: Arc::new(Vec::new()),
        }
    }

    /// Returns the SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns column metadata.
    pub fn columns(&self) -> &[TursoColumn] {
        &self.columns
    }
}
