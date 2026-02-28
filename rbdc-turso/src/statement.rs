//! Statement abstraction for the Turso adapter.
//!
//! Lightweight metadata wrapper around SQL text.
//! Full implementation with column metadata is delivered in WP03.

/// A statement descriptor for the Turso adapter.
///
/// Holds SQL text. The actual compiled statement lives inside the
/// Turso connection; this is a metadata-only handle for structural
/// parity with `SqliteStatement`.
#[derive(Debug, Clone)]
pub struct TursoStatement {
    /// The SQL text.
    pub(crate) sql: String,
}

impl TursoStatement {
    /// Create a new statement from SQL text.
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

    /// Returns the SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }
}
