//! Column metadata for the Turso/libSQL adapter.
//!
//! Provides a column descriptor analogous to `rbdc-sqlite`'s `SqliteColumn`,
//! exposing name, ordinal position, and type information.

use crate::value::TursoDataType;

/// Metadata for a single column in a Turso/libSQL result set.
///
/// Tracks the column's name, ordinal position, and data type.
/// The data type may come from the column's declared type in the schema
/// or be inferred from the actual values in the result set.
#[derive(Debug, Clone)]
pub struct TursoColumn {
    /// Column name as returned by the query.
    pub(crate) name: String,
    /// Zero-based ordinal position in the result set.
    pub(crate) ordinal: usize,
    /// Data type, either from declared column type or inferred.
    pub(crate) type_info: TursoDataType,
}

impl TursoColumn {
    /// Create a new `TursoColumn`.
    pub fn new(name: String, ordinal: usize, type_info: TursoDataType) -> Self {
        Self {
            name,
            ordinal,
            type_info,
        }
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the zero-based ordinal position.
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }

    /// Returns the data type.
    pub fn type_info(&self) -> TursoDataType {
        self.type_info
    }
}
