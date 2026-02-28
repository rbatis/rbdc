//! Row and metadata types for the Turso/libSQL adapter.
//!
//! Implements the `rbdc::db::Row` and `rbdc::db::MetaData` traits,
//! matching the SQLite adapter's behavioral contract for value access,
//! bounds checking, and metadata introspection.

use crate::column::TursoColumn;
use crate::value::{turso_value_to_rbs, TursoValue};
use rbdc::db::{MetaData, Row};
use rbdc::error::Error;
use rbs::Value;
use std::sync::Arc;

/// Implementation of [`Row`] for Turso/libSQL.
///
/// Stores a vector of `TursoValue` (value + type metadata) and
/// shared column metadata. Values are removed on access via `get()`,
/// matching the SQLite adapter's `SqliteRow::try_take` behavior.
#[derive(Debug)]
pub struct TursoRow {
    pub(crate) values: Vec<TursoValue>,
    pub(crate) columns: Arc<Vec<TursoColumn>>,
}

// TursoValue contains libsql::Value (Send) and TursoDataType (Copy).
// Arc<Vec<TursoColumn>> is Send.
unsafe impl Send for TursoRow {}

impl Row for TursoRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(TursoMetaData {
            columns: self.columns.clone(),
        })
    }

    fn get(&mut self, i: usize) -> Result<Value, Error> {
        if i >= self.values.len() {
            return Err(Error::from(format!(
                "column index {} out of range (row has {} columns)",
                i,
                self.values.len()
            )));
        }
        // Remove and return value, consistent with SQLite adapter's
        // `SqliteRow::try_take` which uses `self.values.remove(index)`.
        let tv = self.values.remove(i);
        Ok(turso_value_to_rbs(&tv))
    }
}

/// Metadata for a Turso/libSQL result set.
///
/// Exposes column count, name, and type APIs matching the
/// `rbdc::db::MetaData` trait and SQLite adapter behavior.
#[derive(Debug)]
pub struct TursoMetaData {
    pub(crate) columns: Arc<Vec<TursoColumn>>,
}

impl MetaData for TursoMetaData {
    fn column_len(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.columns[i].name.clone()
    }

    fn column_type(&self, i: usize) -> String {
        self.columns[i].type_info.name().to_string()
    }
}
