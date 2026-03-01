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
/// Stores a vector of `Option<TursoValue>` (value + type metadata) and
/// shared column metadata. Values are consumed on access via `get()`,
/// using `Option::take()` for index-stable destructive reads (matching
/// the MySQL and Postgres adapter pattern).
#[derive(Debug)]
pub struct TursoRow {
    pub(crate) values: Vec<Option<TursoValue>>,
    pub(crate) columns: Arc<Vec<TursoColumn>>,
    /// Whether to attempt JSON detection on TEXT values.
    pub(crate) json_detect: bool,
}

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
        // Take the value, leaving None in place. Index-stable unlike
        // Vec::remove(). Matches the MySQL/Postgres adapter pattern.
        match self.values[i].take() {
            Some(tv) => Ok(turso_value_to_rbs(&tv, self.json_detect)),
            None => Err(Error::from(format!("column index {} already consumed", i))),
        }
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
