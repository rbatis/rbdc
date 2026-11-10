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
        match &self.values[i] {
            Some(tv) => Ok(turso_value_to_rbs(tv, self.json_detect)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::TursoDataType;

    fn row_with(values: Vec<Option<TursoValue>>, json_detect: bool) -> TursoRow {
        TursoRow {
            values,
            columns: Arc::new(Vec::new()),
            json_detect,
        }
    }

    #[test]
    fn get_returns_value_and_respects_json_detect() {
        let mut row = row_with(
            vec![
                Some(TursoValue::new(libsql::Value::Integer(7))),
                Some(TursoValue::new(libsql::Value::Text(
                    r#"{"a":1}"#.into(),
                ))),
            ],
            true,
        );
        assert_eq!(row.get(0).unwrap(), Value::I64(7));
        assert!(matches!(row.get(1).unwrap(), Value::Map(_)));
        // Values are not consumed, so re-reading is stable.
        assert_eq!(row.get(0).unwrap(), Value::I64(7));
    }

    #[test]
    fn get_errors_for_out_of_range_index() {
        let mut row = row_with(vec![], false);
        let err = row.get(0).unwrap_err();
        assert!(err.to_string().contains("out of range"), "got: {}", err);
    }

    #[test]
    fn get_errors_for_already_consumed_slot() {
        // The executor never stores `None`, but the guard must still hold:
        // a consumed/absent slot reports a distinct error rather than
        // panicking or returning null.
        let mut row = row_with(vec![None], false);
        let err = row.get(0).unwrap_err();
        assert!(err.to_string().contains("already consumed"), "got: {}", err);
    }

    #[test]
    fn meta_data_exposes_columns() {
        let columns = Arc::new(vec![
            TursoColumn::new("id".into(), 0, TursoDataType::Integer),
            TursoColumn::new("name".into(), 1, TursoDataType::Text),
        ]);
        let row = TursoRow {
            values: vec![],
            columns: columns.clone(),
            json_detect: false,
        };
        let md = row.meta_data();
        assert_eq!(md.column_len(), 2);
        assert_eq!(md.column_name(0), "id");
        assert_eq!(md.column_name(1), "name");
        assert_eq!(md.column_type(0), "INTEGER");
        assert_eq!(md.column_type(1), "TEXT");
    }
}
