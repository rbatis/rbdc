use rbdc::db::{MetaData, Row};
use rbdc::error::Error;
use rbs::Value;
use std::sync::Arc;

/// Implementation of [`Row`] for Turso/libSQL.
#[derive(Debug)]
pub struct TursoRow {
    pub(crate) values: Vec<Value>,
    pub(crate) column_names: Arc<Vec<String>>,
}

// rbs::Value is Send, Arc<Vec<String>> is Send
unsafe impl Send for TursoRow {}

impl Row for TursoRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(TursoMetaData {
            column_names: self.column_names.clone(),
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
        // Remove and return value (destructive access per rbdc Row contract)
        Ok(self.values.remove(i))
    }
}

/// Metadata for a Turso/libSQL result set.
#[derive(Debug)]
pub struct TursoMetaData {
    pub(crate) column_names: Arc<Vec<String>>,
}

impl MetaData for TursoMetaData {
    fn column_len(&self) -> usize {
        self.column_names.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.column_names[i].clone()
    }

    fn column_type(&self, _i: usize) -> String {
        // Turso does not expose static column type info at the metadata level.
        // Return an empty string; type is determined dynamically from values.
        String::new()
    }
}
