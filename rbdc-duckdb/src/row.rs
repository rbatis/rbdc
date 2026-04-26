use crate::meta_data::DuckDbMetaData;
use rbdc::db::{MetaData, Row};
use rbdc::Error;
use rbs::Value;
use std::fmt::Debug;

pub struct DuckDbRow {
    pub values: Vec<Value>,
    pub column_count: usize,
    pub column_names: Vec<String>,
}

impl DuckDbRow {
    pub fn new(values: Vec<Value>, column_count: usize, column_names: Vec<String>) -> Self {
        Self { values, column_count, column_names }
    }
}

impl Debug for DuckDbRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DuckDbRow {{ values: {:?} }}", self.values)
    }
}

impl Row for DuckDbRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(DuckDbMetaData {
            column_names: self.column_names.clone(),
            column_count: self.column_count,
        })
    }

    fn get(&mut self, i: usize) -> Result<Value, Error> {
        self.values
            .get(i)
            .cloned()
            .ok_or_else(|| Error::from(format!("column index {} out of range", i)))
    }
}
