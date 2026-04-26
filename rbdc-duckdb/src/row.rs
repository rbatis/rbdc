use crate::meta_data::DuckDbMetaData;
use rbdc::db::{MetaData, Row};
use rbdc::Error;
use rbs::Value;
use std::fmt::Debug;

pub struct DuckDbRow {
    pub values: Vec<Value>,
    pub column_count: usize,
}

impl DuckDbRow {
    pub fn new(values: Vec<Value>, column_count: usize) -> Self {
        Self { values, column_count }
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
            column_names: vec![],
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
