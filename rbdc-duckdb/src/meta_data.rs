use rbdc::db::MetaData;
use std::fmt::Debug;

pub struct DuckDbMetaData {
    pub column_names: Vec<String>,
    pub column_count: usize,
}

impl Debug for DuckDbMetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DuckDbMetaData {{ column_count: {} }}",
            self.column_count
        )
    }
}

impl MetaData for DuckDbMetaData {
    fn column_len(&self) -> usize {
        self.column_count
    }

    fn column_name(&self, i: usize) -> String {
        self.column_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{}", i))
    }

    fn column_type(&self, _i: usize) -> String {
        // DuckDB type names
        "ANY".to_string()
    }
}
