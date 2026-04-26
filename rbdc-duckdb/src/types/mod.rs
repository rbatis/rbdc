//! Type conversions between Rust and DuckDB.

mod value;

pub use value::{value_to_param, extract_row_values};

/// DuckDB parameter type
pub type DuckDbParam = Box<dyn duckdb::ToSql>;

/// DuckDB type info
pub struct DuckDbTypeInfo(pub String);

/// DuckDB value reference for decoding
pub enum DuckDbValueRef<'a> {
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(&'a str),
    Binary(&'a [u8]),
}
