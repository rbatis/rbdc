//! Type conversions between Rust and DuckDB.

mod value;

pub use value::{
    Encode, DuckDbArgumentValue, DuckDbType, DuckDbTypeInfo, IsNull, extract_row_values,
    value_to_param, is_json_string,
};