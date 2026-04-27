//! DuckDB database driver for RBDC

pub mod connection;
pub mod driver;
pub mod error;
pub mod meta_data;
pub mod options;
pub mod row;
pub mod types;

pub use connection::DuckDbConnection;
pub use driver::DuckDbDriver;
pub use error::DuckDbError;
pub use meta_data::DuckDbMetaData;
pub use options::{DuckDbConnectOptions, DEFAULT_STATEMENT_CACHE_SIZE};
pub use row::DuckDbRow;

// Re-export traits for convenience
pub use rbdc::db::{Connection, Driver, Placeholder, Row, MetaData, ConnectOptions, ExecResult};
