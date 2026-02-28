//! **Turso/libSQL** database driver for rbdc.
//!
//! This crate provides an async Turso/libSQL backend adapter implementing the
//! standard `Driver`, `Connection`, `Row`, and `ConnectOptions` contracts from
//! the `rbdc` database abstraction layer.
//!
//! ## Backend Selection
//!
//! Backend choice is fixed at adapter initialization/startup. Once configured,
//! the Turso backend is used for all connections with no runtime switching
//! or automatic fallback.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rbdc_turso::{TursoDriver, TursoConnectOptions};
//! use rbdc::db::Driver;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let driver = TursoDriver {};
//! let mut conn = driver.connect("turso://:memory:").await?;
//! conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", vec![]).await?;
//! # Ok(())
//! # }
//! ```

pub mod connection;
pub mod driver;
pub mod error;
pub mod options;
pub mod query_result;
pub mod row;
pub mod statement;
pub mod value;

pub use connection::TursoConnection;
pub use driver::TursoDriver;
pub use driver::TursoDriver as Driver;
pub use error::TursoError;
pub use options::TursoConnectOptions;
pub use query_result::TursoQueryResult;
pub use row::{TursoMetaData, TursoRow};
pub use statement::TursoStatement;
pub use value::{TursoDataType, TursoValue};
