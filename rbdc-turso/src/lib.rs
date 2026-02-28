//! **Turso/libSQL** database driver for rbdc.
//!
//! This crate provides an async Turso/libSQL backend adapter with the same
//! `Driver`, `Connection`, `Row`, and `ConnectOptions` contract as other
//! rbdc adapters (e.g., `rbdc-sqlite`).
//!
//! ## Backend Selection
//!
//! Backend choice is fixed at adapter initialization/startup. There is:
//! - **No runtime backend switching** between Turso and SQLite.
//! - **No automatic fallback** to SQLite if Turso becomes unavailable.
//! - **No data migration** between SQLite and Turso.
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
pub mod row;

pub use driver::TursoDriver;
pub use driver::TursoDriver as Driver;
pub use error::TursoError;
pub use options::TursoConnectOptions;
pub use connection::TursoConnection;
pub use row::{TursoMetaData, TursoRow};
