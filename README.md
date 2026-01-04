# rbdc

[![Crates.io](https://img.shields.io/crates/v/rbdc)](https://crates.io/crates/rbdc)
[![Documentation](https://docs.rs/rbdc/badge.svg)](https://docs.rs/rbdc)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Database driver abstraction layer for Rust, providing a unified interface for [rbatis](https://github.com/rbatis/rbatis).

## Features

- **Safe**: `#![forbid(unsafe_code)]` - 100% safe Rust
- **Async**: Native async support based on Tokio
- **Extensible**: Simple trait definitions for easy driver implementation

## Supported Databases

| Database | Driver |
|----------|--------|
| MySQL | [rbdc-mysql](https://crates.io/crates/rbdc-mysql) |
| PostgreSQL | [rbdc-pg](https://crates.io/crates/rbdc-pg) |
| SQLite | [rbdc-sqlite](https://crates.io/crates/rbdc-sqlite) |
| MSSQL | [rbdc-mssql](https://crates.io/crates/rbdc-mssql) |

## Quick Start

```rust
use rbdc_sqlite::SqliteDriver;
use rbdc::pool::ConnManager;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), rbdc::Error> {
    let pool = FastPool::new(ConnManager::new(
        SqliteDriver {},
        "sqlite://target/test.db"
    )?)?;
    let mut conn = pool.get().await?;
    let v = conn.get_values("SELECT * FROM sqlite_master", vec![]).await?;
    println!("{}", v);
    //if need decode use `let result:Vec<Table> = rbs::from_value(v)?;`
    Ok(())
}
```

## Implement Custom Driver

Implement these 6 traits:

```rust
use rbdc::db::{Driver, MetaData, Row, Connection, ConnectOptions, Placeholder};

impl Driver for YourDriver {}
impl MetaData for YourMetaData {
     //TODO imple methods
}
impl Row for YourRow {
     //TODO imple methods
}
impl Connection for YourConnection {
     //TODO imple methods
}
impl ConnectOptions for YourConnectOptions {
     //TODO imple methods
}
impl Placeholder for YourPlaceholder {
     //TODO imple methods
}
/// use your driver
#[tokio::main]
async fn main() -> Result<(), rbdc::Error> {
    let uri = "YourDriver://****";
    let pool = FastPool::new_url(YourDriver{}, uri)?;
    let mut conn = pool.get().await?;
    let v = conn.get_values("SELECT 1", vec![]).await?;
    println!("{}", v);
}
```

For databases with blocking APIs, refer to `rbdc-sqlite` which uses the `flume` channel library.

See [examples](./examples) for more.

## License

MIT