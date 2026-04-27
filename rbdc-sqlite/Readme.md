# rbdc-sqlite

SQLite database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Features

- High-performance async connection based on libsqlite3
- Full SQLite data type support
- Connection pooling support
- Zero-copy serialization/deserialization
- Bundled SQLite support (no external dependencies)
- SQLCipher encryption support (optional)

## Supported Connection String Formats

### 1. In-memory database
```
sqlite://:memory:
```

### 2. File-based database
```
sqlite://path/to/database.db
```

## Usage

```rust
use rbdc::pool::ConnectionManager;
use rbdc_sqlite::SqliteDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In-memory database
    let uri = "sqlite://:memory:";

    // Or file-based database
    // let uri = "sqlite://path/to/database.db";

    // Create connection manager
    let manager = ConnectionManager::new(SqliteDriver {}, uri)?;

    // Use connection pool
    let pool = rbdc_pool_fast::FastPool::new(manager)?;
    let mut conn = pool.get().await?;

    // Execute query
    let result = conn.exec_decode("SELECT 1 as test", vec![]).await?;
    println!("Result: {:?}", result);

    Ok(())
}
```

## RBDC Architecture

- Database driver abstraction layer
- Zero-copy serialization/deserialization

Data flow: Database -> bytes -> rbs::Value -> Struct(User Define)
Reverse: Struct(User Define) -> rbs::ValueRef -> ref clone() -> Database

### How to Define a Custom Driver?

Implement the following traits and load the driver:
* `impl trait rbdc::db::{Driver, MetaData, Row, Connection, ConnectOptions, Placeholder}`

## Dependencies

- [libsqlite3-sys](https://github.com/rusqlite/rusqlite) - SQLite bindings
- [url](https://github.com/servo/rust-url) - URL parsing
- [percent-encoding](https://github.com/servo/rust-url/tree/master/percent_encoding) - URL encoding

## License

This project is licensed under the same license as rbdc.
