# rbdc-duckdb

DuckDB database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Features

- High-performance async connection for DuckDB
- Full DuckDB data type support
- Connection pooling support
- Zero-copy serialization/deserialization
- In-memory and file-based database support

## Supported Connection String Formats

### 1. In-memory database
```
duckdb://:memory:
```

### 2. File-based database
```
duckdb://path/to/database.db
```

## Usage

```rust
use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;
use rbdc_duckdb::DuckDbDriver;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = FastPool::new_url(DuckDbDriver {}, "duckdb://target/duckdb.db")?;
    let mut conn = pool.get().await?;
    let v = conn
        .exec_decode("SELECT * FROM information_schema.tables", vec![])
        .await?;
    println!("{}", v);
    // if need decode use `let result:Vec<Table> = rbs::from_value(v)?;`
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

- [duckdb](https://github.com/duckdb/duckdb) - DuckDB database
- [url](https://github.com/servo/rust-url) - URL parsing

## License

This project is licensed under the same license as rbdc.
