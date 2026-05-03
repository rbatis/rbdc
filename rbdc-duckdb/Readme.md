# rbdc-duckdb

DuckDB database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Basic Driver Usage

Full example: [example/src/duckdb.rs](../example/src/duckdb.rs)

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

## Usage with rbatis ORM

```rust
use rbatis::RBatis;
use rbatis::Error;

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    let rb = RBatis::new();
    rb.init(rbdc_duckdb::DuckDbDriver {}, "duckdb://target/duckdb.db")?;
    Ok(())
}
```

## Supported Connection String Formats

### 1. In-memory database
```
duckdb://:memory:
```

### 2. File-based database
```
duckdb://path/to/database.db
```

## License

This project is licensed under the same license as rbdc.
