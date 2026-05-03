# rbdc-sqlite

SQLite database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Basic Driver Usage

Full example: [example/src/sqlite.rs](../example/src/sqlite.rs)

```rust
use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;
use rbdc_sqlite::SqliteDriver;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = FastPool::new_url(SqliteDriver {}, "sqlite://target/sqlite.db")?;
    let mut conn = pool.get().await?;
    let v = conn
        .exec_decode("select * from sqlite_master", vec![])
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
    rb.init(rbdc_sqlite::SqliteDriver {}, "sqlite://target/sqlite.db")?;
    Ok(())
}
```

## Supported Connection String Formats

### 1. In-memory database
```
sqlite://:memory:
```

### 2. File-based database
```
sqlite://path/to/database.db
```

## License

This project is licensed under the same license as rbdc.
