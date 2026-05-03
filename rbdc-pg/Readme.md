# rbdc-pg

PostgreSQL database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Basic Driver Usage

Full example: [example/src/pg.rs](../example/src/pg.rs)

```rust
use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pg::PgDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let url = "postgres://postgres:123456@localhost:5432/postgres";
    let pool = FastPool::new_url(PgDriver {}, url)?;
    let mut conn = pool.get().await?;
    let v = conn.exec_decode("select * from user", vec![]).await?;
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
    rb.init(rbdc_pg::PgDriver {}, "postgres://postgres:123456@localhost:5432/postgres")?;
    Ok(())
}
```

## Supported Connection String Formats

### Standard URL format
```
postgres://user:password@host:port/database
```

## License

This project is licensed under the same license as rbdc.
