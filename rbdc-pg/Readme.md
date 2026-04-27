# RBDC

* A database driver abstraction

* Support zero copy serde-ser/de

Database -> bytes -> rbs::Value -> Struct(User Define)
Struct(User Define) -> rbs::ValueRef -> ref clone() -> Database


### How to Define a Custom Driver?

Implement the following traits and load the driver:
* `impl trait rbdc::db::{Driver, MetaData, Row, Connection, ConnectOptions, Placeholder}`


## Example

```rust
use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;
use rbdc_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = FastPool::new_url(PgDriver {}, "postgres://user:password@localhost:5432/database")?;
    let mut conn = pool.get().await?;
    let v = conn
        .exec_decode("SELECT 1 as test", vec![])
        .await?;
    println!("{}", v);
    Ok(())
}
```
