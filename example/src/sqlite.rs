use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;
use rbdc_sqlite::SqliteDriver;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = FastPool::new_url(SqliteDriver {}, "sqlite://target/sqlite.db")?;
    pool.set_conn_max_lifetime(Some(Duration::from_secs(10)))
        .await;
    let mut conn = pool.get().await?;
    let v = conn
        .get_values("select * from sqlite_master", vec![])
        .await?;
    println!("{}", v);
    Ok(())
}
