use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pg::PgDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = FastPool::new_url(PgDriver {}, "postgres://postgres:123456@localhost:5432/postgres")?;
    let mut conn = pool.get().await?;
    let v = conn.get_values("select * from user", vec![]).await?;
    println!("{}", v);
    Ok(())
}
