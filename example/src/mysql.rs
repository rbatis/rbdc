use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::ConnectionManager;
use rbdc::pool::Pool;
use rbdc_mysql::MysqlDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = FastPool::new(ConnectionManager::new(
        MysqlDriver {},
        "mysql://root:123456@localhost:3306/test",
    )?)?;
    let mut conn = pool.get().await?;
    let v = conn.get_values("select * from user", vec![]).await?;
    println!("{}", v);
    Ok(())
}
