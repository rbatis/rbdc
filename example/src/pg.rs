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
    //if need decode use `let result:Vec<Table> = rbs::from_value(v)?;`
    Ok(())
}
