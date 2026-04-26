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
    //if need decode use `let result:Vec<Table> = rbs::from_value(v)?;`
    Ok(())
}
