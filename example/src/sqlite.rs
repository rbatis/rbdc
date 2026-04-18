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
        .exec_decode("select * from sqlite_master", vec![])
        .await?;
    println!("{}", v);
    
    #[derive(Debug,serde::Deserialize,serde::Serialize)]
    pub struct User { 
       pub name:String,
       pub sql:String
    }
    let result:Vec<User> = rbs::from_value(v)?;
    println!("{:?}", result);

    Ok(())
}
