use rbdc::Error;
use rbdc::pool::Pool;
use rbdc_mssql::MssqlDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), Error> {
    //let uri = "jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;";
    //let uri = "sqlserver://SA:TestPass!123456@localhost:1433/master";
    //let uri = "Server=localhost,1433;User Id=SA;Password=TestPass!123456;Database=master;";
    let uri = "mssql://SA:TestPass!123456@localhost:1433/master";
    let pool = FastPool::new_url(MssqlDriver {}, uri)?;
    let mut conn = pool.get().await?;
    let v = conn.exec_decode("SELECT DB_NAME() AS CurrentDatabase", vec![]).await?;
    println!("{}", v);
    //if need decode use `let result:Vec<Table> = rbs::from_value(v)?;`
    Ok(())
}
