use rbdc::pool::Pool;
use rbdc_mssql::MssqlDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() {
    //let uri = "jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;";
    //let uri = "sqlserver://SA:TestPass!123456@localhost:1433/master";
    //let uri = "Server=localhost,1433;User Id=SA;Password=TestPass!123456;Database=master;";
    let uri = "mssql://SA:TestPass!123456@localhost:1433/master";
    let pool = FastPool::new_url(MssqlDriver {}, uri).unwrap();
    let mut conn = pool.get().await.unwrap();
    let v = conn.get_values("SELECT 1", vec![]).await.unwrap();
    println!("{}", v);
}
