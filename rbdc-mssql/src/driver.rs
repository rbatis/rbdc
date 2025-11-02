use crate::{MssqlConnectOptions, MssqlConnection};
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc::{impl_exchange, Error};
use tiberius::Config;

#[derive(Debug)]
pub struct MssqlDriver {}

impl Driver for MssqlDriver {
    fn name(&self) -> &str {
        "mssql"
    }

    fn connect(&self, url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
        let url = url.to_owned();
        Box::pin(async move {
            let mut opt = self.default_option();
            opt.set_uri(&url)?;
            if let Some(opt) = opt.downcast_ref::<MssqlConnectOptions>() {
                let conn = MssqlConnection::establish(&opt.0).await?;
                Ok(Box::new(conn) as Box<dyn Connection>)
            } else {
                Err(Error::from("downcast_ref failure"))
            }
        })
    }

    fn connect_opt<'a>(
        &'a self,
        opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<'a, Result<Box<dyn Connection>, Error>> {
        let opt = opt.downcast_ref::<MssqlConnectOptions>().unwrap();
        Box::pin(async move {
            let conn = MssqlConnection::establish(&opt.0).await?;
            Ok(Box::new(conn) as Box<dyn Connection>)
        })
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        let mut config = Config::new();
        config.trust_cert();
        Box::new(MssqlConnectOptions(config))
    }
}

impl Placeholder for MssqlDriver {
    fn exchange(&self, sql: &str) -> String {
        impl_exchange("@P", 1, sql)
    }
}

#[cfg(test)]
mod test {
    use crate::driver::MssqlDriver;
    use rbdc::db::Placeholder;
    #[test]
    fn test_exchange() {
        let v = "insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        let d = MssqlDriver {};
        let sql = d.exchange(v);
        assert_eq!("insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12)", sql);
    }
    // #[tokio::test]
    // async fn test_mssql_pool() {
    //     use rbdc::pool::Pool;
    //     use rbdc_pool_fast::FastPool;
    //     let task = async move {
    //         //jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]
    //         let uri =
    //             "jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;";
    //         // let pool = Pool::new_url(MssqlDriver {}, "jdbc:sqlserver://SA:TestPass!123456@localhost:1433;database=test").unwrap();
    //         let pool = FastPool::new(ConnManager::new(MssqlDriver {}, uri).unwrap()).unwrap();
    //         let mut conn = pool.get().await.unwrap();
    //         let data = conn
    //             .get_values("SELECT 1", vec![])
    //             .await
    //             .unwrap();
    //         for mut x in data {
    //             println!("row: {}", x);
    //         }
    //     };
    //     task.await;
    // }
}
