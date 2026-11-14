use crate::{MssqlConnectOptions, MssqlConnection};
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc::{impl_exchange, Error};
use rbs::Value;
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
        let opt = opt.downcast_ref::<MssqlConnectOptions>().expect(
            "MssqlDriver::connect_opt requires MssqlConnectOptions, got a different ConnectOptions type",
        );
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

    fn column_type(&self, val: &Value) -> String {
        match val {
            Value::Null => "NULL",
            Value::Bool(_) => "BIT",
            Value::I32(_) => "INT",
            Value::I64(_) => "BIGINT",
            Value::U32(v) if *v <= i32::MAX as u32 => "INT",
            Value::U32(_) => "BIGINT",
            Value::U64(_) => "BIGINT",
            Value::F32(_) => "REAL",
            Value::F64(_) => "FLOAT",
            Value::String(_) => "VARCHAR(MAX)",
            Value::Binary(_) => "VARBINARY(MAX)",
            Value::Array(_) => "VARCHAR(MAX)",
            Value::Map(_) => "VARCHAR(MAX)",
            Value::Ext(t, _) => match *t {
                "Date" => "DATE",
                "DateTime" => "DATETIME2",
                "Time" => "TIME",
                "Decimal" => "DECIMAL",
                "Timestamp" => "BIGINT",
                "Uuid" => "UNIQUEIDENTIFIER",
                _ => "VARCHAR(MAX)",
            },
        }
        .to_string()
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
    use crate::MssqlConnectOptions;
    use futures_util::FutureExt;
    use rbdc::db::{ConnectOptions, Driver, Placeholder};
    use rbs::Value;
    use tiberius::Config;

    #[test]
    fn test_exchange() {
        let v = "insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        let d = MssqlDriver {};
        let sql = d.exchange(v);
        assert_eq!("insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12)", sql);
    }

    #[test]
    fn test_name_and_default_option() {
        let driver = MssqlDriver {};
        assert_eq!(driver.name(), "mssql");

        let option = driver.default_option();
        assert!(option.downcast_ref::<MssqlConnectOptions>().is_some());
    }

    #[test]
    fn test_column_type() {
        let driver = MssqlDriver {};
        let cases = [
            (Value::Null, "NULL"),
            (Value::Bool(true), "BIT"),
            (Value::I32(1), "INT"),
            (Value::I64(1), "BIGINT"),
            (Value::U32(1), "INT"),
            (Value::U64(1), "BIGINT"),
            (Value::F32(1.0), "REAL"),
            (Value::F64(1.0), "FLOAT"),
            (Value::String("s".into()), "VARCHAR(MAX)"),
            (Value::Binary(vec![1]), "VARBINARY(MAX)"),
            (Value::Array(vec![]), "VARCHAR(MAX)"),
            (Value::Map(rbs::value::map::ValueMap::new()), "VARCHAR(MAX)"),
            (Value::Ext("Date", Box::new(Value::Null)), "DATE"),
            (Value::Ext("DateTime", Box::new(Value::Null)), "DATETIME2"),
            (Value::Ext("Time", Box::new(Value::Null)), "TIME"),
            (Value::Ext("Decimal", Box::new(Value::Null)), "DECIMAL"),
            (Value::Ext("Timestamp", Box::new(Value::Null)), "BIGINT"),
            (Value::Ext("Uuid", Box::new(Value::Null)), "UNIQUEIDENTIFIER"),
            (Value::Ext("Json", Box::new(Value::Null)), "VARCHAR(MAX)"),
        ];
        for (value, expected) in cases {
            assert_eq!(driver.column_type(&value), expected, "value: {:?}", value);
        }
    }

    #[test]
    fn test_connect_with_invalid_url_fails_before_connecting() {
        let driver = MssqlDriver {};
        let result = driver
            .connect("Server=a,b,c;Database=db")
            .now_or_never()
            .expect("invalid URL should fail without I/O");
        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "requires MssqlConnectOptions")]
    fn test_connect_opt_wrong_type_panics() {
        #[derive(Debug)]
        struct OtherOptions;

        impl ConnectOptions for OtherOptions {
            fn connect(
                &self,
            ) -> futures_core::future::BoxFuture<'_, Result<Box<dyn rbdc::db::Connection>, rbdc::Error>>
            {
                Box::pin(async { Err(rbdc::Error::from("unused")) })
            }

            fn set_uri(&mut self, _url: &str) -> Result<(), rbdc::Error> {
                Ok(())
            }
        }

        let driver = MssqlDriver {};
        let options = OtherOptions;
        // The downcast happens before the future is returned, so this panics
        // without ever attempting a connection.
        let _ = driver.connect_opt(&options);
    }

    #[test]
    fn test_connect_opt_with_matching_type_is_lazy() {
        let driver = MssqlDriver {};
        let options = MssqlConnectOptions(Config::new());
        let future = driver.connect_opt(&options);
        // Dropping without polling must not open a socket.
        drop(future);
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
    //             .exec_decode("SELECT 1", vec![])
    //             .await
    //             .unwrap();
    //         for mut x in data {
    //             println!("row: {}", x);
    //         }
    //     };
    //     task.await;
    // }
}
