#![allow(mismatched_lifetime_syntaxes)]

use crate::options::PgConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc::{impl_exchange, Error};
use rbs::Value;

#[derive(Debug)]
pub struct PgDriver {}

impl Driver for PgDriver {
    fn name(&self) -> &str {
        "postgres"
    }

    fn connect(&self, url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
        let url = url.to_owned();
        Box::pin(async move {
            let mut opt = self.default_option();
            opt.set_uri(&url)?;
            if let Some(opt) = opt.downcast_ref::<PgConnectOptions>() {
                let conn = opt.connect().await?;
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
        let opt: &PgConnectOptions = opt.downcast_ref().expect(
            "PgDriver::connect_opt requires PgConnectOptions, got a different ConnectOptions type",
        );
        Box::pin(async move {
            let conn = opt.connect().await?;
            Ok(conn)
        })
    }
    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(PgConnectOptions::default())
    }

    fn column_type(&self, val: &Value) -> String {
        match val {
            Value::Null => "UNKNOWN",
            Value::Bool(_) => "BOOL",
            Value::I32(_) => "INT4",
            Value::I64(_) => "INT8",
            Value::U32(_) => "INT4",
            Value::U64(_) => "INT8",
            Value::F32(_) => "FLOAT4",
            Value::F64(_) => "FLOAT8",
            Value::String(_) => "VARCHAR",
            Value::Binary(_) => "BYTEA",
            Value::Array(_) => "JSON",
            Value::Map(_) => "JSON",
            Value::Ext(t, _) => match *t {
                "Uuid" => "UUID",
                "Decimal" | "Numeric" => "NUMERIC",
                "Date" => "DATE",
                "Time" => "TIME",
                "Timestamp" => "TIMESTAMP",
                "DateTime" => "TIMESTAMP",
                "Bool" => "BOOL",
                "Bytea" => "BYTEA",
                "Char" => "CHAR",
                "Name" => "NAME",
                "Int8" => "INT8",
                "Int2" => "INT2",
                "Int4" => "INT4",
                "Text" => "TEXT",
                "Oid" => "OID",
                "Json" | "Jsonb" => "JSON",
                "Point" => "POINT",
                "Lseg" => "LSEG",
                "Path" => "PATH",
                "Box" => "BOX",
                "Polygon" => "POLYGON",
                "Line" => "LINE",
                "Cidr" => "CIDR",
                "Float4" => "FLOAT4",
                "Float8" => "FLOAT8",
                "Circle" => "CIRCLE",
                "Macaddr8" => "MACADDR8",
                "Macaddr" => "MACADDR",
                "Inet" => "INET",
                "Bpchar" => "BPCHAR",
                "Varchar" => "VARCHAR",
                "Timestamptz" => "TIMESTAMPTZ",
                "Interval" => "INTERVAL",
                "Timetz" => "TIMETZ",
                "Bit" => "BIT",
                "Varbit" => "VARBIT",
                "Record" => "RECORD",
                "Int4Range" => "INT4RANGE",
                "NumRange" => "NUMRANGE",
                "TsRange" => "TSRANGE",
                "TstzRange" => "TSTZRANGE",
                "DateRange" => "DATERANGE",
                "Int8Range" => "INT8RANGE",
                "Jsonpath" => "JSONPATH",
                "Money" => "MONEY",
                "Void" => "VOID",
                _ => "UNKNOWN",
            },
        }
        .to_string()
    }
}

impl Placeholder for PgDriver {
    fn exchange(&self, sql: &str) -> String {
        impl_exchange("$", 1, sql)
    }
}

#[cfg(test)]
mod test {
    use crate::driver::PgDriver;
    use crate::options::PgConnectOptions;
    use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
    use rbdc::Error;
    use rbs::Value;

    #[test]
    fn test_default() {}

    #[test]
    fn test_exchange() {
        let v = "insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        let d = PgDriver {};
        let sql = d.exchange(v);
        assert_eq!("insert into biz_activity (id,name,pc_link,h5_link,pc_banner_img,h5_banner_img,sort,status,remark,create_time,version,delete_flag) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", sql);
    }

    #[test]
    fn name_and_default_option() {
        let driver = PgDriver {};
        assert_eq!(driver.name(), "postgres");
        let default = driver.default_option();
        assert!(default.downcast_ref::<PgConnectOptions>().is_some());
    }

    #[test]
    fn column_type_for_every_value_variant() {
        let driver = PgDriver {};
        let cases = [
            (Value::Null, "UNKNOWN"),
            (Value::Bool(true), "BOOL"),
            (Value::I32(1), "INT4"),
            (Value::I64(1), "INT8"),
            (Value::U32(1), "INT4"),
            (Value::U64(1), "INT8"),
            (Value::F32(1.0), "FLOAT4"),
            (Value::F64(1.0), "FLOAT8"),
            (Value::String("x".into()), "VARCHAR"),
            (Value::Binary(vec![1]), "BYTEA"),
            (Value::Array(vec![]), "JSON"),
            (rbs::value!({"a": 1}), "JSON"),
        ];
        for (value, expected) in cases {
            assert_eq!(driver.column_type(&value), expected);
        }

        let ext_cases = [
            ("Uuid", "UUID"),
            ("Decimal", "NUMERIC"),
            ("Numeric", "NUMERIC"),
            ("Date", "DATE"),
            ("Time", "TIME"),
            ("Timestamp", "TIMESTAMP"),
            ("DateTime", "TIMESTAMP"),
            ("Bool", "BOOL"),
            ("Bytea", "BYTEA"),
            ("Char", "CHAR"),
            ("Name", "NAME"),
            ("Int8", "INT8"),
            ("Int2", "INT2"),
            ("Int4", "INT4"),
            ("Text", "TEXT"),
            ("Oid", "OID"),
            ("Json", "JSON"),
            ("Jsonb", "JSON"),
            ("Point", "POINT"),
            ("Lseg", "LSEG"),
            ("Path", "PATH"),
            ("Box", "BOX"),
            ("Polygon", "POLYGON"),
            ("Line", "LINE"),
            ("Cidr", "CIDR"),
            ("Float4", "FLOAT4"),
            ("Float8", "FLOAT8"),
            ("Circle", "CIRCLE"),
            ("Macaddr8", "MACADDR8"),
            ("Macaddr", "MACADDR"),
            ("Inet", "INET"),
            ("Bpchar", "BPCHAR"),
            ("Varchar", "VARCHAR"),
            ("Timestamptz", "TIMESTAMPTZ"),
            ("Interval", "INTERVAL"),
            ("Timetz", "TIMETZ"),
            ("Bit", "BIT"),
            ("Varbit", "VARBIT"),
            ("Record", "RECORD"),
            ("Int4Range", "INT4RANGE"),
            ("NumRange", "NUMRANGE"),
            ("TsRange", "TSRANGE"),
            ("TstzRange", "TSTZRANGE"),
            ("DateRange", "DATERANGE"),
            ("Int8Range", "INT8RANGE"),
            ("Jsonpath", "JSONPATH"),
            ("Money", "MONEY"),
            ("Void", "VOID"),
            ("SomethingElse", "UNKNOWN"),
        ];
        for (name, expected) in ext_cases {
            let value = Value::Ext(name, Box::new(Value::Null));
            assert_eq!(driver.column_type(&value), expected, "ext={name}");
        }
    }

    #[test]
    fn connect_with_invalid_uri_returns_error() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let driver = PgDriver {};
        let result = runtime.block_on(async { driver.connect("not a valid url").await });
        assert!(result.is_err());
    }

    #[derive(Debug)]
    struct DummyOptions;

    impl ConnectOptions for DummyOptions {
        fn connect(
            &self,
        ) -> futures_core::future::BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
            Box::pin(async { Err(Error::from("dummy")) })
        }

        fn set_uri(&mut self, _uri: &str) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    #[should_panic(expected = "requires PgConnectOptions")]
    fn connect_opt_rejects_other_options() {
        let driver = PgDriver {};
        let opts = DummyOptions;
        let _ = driver.connect_opt(&opts);
    }
}

// #[cfg(test)]
// mod test2 {
//     use crate::driver::PgDriver;
//     use rbdc::block_on;
//     use rbdc::datetime::DateTime;
//     use rbdc::db::Driver;
//     use rbdc::db::Placeholder;
//     use rbdc::decimal::Decimal;
//     use rbdc::pool::Pool;
//     use rbdc::timestamp::Timestamp;
//     use rbs::Value;
//
//     #[test]
//     fn test_pg_pool() {
//         let task = async move {
//             let pool = Pool::new_url(
//                 PgDriver {},
//                 "postgres://postgres:123456@localhost:5432/postgres",
//             )
//             .unwrap();
//             std::thread::sleep(std::time::Duration::from_secs(2));
//             let mut conn = pool.get().await.unwrap();
//             let data = conn
//                 .exec_decode("select * from biz_activity", vec![])
//                 .await
//                 .unwrap();
//             for mut x in data {
//                 println!("row: {}", x);
//             }
//         };
//         block_on!(task);
//     }
//
//     #[test]
//     fn test_pg_param() {
//         let task = async move {
//             let mut d = PgDriver {};
//             let mut c = d
//                 .connect("postgres://postgres:123456@localhost:5432/postgres")
//                 .await
//                 .unwrap();
//             let param = vec![
//                 Value::String("http://www.test.com".to_string()),
//                 DateTime::now().into(),
//                 Decimal("1".to_string()).into(),
//                 Value::String("1".to_string()),
//             ];
//             println!("param => {}", Value::Array(param.clone()));
//             let data = c
//                 .exec(
//                     "update biz_activity set pc_link = $1,create_time = $2,delete_flag=$3 where id  = $4",
//                     param,
//                 )
//                 .await
//                 .unwrap();
//             println!("{}", data);
//         };
//         block_on!(task);
//     }
// }
