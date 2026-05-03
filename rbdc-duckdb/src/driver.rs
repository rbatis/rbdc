use crate::options::DuckDbConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Driver, Placeholder};
use rbdc::Error;
use rbs::Value;

#[derive(Debug)]
pub struct DuckDbDriver {}

impl DuckDbDriver {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for DuckDbDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl Driver for DuckDbDriver {
    fn name(&self) -> &str {
        "duckdb"
    }

    fn connect(&self, url: &str) -> BoxFuture<'_, Result<Box<dyn rbdc::db::Connection>, Error>> {
        let url = url.to_owned();
        Box::pin(async move {
            let mut opt = self.default_option();
            opt.set_uri(&url)?;
            opt.connect().await
        })
    }

    fn connect_opt<'a>(
        &'a self,
        opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<'a, Result<Box<dyn rbdc::db::Connection>, Error>> {
        let opt: &DuckDbConnectOptions = opt
            .downcast_ref()
            .expect("DuckDbDriver::connect_opt requires DuckDbConnectOptions");
        opt.connect()
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(DuckDbConnectOptions::default())
    }

    fn column_type(&self, val: &Value) -> String {
        match val {
            Value::Null => "NULL",
            Value::Bool(_) => "BOOLEAN",
            Value::I32(_) => "INTEGER",
            Value::I64(_) => "BIGINT",
            Value::U32(_) => "INTEGER",
            Value::U64(_) => "BIGINT",
            Value::F32(_) => "FLOAT",
            Value::F64(_) => "DOUBLE",
            Value::String(_) => "VARCHAR",
            Value::Binary(_) => "BLOB",
            Value::Array(_) => "VARCHAR",
            Value::Map(_) => "VARCHAR",
            Value::Ext(t, _) => match *t {
                "Date" => "DATE",
                "DateTime" | "Datetime" => "TIMESTAMP",
                "Time" => "TIME",
                "Timestamp" => "BIGINT",
                "Decimal" => "DECIMAL",
                "Json" => "VARCHAR",
                "Uuid" => "UUID",
                _ => "VARCHAR",
            },
        }
        .to_string()
    }
}

impl Placeholder for DuckDbDriver {
    fn exchange(&self, sql: &str) -> String {
        // DuckDB uses ? for placeholders like SQLite
        sql.to_string()
    }
}
