use crate::options::DuckDbConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Driver, Placeholder};
use rbdc::Error;

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
}

impl Placeholder for DuckDbDriver {
    fn exchange(&self, sql: &str) -> String {
        // DuckDB uses ? for placeholders like SQLite
        sql.to_string()
    }
}
