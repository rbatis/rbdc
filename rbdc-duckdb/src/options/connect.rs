use crate::DuckDbConnection;
use futures_core::future::BoxFuture;
use rbdc::db::ConnectOptions;
use rbdc::Error;

#[derive(Debug, Clone)]
pub struct DuckDbConnectOptions {
    pub path: String,
}

impl Default for DuckDbConnectOptions {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
        }
    }
}

impl DuckDbConnectOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn path(mut self, path: &str) -> Self {
        self.path = path.to_string();
        self
    }
}

impl ConnectOptions for DuckDbConnectOptions {
    fn connect(&self) -> BoxFuture<'_, Result<Box<dyn rbdc::db::Connection>, Error>> {
        let opt = self.clone();
        Box::pin(async move {
            let conn = DuckDbConnection::establish(&opt).await?;
            Ok(Box::new(conn) as Box<dyn rbdc::db::Connection>)
        })
    }

    fn set_uri(&mut self, uri: &str) -> Result<(), Error> {
        let uri = uri.trim_start_matches("duckdb://");
        let uri = uri.trim_start_matches("duckdb:/");

        if uri.is_empty() || uri == ":memory:" || uri == "memory" {
            self.path = ":memory:".to_string();
        } else {
            self.path = uri.to_string();
        }
        Ok(())
    }
}
