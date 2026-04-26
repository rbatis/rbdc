use crate::error::DuckDbError;
use crate::options::DuckDbConnectOptions;
use rbdc::Error;
use std::sync::{Arc, Mutex};

pub type DuckDbConn = Arc<Mutex<duckdb::Connection>>;

pub struct DuckDbConnection {
    pub conn: DuckDbConn,
}

impl DuckDbConnection {
    pub async fn establish(options: &DuckDbConnectOptions) -> Result<Self, Error> {
        let config = duckdb::Config::default();
        let conn = if options.path == ":memory:" {
            duckdb::Connection::open_in_memory_with_flags(config)
        } else {
            duckdb::Connection::open_with_flags(&options.path, config)
        }
        .map_err(DuckDbError::from)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}
