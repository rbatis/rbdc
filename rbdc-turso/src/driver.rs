use crate::TursoConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc::Error;

/// Turso/libSQL database driver.
///
/// This driver provides the same `Driver` trait interface as other rbdc adapters
/// (e.g., `SqliteDriver`). Backend selection is fixed at initialization time;
/// once a `TursoDriver` is wired in, all connections go through Turso/libSQL.
///
/// There is no runtime switching or automatic fallback to SQLite.
#[derive(Debug)]
pub struct TursoDriver {}

impl Driver for TursoDriver {
    fn name(&self) -> &str {
        "turso"
    }

    fn connect(&self, url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
        let url = url.to_owned();
        Box::pin(async move {
            let mut opt = self.default_option();
            opt.set_uri(&url)?;
            if let Some(opt) = opt.downcast_ref::<TursoConnectOptions>() {
                let conn = opt.connect().await?;
                Ok(conn)
            } else {
                Err(Error::from("downcast_ref failure"))
            }
        })
    }

    fn connect_opt<'a>(
        &'a self,
        opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<'a, Result<Box<dyn Connection>, Error>> {
        let opt: &TursoConnectOptions = opt.downcast_ref().unwrap();
        Box::pin(async move {
            let conn = opt.connect().await?;
            Ok(conn)
        })
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(TursoConnectOptions::default())
    }
}

impl Placeholder for TursoDriver {
    fn exchange(&self, sql: &str) -> String {
        // libSQL/Turso uses positional `?` placeholders, same as SQLite.
        sql.to_string()
    }
}
