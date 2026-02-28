use crate::TursoConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc::Error;

/// Turso/libSQL database driver.
///
/// Implements the `rbdc::db::Driver` trait using Turso's native async API.
/// Backend selection is fixed at initialization time; once a `TursoDriver`
/// is wired in, all connections go through Turso/libSQL.
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
        match opt.downcast_ref::<TursoConnectOptions>() {
            Some(opt) => Box::pin(async move {
                let conn = opt.connect().await?;
                Ok(conn)
            }),
            None => Box::pin(async move { Err(Error::from("downcast_ref failure")) }),
        }
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(TursoConnectOptions::default())
    }
}

impl Placeholder for TursoDriver {
    fn exchange(&self, sql: &str) -> String {
        // Turso uses positional `?` placeholders.
        sql.to_string()
    }
}
