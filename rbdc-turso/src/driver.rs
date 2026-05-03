use crate::TursoConnectOptions;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc::Error;
use rbs::Value;

/// Turso/libSQL database driver.
///
/// Implements the `rbdc::db::Driver` trait using Turso's native async API.
/// Backend selection is fixed at initialization time; once a `TursoDriver`
/// is wired in, all connections go through Turso/libSQL.
///
/// Accepts `turso://`, `sqlite://`, and `sqlite:` URI schemes for backward
/// compatibility with rbdc-sqlite configurations.
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

    fn column_type(&self, val: &Value) -> String {
        match val {
            Value::Null => "NULL",
            Value::Bool(_) => "INTEGER",
            Value::I32(_) => "INTEGER",
            Value::I64(_) => "INTEGER",
            Value::U32(_) => "INTEGER",
            Value::U64(_) => "INTEGER",
            Value::F32(_) => "REAL",
            Value::F64(_) => "REAL",
            Value::String(_) => "TEXT",
            Value::Binary(_) => "BLOB",
            Value::Array(_) => "TEXT",
            Value::Map(_) => "TEXT",
            Value::Ext(t, _) => match *t {
                "Date" | "DateTime" | "Time" | "Decimal" | "Uuid" => "TEXT",
                "Timestamp" => "INTEGER",
                "Json" => "BLOB",
                _ => "TEXT",
            },
        }
        .to_string()
    }
}

impl Placeholder for TursoDriver {
    fn exchange(&self, sql: &str) -> String {
        // Turso uses positional `?` placeholders.
        sql.to_string()
    }
}
