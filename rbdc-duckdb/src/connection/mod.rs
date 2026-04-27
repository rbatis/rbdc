pub mod conn;
pub mod establish;
pub mod worker;

pub use conn::DuckDbDatabase;
pub use establish::DuckDbConnection;

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::Error;
use rbs::Value;

impl Connection for DuckDbConnection {
    fn exec_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Box<dyn Row>, Error>>, Error>> {
        let sql = sql.to_owned();
        let params = params;

        Box::pin(async move {
            let rx = self.worker.exec_rows(sql, params).await?;

            let stream = futures_util::stream::unfold(rx, |rx| async move {
                match rx.recv().await {
                    Ok(Ok(row)) => Some((Ok(Box::new(row) as Box<dyn Row>), rx)),
                    Ok(Err(e)) => Some((Err(e), rx)),
                    Err(_) => None,
                }
            });

            Ok(stream.boxed() as BoxStream<'_, Result<Box<dyn Row>, Error>>)
        })
    }

    fn exec(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<ExecResult, Error>> {
        let sql = sql.to_owned();
        let params = params;

        Box::pin(async move {
            let affected = self.worker.exec(sql, params).await?;
            Ok(ExecResult {
                rows_affected: affected,
                last_insert_id: Value::Null,
            })
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.worker.ping().await?;
            Ok(())
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.worker.shutdown().await?;
            Ok(())
        })
    }
}
