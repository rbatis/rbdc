use crate::options::DuckDbConnectOptions;
use crate::connection::worker::{Command, DuckDbWorker};
use futures_channel::oneshot;
use rbdc::Error;
use std::sync::atomic::{AtomicU64, Ordering};

static THREAD_ID: AtomicU64 = AtomicU64::new(0);

pub struct DuckDbConnection {
    pub(crate) worker: DuckDbWorker,
}

impl DuckDbConnection {
    /// Returns the number of cached prepared statements for this connection.
    ///
    /// This is useful for testing and monitoring to detect potential memory leaks
    /// from prepared statements that are not being properly released.
    pub fn cached_statements_size(&self) -> usize {
        self.worker.cached_statements_size()
    }

    /// Clears all cached prepared statements for this connection.
    ///
    /// This can be used to explicitly release prepared statement resources
    /// before closing the connection or as part of resource management.
    pub async fn clear_cache(&mut self) -> Result<(), Error> {
        self.worker.clear_cache().await
    }

    pub async fn establish(options: &DuckDbConnectOptions) -> Result<Self, Error> {
        let path = options.path.clone();
        let thread_name = (options.thread_name)(THREAD_ID.fetch_add(1, Ordering::AcqRel));
        let command_channel_size = options.command_channel_size;
        let row_channel_size = options.row_channel_size;
        let shared_database = options.shared_database.clone();
        let worker = DuckDbWorker::establish(
            path,
            thread_name,
            command_channel_size,
            row_channel_size,
            shared_database,
        )
        .await?;
        Ok(Self { worker })
    }
}

impl Drop for DuckDbConnection {
    fn drop(&mut self) {
        // 通知 worker 线程关闭（忽略结果，因为 Drop 中无法 await）
        let (tx, _rx) = oneshot::channel();
        let _ = self.worker.command_tx.send(Command::Shutdown { tx });
    }
}
