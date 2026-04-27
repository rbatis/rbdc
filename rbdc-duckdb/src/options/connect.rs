use crate::connection::DuckDbDatabase;
use crate::DuckDbConnection;
use futures_core::future::BoxFuture;
use parking_lot::Mutex as ParkingMutex;
use rbdc::common::DebugFn;
use rbdc::db::ConnectOptions;
use rbdc::Error;
use std::sync::Arc;

/// Default maximum number of cached prepared statements per connection.
pub const DEFAULT_STATEMENT_CACHE_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub struct DuckDbConnectOptions {
    pub path: String,
    pub(crate) thread_name: Arc<DebugFn<dyn Fn(u64) -> String + Send + Sync + 'static>>,
    pub(crate) command_channel_size: usize,
    pub(crate) row_channel_size: usize,
    /// Maximum number of prepared statements to cache per connection.
    /// Uses LRU eviction when limit is reached.
    pub(crate) statement_cache_size: usize,
    /// 共享的数据库实例，由第一个连接创建，后续连接复用
    pub(crate) shared_database: Arc<ParkingMutex<Option<DuckDbDatabase>>>,
}

impl Default for DuckDbConnectOptions {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            thread_name: Arc::new(DebugFn(|id| format!("rbdc-duckdb-worker-{}", id))),
            command_channel_size: 16,
            row_channel_size: 16,
            statement_cache_size: DEFAULT_STATEMENT_CACHE_SIZE,
            shared_database: Arc::new(ParkingMutex::new(None)),
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

    /// Provide a callback to generate the name of the background worker thread.
    ///
    /// The value passed to the callback is an auto-incremented integer for use as the thread ID.
    pub fn thread_name(
        mut self,
        generator: impl Fn(u64) -> String + Send + Sync + 'static,
    ) -> Self {
        self.thread_name = Arc::new(DebugFn(generator));
        self
    }

    /// Set the maximum number of commands to buffer for the worker thread before backpressure is
    /// applied.
    pub fn command_buffer_size(mut self, size: usize) -> Self {
        self.command_channel_size = size;
        self
    }

    /// Set the maximum number of rows to buffer back to the calling task when a query is executed.
    ///
    /// If the calling task cannot keep up, backpressure will be applied to the worker thread
    /// in order to limit CPU and memory usage.
    pub fn row_buffer_size(mut self, size: usize) -> Self {
        self.row_channel_size = size;
        self
    }

    /// Set the maximum number of prepared statements to cache per connection.
    ///
    /// When the limit is reached, the least recently used statement is evicted
    /// to make room for a new one. Set to 0 to disable caching.
    ///
    /// Default is 128.
    pub fn statement_cache_size(mut self, size: usize) -> Self {
        self.statement_cache_size = size;
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
