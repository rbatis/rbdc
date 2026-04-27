use crate::options::DuckDbConnectOptions;
use crate::connection::worker::DuckDbWorker;
use rbdc::Error;
use std::sync::atomic::{AtomicU64, Ordering};

static THREAD_ID: AtomicU64 = AtomicU64::new(0);

pub struct DuckDbConnection {
    pub(crate) worker: DuckDbWorker,
}

impl DuckDbConnection {
    pub async fn establish(options: &DuckDbConnectOptions) -> Result<Self, Error> {
        let path = options.path.clone();
        let thread_name = (options.thread_name)(THREAD_ID.fetch_add(1, Ordering::AcqRel));
        let command_channel_size = options.command_channel_size;
        let row_channel_size = options.row_channel_size;
        let worker = DuckDbWorker::establish(path, thread_name, command_channel_size, row_channel_size).await?;
        Ok(Self { worker })
    }
}
