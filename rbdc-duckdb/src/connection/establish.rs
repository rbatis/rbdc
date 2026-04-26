use crate::options::DuckDbConnectOptions;
use crate::connection::worker::DuckDbWorker;
use rbdc::Error;

pub struct DuckDbConnection {
    pub(crate) worker: DuckDbWorker,
}

impl DuckDbConnection {
    pub async fn establish(options: &DuckDbConnectOptions) -> Result<Self, Error> {
        let path = options.path.clone();
        let worker = DuckDbWorker::establish(path).await?;
        Ok(Self { worker })
    }
}
