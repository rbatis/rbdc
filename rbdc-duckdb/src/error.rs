use thiserror::Error;

#[derive(Error, Debug)]
pub enum DuckDbError {
    #[error("DuckDB error: {0}")]
    General(String),
}

impl From<DuckDbError> for rbdc::Error {
    fn from(err: DuckDbError) -> Self {
        rbdc::Error::from(err.to_string())
    }
}