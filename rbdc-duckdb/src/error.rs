use thiserror::Error;

#[derive(Error, Debug)]
pub enum DuckDbError {
    #[error("DuckDB error: {0}")]
    General(String),
}

impl From<duckdb::Error> for DuckDbError {
    fn from(err: duckdb::Error) -> Self {
        DuckDbError::General(err.to_string())
    }
}

impl From<DuckDbError> for rbdc::Error {
    fn from(err: DuckDbError) -> Self {
        rbdc::Error::from(err.to_string())
    }
}
