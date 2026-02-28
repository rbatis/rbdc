use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

/// Error type for the Turso/libSQL adapter.
///
/// Wraps errors from the libsql client library and configuration validation.
#[derive(Debug)]
pub enum TursoError {
    /// An error originating from the libsql client library.
    Libsql(libsql::Error),
    /// A configuration or option validation error.
    Configuration(String),
}

impl TursoError {
    /// Create a configuration error with the given message.
    pub fn configuration(msg: impl Into<String>) -> Self {
        Self::Configuration(msg.into())
    }

    /// Returns a human-readable error message.
    pub fn message(&self) -> String {
        match self {
            TursoError::Libsql(e) => e.to_string(),
            TursoError::Configuration(msg) => msg.clone(),
        }
    }
}

impl Display for TursoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TursoError::Libsql(e) => write!(f, "libsql error: {}", e),
            TursoError::Configuration(msg) => write!(f, "turso configuration error: {}", msg),
        }
    }
}

impl StdError for TursoError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            TursoError::Libsql(e) => Some(e),
            TursoError::Configuration(_) => None,
        }
    }
}

impl From<libsql::Error> for TursoError {
    fn from(e: libsql::Error) -> Self {
        Self::Libsql(e)
    }
}

impl From<TursoError> for rbdc::Error {
    fn from(e: TursoError) -> Self {
        Self::from(e.to_string())
    }
}
