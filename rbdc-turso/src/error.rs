use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

/// Error type for the Turso adapter.
///
/// Wraps errors from the libsql client library and maps them to categories
/// that callers can reason about deterministically.
#[derive(Debug)]
pub enum TursoError {
    /// An error originating from the libsql client library.
    Libsql(libsql::Error),
    /// A configuration or option validation error (detected at startup).
    Configuration(String),
    /// A connection-level error (failed to establish, dropped, or unavailable).
    Connection(String),
}

impl TursoError {
    /// Create a configuration error with the given message.
    pub fn configuration(msg: impl Into<String>) -> Self {
        Self::Configuration(msg.into())
    }

    /// Create a connection error with the given message.
    pub fn connection(msg: impl Into<String>) -> Self {
        Self::Connection(msg.into())
    }

    /// Returns `true` if this error indicates the backend is unavailable.
    ///
    /// This is useful for fail-fast assertions in tests: when the Turso backend
    /// is unreachable, the adapter must return an error rather than silently
    /// falling back to any other backend.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, TursoError::Connection(_) | TursoError::Libsql(_))
    }

    /// Returns a human-readable error message.
    pub fn message(&self) -> String {
        match self {
            TursoError::Libsql(e) => e.to_string(),
            TursoError::Configuration(msg) => msg.clone(),
            TursoError::Connection(msg) => msg.clone(),
        }
    }
}

impl Display for TursoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TursoError::Libsql(e) => write!(f, "turso error: {}", e),
            TursoError::Configuration(msg) => write!(f, "turso configuration error: {}", msg),
            TursoError::Connection(msg) => write!(f, "turso connection error: {}", msg),
        }
    }
}

impl StdError for TursoError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            TursoError::Libsql(e) => Some(e),
            TursoError::Configuration(_) => None,
            TursoError::Connection(_) => None,
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
