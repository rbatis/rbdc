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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_configuration_and_connection_helpers() {
        let cfg = TursoError::configuration("bad config");
        assert_eq!(cfg.message(), "bad config");
        assert_eq!(cfg.to_string(), "turso configuration error: bad config");
        assert!(!cfg.is_unavailable());
        assert!(StdError::source(&cfg).is_none());

        let conn = TursoError::connection("offline");
        assert_eq!(conn.message(), "offline");
        assert_eq!(conn.to_string(), "turso connection error: offline");
        assert!(conn.is_unavailable());
        assert!(StdError::source(&conn).is_none());
    }

    #[tokio::test]
    async fn test_libsql_variant_source_and_message() {
        let db = libsql::Builder::new_local(":memory:")
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();

        let libsql_err = conn.execute("NOT VALID SQL", ()).await.unwrap_err();
        let err = TursoError::from(libsql_err);

        assert!(matches!(err, TursoError::Libsql(_)));
        assert!(err.is_unavailable());
        assert!(err.message().contains("SQL") || err.message().contains("syntax"));
        assert!(StdError::source(&err).is_some());
        assert!(err.to_string().starts_with("turso error:"));
    }
}
