use crate::protocol::response::ErrPacket;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

/// An error returned from the MySQL database.
pub struct MySqlDatabaseError(pub(super) ErrPacket);

impl MySqlDatabaseError {
    /// The [SQLSTATE](https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html) code for this error.
    pub fn code(&self) -> Option<&str> {
        self.0.sql_state.as_deref()
    }

    /// The [number](https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html)
    /// for this error.
    ///
    /// MySQL tends to use SQLSTATE as a general error category, and the error number as a more
    /// granular indication of the error.
    pub fn number(&self) -> u16 {
        self.0.error_code
    }

    /// The human-readable error message.
    pub fn message(&self) -> &str {
        &self.0.error_message
    }
}

impl Debug for MySqlDatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlDatabaseError")
            .field("code", &self.code())
            .field("number", &self.number())
            .field("message", &self.message())
            .finish()
    }
}

impl Display for MySqlDatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(code) = &self.code() {
            write!(f, "{} ({}): {}", self.number(), code, self.message())
        } else {
            write!(f, "{}: {}", self.number(), self.message())
        }
    }
}

impl Error for MySqlDatabaseError {}

impl MySqlDatabaseError {
    #[doc(hidden)]
    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    #[doc(hidden)]
    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    #[doc(hidden)]
    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }
}

impl From<MySqlDatabaseError> for rbdc::Error {
    fn from(arg: MySqlDatabaseError) -> Self {
        rbdc::Error::from(arg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(sql_state: Option<&str>) -> MySqlDatabaseError {
        MySqlDatabaseError(ErrPacket {
            error_code: 1146,
            sql_state: sql_state.map(|s| s.to_string()),
            error_message: "table does not exist".to_string(),
        })
    }

    #[test]
    fn test_accessors_and_display_with_state() {
        let e = err(Some("42S02"));
        assert_eq!(e.code(), Some("42S02"));
        assert_eq!(e.number(), 1146);
        assert_eq!(e.message(), "table does not exist");
        assert_eq!(e.to_string(), "1146 (42S02): table does not exist");
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("MySqlDatabaseError"));
        assert!(dbg.contains("42S02"));
        assert!(dbg.contains("1146"));
    }

    #[test]
    fn test_display_without_state() {
        let e = err(None);
        assert_eq!(e.code(), None);
        assert_eq!(e.to_string(), "1146: table does not exist");
        // Exercise the std::error::Error impl.
        let dyn_err: &(dyn Error + Send + Sync + 'static) = e.as_error();
        assert!(dyn_err.to_string().contains("1146"));
    }

    #[test]
    fn test_mutable_and_boxed_error() {
        let mut e = err(Some("HY000"));
        assert_eq!(e.as_error_mut().to_string(), "1146 (HY000): table does not exist");
        let boxed = Box::new(e).into_error();
        assert_eq!(boxed.to_string(), "1146 (HY000): table does not exist");
    }

    #[test]
    fn test_into_rbdc_error() {
        let e = err(Some("42000"));
        let e: rbdc::Error = e.into();
        assert!(e.to_string().contains("42000"));
    }
}
