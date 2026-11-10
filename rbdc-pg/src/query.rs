use crate::arguments::PgArguments;
use crate::statement::PgStatement;
use either::Either;
use rbdc::Error;

/// Raw SQL query with bind parameters. Returned by [`query`][crate::query::query].
#[must_use = "query must be executed to affect database"]
pub struct PgQuery {
    pub statement: Either<String, PgStatement>,
    pub arguments: Vec<rbs::Value>,
    pub persistent: bool,
}

impl PgQuery {
    #[inline]
    pub fn sql(&self) -> &str {
        match self.statement {
            Either::Right(ref statement) => &statement.sql,
            Either::Left(ref sql) => sql,
        }
    }

    pub fn statement(&self) -> Option<&PgStatement> {
        match self.statement {
            Either::Right(ref statement) => Some(&statement),
            Either::Left(_) => None,
        }
    }

    #[inline]
    pub fn take_arguments(self, timezone_sec: Option<i32>) -> Result<Option<PgArguments>, Error> {
        if self.arguments.is_empty() {
            return Ok(None);
        }
        return Ok(Some(PgArguments::from_args(self.arguments, timezone_sec)?));
    }

    #[inline]
    pub fn persistent(&self) -> bool {
        self.persistent
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::statement::PgStatementMetadata;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn raw_query_accessors() {
        let query = PgQuery {
            statement: Either::Left("SELECT 1".to_string()),
            arguments: vec![],
            persistent: false,
        };
        assert_eq!(query.sql(), "SELECT 1");
        assert!(query.statement().is_none());
        assert!(!query.persistent());
        assert!(query.take_arguments(None).unwrap().is_none());
    }

    #[test]
    fn prepared_query_accessors_and_arguments() {
        let metadata = Arc::new(PgStatementMetadata {
            columns: vec![],
            column_names: HashMap::new(),
            parameters: vec![],
        });
        let statement = PgStatement {
            sql: "SELECT $1".to_string(),
            metadata,
        };
        let query = PgQuery {
            statement: Either::Right(statement),
            arguments: vec![rbs::Value::I32(1)],
            persistent: true,
        };
        assert_eq!(query.sql(), "SELECT $1");
        assert_eq!(query.statement().unwrap().sql(), "SELECT $1");
        assert!(query.persistent());

        let mut args = query.take_arguments(Some(3600)).unwrap().unwrap();
        assert_eq!(args.types.len(), 1);
        args.reserve(1, 8);
        assert_eq!(args.buffer.timezone_sec, Some(3600));
    }

    #[test]
    fn take_arguments_propagates_encode_errors() {
        let query = PgQuery {
            statement: Either::Left("SELECT 1".to_string()),
            arguments: vec![rbs::Value::Ext(
                "Uuid",
                Box::new(rbs::Value::String("not-a-uuid".to_string())),
            )],
            persistent: false,
        };
        assert!(query.take_arguments(None).is_err());
    }
}
