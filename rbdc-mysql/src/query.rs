use crate::stmt::{MySqlArguments, MySqlStatement};
use either::Either;
use rbdc::Error;

#[must_use = "query must be executed to affect database"]
pub struct MysqlQuery {
    pub(crate) statement: Either<String, MySqlStatement>,
    pub(crate) arguments: Vec<rbs::Value>,
    pub(crate) persistent: bool,
}
impl MysqlQuery {
    #[inline]
    pub fn sql(&self) -> &str {
        match self.statement {
            Either::Right(ref statement) => &statement.sql,
            Either::Left(ref sql) => sql,
        }
    }

    pub fn statement(&self) -> Option<&MySqlStatement> {
        match self.statement {
            Either::Right(ref statement) => Some(&statement),
            Either::Left(_) => None,
        }
    }

    #[inline]
    pub fn take_arguments(self) -> Result<Option<MySqlArguments>, Error> {
        if self.arguments.is_empty() {
            return Ok(None);
        }
        //Value to MysqlArguments
        return Ok(Some(MySqlArguments::from_args(self.arguments)?));
    }

    #[inline]
    pub fn persistent(&self) -> bool {
        self.persistent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stmt::MySqlStatementMetadata;
    use rbs::Value;

    fn statement(sql: &str) -> MySqlStatement {
        MySqlStatement {
            sql: sql.to_string(),
            metadata: MySqlStatementMetadata::default(),
        }
    }

    #[test]
    fn test_sql_and_statement_for_left() {
        let q = MysqlQuery {
            statement: Either::Left("SELECT 1".to_string()),
            arguments: vec![],
            persistent: false,
        };
        assert_eq!(q.sql(), "SELECT 1");
        assert!(q.statement().is_none());
        assert!(!q.persistent());
    }

    #[test]
    fn test_sql_and_statement_for_right() {
        let q = MysqlQuery {
            statement: Either::Right(statement("SELECT ?")),
            arguments: vec![],
            persistent: true,
        };
        assert_eq!(q.sql(), "SELECT ?");
        assert_eq!(q.statement().unwrap().sql, "SELECT ?");
        assert!(q.persistent());
    }

    #[test]
    fn test_take_arguments_empty() {
        let q = MysqlQuery {
            statement: Either::Left("SELECT 1".to_string()),
            arguments: vec![],
            persistent: false,
        };
        assert!(q.take_arguments().unwrap().is_none());
    }

    #[test]
    fn test_take_arguments_non_empty() {
        let q = MysqlQuery {
            statement: Either::Left("SELECT ?".to_string()),
            arguments: vec![Value::I32(1), Value::String("x".to_string())],
            persistent: false,
        };
        let args = q.take_arguments().unwrap().unwrap();
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn test_take_arguments_error_is_propagated() {
        let q = MysqlQuery {
            statement: Either::Left("SELECT ?".to_string()),
            arguments: vec![Value::Ext("Date", Box::new(Value::String("nope".into())))],
            persistent: false,
        };
        assert!(q.take_arguments().is_err());
    }
}
