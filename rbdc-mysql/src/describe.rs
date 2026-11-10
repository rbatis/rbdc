use crate::result_set::{MySqlColumn, MySqlTypeInfo};
use either::Either;
use std::convert::identity;

/// Provides extended information on a statement.
///
/// Returned from [`Executor::describe`].
///
/// The query macros (e.g., `query!`, `query_as!`, etc.) use the information here to validate
/// output and parameter types; and, generate an anonymous record.
#[derive(Debug)]
#[doc(hidden)]
pub struct Describe {
    pub(crate) columns: Vec<MySqlColumn>,
    pub(crate) parameters: Option<Either<Vec<MySqlTypeInfo>, usize>>,
    pub(crate) nullable: Vec<Option<bool>>,
}

impl Describe {
    /// Gets all columns in this statement.
    pub fn columns(&self) -> &[MySqlColumn] {
        &self.columns
    }

    /// Gets the column information at `index`.
    ///
    /// Panics if `index` is out of bounds.
    pub fn column(&self, index: usize) -> &MySqlColumn {
        &self.columns[index]
    }

    /// Gets the available information for parameters in this statement.
    ///
    /// Some drivers may return more or less than others. As an example, **PostgreSQL** will
    /// return `Some(Either::Left(_))` with a full list of type information for each parameter.
    /// However, **MSSQL** will return `None` as there is no information available.
    pub fn parameters(&self) -> Option<Either<&[MySqlTypeInfo], usize>> {
        self.parameters.as_ref().map(|p| match p {
            Either::Left(params) => Either::Left(&**params),
            Either::Right(count) => Either::Right(*count),
        })
    }

    /// Gets whether a column may be `NULL`, if this information is available.
    pub fn nullable(&self, column: usize) -> Option<bool> {
        self.nullable.get(column).copied().and_then(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbdc::ext::ustr::UStr;

    fn column(index: usize, ty: crate::protocol::text::ColumnType) -> MySqlColumn {
        MySqlColumn {
            ordinal: index,
            name: UStr::new(&format!("c{index}")),
            type_info: MySqlTypeInfo::from_type(ty),
        }
    }

    #[test]
    fn test_columns_and_column() {
        let d = Describe {
            columns: vec![
                column(0, crate::protocol::text::ColumnType::Long),
                column(1, crate::protocol::text::ColumnType::VarChar),
            ],
            parameters: None,
            nullable: vec![],
        };
        assert_eq!(d.columns().len(), 2);
        assert_eq!(&*d.column(1).name, "c1");
    }

    #[test]
    fn test_parameters_none_left_and_right() {
        let mut d = Describe {
            columns: vec![],
            parameters: None,
            nullable: vec![],
        };
        assert!(d.parameters().is_none());

        d.parameters = Some(Either::Left(vec![MySqlTypeInfo::from_type(
            crate::protocol::text::ColumnType::Tiny,
        )]));
        match d.parameters() {
            Some(Either::Left(params)) => assert_eq!(params.len(), 1),
            _ => panic!("expected left parameters"),
        }

        d.parameters = Some(Either::Right(3));
        match d.parameters() {
            Some(Either::Right(count)) => assert_eq!(count, 3),
            _ => panic!("expected right parameters"),
        }
    }

    #[test]
    fn test_nullable() {
        let d = Describe {
            columns: vec![],
            parameters: None,
            nullable: vec![Some(true), Some(false), None],
        };
        assert_eq!(d.nullable(0), Some(true));
        assert_eq!(d.nullable(1), Some(false));
        assert_eq!(d.nullable(2), None);
        assert_eq!(d.nullable(3), None);
    }
}
