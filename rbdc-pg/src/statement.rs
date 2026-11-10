use crate::column::PgColumn;
use crate::type_info::PgTypeInfo;
use either::Either;
use rbdc::ext::ustr::UStr;
use rbdc::Error;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PgStatement {
    pub(crate) sql: String,
    pub(crate) metadata: Arc<PgStatementMetadata>,
}

#[derive(Debug, Default)]
pub struct PgStatementMetadata {
    pub(crate) columns: Vec<PgColumn>,
    pub(crate) column_names: HashMap<UStr, usize>,
    pub(crate) parameters: Vec<PgTypeInfo>,
}

impl PgStatement {
    pub fn to_owned(&self) -> PgStatement {
        PgStatement {
            sql: self.sql.to_string(),
            metadata: self.metadata.clone(),
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn parameters(&self) -> Option<Either<&[PgTypeInfo], usize>> {
        Some(Either::Left(&self.metadata.parameters))
    }

    pub fn columns(&self) -> &[PgColumn] {
        &self.metadata.columns
    }
}

impl PgStatement {
    pub fn index(&self, index: &str) -> Result<usize, Error> {
        self.metadata
            .column_names
            .get(index)
            .ok_or_else(|| Error::from(format!("ColumnNotFound {}", index)))
            .map(|v| *v)
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::type_info::{PgType, PgTypeInfo};
    use either::Either;

    fn statement() -> PgStatement {
        let mut column_names = HashMap::new();
        column_names.insert(UStr::from("id"), 0);
        PgStatement {
            sql: "SELECT 1".to_string(),
            metadata: Arc::new(PgStatementMetadata {
                columns: vec![PgColumn {
                    ordinal: 0,
                    name: UStr::from("id"),
                    type_info: PgTypeInfo(PgType::Int4),
                    relation_id: None,
                    relation_attribute_no: None,
                }],
                column_names,
                parameters: vec![PgTypeInfo(PgType::Int4)],
            }),
        }
    }

    #[test]
    fn accessors_and_index() {
        let stmt = statement();
        assert_eq!(stmt.sql(), "SELECT 1");
        assert_eq!(stmt.columns().len(), 1);
        match stmt.parameters() {
            Some(Either::Left(params)) => assert_eq!(params.len(), 1),
            _ => panic!("expected left parameters"),
        }
        assert_eq!(stmt.index("id").unwrap(), 0);
        assert!(stmt.index("missing").is_err());

        let owned = stmt.to_owned();
        assert_eq!(owned.sql(), stmt.sql());
        assert!(Arc::ptr_eq(&owned.metadata, &stmt.metadata));
    }
}
