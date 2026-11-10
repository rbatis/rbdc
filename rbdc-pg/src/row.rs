use crate::column::PgColumn;
use crate::message::DataRow;
use crate::meta_data::PgMetaData;
use crate::statement::PgStatementMetadata;
use crate::types::decode::Decode;
use crate::value::{PgValueFormat, PgValueRef};
use rbdc::db::MetaData;
use rbdc::Error;
use rbs::Value;
use std::sync::Arc;

/// Implementation of [`Row`] for PostgreSQL.
#[derive(Debug)]
pub struct PgRow {
    pub(crate) data: DataRow,
    pub(crate) format: PgValueFormat,
    pub(crate) metadata: Arc<PgStatementMetadata>,
    /// Session timezone offset in seconds from UTC, used for TIMESTAMPTZ decoding
    pub(crate) timezone_sec: Option<i32>,
}

impl PgRow {
    pub fn columns(&self) -> &[PgColumn] {
        &self.metadata.columns
    }

    pub fn try_get_raw(&self, index: &str) -> Result<PgValueRef<'_>, Error> {
        let index = self.index(index)?;
        let column = &self.metadata.columns[index];
        let value = self.data.get(index);

        Ok(PgValueRef {
            format: self.format,
            type_info: column.type_info.clone(),
            value,
            timezone_sec: self.timezone_sec,
        })
    }

    pub fn try_get(&mut self, index: usize) -> Result<PgValueRef, Error> {
        if (index + 1) > self.metadata.column_names.len() {
            return Err(Error::from(format!("ColumnNotFound={}", index)));
        }
        let column = &self.metadata.columns[index];
        let value = self.data.get(index);
        Ok(PgValueRef {
            value: value,
            type_info: column.type_info.clone(),
            format: self.format,
            timezone_sec: self.timezone_sec,
        })
    }
}

impl PgRow {
    pub fn index(&self, idx: &str) -> Result<usize, Error> {
        self.metadata
            .column_names
            .get(idx)
            .ok_or_else(|| Error::from("ColumnNotFound=".to_string() + idx))
            .map(|v| *v)
    }
}

impl rbdc::db::Row for PgRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(PgMetaData {
            metadata: self.metadata.clone(),
        })
    }

    fn get(&mut self, i: usize) -> Result<Value, Error> {
        match self.try_get(i) {
            Err(e) => Err(Error::from(format!("get error  index:{},error:{}", i, e))),
            Ok(v) => Value::decode(v),
        }
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::column::PgColumn;
    use crate::type_info::{PgType, PgTypeInfo};
    use rbdc::ext::ustr::UStr;
    use rbdc::db::Row as _;

    fn metadata() -> Arc<PgStatementMetadata> {
        let mut column_names = std::collections::HashMap::new();
        column_names.insert(UStr::from("a"), 0);
        column_names.insert(UStr::from("b"), 1);
        Arc::new(PgStatementMetadata {
            columns: vec![
                PgColumn {
                    ordinal: 0,
                    name: UStr::from("a"),
                    type_info: PgTypeInfo(PgType::Int4),
                    relation_id: None,
                    relation_attribute_no: None,
                },
                PgColumn {
                    ordinal: 1,
                    name: UStr::from("b"),
                    type_info: PgTypeInfo(PgType::Text),
                    relation_id: Some(3),
                    relation_attribute_no: Some(4),
                },
            ],
            column_names,
            parameters: vec![PgTypeInfo(PgType::Int4)],
        })
    }

    fn row() -> PgRow {
        PgRow {
            data: DataRow {
                storage: vec![Some(vec![0, 0, 0, 7]), None],
                values: vec![Some(0..4), None],
            },
            format: PgValueFormat::Binary,
            metadata: metadata(),
            timezone_sec: Some(3600),
        }
    }

    #[test]
    fn columns_and_index() {
        let row = row();
        assert_eq!(row.columns().len(), 2);
        assert_eq!(row.columns()[0].ordinal(), 0);
        assert_eq!(row.columns()[0].name(), "a");
        assert_eq!(row.columns()[0].type_info(), &PgTypeInfo(PgType::Int4));
        assert_eq!(row.index("a").unwrap(), 0);
        assert_eq!(row.index("b").unwrap(), 1);
        assert!(row.index("missing").is_err());
    }

    #[test]
    fn try_get_raw_and_try_get() {
        let mut row = row();
        let value = row.try_get_raw("a").unwrap();
        assert_eq!(value.value, Some(&[0, 0, 0, 7][..]));
        assert_eq!(value.format(), PgValueFormat::Binary);
        assert_eq!(value.timezone_sec, Some(3600));
        assert!(!value.is_null());
        assert_eq!(value.type_info().as_ref(), &PgTypeInfo(PgType::Int4));
        assert!(row.try_get_raw("missing").is_err());

        let value = row.try_get(0).unwrap();
        assert_eq!(value.value, Some(&[0, 0, 0, 7][..]));
        assert!(!value.is_null());

        let value = row.try_get(1).unwrap();
        assert!(value.is_null());
        assert!(value.as_bytes().is_err());

        assert!(row.try_get(2).is_err());
    }

    #[test]
    fn row_get_and_meta_data() {
        let mut row = row();
        assert_eq!(row.get(0).unwrap(), Value::I32(7));
        assert_eq!(row.get(1).unwrap(), Value::Null);
        assert!(row.get(9).is_err());

        let meta = row.meta_data();
        assert_eq!(meta.column_len(), 2);
        assert_eq!(meta.column_name(0), "a");
        assert_eq!(meta.column_name(1), "b");
        assert_eq!(meta.column_name(9), "");
        assert_eq!(meta.column_type(1), "b");
        // Debug impl for PgRow
        assert!(format!("{row:?}").contains("PgRow"));
    }
}
