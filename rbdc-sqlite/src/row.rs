#![allow(clippy::rc_buffer)]

use crate::decode::Decode;
use crate::statement::StatementHandle;
use crate::{SqliteColumn, SqliteValue, SqliteValueRef};
use rbdc::db::{MetaData, Row};
use rbdc::error::Error;
use rbdc::ext::ustr::UStr;
use rbs::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Implementation of [`Row`] for SQLite.
#[derive(Debug)]
pub struct SqliteRow {
    pub(crate) values: Vec<SqliteValue>,
    pub(crate) columns: Arc<Vec<SqliteColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
}

// Accessing values from the statement object is
// safe across threads as long as we don't call [sqlite3_step]

// we block ourselves from doing that by only exposing
// a set interface on [StatementHandle]

unsafe impl Send for SqliteRow {}

unsafe impl Sync for SqliteRow {}

impl SqliteRow {
    pub(crate) fn current(
        statement: &StatementHandle,
        columns: &Arc<Vec<SqliteColumn>>,
        column_names: &Arc<HashMap<UStr, usize>>,
    ) -> Self {
        let size = statement.column_count();
        let mut values = Vec::with_capacity(size);

        for i in 0..columns.len() {
            values.push(unsafe {
                let raw = statement.column_value(i);
                SqliteValue::new(raw, columns[i].type_info.clone())
            });
        }

        Self {
            values: values,
            columns: Arc::clone(columns),
            column_names: Arc::clone(column_names),
        }
    }
}

#[derive(Debug)]
pub struct SqliteMetaData {
    pub columns: Arc<Vec<SqliteColumn>>,
}

impl MetaData for SqliteMetaData {
    fn column_len(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.columns[i].name.to_string()
    }

    fn column_type(&self, i: usize) -> String {
        self.columns[i].type_info.to_string()
    }
}

impl Row for SqliteRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(SqliteMetaData {
            columns: self.columns.clone(),
        })
    }

    fn get(&mut self, i: usize) -> Result<Value, Error> {
        match self.try_get_raw(i) {
            Err(e) => Err(Error::from(format!("get error index:{},error:{}", i, e))),
            Ok(v) => Value::decode(v),
        }
    }
}

impl SqliteRow {
    fn columns(&self) -> &[SqliteColumn] {
        &self.columns
    }

    fn try_get_raw(&self, index: usize) -> Result<SqliteValueRef<'_>, Error> {
        Ok(SqliteValueRef::value(&self.values[index]))
    }

    fn try_get(&mut self, index: usize) -> Result<SqliteValue, Error> {
        if (index + 1) > self.values.len() {
            return Err(Error::from("try_get out of range!"));
        }
        Ok(self.values.remove(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libsqlite3_sys::{
        sqlite3, sqlite3_close, sqlite3_column_value, sqlite3_finalize, sqlite3_open,
        sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt, SQLITE_OK, SQLITE_ROW,
    };
    use std::ffi::CString;
    use std::ptr;

    fn one_value() -> SqliteValue {
        let filename = CString::new(":memory:").unwrap();
        let query = CString::new("SELECT 9").unwrap();
        unsafe {
            let mut db: *mut sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(filename.as_ptr(), &mut db), SQLITE_OK);
            let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, query.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let value = SqliteValue::new(
                sqlite3_column_value(stmt, 0),
                crate::SqliteTypeInfo::null(),
            );
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            value
        }
    }

    #[test]
    fn test_try_get_success() {
        let mut row = SqliteRow {
            values: vec![one_value()],
            columns: Arc::new(Vec::new()),
            column_names: Arc::new(HashMap::new()),
        };
        let value = row.try_get(0).expect("value");
        assert_eq!(value.int(), 9);
        assert!(row.try_get(0).is_err());
    }

    #[test]
    fn test_empty_row_helpers() {
        let row = SqliteRow {
            values: Vec::new(),
            columns: Arc::new(Vec::new()),
            column_names: Arc::new(HashMap::new()),
        };
        assert!(row.columns().is_empty());

        let mut row = row;
        assert!(row.try_get(0).is_err());
        let md = row.meta_data();
        assert_eq!(md.column_len(), 0);
        assert!(row.columns().is_empty());
        assert!(format!("{:?}", row).contains("SqliteRow"));
    }
}
