use std::ffi::CString;
use std::ptr;

use libsqlite3_sys::{
    sqlite3, sqlite3_close, sqlite3_column_value, sqlite3_finalize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt, SQLITE_OK, SQLITE_ROW,
};

use rbdc_sqlite::decode::Decode;
use rbdc_sqlite::type_info::SqliteTypeInfo;
use rbdc_sqlite::value::{SqliteValue, SqliteValueRef};
use rbs::Value;

/// Test that SQLite NULL creates a proper SqliteValue which decodes to Value::Null.
#[test]
fn test_decode_null() {
    unsafe {
        let filename = CString::new(":memory:").unwrap();
        let sql = CString::new("SELECT NULL").unwrap();

        let mut db: *mut sqlite3 = ptr::null_mut();
        let rc = sqlite3_open(filename.as_ptr(), &mut db);
        assert_eq!(rc, SQLITE_OK);

        let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
        let rc2 = sqlite3_prepare_v2(
            db,
            sql.as_ptr(),
            -1,
            &mut stmt,
            ptr::null_mut(),
        );
        assert_eq!(rc2, SQLITE_OK);

        let step_rc = sqlite3_step(stmt);
        assert_eq!(step_rc, SQLITE_ROW);

        let raw_val = sqlite3_column_value(stmt, 0);
        let sqlite_val = SqliteValue::new(raw_val, SqliteTypeInfo::null());
        let val_ref = SqliteValueRef::value(&sqlite_val);

        let result = <Value as Decode>::decode(val_ref).unwrap();
        assert_eq!(result, Value::Null);

        sqlite3_finalize(stmt);
        sqlite3_close(db);
    }
}
