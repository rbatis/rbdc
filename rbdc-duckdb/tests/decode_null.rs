use std::ffi::CString;
use std::ptr;

use libduckdb_sys::{
    duckdb_close, duckdb_column_count, duckdb_connect, duckdb_connection, duckdb_database,
    duckdb_destroy_result, duckdb_disconnect, duckdb_open, duckdb_query, duckdb_result,
    DuckDBSuccess,
};

use rbdc_duckdb::types::extract_row_values;
use rbs::Value;

/// Test that DuckDB NULL returns Value::Null via extract_row_values.
#[test]
fn test_decode_null() {
    unsafe {
        // Open in-memory DuckDB (null path = :memory:)
        let mut db: duckdb_database = ptr::null_mut();
        let rc = duckdb_open(ptr::null(), &mut db);
        assert_eq!(rc, DuckDBSuccess);

        let mut con: duckdb_connection = ptr::null_mut();
        let rc2 = duckdb_connect(db, &mut con);
        assert_eq!(rc2, DuckDBSuccess);

        let sql = CString::new("SELECT NULL AS col").unwrap();
        let mut result: duckdb_result = std::mem::zeroed();
        duckdb_query(con, sql.as_ptr(), &mut result);

        let col_count = duckdb_column_count(&mut result);
        assert_eq!(col_count, 1, "expected 1 column");

        let values = extract_row_values(&mut result, 0, col_count as usize);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::Null);

        duckdb_destroy_result(&mut result);
        duckdb_disconnect(&mut con);
        duckdb_close(&mut db);
    }
}
