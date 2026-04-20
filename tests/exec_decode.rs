//! Tests for exec_decode CSV format output
//!
//! exec_decode returns Value::Array where:
//! - First element: array of column names
//! - Subsequent elements: arrays of row values
//! Format: [['col1','col2'], ['val1','val2'], ['val3','val4']]

use std::fmt::Debug;
use futures_core::future::BoxFuture;
use rbs::Value;
use rbdc::db::{Connection, MetaData, Row};

// Mock MetaData for testing
#[derive(Debug, Clone)]
struct MockMetaData {
    column_names: Vec<String>,
}

impl MockMetaData {
    fn new(names: Vec<&str>) -> Self {
        Self {
            column_names: names.into_iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl MetaData for MockMetaData {
    fn column_len(&self) -> usize {
        self.column_names.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.column_names[i].clone()
    }

    fn column_type(&self, _i: usize) -> String {
        "TEXT".to_string()
    }
}

// Mock Row that stores a copy of values for each get() call
// Real databases may have different get() semantics, but for testing
// exec_decode's structure, we just need consistent value access
#[derive(Debug)]
struct MockRow {
    metadata: MockMetaData,
    values: Vec<Value>,
}

impl MockRow {
    fn new(cols: Vec<&str>, vals: Vec<Value>) -> Self {
        Self {
            metadata: MockMetaData::new(cols),
            values: vals,
        }
    }
}

impl Row for MockRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(MockMetaData {
            column_names: self.metadata.column_names.clone(),
        })
    }

    // This mock does NOT use remove() - it just returns values by index
    // This is a simplification to test the CSV output structure
    fn get(&mut self, i: usize) -> Result<Value, rbdc::Error> {
        if i >= self.values.len() {
            return Ok(Value::Null);
        }
        Ok(self.values[i].clone())
    }
}

// Mock Connection that returns predefined rows
#[derive(Debug)]
struct MockConnection {
    rows: std::sync::Mutex<Vec<Box<dyn Row>>>,
}

impl MockConnection {
    fn with_rows(rows: Vec<Box<dyn Row>>) -> Self {
        Self {
            rows: std::sync::Mutex::new(rows),
        }
    }
}

impl Connection for MockConnection {
    fn exec_rows(
        &mut self,
        _sql: &str,
        _params: Vec<Value>,
    ) -> BoxFuture<'_, Result<Vec<Box<dyn Row>>, rbdc::Error>> {
        let rows = self.rows.lock().unwrap().drain(..).collect();
        Box::pin(async move { Ok(rows) })
    }

    fn exec(
        &mut self,
        _sql: &str,
        _params: Vec<Value>,
    ) -> BoxFuture<'_, Result<rbdc::db::ExecResult, rbdc::Error>> {
        Box::pin(async move { Ok(rbdc::db::ExecResult::default()) })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), rbdc::Error>> {
        Box::pin(async { Ok(()) })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), rbdc::Error>> {
        Box::pin(async { Ok(()) })
    }
}

fn exec_decode_sync(conn: &mut dyn Connection, sql: &str) -> Result<Value, rbdc::Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(conn.exec_decode(sql, vec![]))
}

#[test]
fn test_exec_decode_empty_result() {
    let conn = MockConnection::with_rows(vec![]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT * FROM empty");
    let value = result.unwrap();
    // When there are no rows, we get an empty array
    match value {
        Value::Array(arr) => {
            assert!(arr.is_empty(), "Empty result should give empty array");
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_exec_decode_single_row_single_column() {
    let row = MockRow::new(vec!["id"], vec![Value::I64(42)]);
    let conn = MockConnection::with_rows(vec![Box::new(row)]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT id FROM t");
    let value = result.unwrap();
    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 2); // column row + 1 data row
            // First row is columns (in reverse order due to .rev())
            match &arr[0] {
                Value::Array(cols) => {
                    assert_eq!(cols.len(), 1);
                    assert_eq!(cols[0], Value::String("id".to_string()));
                }
                _ => panic!("Expected columns Array"),
            }
            // Second row is data
            match &arr[1] {
                Value::Array(vals) => {
                    assert_eq!(vals.len(), 1);
                    assert_eq!(vals[0], Value::I64(42));
                }
                _ => panic!("Expected values Array"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_exec_decode_multiple_columns_multiple_rows() {
    let row1 = MockRow::new(
        vec!["id", "name", "score"],
        vec![Value::I64(1), Value::String("Alice".to_string()), Value::I64(100)],
    );
    let row2 = MockRow::new(
        vec!["id", "name", "score"],
        vec![Value::I64(2), Value::String("Bob".to_string()), Value::I64(85)],
    );
    let conn = MockConnection::with_rows(vec![Box::new(row1), Box::new(row2)]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT id, name, score FROM t");
    let value = result.unwrap();
    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3); // column row + 2 data rows
            // First row is columns
            match &arr[0] {
                Value::Array(cols) => {
                    assert_eq!(cols.len(), 3);
                    // Due to .rev(), we get column_name(2),column_name(1),column_name(0)
                    // = "score", "name", "id"
                    assert_eq!(cols[0], Value::String("score".to_string()));
                    assert_eq!(cols[1], Value::String("name".to_string()));
                    assert_eq!(cols[2], Value::String("id".to_string()));
                }
                _ => panic!("Expected columns Array"),
            }
            // Row 1 data
            match &arr[1] {
                Value::Array(vals) => {
                    assert_eq!(vals.len(), 3);
                    // get(2),get(1),get(0) with our mock (no remove) gives vals[2],vals[1],vals[0]
                    // = 100, "Alice", 1
                    assert_eq!(vals[0], Value::I64(100));
                    assert_eq!(vals[1], Value::String("Alice".to_string()));
                    assert_eq!(vals[2], Value::I64(1));
                }
                _ => panic!("Expected values Array"),
            }
            // Row 2 data
            match &arr[2] {
                Value::Array(vals) => {
                    assert_eq!(vals.len(), 3);
                    assert_eq!(vals[0], Value::I64(85));
                    assert_eq!(vals[1], Value::String("Bob".to_string()));
                    assert_eq!(vals[2], Value::I64(2));
                }
                _ => panic!("Expected values Array"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_exec_decode_with_null_values() {
    let row = MockRow::new(
        vec!["id", "name", "email"],
        vec![Value::I64(1), Value::String("Alice".to_string()), Value::Null],
    );
    let conn = MockConnection::with_rows(vec![Box::new(row)]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT id, name, email FROM t");
    let value = result.unwrap();
    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            // Row data: get(2),get(1),get(0) = Null, "Alice", 1
            match &arr[1] {
                Value::Array(vals) => {
                    assert_eq!(vals.len(), 3);
                    assert_eq!(vals[0], Value::Null);
                    assert_eq!(vals[1], Value::String("Alice".to_string()));
                    assert_eq!(vals[2], Value::I64(1));
                }
                _ => panic!("Expected values Array"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_exec_decode_various_types() {
    let row = MockRow::new(
        vec!["int_val", "str_val", "float_val", "bool_val"],
        vec![
            Value::I64(42),
            Value::String("hello".to_string()),
            Value::F64(3.14),
            Value::Bool(true),
        ],
    );
    let conn = MockConnection::with_rows(vec![Box::new(row)]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT * FROM t");
    let value = result.unwrap();
    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            // Values collected via get(3),get(2),get(1),get(0) = true, 3.14, "hello", 42
            match &arr[1] {
                Value::Array(vals) => {
                    assert_eq!(vals.len(), 4);
                    assert_eq!(vals[0], Value::Bool(true));
                    assert_eq!(vals[1], Value::F64(3.14));
                    assert_eq!(vals[2], Value::String("hello".to_string()));
                    assert_eq!(vals[3], Value::I64(42));
                }
                _ => panic!("Expected values Array"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_exec_decode_column_and_value_positions_align() {
    // This test verifies that columns[i] corresponds to values[i] for each row
    // After exec_decode with .rev(), both columns and values are reversed
    // So columns = [col2, col1, col0] and values = [val2, val1, val0]
    // Position 0: col2 -> val2, Position 1: col1 -> val1, Position 2: col0 -> val0
    let row = MockRow::new(
        vec!["first", "second", "third"],
        vec![Value::String("a".to_string()), Value::String("b".to_string()), Value::String("c".to_string())],
    );
    let conn = MockConnection::with_rows(vec![Box::new(row)]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT first, second, third FROM t");
    let value = result.unwrap();

    if let Value::Array(arr) = value {
        assert_eq!(arr.len(), 2);

        // Columns are reversed: [third, second, first]
        if let Value::Array(cols) = &arr[0] {
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0], Value::String("third".to_string()));
            assert_eq!(cols[1], Value::String("second".to_string()));
            assert_eq!(cols[2], Value::String("first".to_string()));
        }

        // Values are reversed: [c, b, a]
        if let Value::Array(vals) = &arr[1] {
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0], Value::String("c".to_string()));
            assert_eq!(vals[1], Value::String("b".to_string()));
            assert_eq!(vals[2], Value::String("a".to_string()));
        }
    } else {
        panic!("Expected Array");
    }
}

#[test]
fn test_exec_decode_correct_length_fields() {
    // Verify that each row has the correct number of fields matching columns
    let row = MockRow::new(
        vec!["a", "b", "c", "d", "e"],
        vec![
            Value::I64(1),
            Value::I64(2),
            Value::I64(3),
            Value::I64(4),
            Value::I64(5),
        ],
    );
    let conn = MockConnection::with_rows(vec![Box::new(row)]);
    let mut conn = conn;
    let result = exec_decode_sync(&mut conn, "SELECT a, b, c, d, e FROM t");
    let value = result.unwrap();

    if let Value::Array(arr) = value {
        assert_eq!(arr.len(), 2); // columns + 1 data row
        assert_eq!(arr[0].as_array().unwrap().len(), 5); // 5 columns
        assert_eq!(arr[1].as_array().unwrap().len(), 5); // 5 values
    } else {
        panic!("Expected Array");
    }
}
