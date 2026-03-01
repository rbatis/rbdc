//! Parity tests for Turso adapter value, row, and metadata behavior.
//!
//! These tests validate that the Turso adapter produces the same
//! `rbs::Value` outputs and row/metadata behavior as the SQLite adapter
//! for representative shared scenarios.
//!
//! Each test scenario has a stable identifier (PAR-NNN) for traceability.

use rbdc::db::{Connection, Driver};
use rbs::Value;

/// Helper: create an in-memory Turso connection.
async fn turso_conn() -> Box<dyn Connection> {
    let driver = rbdc_turso::TursoDriver {};
    driver.connect("turso://:memory:").await.unwrap()
}

// ────────────────────────────────────────────────────────────────────
// PAR-001: Null values
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_001_null_value() {
    let mut conn = turso_conn().await;
    conn.exec("CREATE TABLE par001 (id INTEGER, val TEXT)", vec![])
        .await
        .unwrap();
    conn.exec(
        "INSERT INTO par001 (id, val) VALUES (?, ?)",
        vec![Value::I64(1), Value::Null],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT val FROM par001 WHERE id = 1", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // SQLite adapter: NULL → Value::Null
    let val = rows[0].get(0).unwrap();
    assert_eq!(val, Value::Null);

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-002: Integer values (positive, negative, zero, extremes)
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_002_integer_values() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par002 (id INTEGER PRIMARY KEY, val INTEGER)",
        vec![],
    )
    .await
    .unwrap();

    let cases: Vec<(i64, &str)> = vec![
        (0, "zero"),
        (1, "positive"),
        (-1, "negative"),
        (i64::MAX, "i64_max"),
        (i64::MIN, "i64_min"),
        (42, "typical"),
    ];

    for (i, (val, _label)) in cases.iter().enumerate() {
        conn.exec(
            "INSERT INTO par002 (id, val) VALUES (?, ?)",
            vec![Value::I64(i as i64 + 1), Value::I64(*val)],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT val FROM par002 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len());

    // SQLite adapter: INTEGER → Value::I64
    for (i, (expected, label)) in cases.iter().enumerate() {
        let val = rows[i].get(0).unwrap();
        assert_eq!(val, Value::I64(*expected), "failed for case: {}", label);
    }

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-003: Real/float values
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_003_real_values() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par003 (id INTEGER PRIMARY KEY, val REAL)",
        vec![],
    )
    .await
    .unwrap();

    let cases: Vec<(f64, &str)> = vec![
        (0.0, "zero"),
        (3.14, "pi"),
        (-1.5, "negative"),
        (1e300, "large"),
        (1e-300, "tiny"),
    ];

    for (i, (val, _)) in cases.iter().enumerate() {
        conn.exec(
            "INSERT INTO par003 (id, val) VALUES (?, ?)",
            vec![Value::I64(i as i64 + 1), Value::F64(*val)],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT val FROM par003 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len());

    for (i, (expected, label)) in cases.iter().enumerate() {
        let val = rows[i].get(0).unwrap();
        match val {
            Value::F64(f) => {
                assert!(
                    (f - expected).abs() < f64::EPSILON || f == *expected,
                    "failed for case {}: got {}, expected {}",
                    label,
                    f,
                    expected
                );
            }
            // Some databases may return 0.0 as Integer(0); accept both
            Value::I64(0) if *expected == 0.0 => {}
            other => panic!("unexpected value for case {}: {:?}", label, other),
        }
    }

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-004: Text/string values
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_004_text_values() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par004 (id INTEGER PRIMARY KEY, val TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let cases: Vec<(&str, &str)> = vec![
        ("hello world", "plain"),
        ("", "empty"),
        ("unicode: 日本語 🦀", "unicode"),
        ("line\nbreak", "newline"),
        ("single'quote", "quote"),
    ];

    for (i, (val, _)) in cases.iter().enumerate() {
        conn.exec(
            "INSERT INTO par004 (id, val) VALUES (?, ?)",
            vec![Value::I64(i as i64 + 1), Value::String(val.to_string())],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT val FROM par004 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len());

    for (i, (expected, label)) in cases.iter().enumerate() {
        let val = rows[i].get(0).unwrap();
        assert_eq!(
            val,
            Value::String(expected.to_string()),
            "failed for case: {}",
            label
        );
    }

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-005: Blob values
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_005_blob_values() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par005 (id INTEGER PRIMARY KEY, val BLOB)",
        vec![],
    )
    .await
    .unwrap();

    let cases: Vec<(Vec<u8>, &str)> = vec![
        (vec![0xDE, 0xAD, 0xBE, 0xEF], "deadbeef"),
        (vec![], "empty"),
        (vec![0; 256], "zeros"),
        ((0u8..=255).collect(), "all_bytes"),
    ];

    for (i, (val, _)) in cases.iter().enumerate() {
        conn.exec(
            "INSERT INTO par005 (id, val) VALUES (?, ?)",
            vec![Value::I64(i as i64 + 1), Value::Binary(val.clone())],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT val FROM par005 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len());

    for (i, (expected, label)) in cases.iter().enumerate() {
        let val = rows[i].get(0).unwrap();
        assert_eq!(
            val,
            Value::Binary(expected.clone()),
            "failed for case: {}",
            label
        );
    }

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-006: JSON text decoding (default: off, opt-in via json_detect)
// ────────────────────────────────────────────────────────────────────

/// Helper: create an in-memory Turso connection with json_detect enabled.
async fn turso_conn_json() -> Box<dyn Connection> {
    let driver = rbdc_turso::TursoDriver {};
    driver
        .connect("turso://:memory:?json_detect=true")
        .await
        .unwrap()
}

#[tokio::test]
async fn par_006_json_text_default_no_detection() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par006 (id INTEGER PRIMARY KEY, val TEXT)",
        vec![],
    )
    .await
    .unwrap();

    // Store JSON-shaped strings
    let cases: Vec<&str> = vec![
        r#"{"key":"value"}"#,
        "[1,2,3]",
        "null",
        "plain text",
        "123",
    ];

    for (i, val) in cases.iter().enumerate() {
        conn.exec(
            "INSERT INTO par006 (id, val) VALUES (?, ?)",
            vec![Value::I64(i as i64 + 1), Value::String(val.to_string())],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT val FROM par006 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len());

    // With json_detect=false (default), ALL text comes back as String
    for (i, expected) in cases.iter().enumerate() {
        assert_eq!(
            rows[i].get(0).unwrap(),
            Value::String(expected.to_string()),
            "PAR-006: all text should be String when json_detect is off (index {})",
            i
        );
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn par_006_json_text_with_detection_enabled() {
    let mut conn = turso_conn_json().await;
    conn.exec(
        "CREATE TABLE par006j (id INTEGER PRIMARY KEY, val TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let cases: Vec<&str> = vec![
        r#"{"key":"value"}"#,
        "[1,2,3]",
        "null",
        "plain text",
        "123",
    ];

    for (i, val) in cases.iter().enumerate() {
        conn.exec(
            "INSERT INTO par006j (id, val) VALUES (?, ?)",
            vec![Value::I64(i as i64 + 1), Value::String(val.to_string())],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT val FROM par006j ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len());

    // JSON object → deserialized as Map
    assert!(matches!(rows[0].get(0).unwrap(), Value::Map(_)), "PAR-006j: json_object");
    // JSON array → deserialized as Array
    assert!(matches!(rows[1].get(0).unwrap(), Value::Array(_)), "PAR-006j: json_array");
    // "null" → deserialized as Null (known data-loss edge case)
    assert_eq!(rows[2].get(0).unwrap(), Value::Null, "PAR-006j: json_null");
    // Plain text → String
    assert_eq!(
        rows[3].get(0).unwrap(),
        Value::String("plain text".into()),
        "PAR-006j: plain_text"
    );
    // Numeric string → String (not parsed as number)
    assert_eq!(
        rows[4].get(0).unwrap(),
        Value::String("123".into()),
        "PAR-006j: numeric_string"
    );

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-007: Boolean as integer (1/0)
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_007_bool_parameter_binding() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par007 (id INTEGER PRIMARY KEY, flag INTEGER)",
        vec![],
    )
    .await
    .unwrap();

    conn.exec(
        "INSERT INTO par007 (id, flag) VALUES (?, ?)",
        vec![Value::I64(1), Value::Bool(true)],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO par007 (id, flag) VALUES (?, ?)",
        vec![Value::I64(2), Value::Bool(false)],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT flag FROM par007 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    // SQLite stores booleans as INTEGER 1/0, reads back as I64
    assert_eq!(rows[0].get(0).unwrap(), Value::I64(1));
    assert_eq!(rows[1].get(0).unwrap(), Value::I64(0));

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-008: Metadata column count and names
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_008_metadata_columns() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par008 (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO par008 VALUES (1, 'test', 9.5, X'CAFE')",
        vec![],
    )
    .await
    .unwrap();

    let rows = conn
        .get_rows("SELECT id, name, score, data FROM par008", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let md = rows[0].meta_data();
    assert_eq!(md.column_len(), 4);
    assert_eq!(md.column_name(0), "id");
    assert_eq!(md.column_name(1), "name");
    assert_eq!(md.column_name(2), "score");
    assert_eq!(md.column_name(3), "data");

    // Type strings should be non-empty for typed columns
    let id_type = md.column_type(0);
    let name_type = md.column_type(1);
    let score_type = md.column_type(2);
    let data_type = md.column_type(3);

    // Types should reflect actual value types (INTEGER, TEXT, REAL, BLOB)
    assert!(!id_type.is_empty(), "id type should not be empty, got: {}", id_type);
    assert!(!name_type.is_empty(), "name type should not be empty, got: {}", name_type);
    assert!(!score_type.is_empty(), "score type should not be empty, got: {}", score_type);
    assert!(!data_type.is_empty(), "data type should not be empty, got: {}", data_type);

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-009: Row access bounds checking and consumed-value errors
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_009_row_access_out_of_bounds() {
    let mut conn = turso_conn().await;
    conn.exec("CREATE TABLE par009 (a INTEGER, b TEXT)", vec![])
        .await
        .unwrap();
    conn.exec(
        "INSERT INTO par009 VALUES (1, 'x')",
        vec![],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT a, b FROM par009", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // Valid access by index — indices are stable (Option::take pattern)
    let a = rows[0].get(0).unwrap();
    assert_eq!(a, Value::I64(1));
    let b = rows[0].get(1).unwrap();
    assert_eq!(b, Value::String("x".into()));

    // Accessing an already-consumed index should error
    let result = rows[0].get(0);
    assert!(result.is_err(), "should error on already-consumed column");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("already consumed"), "got: {}", err);

    // Out of bounds should also error
    let result = rows[0].get(99);
    assert!(result.is_err(), "should error on out-of-bounds access");

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-010: Rows affected and last_insert_id for INSERT
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_010_exec_result_insert() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par010 (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let r1 = conn
        .exec(
            "INSERT INTO par010 (val) VALUES (?)",
            vec![Value::String("first".into())],
        )
        .await
        .unwrap();
    assert_eq!(r1.rows_affected, 1);
    // last_insert_id should be > 0 (stored as I64 to preserve signed rowid semantics)
    match &r1.last_insert_id {
        Value::I64(id) => assert!(*id > 0, "last_insert_id should be > 0"),
        other => panic!("expected I64 for last_insert_id, got: {:?}", other),
    }

    let r2 = conn
        .exec(
            "INSERT INTO par010 (val) VALUES (?)",
            vec![Value::String("second".into())],
        )
        .await
        .unwrap();
    assert_eq!(r2.rows_affected, 1);
    // Second insert should have a higher rowid
    match (&r1.last_insert_id, &r2.last_insert_id) {
        (Value::I64(id1), Value::I64(id2)) => {
            assert!(id2 > id1, "rowids should increment: {} > {}", id2, id1);
        }
        _ => panic!("unexpected last_insert_id types"),
    }

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-011: Rows affected for UPDATE and DELETE
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_011_exec_result_update_delete() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par011 (id INTEGER PRIMARY KEY, val TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec("INSERT INTO par011 VALUES (1, 'a')", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO par011 VALUES (2, 'b')", vec![])
        .await
        .unwrap();
    conn.exec("INSERT INTO par011 VALUES (3, 'c')", vec![])
        .await
        .unwrap();

    // Update 2 rows
    let r = conn
        .exec(
            "UPDATE par011 SET val = 'x' WHERE id <= 2",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(r.rows_affected, 2);

    // Delete 1 row
    let r = conn
        .exec("DELETE FROM par011 WHERE id = 3", vec![])
        .await
        .unwrap();
    assert_eq!(r.rows_affected, 1);

    // Delete 0 rows
    let r = conn
        .exec("DELETE FROM par011 WHERE id = 999", vec![])
        .await
        .unwrap();
    assert_eq!(r.rows_affected, 0);

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-012: Mixed types in a single row
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_012_mixed_types_single_row() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par012 (i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO par012 VALUES (?, ?, ?, ?, ?)",
        vec![
            Value::I64(42),
            Value::F64(2.718),
            Value::String("hello".into()),
            Value::Binary(vec![0xFF]),
            Value::Null,
        ],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT i, r, t, b, n FROM par012", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let md = rows[0].meta_data();
    assert_eq!(md.column_len(), 5);

    // Values accessed in reverse order (like get_values does) to verify
    // index-stable access via Option::take pattern
    let n = rows[0].get(4).unwrap();
    assert_eq!(n, Value::Null);

    let b = rows[0].get(3).unwrap();
    assert_eq!(b, Value::Binary(vec![0xFF]));

    let t = rows[0].get(2).unwrap();
    assert_eq!(t, Value::String("hello".into()));

    let r = rows[0].get(1).unwrap();
    match r {
        Value::F64(f) => assert!((f - 2.718).abs() < 1e-10),
        other => panic!("expected F64, got {:?}", other),
    }

    let i = rows[0].get(0).unwrap();
    assert_eq!(i, Value::I64(42));

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-013: get_values convenience method
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_013_get_values() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par013 (id INTEGER PRIMARY KEY, name TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO par013 VALUES (1, 'alice'), (2, 'bob')",
        vec![],
    )
    .await
    .unwrap();

    let values = conn
        .get_values("SELECT id, name FROM par013 ORDER BY id", vec![])
        .await
        .unwrap();

    // get_values returns Value::Array of Value::Map
    match values {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            // Each element should be a Map with 'id' and 'name' keys
            for item in &arr {
                assert!(matches!(item, Value::Map(_)));
            }
        }
        other => panic!("expected Array, got {:?}", other),
    }

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-014: Empty result set
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_014_empty_result_set() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par014 (id INTEGER PRIMARY KEY)",
        vec![],
    )
    .await
    .unwrap();

    let rows = conn
        .get_rows("SELECT id FROM par014", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);

    let values = conn
        .get_values("SELECT id FROM par014", vec![])
        .await
        .unwrap();
    assert_eq!(values, Value::Array(vec![]));

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-015: Metadata for aliased columns
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_015_aliased_column_names() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par015 (id INTEGER, long_column_name TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO par015 VALUES (1, 'test')",
        vec![],
    )
    .await
    .unwrap();

    let rows = conn
        .get_rows(
            "SELECT id AS row_id, long_column_name AS name FROM par015",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let md = rows[0].meta_data();
    assert_eq!(md.column_name(0), "row_id");
    assert_eq!(md.column_name(1), "name");

    conn.close().await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// PAR-016: Multiple rows iteration
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn par_016_multiple_rows() {
    let mut conn = turso_conn().await;
    conn.exec(
        "CREATE TABLE par016 (id INTEGER PRIMARY KEY)",
        vec![],
    )
    .await
    .unwrap();

    for i in 1..=100 {
        conn.exec(
            "INSERT INTO par016 VALUES (?)",
            vec![Value::I64(i)],
        )
        .await
        .unwrap();
    }

    let mut rows = conn
        .get_rows("SELECT id FROM par016 ORDER BY id", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 100);

    for (i, row) in rows.iter_mut().enumerate() {
        let val = row.get(0).unwrap();
        assert_eq!(val, Value::I64(i as i64 + 1));
    }

    conn.close().await.unwrap();
}
