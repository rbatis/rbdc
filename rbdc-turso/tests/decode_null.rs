use rbdc_turso::value::{turso_value_to_rbs, TursoValue};
use rbs::Value;

/// Verify that a null Turso value converts to Value::Null.
#[test]
fn test_null_to_rbs() {
    let tv = TursoValue::new(libsql::Value::Null);
    assert!(tv.is_null());
    assert_eq!(turso_value_to_rbs(&tv, false), Value::Null);
}
