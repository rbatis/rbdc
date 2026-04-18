use rbs::Value;
use rbdc::db::IntoMaps;

#[test]
fn test_into_maps_empty_array() {
    let result: Value = Value::Array(vec![]).into_maps();
    assert!(matches!(result, Value::Array(arr) if arr.is_empty()));
}

#[test]
fn test_into_maps_non_array_value() {
    let result: Value = Value::String("not an array".to_string()).into_maps();
    assert!(matches!(result, Value::Array(arr) if arr.is_empty()));
}

#[test]
fn test_into_maps_only_columns_no_data() {
    let input = Value::Array(vec![Value::Array(vec![Value::String("col1".to_string()), Value::String("col2".to_string())])]);
    let result: Value = input.into_maps();
    assert!(matches!(result, Value::Array(arr) if arr.is_empty()));
}

#[test]
fn test_into_maps_normal_case() {
    let input = Value::Array(vec![
        Value::Array(vec![Value::String("id".to_string()), Value::String("name".to_string())]),
        Value::Array(vec![Value::I64(1), Value::String("Alice".to_string())]),
        Value::Array(vec![Value::I64(2), Value::String("Bob".to_string())]),
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                Value::Map(map) => {
                    assert_eq!(map["id"], Value::I64(1));
                    assert_eq!(map["name"], Value::String("Alice".to_string()));
                }
                _ => panic!("Expected Map"),
            }
            match &arr[1] {
                Value::Map(map) => {
                    assert_eq!(map["id"], Value::I64(2));
                    assert_eq!(map["name"], Value::String("Bob".to_string()));
                }
                _ => panic!("Expected Map"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_into_maps_data_row_not_array() {
    let input = Value::Array(vec![
        Value::Array(vec![Value::String("col1".to_string()), Value::String("col2".to_string())]),
        Value::I64(123), // data row is not an array
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                Value::Map(map) if map.is_empty() => {}
                _ => panic!("Expected empty Map"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_into_maps_non_string_column_name() {
    let input = Value::Array(vec![
        Value::Array(vec![Value::I64(1), Value::I64(2)]), // column names are integers
        Value::Array(vec![Value::String("val1".to_string()), Value::String("val2".to_string())]),
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                Value::Map(map) => {
                    // column names are converted to strings via to_string()
                    assert_eq!(map["1"], Value::String("val1".to_string()));
                    assert_eq!(map["2"], Value::String("val2".to_string()));
                }
                _ => panic!("Expected Map"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_into_maps_with_null_values() {
    let input = Value::Array(vec![
        Value::Array(vec![Value::String("id".to_string()), Value::String("name".to_string())]),
        Value::Array(vec![Value::I64(1), Value::Null]),
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                Value::Map(map) => {
                    assert_eq!(map["id"], Value::I64(1));
                    assert_eq!(map["name"], Value::Null);
                }
                _ => panic!("Expected Map"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_into_maps_single_column_single_row() {
    let input = Value::Array(vec![
        Value::Array(vec![Value::String("id".to_string())]),
        Value::Array(vec![Value::I64(42)]),
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                Value::Map(map) => {
                    assert_eq!(map["id"], Value::I64(42));
                }
                _ => panic!("Expected Map"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_into_maps_mismatched_column_data_length() {
    // columns have 3, but data has 2
    let input = Value::Array(vec![
        Value::Array(vec![Value::String("c1".to_string()), Value::String("c2".to_string()), Value::String("c3".to_string())]),
        Value::Array(vec![Value::I64(1), Value::I64(2)]), // only 2 values
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                Value::Map(map) => {
                    assert_eq!(map.len(), 2); // only 2 entries inserted
                    assert_eq!(map["c1"], Value::I64(1));
                    assert_eq!(map["c2"], Value::I64(2));
                    assert_eq!(map["c3"], Value::Null); // Index returns Null for missing key
                }
                _ => panic!("Expected Map"),
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_into_maps_multiple_rows() {
    let input = Value::Array(vec![
        Value::Array(vec![Value::String("name".to_string()), Value::String("score".to_string())]),
        Value::Array(vec![Value::String("Alice".to_string()), Value::I64(100)]),
        Value::Array(vec![Value::String("Bob".to_string()), Value::I64(85)]),
        Value::Array(vec![Value::String("Carol".to_string()), Value::I64(92)]),
    ]);
    let result: Value = input.into_maps();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
            for (i, row) in arr.iter().enumerate() {
                assert!(matches!(row, Value::Map(_)), "Row {} should be a Map", i);
            }
        }
        _ => panic!("Expected Array"),
    }
}
