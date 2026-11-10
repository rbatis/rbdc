use rbs::value::map::ValueMap;
use rbs::Value;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn map_value() -> Value {
    let mut m = ValueMap::new();
    m.insert(Value::String("a".into()), Value::I32(1));
    m.insert(Value::String("b".into()), Value::I32(2));
    Value::Map(m)
}

#[test]
fn test_index_usize_array() {
    let arr = Value::Array(vec![Value::I32(10), Value::I32(20)]);
    assert_eq!(arr[0usize], Value::I32(10));
    assert_eq!(arr[1usize], Value::I32(20));

    let wrapped = Value::Ext(
        "Arr",
        Box::new(Value::Array(vec![Value::String("x".into())])),
    );
    assert_eq!(wrapped[0usize], Value::String("x".into()));
}

#[test]
fn test_index_usize_non_array_returns_null() {
    assert_eq!(Value::Null[0usize], Value::Null);
    assert_eq!(Value::Bool(true)[0usize], Value::Null);
    assert_eq!(Value::I32(1)[0usize], Value::Null);
    assert_eq!(Value::I64(1)[0usize], Value::Null);
    assert_eq!(Value::U32(1)[0usize], Value::Null);
    assert_eq!(Value::U64(1)[0usize], Value::Null);
    assert_eq!(Value::F32(1.0)[0usize], Value::Null);
    assert_eq!(Value::F64(1.0)[0usize], Value::Null);
    assert_eq!(Value::String("s".into())[0usize], Value::Null);
    assert_eq!(Value::Binary(vec![1])[0usize], Value::Null);
    assert_eq!(map_value()[0usize], Value::Null);
}

#[test]
fn test_index_mut_usize_array() {
    let mut arr = Value::Array(vec![Value::I32(1)]);
    arr[0usize] = Value::I32(42);
    assert_eq!(arr[0usize], Value::I32(42));

    let mut wrapped = Value::Ext("Arr", Box::new(Value::Array(vec![Value::I32(1)])));
    wrapped[0usize] = Value::I32(7);
    assert_eq!(wrapped[0usize], Value::I32(7));
}

#[test]
fn test_index_mut_usize_panics_on_non_array() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut v = Value::I32(1);
        v[0usize] = Value::Null;
    }));
    assert!(result.is_err());
}

#[test]
fn test_index_str_map() {
    let v = map_value();
    assert_eq!(v["a"], Value::I32(1));
    assert_eq!(v["b"], Value::I32(2));
    assert_eq!(v["missing"], Value::Null);

    let wrapped = Value::Ext("Map", Box::new(map_value()));
    assert_eq!(wrapped["a"], Value::I32(1));
}

#[test]
fn test_index_str_non_map_returns_null() {
    assert_eq!(Value::Null["a"], Value::Null);
    assert_eq!(Value::Bool(true)["a"], Value::Null);
    assert_eq!(Value::I32(1)["a"], Value::Null);
    assert_eq!(Value::I64(1)["a"], Value::Null);
    assert_eq!(Value::U32(1)["a"], Value::Null);
    assert_eq!(Value::U64(1)["a"], Value::Null);
    assert_eq!(Value::F32(1.0)["a"], Value::Null);
    assert_eq!(Value::F64(1.0)["a"], Value::Null);
    assert_eq!(Value::String("s".into())["a"], Value::Null);
    assert_eq!(Value::Binary(vec![1])["a"], Value::Null);
    assert_eq!(Value::Array(vec![])["a"], Value::Null);
}

#[test]
fn test_index_mut_str_map() {
    let mut v = map_value();
    v["a"] = Value::I32(11);
    assert_eq!(v["a"], Value::I32(11));
    // Missing keys are created as Null first.
    v["new"] = Value::Bool(true);
    assert_eq!(v["new"], Value::Bool(true));
}

#[test]
fn test_index_mut_str_ext() {
    let mut v = Value::Ext("Map", Box::new(map_value()));
    v["a"] = Value::I32(99);
    assert_eq!(v["a"], Value::I32(99));
    v["new"] = Value::Null;
    assert_eq!(v["new"], Value::Null);
}

#[test]
fn test_index_mut_str_panics_on_non_map() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut v = Value::I32(1);
        v["a"] = Value::Null;
    }));
    assert!(result.is_err());
}

#[test]
fn test_index_value_array_and_map() {
    let arr = Value::Array(vec![Value::I32(10), Value::I32(20)]);
    assert_eq!(arr[Value::U32(1)], Value::I32(20));
    assert_eq!(arr[Value::U64(0)], Value::I32(10));
    // Non-numeric index defaults to zero.
    assert_eq!(arr[Value::String("nope".into())], Value::I32(10));

    let v = map_value();
    assert_eq!(v[Value::String("a".into())], Value::I32(1));
    assert_eq!(v[Value::I32(1)], Value::Null);

    let wrapped = Value::Ext("Arr", Box::new(Value::Array(vec![Value::I32(5)])));
    assert_eq!(wrapped[Value::U32(0)], Value::I32(5));

    assert_eq!(Value::I32(1)[Value::U32(0)], Value::Null);
}

#[test]
fn test_index_ref_value_array_and_map() {
    let arr = Value::Array(vec![Value::I32(10), Value::I32(20)]);
    assert_eq!(arr[&Value::U32(1)], Value::I32(20));

    let v = map_value();
    assert_eq!(v[&Value::String("b".into())], Value::I32(2));

    let wrapped = Value::Ext("Map", Box::new(map_value()));
    assert_eq!(wrapped[&Value::String("a".into())], Value::I32(1));

    assert_eq!(Value::Null[&Value::U32(0)], Value::Null);
}

#[test]
fn test_index_mut_value_array_and_map() {
    let mut arr = Value::Array(vec![Value::I32(1)]);
    arr[Value::U32(0)] = Value::I32(2);
    assert_eq!(arr[Value::U32(0)], Value::I32(2));

    let mut v = map_value();
    v[Value::String("a".into())] = Value::I32(3);
    assert_eq!(v["a"], Value::I32(3));

    let mut wrapped = Value::Ext("Arr", Box::new(Value::Array(vec![Value::I32(1)])));
    wrapped[Value::U32(0)] = Value::I32(4);
    assert_eq!(wrapped[Value::U32(0)], Value::I32(4));

    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut scalar = Value::Null;
        scalar[Value::U32(0)] = Value::Null;
    }));
    assert!(result.is_err());
}

#[test]
fn test_index_mut_ref_value_array_and_map() {
    let mut arr = Value::Array(vec![Value::I32(1)]);
    arr[&Value::U32(0)] = Value::I32(2);
    assert_eq!(arr[&Value::U32(0)], Value::I32(2));

    let mut v = map_value();
    v[&Value::String("b".into())] = Value::I32(3);
    assert_eq!(v["b"], Value::I32(3));

    let mut wrapped = Value::Ext("Map", Box::new(map_value()));
    wrapped[&Value::String("a".into())] = Value::I32(4);
    assert_eq!(wrapped["a"], Value::I32(4));

    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut scalar = Value::Bool(true);
        scalar[&Value::U32(0)] = Value::Null;
    }));
    assert!(result.is_err());
}

#[test]
fn test_insert_scalar_values_returns_none() {
    let scalars = vec![
        Value::Null,
        Value::Bool(true),
        Value::I32(1),
        Value::I64(1),
        Value::U32(1),
        Value::U64(1),
        Value::F32(1.0),
        Value::F64(1.0),
        Value::String("s".into()),
        Value::Binary(vec![1]),
    ];
    for mut v in scalars {
        assert_eq!(v.insert(Value::String("k".into()), Value::Null), None);
    }
}

#[test]
fn test_insert_array() {
    let mut arr = Value::Array(vec![Value::I32(1), Value::I32(3)]);
    assert_eq!(arr.insert(Value::U32(1), Value::I32(2)), None);
    assert_eq!(
        arr,
        Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)])
    );
    // Non-numeric key defaults to index 0.
    arr.insert(Value::String("zero".into()), Value::I32(0));
    assert_eq!(arr[0usize], Value::I32(0));
}

#[test]
fn test_insert_map_and_ext() {
    let mut v = map_value();
    assert_eq!(v.insert(Value::String("a".into()), Value::I32(10)), Some(Value::I32(1)));
    assert_eq!(v["a"], Value::I32(10));

    let mut wrapped = Value::Ext("Map", Box::new(map_value()));
    assert_eq!(
        wrapped.insert(Value::String("b".into()), Value::I32(20)),
        Some(Value::I32(2))
    );
    assert_eq!(wrapped["b"], Value::I32(20));

    let mut wrapped_scalar = Value::Ext("Scalar", Box::new(Value::I32(1)));
    assert_eq!(
        wrapped_scalar.insert(Value::String("k".into()), Value::Null),
        None
    );
}

#[test]
fn test_remove_scalar_values_returns_null() {
    let scalars = vec![
        Value::Null,
        Value::Bool(true),
        Value::I32(1),
        Value::I64(1),
        Value::U32(1),
        Value::U64(1),
        Value::F32(1.0),
        Value::F64(1.0),
        Value::String("s".into()),
        Value::Binary(vec![1]),
    ];
    for mut v in scalars {
        assert_eq!(v.remove(&Value::String("k".into())), Value::Null);
    }
}

#[test]
fn test_remove_array() {
    let mut arr = Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]);
    assert_eq!(arr.remove(&Value::U32(1)), Value::I32(2));
    assert_eq!(arr, Value::Array(vec![Value::I32(1), Value::I32(3)]));
    // Out-of-range index returns Null.
    assert_eq!(arr.remove(&Value::U32(99)), Value::Null);
    // Non-numeric index defaults to zero.
    assert_eq!(arr.remove(&Value::String("x".into())), Value::I32(1));
}

#[test]
fn test_remove_map_and_ext() {
    let mut v = map_value();
    assert_eq!(v.remove(&Value::String("a".into())), Value::I32(1));
    assert_eq!(v.remove(&Value::String("a".into())), Value::Null);

    let mut wrapped = Value::Ext("Map", Box::new(map_value()));
    assert_eq!(
        wrapped.remove(&Value::String("b".into())),
        Value::I32(2)
    );

    let mut wrapped_scalar = Value::Ext("Scalar", Box::new(Value::I32(1)));
    assert_eq!(
        wrapped_scalar.remove(&Value::String("k".into())),
        Value::Null
    );
}
