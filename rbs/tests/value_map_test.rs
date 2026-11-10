use rbs::from_value;
use rbs::value::map::ValueMap;
use rbs::Value;

fn sample() -> ValueMap {
    let mut m = ValueMap::new();
    m.insert(Value::String("a".into()), Value::I32(1));
    m.insert(Value::String("b".into()), Value::String("two".into()));
    m
}

#[test]
fn test_new_with_capacity_len_is_empty() {
    let empty = ValueMap::new();
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());

    let mut m = ValueMap::with_capacity(4);
    assert!(m.is_empty());
    m.insert(Value::String("k".into()), Value::I32(1));
    assert_eq!(m.len(), 1);
    assert!(!m.is_empty());
}

#[test]
fn test_insert_and_get() {
    let mut m = ValueMap::new();
    assert_eq!(m.insert(Value::String("k".into()), Value::I32(1)), None);
    assert_eq!(
        m.insert(Value::String("k".into()), Value::I32(2)),
        Some(Value::I32(1))
    );

    assert_eq!(m.get(&Value::String("k".into())), &Value::I32(2));
    assert_eq!(m.get(&Value::String("missing".into())), &Value::Null);
}

#[test]
fn test_get_mut() {
    let mut m = sample();
    assert_eq!(m.get_mut(&Value::String("missing".into())), None);
    let slot = m.get_mut(&Value::String("a".into())).unwrap();
    *slot = Value::I32(42);
    assert_eq!(m.get(&Value::String("a".into())), &Value::I32(42));
}

#[test]
fn test_remove_and_rm() {
    let mut m = sample();
    assert_eq!(m.remove(&Value::String("a".into())), Value::I32(1));
    assert_eq!(m.remove(&Value::String("a".into())), Value::Null);
    assert_eq!(m.rm(&Value::String("b".into())), Value::String("two".into()));
    assert!(m.is_empty());
}

#[test]
fn test_index_str() {
    let m = sample();
    assert_eq!(m["a"], Value::I32(1));
    assert_eq!(m["missing"], Value::Null);
}

#[test]
fn test_index_i64() {
    let mut m = ValueMap::new();
    m.insert(Value::I64(7), Value::Bool(true));
    assert_eq!(m[7i64], Value::Bool(true));
    assert_eq!(m[8i64], Value::Null);
}

#[test]
fn test_index_mut_str() {
    let mut m = sample();
    m["a"] = Value::I32(10);
    assert_eq!(m["a"], Value::I32(10));
    // Missing keys are inserted with a Null placeholder first.
    m["new"] = Value::String("v".into());
    assert_eq!(m["new"], Value::String("v".into()));
}

#[test]
fn test_index_mut_i64() {
    let mut m = ValueMap::new();
    m.insert(Value::I64(1), Value::I32(1));
    m[1i64] = Value::I32(2);
    assert_eq!(m[1i64], Value::I32(2));
    m[2i64] = Value::Bool(false);
    assert_eq!(m[2i64], Value::Bool(false));
}

#[test]
fn test_into_iterator_shared() {
    let m = sample();
    let mut count = 0;
    for (k, v) in &m {
        assert!(k.is_str());
        assert!(!v.is_null());
        count += 1;
    }
    assert_eq!(count, 2);
}

#[test]
fn test_into_iterator_mut() {
    let mut m = sample();
    for (_k, v) in &mut m {
        *v = Value::Null;
    }
    assert!(m.get(&Value::String("a".into())).is_null());
    assert!(m.get(&Value::String("b".into())).is_null());
}

#[test]
fn test_into_iterator_owned() {
    let m = sample();
    let pairs: Vec<(Value, Value)> = m.into_iter().collect();
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].0, Value::String("a".into()));
}

#[test]
fn test_clone_debug_partial_eq() {
    let m = sample();
    let clone = m.clone();
    assert_eq!(m, clone);
    let dbg = format!("{:?}", m);
    assert!(dbg.contains('a'));
    assert!(!dbg.is_empty());
}

#[test]
fn test_display() {
    let empty = ValueMap::new();
    assert_eq!(format!("{}", empty), "{}");

    let mut m = ValueMap::new();
    m.insert(Value::String("a".into()), Value::I32(1));
    assert_eq!(format!("{}", m), r#"{"a":1}"#);

    m.insert(Value::String("b".into()), Value::String("x".into()));
    assert_eq!(format!("{}", m), r#"{"a":1,"b":"x"}"#);
}

#[test]
fn test_serde_serialize() {
    let m = sample();
    let v = rbs::value(&m).unwrap();
    let round = v.as_map().expect("serialized map");
    assert_eq!(round.len(), 2);
    assert_eq!(round["a"], Value::I32(1));
    assert_eq!(round["b"], Value::String("two".into()));
}

#[test]
fn test_serde_deserialize() {
    let m = sample();
    let v = Value::Map(m.clone());
    let decoded: ValueMap = from_value(v).unwrap();
    assert_eq!(decoded, m);

    let decoded_ref: ValueMap = rbs::from_value_ref(&Value::Map(m.clone())).unwrap();
    assert_eq!(decoded_ref, m);
}

#[test]
fn test_deserialize_wrong_type_reports_expectation() {
    // Exercises the visitor's `expecting` implementation via an invalid type.
    let err = from_value::<ValueMap>(Value::I32(1)).unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn test_value_map_macro() {
    let m = rbs::value_map! {
        "a": 1,
        "b": true,
    };
    assert_eq!(m.len(), 2);
    assert_eq!(m["a"], Value::I32(1));
    assert_eq!(m["b"], Value::Bool(true));

    let empty = rbs::value_map! {};
    assert!(empty.is_empty());
}
