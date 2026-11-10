use rbs::value::map::ValueMap;
use rbs::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn sample_map() -> ValueMap {
    let mut m = ValueMap::new();
    m.insert(Value::String("a".into()), Value::I32(1));
    m.insert(Value::String("b".into()), Value::String("two".into()));
    m
}

fn ext(inner: Value) -> Value {
    Value::Ext("TestType", Box::new(inner))
}

#[test]
fn test_into_ext() {
    let v = Value::I32(1);
    let wrapped = v.clone().into_ext("MyType");
    assert_eq!(wrapped, Value::Ext("MyType", Box::new(Value::I32(1))));
    // Wrapping an already-wrapped value returns it unchanged.
    let again = wrapped.clone().into_ext("OtherType");
    assert_eq!(again, wrapped);
}

#[test]
fn test_is_empty_all_variants() {
    assert!(Value::Null.is_empty());
    assert!(!Value::Bool(false).is_empty());
    assert!(!Value::I32(0).is_empty());
    assert!(!Value::I64(0).is_empty());
    assert!(!Value::U32(0).is_empty());
    assert!(!Value::U64(0).is_empty());
    assert!(!Value::F32(0.0).is_empty());
    assert!(!Value::F64(0.0).is_empty());
    assert!(Value::String(String::new()).is_empty());
    assert!(!Value::String("x".into()).is_empty());
    assert!(Value::Binary(vec![]).is_empty());
    assert!(!Value::Binary(vec![1]).is_empty());
    assert!(Value::Array(vec![]).is_empty());
    assert!(!Value::Array(vec![Value::Null]).is_empty());
    assert!(Value::Map(ValueMap::new()).is_empty());
    assert!(!Value::Map(sample_map()).is_empty());
    // Ext delegates emptiness to its inner value.
    assert!(ext(Value::Null).is_empty());
    assert!(ext(Value::String(String::new())).is_empty());
    assert!(!ext(Value::I32(0)).is_empty());
}

#[test]
fn test_len_all_variants() {
    assert_eq!(Value::Null.len(), 0);
    assert_eq!(Value::Bool(true).len(), 0);
    assert_eq!(Value::I32(1).len(), 0);
    assert_eq!(Value::I64(1).len(), 0);
    assert_eq!(Value::U32(1).len(), 0);
    assert_eq!(Value::U64(1).len(), 0);
    assert_eq!(Value::F32(1.0).len(), 0);
    assert_eq!(Value::F64(1.0).len(), 0);
    assert_eq!(Value::String("abcd".into()).len(), 4);
    assert_eq!(Value::Binary(vec![1, 2, 3]).len(), 3);
    assert_eq!(Value::Array(vec![Value::Null, Value::Null]).len(), 2);
    assert_eq!(Value::Map(sample_map()).len(), 2);
    assert_eq!(ext(Value::String("abc".into())).len(), 3);
    assert_eq!(ext(Value::I32(1)).len(), 0);
}

#[test]
fn test_is_debug_mode() {
    assert_eq!(
        rbs::is_debug_mode(),
        cfg!(debug_assertions) && cfg!(feature = "debug_mode")
    );
}

#[test]
fn test_is_predicates() {
    assert!(Value::I32(1).is_i32());
    assert!(!Value::I64(1).is_i32());
    assert!(Value::I64(1).is_i64());
    assert!(!Value::I32(1).is_i64());
    assert!(Value::U64(1).is_u64());
    assert!(!Value::U32(1).is_u64());
    assert!(Value::F32(1.0).is_f32());
    assert!(!Value::F64(1.0).is_f32());
    assert!(Value::F64(1.0).is_f64());
    assert!(!Value::F32(1.0).is_f64());
    assert!(Value::I64(1).is_number());
    assert!(Value::U64(1).is_number());
    assert!(Value::F32(1.0).is_number());
    assert!(Value::F64(1.0).is_number());
    assert!(!Value::I32(1).is_number());
    assert!(!Value::String("1".into()).is_number());
    assert!(ext(Value::Bool(true)).is_bool());
    assert!(ext(Value::String("s".into())).is_str());
    assert!(ext(Value::Binary(vec![1])).is_bin());
    assert!(ext(Value::Array(vec![])).is_array());
    assert!(ext(Value::Map(sample_map())).is_map());
}

#[test]
fn test_as_bool_ext() {
    assert_eq!(ext(Value::Bool(true)).as_bool(), Some(true));
    assert_eq!(ext(Value::I32(1)).as_bool(), None);
}

#[test]
fn test_as_i64_all_number_variants() {
    assert_eq!(Value::F32(3.9).as_i64(), Some(3));
    assert_eq!(Value::F64(4.9).as_i64(), Some(4));
    assert_eq!(Value::U32(5).as_i64(), Some(5));
    assert_eq!(Value::U64(6).as_i64(), Some(6));
    assert_eq!(Value::I32(7).as_i64(), Some(7));
    assert_eq!(Value::I64(8).as_i64(), Some(8));
    assert_eq!(ext(Value::I64(9)).as_i64(), Some(9));
    assert_eq!(Value::String("10".into()).as_i64(), None);
}

#[test]
fn test_as_u64_all_number_variants() {
    assert_eq!(Value::F32(3.9).as_u64(), Some(3));
    assert_eq!(Value::F64(4.9).as_u64(), Some(4));
    assert_eq!(Value::I32(5).as_u64(), Some(5));
    assert_eq!(Value::I64(6).as_u64(), Some(6));
    assert_eq!(Value::U32(7).as_u64(), Some(7));
    assert_eq!(Value::U64(8).as_u64(), Some(8));
    assert_eq!(ext(Value::U32(9)).as_u64(), Some(9));
    assert_eq!(Value::Bool(true).as_u64(), None);
}

#[test]
fn test_as_f64_all_number_variants() {
    assert_eq!(Value::I32(1).as_f64(), Some(1.0));
    assert_eq!(Value::U32(2).as_f64(), Some(2.0));
    assert_eq!(Value::I64(3).as_f64(), Some(3.0));
    assert_eq!(Value::U64(4).as_f64(), Some(4.0));
    assert_eq!(Value::F32(5.5).as_f64(), Some(5.5));
    assert_eq!(Value::F64(6.5).as_f64(), Some(6.5));
    assert_eq!(ext(Value::I32(7)).as_f64(), Some(7.0));
    assert_eq!(Value::String("8".into()).as_f64(), None);
}

#[test]
fn test_string_accessors() {
    assert_eq!(ext(Value::String("hello".into())).as_str(), Some("hello"));
    assert_eq!(
        ext(Value::String("hello".into())).as_string(),
        Some("hello".to_string())
    );
    assert_eq!(Value::I32(1).as_string(), None);
    assert_eq!(ext(Value::String("x".into())).into_string(), Some("x".to_string()));
    assert_eq!(Value::I32(1).into_string(), None);
    assert_eq!(
        Value::String("y".into()).into_string(),
        Some("y".to_string())
    );
}

#[test]
fn test_into_bytes_all_variants() {
    assert_eq!(Value::Binary(vec![1, 2]).into_bytes(), Some(vec![1, 2]));
    assert_eq!(ext(Value::I32(3)).into_bytes(), Some(b"3".to_vec()));
    assert_eq!(Value::Null.into_bytes(), Some(vec![]));
    assert_eq!(Value::Bool(true).into_bytes(), Some(b"true".to_vec()));
    assert_eq!(Value::I32(-5).into_bytes(), Some(b"-5".to_vec()));
    assert_eq!(Value::I64(-6).into_bytes(), Some(b"-6".to_vec()));
    assert_eq!(Value::U32(7).into_bytes(), Some(b"7".to_vec()));
    assert_eq!(Value::U64(8).into_bytes(), Some(b"8".to_vec()));
    assert_eq!(Value::F32(1.5).into_bytes(), Some(b"1.5".to_vec()));
    assert_eq!(Value::F64(2.5).into_bytes(), Some(b"2.5".to_vec()));
    assert_eq!(Value::String("hi".into()).into_bytes(), Some(b"hi".to_vec()));
    assert_eq!(
        Value::Array(vec![Value::I32(1)]).into_bytes(),
        Some(b"[1]".to_vec())
    );
    assert_eq!(
        Value::Map(sample_map()).into_bytes(),
        Some(br#"{"a":1,"b":"two"}"#.to_vec())
    );
}

#[test]
fn test_as_slice_variants() {
    assert_eq!(Value::Binary(vec![1, 2]).as_slice(), Some(&[1u8, 2][..]));
    assert_eq!(Value::String("abc".into()).as_slice(), Some(&b"abc"[..]));
    assert_eq!(ext(Value::String("xyz".into())).as_slice(), Some(&b"xyz"[..]));
    assert_eq!(Value::Null.as_slice(), None);
}

#[test]
fn test_array_accessors() {
    let inner = Value::Array(vec![Value::I32(1)]);
    let wrapped = ext(inner.clone());
    assert_eq!(wrapped.as_array(), inner.as_array());
    assert_eq!(wrapped.into_array(), inner.clone().into_array());
    assert_eq!(Value::I32(1).into_array(), None);
    assert_eq!(Value::Null.as_array(), None);
}

#[test]
fn test_map_accessors() {
    let inner = Value::Map(sample_map());
    let wrapped = ext(inner.clone());
    assert_eq!(wrapped.as_map(), inner.as_map());
    assert_eq!(wrapped.clone().into_map(), inner.clone().into_map());
    assert_eq!(Value::I32(1).into_map(), None);
    assert_eq!(Value::Null.as_map(), None);
    // Maps convert into ValueMap directly.
    let m: ValueMap = inner.clone().into();
    assert_eq!(m.len(), 2);
    let empty: ValueMap = Value::I32(1).into();
    assert!(empty.is_empty());
}

#[test]
fn test_as_ext_non_ext() {
    assert_eq!(Value::Null.as_ext(), None);
    assert_eq!(Value::I32(1).as_ext(), None);
}

#[test]
fn test_display_all_variants() {
    assert_eq!(Value::I32(-1).to_string(), "-1");
    assert_eq!(Value::U64(3).to_string(), "3");
    assert_eq!(Value::U32(4).to_string(), "4");
    assert_eq!(Value::F32(1.5).to_string(), "1.5");
    assert_eq!(Value::F64(2.5).to_string(), "2.5");
    assert_eq!(Value::Array(vec![]).to_string(), "[]");
    assert_eq!(
        Value::Array(vec![Value::I32(1), Value::I32(2)]).to_string(),
        "[1,2]"
    );
    assert_eq!(Value::Array(vec![Value::I32(1)]).to_string(), "[1]");
    assert_eq!(Value::Map(sample_map()).to_string(), r#"{"a":1,"b":"two"}"#);
    assert_eq!(Value::Map(ValueMap::new()).to_string(), "{}");
    assert_eq!(ext(Value::I32(42)).to_string(), "42");

    let short = vec![1u8, 2, 3];
    assert_eq!(Value::Binary(short.clone()).to_string(), format!("{:?}", short));
    let long: Vec<u8> = (0u8..40).collect();
    assert_eq!(
        Value::Binary(long.clone()).to_string(),
        format!("{:?}", &long[0..32])
    );
}

#[test]
fn test_from_isize_and_vec_value_and_tuple() {
    assert_eq!(Value::from(42isize), Value::I64(42));
    assert_eq!(
        Value::from(vec![Value::I32(1), Value::I32(2)]),
        Value::Array(vec![Value::I32(1), Value::I32(2)])
    );
    assert_eq!(
        Value::from(("tag", Value::Bool(true))),
        Value::Ext("tag", Box::new(Value::Bool(true)))
    );
}

#[test]
fn test_into_vec_value() {
    let arr = Value::Array(vec![Value::I32(1), Value::I32(2)]);
    let into: Vec<Value> = arr.into();
    assert_eq!(into, vec![Value::I32(1), Value::I32(2)]);
    let empty: Vec<Value> = Value::I32(1).into();
    assert!(empty.is_empty());
}

#[test]
fn test_from_iterator_for_value() {
    let v: Value = vec![1i32, 2, 3].into_iter().collect();
    assert_eq!(
        v,
        Value::Array(vec![Value::I64(1), Value::I64(2), Value::I64(3)])
    );
    let v: Value = std::iter::empty::<i32>().collect();
    assert_eq!(v, Value::Array(vec![]));
}

#[test]
fn test_into_iterator_owned() {
    // Map
    let v = Value::Map(sample_map());
    let pairs: Vec<(Value, Value)> = v.into_iter().collect();
    assert_eq!(pairs.len(), 2);

    // Array
    let v = Value::Array(vec![Value::String("a".into()), Value::String("b".into())]);
    let pairs: Vec<(Value, Value)> = v.into_iter().collect();
    assert_eq!(pairs[0], (Value::U32(0), Value::String("a".into())));
    assert_eq!(pairs[1], (Value::U32(1), Value::String("b".into())));

    // Ext delegates to the inner value.
    let v = ext(Value::Array(vec![Value::I32(1)]));
    let pairs: Vec<(Value, Value)> = v.into_iter().collect();
    assert_eq!(pairs, vec![(Value::U32(0), Value::I32(1))]);

    // Scalars yield no entries.
    let pairs: Vec<(Value, Value)> = Value::I32(1).into_iter().collect();
    assert!(pairs.is_empty());
}

#[test]
fn test_into_iterator_borrowed() {
    // Map
    let v = Value::Map(sample_map());
    let pairs: Vec<(Value, &Value)> = (&v).into_iter().collect();
    assert_eq!(pairs.len(), 2);
    assert!(pairs.iter().all(|(_, val)| !val.is_null()));

    // Array
    let v = Value::Array(vec![Value::I32(1), Value::I32(2)]);
    let pairs: Vec<(Value, &Value)> = (&v).into_iter().collect();
    assert_eq!(pairs[1], (Value::U32(1), &Value::I32(2)));

    // Ext delegates.
    let v = ext(Value::Array(vec![Value::I32(9)]));
    let pairs: Vec<(Value, &Value)> = (&v).into_iter().collect();
    assert_eq!(pairs[0], (Value::U32(0), &Value::I32(9)));

    // Scalar
    let v = Value::Bool(true);
    let pairs: Vec<(Value, &Value)> = (&v).into_iter().collect();
    assert!(pairs.is_empty());
}

#[test]
fn test_into_iterator_double_ref() {
    let v = Value::Array(vec![Value::I32(1)]);
    let r = &v;
    let pairs: Vec<(Value, &Value)> = (&r).into_iter().collect();
    assert_eq!(pairs, vec![(Value::U32(0), &Value::I32(1))]);
}

#[test]
fn test_from_value_for_primitives() {
    assert!(bool::from(Value::Bool(true)));
    assert!(!bool::from(&Value::Bool(false)));
    assert!(!bool::from(Value::Null));

    assert_eq!(f64::from(Value::I32(2)), 2.0);
    assert_eq!(f64::from(&Value::F64(2.5)), 2.5);

    assert_eq!(i64::from(Value::U32(3)), 3);
    assert_eq!(i64::from(&Value::I64(4)), 4);

    assert_eq!(u64::from(Value::I32(5)), 5);
    assert_eq!(u64::from(&Value::U64(6)), 6);

    assert_eq!(String::from(Value::String("x".into())), "x");
    assert_eq!(String::from(&Value::String("y".into())), "y");
    assert_eq!(String::from(Value::Bool(true)), "");
}

fn hash_of(v: &Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn test_hash_all_variants() {
    let variants = vec![
        Value::Null,
        Value::Bool(true),
        Value::I32(1),
        Value::I64(2),
        Value::U32(3),
        Value::U64(4),
        Value::F32(5.5),
        Value::F64(6.5),
        Value::String("s".into()),
        Value::Binary(vec![1, 2]),
        Value::Array(vec![Value::I32(1), Value::String("x".into())]),
        Value::Map(sample_map()),
        ext(Value::I32(7)),
    ];
    for v in &variants {
        // Hashing should be deterministic for equal values.
        assert_eq!(hash_of(v), hash_of(&v.clone()));
    }
    // Different variants should generally hash differently (smoke check).
    assert_ne!(hash_of(&Value::Null), hash_of(&Value::Bool(false)));
}

#[test]
fn test_map_hash_uses_entries() {
    let mut a = ValueMap::new();
    a.insert(Value::String("k".into()), Value::I32(1));
    let mut b = ValueMap::new();
    b.insert(Value::String("k".into()), Value::I32(2));
    assert_ne!(hash_of(&Value::Map(a)), hash_of(&Value::Map(b)));
}

#[test]
fn test_default_and_debug() {
    assert_eq!(Value::default(), Value::Null);
    let dbg = format!("{:?}", Value::Array(vec![Value::I32(1)]));
    assert!(dbg.contains("Array"));
}
