use rbs::value::map::ValueMap;
use rbs::{from_value, from_value_ref, value, value_def, Value};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Serialization fixtures
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct UnitStruct;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct NewtypeStruct(i32);

#[derive(Serialize)]
struct TupleStruct(i8, i16, u8, u16, char);

#[derive(Serialize)]
struct Plain {
    a: i32,
    b: String,
}

struct RawBytes;

impl Serialize for RawBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&[1u8, 2, 3])
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum TestEnum {
    Unit,
    New(i32),
    Tup(i32, i32),
    Struct { x: i32 },
}

// ---------------------------------------------------------------------------
// Deserialization fixtures
// ---------------------------------------------------------------------------

enum Action {
    Unit,
    Some,
    Newtype,
    ByteBuf,
}

struct ActionDe {
    action: Action,
}

impl<'de> serde::Deserializer<'de> for ActionDe {
    type Error = rbs::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.action {
            Action::Unit => visitor.visit_unit(),
            Action::Some => {
                let de: serde::de::value::U32Deserializer<rbs::Error> = 7u32.into_deserializer();
                visitor.visit_some(de)
            }
            Action::Newtype => {
                let de: serde::de::value::U32Deserializer<rbs::Error> = 7u32.into_deserializer();
                visitor.visit_newtype_struct(de)
            }
            Action::ByteBuf => visitor.visit_byte_buf(vec![1, 2, 3]),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// Deserializes only the first entry of a map, so that a `MapDeserializer`
/// with leftover entries reports an invalid length.
#[derive(Debug)]
struct FirstEntry;

impl<'de> Deserialize<'de> for FirstEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FirstEntryVisitor;

        impl<'de> serde::de::Visitor<'de> for FirstEntryVisitor {
            type Value = FirstEntry;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let _: Option<(String, Value)> = map.next_entry()?;
                Ok(FirstEntry)
            }
        }

        deserializer.deserialize_map(FirstEntryVisitor)
    }
}

fn sample_map() -> ValueMap {
    let mut m = ValueMap::new();
    m.insert(Value::String("a".into()), Value::I32(1));
    m
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

#[test]
fn test_serialize_integer_and_char_widths() {
    assert_eq!(value(1i8).unwrap(), Value::I32(1));
    assert_eq!(value(1i16).unwrap(), Value::I32(1));
    assert_eq!(value(1i32).unwrap(), Value::I32(1));
    assert_eq!(value(1i64).unwrap(), Value::I64(1));
    assert_eq!(value(1u8).unwrap(), Value::U32(1));
    assert_eq!(value(1u16).unwrap(), Value::U32(1));
    assert_eq!(value(1u32).unwrap(), Value::U32(1));
    assert_eq!(value(1u64).unwrap(), Value::U64(1));
    assert_eq!(value(1f32).unwrap(), Value::F32(1.0));
    assert_eq!(value(1f64).unwrap(), Value::F64(1.0));
    assert_eq!(value('z').unwrap(), Value::String("z".into()));
    assert_eq!(value(true).unwrap(), Value::Bool(true));
}

#[test]
fn test_serialize_bytes_unit_and_option() {
    assert_eq!(value(RawBytes).unwrap(), Value::Binary(vec![1, 2, 3]));
    assert_eq!(value(()).unwrap(), Value::Null);
    assert_eq!(value(UnitStruct).unwrap(), Value::Array(vec![]));
    assert_eq!(value(Option::<i32>::None).unwrap(), Value::Null);
    assert_eq!(value(Some(3i32)).unwrap(), Value::I32(3));
}

#[test]
fn test_serialize_newtype_and_tuple_structs() {
    let newtype = value(NewtypeStruct(5)).unwrap();
    assert_eq!(
        newtype,
        Value::Ext("NewtypeStruct", Box::new(Value::I32(5)))
    );

    let tuple_struct = value(TupleStruct(1, 2, 3, 4, 'x')).unwrap();
    match tuple_struct {
        Value::Ext(name, inner) => {
            assert_eq!(name, "TupleStruct");
            assert_eq!(
                *inner,
                Value::Array(vec![
                    Value::I32(1),
                    Value::I32(2),
                    Value::U32(3),
                    Value::U32(4),
                    Value::String("x".into()),
                ])
            );
        }
        other => panic!("expected Ext, got {:?}", other),
    }
}

#[test]
fn test_serialize_tuple_and_struct() {
    let tuple = value((1i32, 2i32)).unwrap();
    assert_eq!(tuple, Value::Array(vec![Value::I32(1), Value::I32(2)]));

    let plain = value(Plain {
        a: 1,
        b: "s".into(),
    })
    .unwrap();
    let map = plain.as_map().expect("struct should serialize to map");
    assert_eq!(map["a"], Value::I32(1));
    assert_eq!(map["b"], Value::String("s".into()));
}

#[test]
fn test_serialize_enum_variants() {
    assert_eq!(
        value(TestEnum::Unit).unwrap(),
        Value::String("Unit".into())
    );

    let newtype = value(TestEnum::New(7)).unwrap();
    match newtype {
        Value::Map(m) => {
            assert_eq!(m["New"], Value::I32(7));
        }
        other => panic!("expected map, got {:?}", other),
    }

    let err = value(TestEnum::Tup(1, 2)).unwrap_err();
    assert!(err.to_string().contains("serialize_tuple_variant"));

    let err = value(TestEnum::Struct { x: 1 }).unwrap_err();
    assert!(err.to_string().contains("serialize_struct_variant"));
}

#[test]
fn test_value_def() {
    assert_eq!(value_def(5i32), Value::I32(5));
    // Unsupported tuple-variant serialization falls back to Null.
    assert_eq!(value_def(TestEnum::Tup(1, 2)), Value::Null);
}

#[test]
fn test_value_shortcut_paths() {
    let v = Value::Map(sample_map());
    // T = Value (moves out)
    assert_eq!(value(v.clone()).unwrap(), v);
    // T = &Value
    assert_eq!(value(&v).unwrap(), v);
    // T = &&Value
    let r = &v;
    assert_eq!(value(&r).unwrap(), v);
}

#[test]
fn test_serialize_value_via_value_map() {
    // Serializing a ValueMap routes every entry through `Serialize for Value`,
    // covering all Value variants.
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
        Value::Array(vec![Value::I32(1)]),
        Value::Map(sample_map()),
        Value::Ext("T", Box::new(Value::I32(9))),
    ];
    let mut m = ValueMap::new();
    for (i, v) in variants.iter().enumerate() {
        m.insert(Value::String(format!("k{}", i)), v.clone());
    }
    let out = value(&m).unwrap();
    let map = out.as_map().expect("map expected");
    assert_eq!(map.len(), variants.len());
    assert_eq!(map["k0"], Value::Null);
    assert_eq!(map["k10"], Value::Array(vec![Value::I32(1)]));
    assert_eq!(
        map["k12"],
        Value::Ext("T", Box::new(Value::I32(9)))
    );
}

// ---------------------------------------------------------------------------
// Deserialization
// ---------------------------------------------------------------------------

#[test]
fn test_deserialize_primitives_from_value() {
    assert!(from_value::<bool>(Value::Bool(true)).unwrap());
    assert_eq!(from_value::<i32>(Value::I32(4)).unwrap(), 4);
    assert_eq!(from_value::<i64>(Value::I64(-1)).unwrap(), -1);
    assert_eq!(from_value::<u32>(Value::U32(3)).unwrap(), 3);
    assert_eq!(from_value::<u64>(Value::U64(2)).unwrap(), 2);
    assert_eq!(from_value::<f32>(Value::F32(2.5)).unwrap(), 2.5);
    assert_eq!(from_value::<f64>(Value::F64(1.5)).unwrap(), 1.5);
    assert_eq!(
        from_value::<String>(Value::String("s".into())).unwrap(),
        "s"
    );
    assert_eq!(from_value::<char>(Value::String("x".into())).unwrap(), 'x');
    assert_eq!(
        from_value::<Vec<i32>>(Value::Array(vec![
            Value::I32(1),
            Value::I32(2),
            Value::I32(3)
        ]))
        .unwrap(),
        vec![1, 2, 3]
    );
}

#[test]
fn test_from_value_ref_paths() {
    assert_eq!(from_value_ref::<i32>(&Value::I32(1)).unwrap(), 1);
    assert_eq!(
        from_value_ref::<Vec<Value>>(&Value::Array(vec![Value::Null])).unwrap(),
        vec![Value::Null]
    );
}

#[test]
fn test_option_paths() {
    assert_eq!(
        from_value::<Option<Value>>(Value::I32(1)).unwrap(),
        Some(Value::I32(1))
    );
    assert_eq!(from_value::<Option<Value>>(Value::Null).unwrap(), None);
    assert_eq!(from_value::<Option<i32>>(Value::Null).unwrap(), None);
}

#[test]
fn test_deserialize_value_visitor_all_variants() {
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
        Value::Ext("T", Box::new(Value::I32(9))),
    ];
    for v in variants {
        let decoded: Value = from_value(v.clone()).unwrap();
        if v.is_ext() {
            // The Ext tag is transparent during deserialization.
            assert_eq!(decoded, Value::I32(9));
        } else {
            assert_eq!(decoded, v);
        }
    }
}

#[test]
fn test_value_deserialize_visitor_special_methods() {
    let v: Value = Deserialize::deserialize(ActionDe {
        action: Action::Unit,
    })
    .unwrap();
    assert_eq!(v, Value::Null);

    let v: Value = Deserialize::deserialize(ActionDe {
        action: Action::Some,
    })
    .unwrap();
    assert_eq!(v, Value::U32(7));

    let v: Value = Deserialize::deserialize(ActionDe {
        action: Action::Newtype,
    })
    .unwrap();
    assert_eq!(v, Value::U32(7));

    let v: Value = Deserialize::deserialize(ActionDe {
        action: Action::ByteBuf,
    })
    .unwrap();
    assert_eq!(v, Value::Binary(vec![1, 2, 3]));
}

#[test]
fn test_unit_struct_deserialization() {
    assert_eq!(from_value::<UnitStruct>(Value::Null).unwrap(), UnitStruct);
    let err = from_value::<UnitStruct>(Value::Array(vec![Value::I32(1)])).unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn test_newtype_struct_deserialization() {
    assert_eq!(
        from_value::<NewtypeStruct>(Value::I32(5)).unwrap(),
        NewtypeStruct(5)
    );
}

#[test]
fn test_sequence_invalid_length() {
    let err = from_value::<[i32; 2]>(Value::Array(vec![
        Value::I32(1),
        Value::I32(2),
        Value::I32(3),
    ]))
    .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn test_map_invalid_length() {
    let mut m = ValueMap::new();
    m.insert(Value::String("a".into()), Value::I32(1));
    m.insert(Value::String("b".into()), Value::I32(2));
    let err = from_value::<FirstEntry>(Value::Map(m)).unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn test_map_value_error_appends_key() {
    let mut m = ValueMap::new();
    m.insert(
        Value::String("bad".into()),
        Value::String("not-a-number".into()),
    );
    let err = from_value::<HashMap<String, i32>>(Value::Map(m)).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("bad"), "unexpected message: {}", msg);
}

#[test]
fn test_enum_unit_variant() {
    assert_eq!(
        from_value::<TestEnum>(Value::String("Unit".into())).unwrap(),
        TestEnum::Unit
    );

    let mut m = ValueMap::new();
    m.insert(Value::String("Unit".into()), Value::Null);
    assert_eq!(
        from_value::<TestEnum>(Value::Map(m)).unwrap(),
        TestEnum::Unit
    );
}

#[test]
fn test_enum_newtype_variant() {
    let mut m = ValueMap::new();
    m.insert(Value::String("New".into()), Value::I32(7));
    assert_eq!(
        from_value::<TestEnum>(Value::Map(m)).unwrap(),
        TestEnum::New(7)
    );
}

#[test]
fn test_enum_newtype_variant_errors() {
    // Variant value is not a map at all.
    let err = from_value::<TestEnum>(Value::String("New".into())).unwrap_err();
    assert!(err.to_string().contains("newtype_variant"));

    // Variant value maps to an empty map.
    let mut outer = ValueMap::new();
    outer.insert(Value::String("New".into()), Value::Map(ValueMap::new()));
    let err = from_value::<TestEnum>(Value::Map(outer)).unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn test_enum_invalid_inputs() {
    // Empty map.
    let err = from_value::<TestEnum>(Value::Map(ValueMap::new())).unwrap_err();
    assert!(!err.to_string().is_empty());

    // Non string / non map input.
    let err = from_value::<TestEnum>(Value::I32(1)).unwrap_err();
    assert!(!err.to_string().is_empty());

    // Tuple variants are not implemented.
    let mut m = ValueMap::new();
    m.insert(
        Value::String("Tup".into()),
        Value::Array(vec![Value::I32(1), Value::I32(2)]),
    );
    let err = from_value::<TestEnum>(Value::Map(m)).unwrap_err();
    assert!(err.to_string().contains("tuple_variant"));

    // Struct variants are not implemented.
    let mut inner = ValueMap::new();
    inner.insert(Value::String("x".into()), Value::I32(1));
    let mut m = ValueMap::new();
    m.insert(Value::String("Struct".into()), Value::Map(inner));
    let err = from_value::<TestEnum>(Value::Map(m)).unwrap_err();
    assert!(err.to_string().contains("struct_variant"));
}

#[test]
fn test_deserialize_value_map() {
    let m = sample_map();
    let decoded: ValueMap = from_value(Value::Map(m.clone())).unwrap();
    assert_eq!(decoded, m);
}
