//! Coverage for the `Deserialize for Value` visitor: sequence/map visitors with
//! and without size hints, plus the unsupported-type error path.

use rbs::value::map::ValueMap;
use rbs::Value;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, forward_to_deserialize_any};

enum Action {
    SeqNone,
    SeqSome(usize),
    MapNone,
    MapSome(usize),
    Unsupported,
}

struct ActionDe(Action);

struct TestSeq {
    hint: Option<usize>,
    items: std::vec::IntoIter<u32>,
}

impl<'de> SeqAccess<'de> for TestSeq {
    type Error = rbs::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.items.next() {
            Some(v) => seed
                .deserialize(serde::de::value::U32Deserializer::<rbs::Error>::new(v))
                .map(Some),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        self.hint
    }
}

// A map access that keeps both key and value so `next_value_seed` can yield it.
struct TestMap2 {
    hint: Option<usize>,
    entries: std::vec::IntoIter<(u32, u32)>,
    pending: Option<u32>,
}

impl<'de> MapAccess<'de> for TestMap2 {
    type Error = rbs::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.entries.next() {
            Some((k, v)) => {
                self.pending = Some(v);
                seed.deserialize(serde::de::value::U32Deserializer::<rbs::Error>::new(k))
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let v = self.pending.take().expect("value requested before key");
        seed.deserialize(serde::de::value::U32Deserializer::<rbs::Error>::new(v))
    }

    fn size_hint(&self) -> Option<usize> {
        self.hint
    }
}

impl<'de> Deserializer<'de> for ActionDe {
    type Error = rbs::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Action::SeqNone => visitor.visit_seq(TestSeq {
                hint: None,
                items: vec![1, 2].into_iter(),
            }),
            Action::SeqSome(hint) => visitor.visit_seq(TestSeq {
                hint: Some(hint),
                items: vec![3].into_iter(),
            }),
            Action::MapNone => visitor.visit_map(TestMap2 {
                hint: None,
                entries: vec![(1, 2)].into_iter(),
                pending: None,
            }),
            Action::MapSome(hint) => visitor.visit_map(TestMap2 {
                hint: Some(hint),
                entries: vec![(3, 4)].into_iter(),
                pending: None,
            }),
            // `ValueVisitor` does not implement `visit_u128`, so the default
            // `Visitor::visit_u128` reports an invalid type and formats
            // `ValueVisitor::expecting`.
            Action::Unsupported => visitor.visit_u128(9),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

#[test]
fn value_visitor_seq_without_size_hint() {
    let v: Value = Deserialize::deserialize(ActionDe(Action::SeqNone)).unwrap();
    assert_eq!(v, Value::Array(vec![Value::U32(1), Value::U32(2)]));
}

#[test]
fn value_visitor_seq_with_size_hint() {
    let v: Value = Deserialize::deserialize(ActionDe(Action::SeqSome(8))).unwrap();
    assert_eq!(v, Value::Array(vec![Value::U32(3)]));
}

#[test]
fn value_visitor_map_without_size_hint() {
    let v: Value = Deserialize::deserialize(ActionDe(Action::MapNone)).unwrap();
    let mut expected = ValueMap::new();
    expected.insert(Value::U32(1), Value::U32(2));
    assert_eq!(v, Value::Map(expected));
}

#[test]
fn value_visitor_map_with_size_hint() {
    let v: Value = Deserialize::deserialize(ActionDe(Action::MapSome(8))).unwrap();
    let mut expected = ValueMap::new();
    expected.insert(Value::U32(3), Value::U32(4));
    assert_eq!(v, Value::Map(expected));
}

#[test]
fn value_visitor_reports_expectation_for_unsupported_type() {
    let err = Deserialize::deserialize(ActionDe(Action::Unsupported))
        .map(|_: Value| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("any valid MessagePack value"),
        "unexpected error: {err}"
    );
}
