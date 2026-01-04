use crate::{DateTime, Error};
use rbs::Value;
use serde::Deserializer;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

/// Timestamp wrapper around DateTime
#[derive(serde::Serialize, Clone, Eq, PartialEq, Hash)]
#[serde(rename = "Timestamp")]
pub struct Timestamp(pub DateTime);

impl Timestamp {
    #[deprecated(note = "please use utc()")]
    pub fn now() -> Self {
        Self(DateTime::utc())
    }
    /// utc time
    pub fn utc() -> Self {
        Self(DateTime::utc())
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        match Value::deserialize(deserializer) {
            Ok(v) => {
                // Try to extract timestamp from Value
                match &v {
                    Value::I64(i) => Ok(Timestamp(DateTime::from_timestamp_millis(*i))),
                    Value::U64(u) => Ok(Timestamp(DateTime::from_timestamp_millis(*u as i64))),
                    Value::I32(i) => Ok(Timestamp(DateTime::from_timestamp_millis(*i as i64))),
                    Value::U32(u) => Ok(Timestamp(DateTime::from_timestamp_millis(*u as i64))),
                    Value::Ext("Timestamp", inner) => {
                        match inner.as_i64() {
                            Some(i) => Ok(Timestamp(DateTime::from_timestamp_millis(i))),
                            None => Err(Error::custom("warn type decode Json")),
                        }
                    }
                    _ => Err(Error::custom("warn type decode Json")),
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timestamp({})", self.0)
    }
}

impl From<Timestamp> for Value {
    fn from(arg: Timestamp) -> Self {
        Value::Ext("Timestamp", Box::new(Value::I64(arg.0.unix_timestamp_millis())))
    }
}

impl FromStr for Timestamp {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Timestamp(DateTime::from_str(s)?))
    }
}

impl From<Timestamp> for fastdate::DateTime {
    fn from(value: Timestamp) -> Self {
        value.0 .0
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp(DateTime::default())
    }
}

impl From<DateTime> for Timestamp {
    fn from(value: DateTime) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for DateTime {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

#[cfg(test)]
mod test {
    use crate::DateTime;
    use crate::timestamp::Timestamp;
    use rbs::Value;

    #[test]
    fn test_from_timestamp() {
        let v = Timestamp::utc();
        let dt: DateTime = v.into();
        println!("{}", dt);
    }

    #[test]
    fn test_ser_de() {
        let dt = Timestamp::utc();
        let v = serde_json::to_value(&dt).unwrap();
        let new_dt: Timestamp = serde_json::from_value(v).unwrap();
        assert_eq!(new_dt, dt);
    }

    #[test]
    fn test_decode_timestamp_u64() {
        let result: Timestamp = rbs::from_value(Value::U64(1)).unwrap();
        assert_eq!(result.0.unix_timestamp_millis(), 1);
    }

    #[test]
    fn test_decode_timestamp_ext() {
        let result: Timestamp = rbs::from_value(Value::Ext("Timestamp", Box::new(Value::U64(1)))).unwrap();
        assert_eq!(result.0.unix_timestamp_millis(), 1);
    }
}
