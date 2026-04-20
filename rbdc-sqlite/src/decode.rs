use crate::SqliteValueRef;
use rbdc::Error;

pub trait Decode {
    fn decode(value: SqliteValueRef) -> Result<Self, Error>
    where
        Self: Sized;
}
