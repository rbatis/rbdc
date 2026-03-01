//! Type conversion helpers for the Turso/libSQL adapter.
//!
//! These modules provide value encoding/decoding helpers that normalize
//! Turso data values into the same `rbs::Value` contract outputs as the
//! SQLite adapter, ensuring behavioral parity at the public API level.
//!
//! # Type Mapping
//!
//! | Rust/rbs type        | libsql wire type   | SQLite equivalent |
//! |----------------------|--------------------|-------------------|
//! | `Value::Null`        | `Null`             | NULL              |
//! | `Value::Bool`        | `Integer(0/1)`     | INTEGER (BOOLEAN) |
//! | `Value::I32/I64`     | `Integer`          | INTEGER           |
//! | `Value::U32/U64`     | `Integer`          | INTEGER           |
//! | `Value::F32/F64`     | `Real`             | REAL              |
//! | `Value::String`      | `Text`             | TEXT              |
//! | `Value::Binary`      | `Blob`             | BLOB              |
//! | `Value::Ext("Date")` | `Text`             | TEXT              |
//! | `Value::Array/Map`   | `Text` (JSON)      | TEXT (JSON)       |

pub mod bool;
pub mod bytes;
pub mod float;
pub mod int;
pub mod null;
pub mod str;
