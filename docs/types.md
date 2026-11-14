# RBDC Data Type and Table Field Mapping Rules

> This document is the authoritative reference for rbdc driver type mappings. Whenever you change the encoding/decoding behavior of any `driver.rs`, `src/types`, `TypeInfo`, or `Value`, you must update this document and the corresponding tests.
>
> Last updated: 2026-11-15

## 1. Mandatory Rules

1. **Arrays and Maps are always stored as JSON**
   - In rbdc, `rbs::Value::Array` and `rbs::Value::Map` represent JSON data, not native database arrays.
   - MySQL: `JSON`
   - PostgreSQL: `JSON`; the bound parameter is JSON, not a native PostgreSQL array
   - SQL Server: JSON text/binary; use `VARCHAR(MAX)` or `NVARCHAR(MAX)` for columns
   - SQLite/Turso: `TEXT`
   - DuckDB: `VARCHAR`
   - Native PostgreSQL array decoding is a compatibility feature and does not change the JSON binding semantics of `Value::Array`.

2. **The mapping for each `rbs::Value` variant must be fixed in every driver**
   - Do not dynamically switch binding types between `i32/i64`, `INT/BIGINT`, or `INT4/INT8/NUMERIC` based on numeric magnitude.
   - Choose a fixed type when it can represent the value losslessly; fail explicitly when it cannot.
   - Table field types, parameter `TypeInfo`, and encoding formats must be consistent with one another.

3. **Treat DDL types and protocol binding types separately**
   - `Driver::column_type(&Value)`: recommended table/column type.
   - `TypeInfo for Value`: database protocol parameter type, which may differ from the DDL name.
   - For example, the DDL types and Connector/C-bindable wire types for MySQL `Enum/Set/Geometry/Bit` differ.

4. **Never silently produce incorrect values**
   - Integer out-of-range values must not be truncated.
   - Invalid time, Decimal, UUID, or BIT values must not become `0`, an empty string, or NULL.
   - An unknown `Ext` must not be bound to NULL by default; it should produce an error, or be handled as text when the driver explicitly supports it.

5. **Extended type names must be stable**
   - Use PascalCase, such as `DateTime`, not `Datetime`.
   - `Ext` names are part of the external data protocol; renaming one requires a compatibility assessment.
   - `TypeInfo` enums/structs must support serde round trips; enums based on `rbs::Value::Ext` usually require a custom `Deserialize` implementation.

## 2. Authoritative Entry Points in the Source

| Item | Path |
|---|---|
| MySQL | `rbdc-mysql/src/driver.rs`, `rbdc-mysql/src/types/value.rs` |
| PostgreSQL | `rbdc-pg/src/driver.rs`, `rbdc-pg/src/types/value.rs`, `rbdc-pg/src/type_info.rs` |
| SQL Server | `rbdc-mssql/src/driver.rs`, `rbdc-mssql/src/encode.rs`, `rbdc-mssql/src/decode.rs` |
| SQLite | `rbdc-sqlite/src/driver.rs`, `rbdc-sqlite/src/type_info.rs`, `rbdc-sqlite/src/types/` |
| Turso/libSQL | `rbdc-turso/src/driver.rs`, `rbdc-turso/src/value.rs`, `rbdc-turso/src/types/` |
| DuckDB | `rbdc-duckdb/src/driver.rs`, `rbdc-duckdb/src/types/value.rs` |
| Value/serde | `rbs/src/value/mod.rs`, `rbs/src/value_serde/` |

## 3. Basic `Value` Mappings

The following table lists the recommended DDL column types; parameter binding must follow the corresponding `TypeInfo` and encoding implementation.

| `rbs::Value` | MySQL | PostgreSQL | SQL Server | SQLite | Turso | DuckDB |
|---|---|---|---|---|---|---|
| `Null` | `NULL` | `UNKNOWN` | `NULL` | `NULL` | `NULL` | `NULL` |
| `Bool` | `TINYINT` | `BOOL` | `BIT` | `BOOLEAN` | `INTEGER` | `BOOLEAN` |
| `I32` | `INT` | `INT4` | `INT` | `INTEGER` | `INTEGER` | `INTEGER` |
| `I64` | `BIGINT` | `INT8` | `BIGINT` | `INTEGER` | `INTEGER` | `BIGINT` |
| `U32` | `INT UNSIGNED` | `INT8` | `INT` | `INTEGER` | `INTEGER` | `UINTEGER` |
| `U64` | `BIGINT UNSIGNED` | `NUMERIC` | `BIGINT` | `INTEGER` | `INTEGER` | `UBIGINT` |
| `F32` | `FLOAT` | `FLOAT4` | `REAL` | `REAL` | `REAL` | `FLOAT` |
| `F64` | `DOUBLE` | `FLOAT8` | `FLOAT` | `REAL` | `REAL` | `DOUBLE` |
| `String` | `TEXT` | `VARCHAR` | `VARCHAR(MAX)` | `TEXT` | `TEXT` | `VARCHAR` |
| `Binary` | `BLOB` | `BYTEA` | `VARBINARY(MAX)` | `BLOB` | `BLOB` | `BLOB` |
| `Array` | `JSON` | `JSON` | `VARCHAR(MAX)` | `TEXT` | `TEXT` | `VARCHAR` |
| `Map` | `JSON` | `JSON` | `VARCHAR(MAX)` | `TEXT` | `TEXT` | `VARCHAR` |

### 3.1 Unsigned Integer Constraints

| Driver | Rule |
|---|---|
| MySQL | `U32 = INT UNSIGNED`, `U64 = BIGINT UNSIGNED`; the full range can be bound |
| PostgreSQL | No unsigned types: `U32 = INT8`, `U64 = NUMERIC` |
| SQL Server | `U32 -> i32`, `U64 -> i64`; out-of-range values produce an error |
| SQLite/Turso | Signed 64-bit INTEGER only; `U64 > i64::MAX` produces an error |
| DuckDB | `U32 = UINTEGER`, `U64 = UBIGINT` |

Do not use `as i32/as i64` for unchecked conversions.

## 4. Extended Type Semantics

### 4.1 Time, Timestamp, Decimal, and UUID

| `Ext` | Semantics |
|---|---|
| `Date` | Date string, for example `2024-01-15` |
| `Time` | Time string; the driver encodes/decodes it according to the database format |
| `DateTime` | A complete date and time with timezone semantics, or a database `DATETIME` value |
| `Timestamp` | **Unix epoch milliseconds (`i64`)**, not SQL Server `timestamp/rowversion` |
| `Decimal` | High-precision decimal string |
| `Uuid` | UUID string; converted to text or a native UUID according to driver capabilities |
| `Json` | JSON text; `Value::Array/Map` are the preferred structured JSON entry points |
| `Jsonb` | PostgreSQL JSONB extension entry point |

- SQL Server `DateTimeOffset` uses a separate `Ext("DateTimeOffset")`.
- PostgreSQL `DateTime` always maps to `TIMESTAMPTZ`; `Timestamp` maps to `TIMESTAMP`.
- Before encoding, MySQL `Timestamp` is converted to UTC date-time components; do not write local time directly as an epoch value.

### 4.2 JSON Decoding

- MySQL JSON columns decode to structured `Value`s.
- PostgreSQL `JSON/JSONB` decodes to structured `Value`s.
- SQLite automatically attempts to parse TEXT that has a JSON shape.
- Turso is controlled by `json_detect`, which is disabled by default.
- DuckDB attempts to parse VARCHAR text that has a JSON shape.
- SQL Server JSON is stored in ordinary string columns and does not undergo automatic JSON type detection.

## 5. MySQL

### 5.1 DDL and Binding Types

| `Ext` | DDL Type | Parameter Wire Type | Notes |
|---|---|---|---|
| `Uuid` | `TEXT` | `VarChar` | UUID text |
| `Decimal` | `DECIMAL` | `NEWDECIMAL` | Strict Decimal parsing |
| `Year` | `YEAR` | Two-byte `SHORT` | Accepts a number or numeric string |
| `Date` | `DATE` | `DATE` | MySQL binary date |
| `Time` | `TIME` | `TIME` | Supports microseconds |
| `Timestamp` | `TIMESTAMP` | `TIMESTAMP` | Input is `i64` epoch milliseconds |
| `DateTime` | `DATETIME` | `DATETIME` | Supports microseconds |
| `Json` | `JSON` | `JSON` | JSON text |
| `Enum` | `TEXT` | `VarChar` | Connector/C binds it as a string |
| `Set` | `TEXT` | `VarChar` | Comma-separated string |
| `Geometry` | `GEOMETRY` | `String` | For direct writes, the payload must be `4-byte little-endian SRID + WKB` |
| `Bit` | `BIT` | `BLOB` | Numeric input is converted to the shortest big-endian byte sequence |
| `Blob/TinyBlob/MediumBlob/LongBlob` | `BLOB` | `BLOB` | Binary |

### 5.2 Spatial Types

To write directly to a GEOMETRY column:

```rust
Value::Ext("Geometry", Box::new(Value::Binary(srid_and_wkb)))
```

- `srid_and_wkb`: a 4-byte little-endian SRID followed by WKB.
- When using SQL functions, you can pass pure WKB: `ST_GeomFromWKB(?)`.
- GEOMETRY results decode to `Value::Ext("Geometry", Value::Binary(...))`.
- Do not bind parameters using MySQL's unsupported wire type `0xff GEOMETRY`.

### 5.3 BIT

- `Value::Ext("Bit", U64(n))`: encoded as the shortest big-endian byte sequence.
- Reading a `BIT` result converts it to `Value::U64`.
- `BIT` parameters use the `BLOB` wire type; the MySQL `BIT` parameter type cannot be used directly.

## 6. PostgreSQL

### 6.1 DDL and Parameter Types

| `Ext` | PostgreSQL Type |
|---|---|
| `Uuid` | `UUID` |
| `Decimal` / `Numeric` | `NUMERIC` |
| `Date` | `DATE` |
| `Time` | `TIME` |
| `Timestamp` | `TIMESTAMP` |
| `DateTime` | `TIMESTAMPTZ` |
| `Bool` | `BOOL` |
| `Bytea` | `BYTEA` |
| `Char` | `CHAR` |
| `Name` | `NAME` |
| `Int2` | `INT2` |
| `Int4` | `INT4` |
| `Int8` | `INT8` |
| `Text` | `TEXT` |
| `Oid` | `OID` |
| `Json` | `JSON` |
| `Jsonb` | `JSONB` |
| `Point/Lseg/Path/Box/Polygon/Line/Circle` | Corresponding geometric type |
| `Cidr/Inet/Macaddr/Macaddr8` | Corresponding network type |
| `Bit/Varbit` | Corresponding bit-string type |
| `Interval` | `INTERVAL` |
| `Timetz` | `TIMETZ` |
| `Int4Range/NumRange/TsRange/TstzRange/DateRange/Int8Range` | Corresponding range type |
| `Money` | `MONEY` |
| `Jsonpath` | `JSONPATH` |
| `Hstore` | Extension type `hstore` |
| `TsVector` / `TsQuery` | Full-text search types |

### 6.2 Array Rules

- `Value::Array` / `Value::Map` are always bound as JSON parameters, usually corresponding to `JSON/JSONB`.
- Native PostgreSQL arrays are decoded only as a compatibility feature for database results.
- Current native array decoding supports only one-dimensional arrays, and the lower bound must be 1.
- Do not treat `Value::Array` as a native PostgreSQL array parameter.
- The type layer has a `Vec<T>` native array codec, but the formal semantics of dynamic `Value` binding remain JSON.

### 6.3 range, enum, domain, composite, and hstore

- Range results are decoded from text results into `Value::Ext("XxxRange", Value::String(...))`.
- Custom enums decode to `Value::String`.
- Enum arrays decode to `Value::Array` with string elements.
- Domains are usually decoded according to their underlying type.
- Composite results may be `Value::Ext("Custom", Value::String(...))`.
- Small, untyped custom arrays are parsed as generic PostgreSQL text arrays when their type OID cannot be resolved.
- `Hstore` outputs `Value::Ext("Hstore", Value::Map(...))`; input supports a Map, a `key=>value` string, or binary.

### 6.4 Internal OID Rules

Internal queries against `pg_type/pg_range/pg_enum` must use:

```rust
Value::Ext("Oid", Box::new(Value::U32(oid)))
```

Do not use `oid.0.into()`, because the public binding rule for `U32` is `INT8`, which causes a type error when compared with PostgreSQL `oid`.

### 6.5 Text Result Format

The following types request text results from PostgreSQL to avoid exposing unparsable protocol binary data:

- `tsvector`
- `tsquery`
- range
- enum/composite
- hstore and hstore arrays
- range/custom arrays

Ordinary scalars and built-in arrays continue to use the binary result format.

## 7. SQL Server

| `Ext` | DDL Type | Binding/Notes |
|---|---|---|
| `Date` | `DATE` | `chrono::NaiveDate` |
| `DateTime` | `DATETIME2` | `chrono::NaiveDateTime` |
| `Time` | `TIME` | `chrono::NaiveTime` |
| `DateTimeOffset` | `DATETIMEOFFSET` | `chrono::DateTime<FixedOffset>` |
| `Decimal` | `DECIMAL` | `BigDecimal` |
| `Timestamp` | `BIGINT` | Epoch milliseconds, not ROWVERSION |
| `Uuid` | `UNIQUEIDENTIFIER` | Invalid UUIDs produce an error |
| `Json/Jsonb` | `NVARCHAR(MAX)` | JSON text |
| `Xml` | `XML` | XML text |
| `Geometry` | `GEOMETRY` | WKB bytes |
| `Geography` | `GEOGRAPHY` | WKB bytes |

### Spatial Type Binding

SQL Server recommends binding WKB through functions:

```sql
geometry::STGeomFromWKB(@P1, 4326)
geography::STGeomFromWKB(@P1, 4326)
```

`Value::Ext("Geometry"/"Geography", Value::Binary(wkb))` binds as `VARBINARY`. Do not assume that SQL Server's internal CLR spatial serialization format is the same as WKB.

## 8. SQLite

SQLite uses dynamic storage types; `column_type` is the recommended declared type, while actual decoding is based on the storage class.

| `Ext` | DDL Type | Storage/Decoding |
|---|---|---|
| `Date` | `TEXT` | Text |
| `DateTime` | `TEXT` | Text |
| `Time` | `TEXT` | Text |
| `Timestamp` | `INTEGER` | epoch milliseconds as `i64` |
| `Decimal` | `NUMERIC` | Text representation, decoded as a string |
| `Json` | `TEXT` | JSON text |
| `Uuid` | `TEXT` | UUID text |

Rules:

- `Array/Map` -> JSON TEXT.
- Actual INTEGER -> `Value::I64`.
- Actual REAL -> `Value::F64`.
- Actual TEXT -> `Value::String`; text with a JSON shape is automatically parsed into a structured Value.
- Actual BLOB -> `Value::Binary`.
- Encoding reports an error when `U64 > i64::MAX`.
- Unknown `Ext` values must not be bound as NULL.

## 9. Turso/libSQL

Turso is based on libSQL and has rules similar to SQLite, but JSON detection is disabled by default.

| `Ext` | DDL Type | Storage/Decoding |
|---|---|---|
| `Date/DateTime/Time` | `TEXT` | Text |
| `Timestamp` | `INTEGER` | epoch milliseconds as `i64` |
| `Decimal` | `TEXT` | Text |
| `Json` | `TEXT` | JSON text as-is |
| `Uuid` | `TEXT` | UUID text |

Rules:

- `Array/Map` -> JSON TEXT.
- `Ext("Json", String)` -> TEXT without adding extra quotes.
- `Ext("Json", Binary)` -> BLOB; String or Array/Map is recommended for ordinary JSON.
- When `json_detect=true`, TEXT with a JSON shape is decoded into a structured Value; this is disabled by default.
- `U64 > i64::MAX` produces an error.
- Both local in-memory and local file databases use local mode and do not require Docker.

## 10. DuckDB

| `Ext` | DDL Type | Binding/Decoding |
|---|---|---|
| `Date` | `DATE` | `Value::Ext("Date", String)` |
| `DateTime` | `TIMESTAMP` | Binds timestamp/timestamptz according to the parameter type |
| `Time` | `TIME` | Time |
| `Timestamp` | `BIGINT` | epoch milliseconds |
| `Decimal` | `DECIMAL` | Decimal string |
| `Json` | `VARCHAR` | JSON text |
| `Uuid` | `UUID` | 32-character hexadecimal UUID converted to `u128` |
| `Bit` | `BIT` | DuckDB padding bitstring format |
| `Enum` | Database enum | Bound by enum dictionary index |
| `HugeInt` | `HUGEINT` | `i128` |
| `UHugeInt` | `UHUGEINT` | `u128` |

Rules:

- `Array/Map` -> JSON VARCHAR.
- UUID results decode to `Value::Ext("Uuid", String)`.
- BIT results decode to `Value::Ext("Bit", String)`, for example `"101"`.
- `TIMESTAMPTZ` decodes to `Value::Ext("DateTime", String)`.
- `Ext("TimestampTz")` is a compatibility entry point and uses `DateTime` internally.
- DuckDB LIST/STRUCT/MAP/ARRAY/UNION decoding depends on the C API version; it may produce a structured Value, a string, or NULL, and full consistency across versions cannot be guaranteed.

## 11. Key Result Decoding Mappings

| Database Result | RBDC Value |
|---|---|
| MySQL unsigned TINY/SHORT/INT | `U32` |
| MySQL unsigned BIGINT | `U64` |
| MySQL BIT | `U64` |
| MySQL YEAR | `Ext("Year", String)` |
| PostgreSQL INT2 | `I32` |
| PostgreSQL INT4 | `I32` |
| PostgreSQL INT8 | `I64` |
| PostgreSQL OID | `Ext("Oid", U32)` |
| PostgreSQL JSON/JSONB | Structured Value |
| PostgreSQL range | `Ext("XxxRange", String)` |
| PostgreSQL enum | `String` |
| PostgreSQL hstore | `Ext("Hstore", Map)` |
| SQL Server UNIQUEIDENTIFIER | `Ext("Uuid", String)` |
| SQL Server XML | `Ext("Xml", String)` |
| SQL Server DATETIME/DATETIME2/DATETIMEOFFSET | `Ext("DateTime", String)` |
| SQLite/Turso INTEGER | `I64` |
| SQLite/Turso REAL | `F64` |
| SQLite/Turso TEXT | `String` or a structured JSON value |
| SQLite/Turso BLOB | `Binary` |

## 12. Checklist for Changing Type Mappings

1. Consult the official database protocol/driver source to confirm the wire type, byte order, length prefix, and format.
2. First define a fixed `Value -> SQL` mapping; do not change the target SQL type based on numeric magnitude.
3. Update all of the following together:
   - `Driver::column_type`
   - `TypeInfo for Value`
   - `Encode`
   - `Decode`
   - `Ext` branches
   - serde `Deserialize` (if required)
4. Add tests:
   - Minimum and maximum values, and out-of-range errors
   - NULL
   - Empty values/empty arrays/empty strings
   - Round trips against a real database
   - `TypeInfo` serde round trip
5. Verify against real databases using Docker or local instances:
   - MySQL, PostgreSQL, SQL Server: Docker
   - SQLite, Turso, DuckDB: test directly against local databases
6. Update `docs/types.md`.
7. Increment the third version component in the `Cargo.toml` of each affected crate.
8. Run the full suite: `cargo test --workspace -- --test-threads=1`.
9. Temporary `example` test code must be reverted before committing.

## 13. Historical Mistakes That Must Not Be Repeated

- Do not use `Datetime` as the canonical name; always use `DateTime`.
- Do not let the same `U32/U64` drift among `INT4/INT8/NUMERIC` based on value magnitude.
- Do not silently downgrade an out-of-range `u64` to a string.
- Do not silently write invalid Decimal/Year/UUID/Timestamp values as 0, an empty string, or NULL.
- Do not silently bind an unknown `Ext` as NULL.
- Do not use `oid.0.into()` to handle PostgreSQL OID parameters.
- Do not change `Value::Array/Map` to native database array binding; they must retain JSON semantics.
- Do not declare MySQL GEOMETRY parameters using an unbindable wire type.
- Do not treat SQL Server WKB as the internal CLR spatial serialization format.
- Do not change only the binding and omit decoding, DDL, serde, or tests.