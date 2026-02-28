# Turso Adapter: Candidate Deviations from SQLite Parity

Status: **requires governance decision** (input for WP05)

## DEV-001: column_type reports runtime value type, not declared schema type

- **Scenario**: PAR-008 (metadata_columns)
- **SQLite behavior**: `MetaData::column_type()` returns the *declared* column type
  from the schema (e.g. "INTEGER", "TEXT", "REAL", "BLOB") via `SqliteColumn::type_info`,
  which is populated at statement compilation time from the schema.
- **Turso behavior**: `column_type()` returns the type of the *first row's values*
  as reported by `libsql::Rows::column_type()`. For empty result sets, returns "NULL"
  since there are no values to infer from.
- **User impact**: Code that inspects column types on empty result sets will see "NULL"
  instead of the declared type. Code that relies on exact type string matching
  (e.g. "BOOLEAN", "DATETIME") will see only the 5 base types (INTEGER, REAL, TEXT,
  BLOB, NULL) since Turso does not expose declared type aliases.
- **Linked tests**: PAR-008, PAR-014
- **Recommendation**: Accept deviation. The rbdc `MetaData` trait contract does not
  specify whether declared or runtime types are returned. Consumers should not depend
  on specific type strings beyond the 5 SQLite storage classes.

## DEV-002: Boolean values round-trip as integers, not bools

- **Scenario**: PAR-007 (bool_parameter_binding)
- **SQLite behavior**: `Value::Bool(true)` is encoded as INTEGER 1, read back as
  `Value::I64(1)` (not `Value::Bool(true)`). The SQLite adapter only produces
  `Value::Bool` when the *declared* column type is BOOLEAN and the `DataType::Bool`
  path is hit during decode.
- **Turso behavior**: Same as SQLite. `Value::Bool` → INTEGER → `Value::I64`.
  This is actually parity, not a deviation, but noted here because it is a common
  source of confusion.
- **User impact**: None. Both adapters behave identically.
- **Linked tests**: PAR-007
- **Recommendation**: Not a deviation. Document for awareness.

## DEV-003: JSON text decoding heuristic may differ on malformed JSON

- **Scenario**: PAR-006 (json_text_decoding)
- **SQLite behavior**: Text values matching `is_json_string()` (starts with `{`/`[`
  or equals `"null"`) are attempted as JSON parse. If parse fails, returned as String.
- **Turso behavior**: Identical heuristic (`is_json_string` ported from SQLite adapter).
- **User impact**: None for well-formed data. For edge cases like `{not json}`, both
  adapters will return it as a String (parse fails, fallback). This is parity.
- **Linked tests**: PAR-006 (json_object, json_array, json_null, invalid JSON fallback)
- **Recommendation**: Not a deviation. Parity confirmed.

## DEV-004: last_insert_id type is Value::U64, not Value::I64

- **Scenario**: PAR-010, PAR-011
- **SQLite behavior**: The SQLite adapter's `ExecResult.last_insert_id` value depends
  on the connection module implementation. The raw `last_insert_rowid()` returns `i64`.
- **Turso behavior**: `ExecResult.last_insert_id` is stored as `Value::U64` (cast from
  `i64`). This matches the WP02 connection implementation.
- **User impact**: Code that pattern-matches on `Value::I64` for last_insert_id will
  not match. However, `ExecResult.last_insert_id` is typed as `Value` (not `i64`),
  so consumers should handle both variants.
- **Linked tests**: PAR-010
- **Recommendation**: Requires governance decision. Consider standardizing on `Value::I64`
  across all adapters for consistency, or document that the type varies.
