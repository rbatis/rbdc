//! Focused coverage tests for rbdc-turso's public helper surface.
//!
//! Unlike the behavioral integration suites, these tests call each public
//! helper/conversion directly so that small utility functions (type encoders,
//! value conversions, option builders, driver type mapping, error helpers,
//! statement/query-result helpers and column metadata) are exercised even if
//! only through their narrow API surface.
//!
//! All tests use in-memory libSQL or pure value conversion and are
//! deterministic, offline, and fast.

use futures_core::future::BoxFuture;
use futures_util::StreamExt;
use rbdc::db::{ConnectOptions, Connection, Driver};
use rbdc_turso::column::TursoColumn;
use rbdc_turso::error::TursoError;
use rbdc_turso::query_result::TursoQueryResult;
use rbdc_turso::statement::TursoStatement;
use rbdc_turso::types::{bool as tbool, bytes, float, int, null, str as tstr};
use rbdc_turso::value::{
    is_json_string, libsql_to_value, turso_value_to_rbs, value_to_libsql, TursoDataType, TursoValue,
};
use rbdc_turso::{TursoConnectOptions, TursoDriver};
use rbs::Value;

// ──────────────────────────────────────────────────────────────────────
// types/* encode/decode helpers
// ──────────────────────────────────────────────────────────────────────

#[test]
fn types_bool_helpers() {
    assert_eq!(tbool::decode_bool(0), Value::Bool(false));
    assert_eq!(tbool::decode_bool(1), Value::Bool(true));
    assert_eq!(tbool::decode_bool(-7), Value::Bool(true));
    assert!(matches!(tbool::encode_bool(true), libsql::Value::Integer(1)));
    assert!(matches!(tbool::encode_bool(false), libsql::Value::Integer(0)));
}

#[test]
fn types_bytes_helpers() {
    let data = vec![0u8, 1, 2, 255];
    assert_eq!(bytes::decode_blob(data.clone()), Value::Binary(data.clone()));
    assert_eq!(bytes::decode_blob(vec![]), Value::Binary(vec![]));
    assert!(matches!(
        bytes::encode_blob(data.clone()),
        libsql::Value::Blob(b) if b == data
    ));
}

#[test]
fn types_float_helpers() {
    assert_eq!(float::decode_real(1.25), Value::F64(1.25));
    assert_eq!(float::decode_real(-0.5), Value::F64(-0.5));
    assert!(matches!(float::encode_f32(2.5f32), libsql::Value::Real(f) if f == 2.5));
    assert!(matches!(float::encode_f64(-9.5), libsql::Value::Real(f) if f == -9.5));
}

#[test]
fn types_int_helpers() {
    assert_eq!(int::decode_integer(i64::MIN), Value::I64(i64::MIN));
    assert_eq!(int::decode_integer(0), Value::I64(0));
    assert!(matches!(int::encode_i32(-3), libsql::Value::Integer(-3)));
    assert!(matches!(int::encode_i64(9), libsql::Value::Integer(9)));
    assert!(matches!(int::encode_u32(u32::MAX), libsql::Value::Integer(n) if n == u32::MAX as i64));
    assert!(matches!(int::encode_u64(123456789), libsql::Value::Integer(123456789)));
}

#[test]
fn types_null_helpers() {
    assert_eq!(null::decode_null(), Value::Null);
    assert!(matches!(null::encode_null(), libsql::Value::Null));
}

#[test]
fn types_str_helpers() {
    assert_eq!(
        tstr::decode_text("hello", false),
        Value::String("hello".into())
    );
    assert!(matches!(
        tstr::decode_text(r#"{"a":1}"#, true),
        Value::Map(_)
    ));
    // JSON-shaped but invalid: falls back to String.
    assert_eq!(
        tstr::decode_text("{invalid}", true),
        Value::String("{invalid}".into())
    );
    assert!(matches!(
        tstr::encode_text("encoded".into()),
        libsql::Value::Text(s) if s == "encoded"
    ));
}

// ──────────────────────────────────────────────────────────────────────
// value.rs conversions
// ──────────────────────────────────────────────────────────────────────

#[test]
fn data_type_name_display_and_from_value() {
    let cases = [
        (TursoDataType::Null, "NULL"),
        (TursoDataType::Integer, "INTEGER"),
        (TursoDataType::Real, "REAL"),
        (TursoDataType::Text, "TEXT"),
        (TursoDataType::Blob, "BLOB"),
    ];
    for (dt, name) in cases {
        assert_eq!(dt.name(), name);
        assert_eq!(dt.to_string(), name);
    }

    assert_eq!(
        TursoDataType::from(&libsql::Value::Null),
        TursoDataType::Null
    );
    assert_eq!(
        TursoDataType::from(&libsql::Value::Integer(1)),
        TursoDataType::Integer
    );
    assert_eq!(
        TursoDataType::from(&libsql::Value::Real(1.0)),
        TursoDataType::Real
    );
    assert_eq!(
        TursoDataType::from(&libsql::Value::Text("t".into())),
        TursoDataType::Text
    );
    assert_eq!(
        TursoDataType::from(&libsql::Value::Blob(vec![1])),
        TursoDataType::Blob
    );
}

#[test]
fn turso_value_accessors_cover_mismatches() {
    let null_v = TursoValue::new(libsql::Value::Null);
    assert!(null_v.is_null());
    assert_eq!(null_v.data_type(), TursoDataType::Null);
    assert_eq!(null_v.as_integer(), None);
    assert_eq!(null_v.as_real(), None);
    assert_eq!(null_v.as_text(), None);
    assert_eq!(null_v.as_blob(), None);

    let int_v = TursoValue::with_type(libsql::Value::Integer(5), TursoDataType::Text);
    assert!(!int_v.is_null());
    // Explicit type wins over the inferred one.
    assert_eq!(int_v.data_type(), TursoDataType::Text);
    assert_eq!(int_v.as_integer(), Some(5));
    assert_eq!(int_v.as_text(), None);

    let real_v = TursoValue::new(libsql::Value::Real(2.5));
    assert_eq!(real_v.as_real(), Some(2.5));
    assert_eq!(real_v.as_integer(), None);

    let text_v = TursoValue::new(libsql::Value::Text("x".into()));
    assert_eq!(text_v.as_text(), Some("x"));

    let blob_v = TursoValue::new(libsql::Value::Blob(vec![9, 8]));
    assert_eq!(blob_v.as_blob(), Some(&[9u8, 8][..]));
    assert_eq!(blob_v.as_text(), None);
}

#[test]
fn json_detection_falls_back_on_invalid_json() {
    // Starts with `{` and ends with `}`, so the heuristic triggers, but
    // serde cannot parse it -> must fall back to the original String.
    let tv = TursoValue::new(libsql::Value::Text("{not valid json}".into()));
    assert_eq!(
        turso_value_to_rbs(&tv, true),
        Value::String("{not valid json}".into())
    );

    // Not JSON-shaped at all.
    let tv = TursoValue::new(libsql::Value::Text("plain".into()));
    assert_eq!(turso_value_to_rbs(&tv, true), Value::String("plain".into()));

    // Valid JSON array/object.
    let obj = TursoValue::new(libsql::Value::Text(r#"{"k":1}"#.into()));
    assert!(matches!(turso_value_to_rbs(&obj, true), Value::Map(_)));
    let arr = TursoValue::new(libsql::Value::Text("[true]".into()));
    assert!(matches!(turso_value_to_rbs(&arr, true), Value::Array(_)));
}

#[test]
fn is_json_string_heuristic() {
    assert!(is_json_string("null"));
    assert!(is_json_string("{}"));
    assert!(is_json_string("[]"));
    assert!(!is_json_string("{"));
    assert!(!is_json_string("}"));
    assert!(!is_json_string("["));
    assert!(!is_json_string(""));
    assert!(!is_json_string("123"));
}

#[test]
fn libsql_to_value_maps_all_variants() {
    assert_eq!(libsql_to_value(libsql::Value::Null), Value::Null);
    assert_eq!(libsql_to_value(libsql::Value::Integer(-1)), Value::I64(-1));
    assert_eq!(libsql_to_value(libsql::Value::Real(0.5)), Value::F64(0.5));
    assert_eq!(
        libsql_to_value(libsql::Value::Text("t".into())),
        Value::String("t".into())
    );
    assert_eq!(
        libsql_to_value(libsql::Value::Blob(vec![1, 2])),
        Value::Binary(vec![1, 2])
    );
    // json_detect is always disabled for the convenience wrapper.
    assert_eq!(
        libsql_to_value(libsql::Value::Text(r#"{"a":1}"#.into())),
        Value::String(r#"{"a":1}"#.into())
    );
}

#[test]
fn value_to_libsql_covers_scalar_variants() {
    assert!(matches!(value_to_libsql(&Value::Null).unwrap(), libsql::Value::Null));
    assert!(matches!(
        value_to_libsql(&Value::Bool(true)).unwrap(),
        libsql::Value::Integer(1)
    ));
    assert!(matches!(
        value_to_libsql(&Value::Bool(false)).unwrap(),
        libsql::Value::Integer(0)
    ));
    assert!(matches!(
        value_to_libsql(&Value::I32(-2)).unwrap(),
        libsql::Value::Integer(-2)
    ));
    assert!(matches!(
        value_to_libsql(&Value::I64(3)).unwrap(),
        libsql::Value::Integer(3)
    ));
    assert!(matches!(
        value_to_libsql(&Value::U32(4)).unwrap(),
        libsql::Value::Integer(4)
    ));
    assert!(matches!(
        value_to_libsql(&Value::U64(5)).unwrap(),
        libsql::Value::Integer(5)
    ));
    assert!(matches!(
        value_to_libsql(&Value::F32(1.5f32)).unwrap(),
        libsql::Value::Real(f) if f == 1.5
    ));
    assert!(matches!(
        value_to_libsql(&Value::F64(2.5)).unwrap(),
        libsql::Value::Real(f) if f == 2.5
    ));
    assert!(matches!(
        value_to_libsql(&Value::String("s".into())).unwrap(),
        libsql::Value::Text(ref s) if s == "s"
    ));
    assert!(matches!(
        value_to_libsql(&Value::Binary(vec![7])).unwrap(),
        libsql::Value::Blob(ref b) if b == &vec![7]
    ));
}

#[test]
fn value_to_libsql_covers_ext_and_collections() {
    // Date-like tags stringify non-string payloads.
    for tag in ["Date", "DateTime", "Time", "Decimal", "Uuid"] {
        let out = value_to_libsql(&Value::Ext(tag, Box::new(Value::String("v".into())))).unwrap();
        assert!(matches!(out, libsql::Value::Text(ref s) if s == "v"));

        let out = value_to_libsql(&Value::Ext(tag, Box::new(Value::I64(123)))).unwrap();
        assert!(matches!(out, libsql::Value::Text(ref s) if s == "123"));
    }

    assert!(matches!(
        value_to_libsql(&Value::Ext("Timestamp", Box::new(Value::I64(77)))).unwrap(),
        libsql::Value::Integer(77)
    ));
    // Non-integer timestamp payload defaults to 0.
    assert!(matches!(
        value_to_libsql(&Value::Ext("Timestamp", Box::new(Value::String("x".into())))).unwrap(),
        libsql::Value::Integer(0)
    ));

    assert!(matches!(
        value_to_libsql(&Value::Ext("Json", Box::new(Value::Binary(vec![1, 2])))).unwrap(),
        libsql::Value::Blob(ref b) if b == &vec![1, 2]
    ));
    // Non-binary JSON payload is serialized as text bytes.
    let json_out =
        value_to_libsql(&Value::Ext("Json", Box::new(Value::String(r#"{"a":1}"#.into()))))
            .unwrap();
    // Value's Display wraps strings in quotes, so the blob is `"{"a":1}"`.
    let expected = format!("\"{}\"", r#"{"a":1}"#).into_bytes();
    match json_out {
        libsql::Value::Blob(b) => assert_eq!(b, expected),
        other => panic!("expected Blob from Json ext, got: {:?}", other),
    }

    // Unknown ext tags dispatch on the inner value type.
    assert!(matches!(
        value_to_libsql(&Value::Ext("Custom", Box::new(Value::String("z".into())))).unwrap(),
        libsql::Value::Text(ref s) if s == "z"
    ));
    assert!(matches!(
        value_to_libsql(&Value::Ext("Custom", Box::new(Value::I64(-1)))).unwrap(),
        libsql::Value::Integer(-1)
    ));
    assert!(matches!(
        value_to_libsql(&Value::Ext("Custom", Box::new(Value::U64(8)))).unwrap(),
        libsql::Value::Integer(8)
    ));
    assert!(matches!(
        value_to_libsql(&Value::Ext("Custom", Box::new(Value::F64(1.5)))).unwrap(),
        libsql::Value::Real(f) if f == 1.5
    ));
    assert!(matches!(
        value_to_libsql(&Value::Ext("Custom", Box::new(Value::Null))).unwrap(),
        libsql::Value::Text(_)
    ));

    // Arrays and maps are JSON-encoded into text.
    assert!(matches!(
        value_to_libsql(&Value::Array(vec![Value::I64(1), Value::I64(2)])).unwrap(),
        libsql::Value::Text(ref s) if s == "[1,2]"
    ));
    let map: Value = serde_json::from_str(r#"{"k":"v"}"#).unwrap();
    assert!(matches!(
        value_to_libsql(&map).unwrap(),
        libsql::Value::Text(ref s) if s.contains("\"k\"")
    ));
}

// ──────────────────────────────────────────────────────────────────────
// column / statement / query_result helpers
// ──────────────────────────────────────────────────────────────────────

#[test]
fn column_accessors_and_clone() {
    let col = TursoColumn::new("name".into(), 3, TursoDataType::Text);
    assert_eq!(col.name(), "name");
    assert_eq!(col.ordinal(), 3);
    assert_eq!(col.type_info(), TursoDataType::Text);
    let cloned = col.clone();
    assert_eq!(cloned.name(), "name");
    assert_eq!(format!("{:?}", cloned), format!("{:?}", col));
}

#[test]
fn statement_helpers() {
    let stmt = TursoStatement::new("SELECT ?");
    assert_eq!(stmt.sql(), "SELECT ?");
    assert!(stmt.columns().is_empty());
    let cloned = stmt.clone();
    assert_eq!(cloned.sql(), "SELECT ?");
    assert!(cloned.columns().is_empty());
    assert!(format!("{:?}", stmt).contains("SELECT ?"));
}

#[test]
fn query_result_conversions_and_extend() {
    let r = TursoQueryResult::new(4, 11);
    assert_eq!(r.rows_affected(), 4);
    assert_eq!(r.last_insert_rowid(), 11);

    let exec = r.to_exec_result();
    assert_eq!(exec.rows_affected, 4);
    assert_eq!(exec.last_insert_id, Value::I64(11));

    let exec2: rbdc::db::ExecResult = TursoQueryResult::new(1, 2).into();
    assert_eq!(exec2.rows_affected, 1);
    assert_eq!(exec2.last_insert_id, Value::I64(2));

    let default = TursoQueryResult::default();
    assert_eq!(default.rows_affected(), 0);
    assert_eq!(default.last_insert_rowid(), 0);

    let mut acc = TursoQueryResult::new(5, 1);
    acc.extend(vec![
        TursoQueryResult::new(2, 9),
        TursoQueryResult::new(3, 20),
    ]);
    assert_eq!(acc.rows_affected(), 10);
    assert_eq!(acc.last_insert_rowid(), 20);
    // Extending with an empty iterator leaves the accumulator unchanged.
    acc.extend(Vec::<TursoQueryResult>::new());
    assert_eq!(acc.rows_affected(), 10);
}

// ──────────────────────────────────────────────────────────────────────
// error helpers
// ──────────────────────────────────────────────────────────────────────

#[test]
fn error_configuration_and_connection_variants() {
    let cfg = TursoError::configuration("cfg");
    assert_eq!(cfg.message(), "cfg");
    assert_eq!(cfg.to_string(), "turso configuration error: cfg");
    assert!(!cfg.is_unavailable());
    assert!(std::error::Error::source(&cfg).is_none());

    let conn = TursoError::connection("conn");
    assert_eq!(conn.message(), "conn");
    assert_eq!(conn.to_string(), "turso connection error: conn");
    assert!(conn.is_unavailable());
    assert!(std::error::Error::source(&conn).is_none());

    // Conversion to the rbdc error type preserves the display string.
    let rbdc_err: rbdc::Error = TursoError::connection("boom").into();
    assert!(rbdc_err.to_string().contains("boom"));
}

#[tokio::test]
async fn error_libsql_variant_from_real_error() {
    let db = libsql::Builder::new_local(":memory:")
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    let libsql_err = conn.execute("THIS IS NOT SQL", ()).await.unwrap_err();

    let err = TursoError::from(libsql_err);
    assert!(matches!(err, TursoError::Libsql(_)));
    assert!(err.is_unavailable());
    assert!(!err.message().is_empty());
    assert!(std::error::Error::source(&err).is_some());
    assert!(err.to_string().starts_with("turso error:"));
}

// ──────────────────────────────────────────────────────────────────────
// driver: column_type mapping and connect_opt downcast failure
// ──────────────────────────────────────────────────────────────────────

#[test]
fn driver_column_type_maps_every_value_variant() {
    let driver = TursoDriver {};

    assert_eq!(driver.column_type(&Value::Null), "NULL");
    assert_eq!(driver.column_type(&Value::Bool(true)), "INTEGER");
    assert_eq!(driver.column_type(&Value::I32(1)), "INTEGER");
    assert_eq!(driver.column_type(&Value::I64(1)), "INTEGER");
    assert_eq!(driver.column_type(&Value::U32(1)), "INTEGER");
    assert_eq!(driver.column_type(&Value::U64(1)), "INTEGER");
    assert_eq!(driver.column_type(&Value::F32(1.0)), "REAL");
    assert_eq!(driver.column_type(&Value::F64(1.0)), "REAL");
    assert_eq!(driver.column_type(&Value::String("s".into())), "TEXT");
    assert_eq!(driver.column_type(&Value::Binary(vec![1])), "BLOB");
    assert_eq!(driver.column_type(&Value::Array(vec![])), "TEXT");
    assert_eq!(
        driver.column_type(&Value::Map(rbs::value::map::ValueMap::new())),
        "TEXT"
    );

    for tag in ["Date", "DateTime", "Time", "Decimal", "Uuid"] {
        assert_eq!(
            driver.column_type(&Value::Ext(tag, Box::new(Value::Null))),
            "TEXT",
            "tag {}",
            tag
        );
    }
    assert_eq!(
        driver.column_type(&Value::Ext("Timestamp", Box::new(Value::I64(1)))),
        "INTEGER"
    );
    assert_eq!(
        driver.column_type(&Value::Ext("Json", Box::new(Value::Null))),
        "BLOB"
    );
    assert_eq!(
        driver.column_type(&Value::Ext("UnknownTag", Box::new(Value::Null))),
        "TEXT"
    );
}

/// A `ConnectOptions` implementation that is not `TursoConnectOptions`.
/// Passing it to `connect_opt` must produce the downcast-failure error
/// instead of attempting a connection.
#[derive(Debug)]
struct ForeignOptions;

impl ConnectOptions for ForeignOptions {
    fn connect(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn Connection>, rbdc::Error>> {
        Box::pin(async { Err(rbdc::Error::from("foreign options must not connect")) })
    }

    fn set_uri(&mut self, _uri: &str) -> Result<(), rbdc::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn connect_opt_rejects_foreign_options() {
    let driver = TursoDriver {};
    let err = match driver.connect_opt(&ForeignOptions).await {
        Ok(_) => panic!("foreign ConnectOptions must be rejected"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("downcast_ref failure"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn connect_rejects_malformed_uri() {
    let driver = TursoDriver {};
    let err = match driver.connect("turso:not-a-valid-scheme").await {
        Ok(_) => panic!("turso: without // must be rejected"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("invalid URI scheme"));
}

#[tokio::test]
async fn connect_opt_with_turso_options_succeeds() {
    let driver = TursoDriver {};
    let opts = TursoConnectOptions::new();
    let mut conn = driver.connect_opt(&opts).await.unwrap();
    conn.ping().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────
// options parsing / builders
// ──────────────────────────────────────────────────────────────────────

#[test]
fn options_builders_and_predicates() {
    let opts = TursoConnectOptions::new()
        .url("https://db.example.com")
        .auth_token("tok")
        .json_detect(true);
    assert!(opts.is_remote());
    assert!(!opts.is_in_memory());
    assert!(opts.is_json_detect());
    assert!(opts.validate().is_ok());

    let http = TursoConnectOptions::new()
        .url("http://db.example.com")
        .auth_token("tok");
    assert!(http.is_remote());
    assert!(http.validate().is_ok());

    let libsql = TursoConnectOptions::new()
        .url("libsql://db.example.com")
        .auth_token("tok");
    assert!(libsql.is_remote());

    // Builder toggling json_detect off.
    let off = TursoConnectOptions::new().json_detect(true).json_detect(false);
    assert!(!off.is_json_detect());

    // A whitespace-only token is treated as missing for remote URLs.
    let blank = TursoConnectOptions::new().url("libsql://x").auth_token("  \t ");
    assert!(blank.validate().is_err());
}

#[test]
fn options_parse_bare_path_with_query() {
    let opts: TursoConnectOptions = "turso://relative/file.db?json_detect=1"
        .parse()
        .unwrap();
    assert!(!opts.is_in_memory());
    assert!(!opts.is_remote());
    assert!(opts.is_json_detect());
}

#[test]
fn options_explicit_url_wins_over_path() {
    let opts: TursoConnectOptions =
        "turso://ignored/path.db?url=libsql://real.example.com&token=t"
            .parse()
            .unwrap();
    assert!(opts.is_remote());
}

#[test]
fn options_parse_sqlite_scheme_with_query() {
    let opts: TursoConnectOptions = "sqlite://file.db?json_detect=true".parse().unwrap();
    assert!(!opts.is_in_memory());
    assert!(!opts.is_remote());
    assert!(opts.is_json_detect());
}

#[test]
fn options_parse_empty_string_is_memory() {
    // No scheme prefix and no path -> empty rest maps to in-memory.
    let opts: TursoConnectOptions = "".parse().unwrap();
    assert!(opts.is_in_memory());
}

#[test]
fn options_set_uri_roundtrip() {
    let mut opts = TursoConnectOptions::new();
    opts.set_uri("sqlite://example.db").unwrap();
    assert!(!opts.is_in_memory());
    assert!(!opts.is_remote());

    opts.set_uri("turso://:memory:").unwrap();
    assert!(opts.is_in_memory());

    // Invalid remote URI fails during set_uri (parsing), leaving the
    // previous configuration converted into an error result.
    assert!(opts.set_uri("turso://?nope=1").is_err());
}

// ──────────────────────────────────────────────────────────────────────
// connection formatting / lifecycle
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn connection_debug_impl() {
    let opts = TursoConnectOptions::new();
    let conn = opts.connect_turso().await.unwrap();
    assert_eq!(format!("{:?}", conn), "TursoConnection");
}

#[tokio::test]
async fn connection_query_and_stream_consume_all_rows() {
    let mut conn = TursoConnectOptions::new().connect_turso().await.unwrap();
    conn.exec(
        "CREATE TABLE public_api_rows (id INTEGER PRIMARY KEY, val TEXT)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO public_api_rows (id, val) VALUES (?, ?)",
        vec![Value::I64(1), Value::String("a".into())],
    )
    .await
    .unwrap();

    let mut stream = conn
        .exec_rows("SELECT id, val FROM public_api_rows", vec![])
        .await
        .unwrap();
    let mut n = 0usize;
    while let Some(row) = stream.next().await {
        let mut row = row.unwrap();
        assert_eq!(row.get(0).unwrap(), Value::I64(1));
        assert_eq!(row.get(1).unwrap(), Value::String("a".into()));
        // Out-of-range access on a live row is an error.
        assert!(row.get(2).is_err());
        n += 1;
    }
    assert_eq!(n, 1);
}
