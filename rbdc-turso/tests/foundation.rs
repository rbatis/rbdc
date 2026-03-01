//! Foundation tests for the rbdc-turso crate.
//!
//! These tests validate:
//! - URI parsing success and failure cases
//! - Required options checks
//! - Startup-only activation semantics (no runtime switching by API design)
//!
//! All tests are deterministic and do not require a live Turso endpoint.

use rbdc::db::{ConnectOptions, Driver, Placeholder};
use std::str::FromStr;

// ──────────────────────────────────────────────────────────────────────
// URI parsing tests
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_parse_in_memory() {
    let opts = rbdc_turso::TursoConnectOptions::from_str("turso://:memory:").unwrap();
    assert!(opts.is_in_memory());
    assert!(!opts.is_remote());
}

#[test]
fn test_parse_rejects_turso_colon_without_slashes() {
    // "turso:" without "//" is now rejected as an invalid scheme
    let result = rbdc_turso::TursoConnectOptions::from_str("turso::memory:");
    assert!(result.is_err(), "turso: without // should be rejected");
}

#[test]
fn test_parse_local_file() {
    let opts = rbdc_turso::TursoConnectOptions::from_str("turso://path/to/db.sqlite").unwrap();
    assert!(!opts.is_in_memory());
    assert!(!opts.is_remote());
}

#[test]
fn test_parse_remote_with_url_and_token() {
    let opts = rbdc_turso::TursoConnectOptions::from_str(
        "turso://?url=libsql://my-db.turso.io&token=secret123",
    )
    .unwrap();
    assert!(!opts.is_in_memory());
    assert!(opts.is_remote());
}

#[test]
fn test_parse_remote_without_token_parses_but_validation_fails() {
    // Parsing succeeds (URI is syntactically valid)
    let opts =
        rbdc_turso::TursoConnectOptions::from_str("turso://?url=libsql://my-db.turso.io").unwrap();
    assert!(opts.is_remote());

    // But validation rejects it because remote connections need a token
    let result = opts.validate();
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("auth_token"),
        "Error should mention auth_token requirement, got: {}",
        err_msg
    );
}

#[test]
fn test_parse_remote_with_empty_token_validation_fails() {
    let opts = rbdc_turso::TursoConnectOptions::from_str(
        "turso://?url=libsql://my-db.turso.io&token=",
    )
    .unwrap();
    assert!(opts.is_remote());

    let result = opts.validate();
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("must not be empty"),
        "Error should mention empty auth_token, got: {}",
        err_msg
    );
}

#[test]
fn test_parse_query_without_url_or_path_fails() {
    let result = rbdc_turso::TursoConnectOptions::from_str("turso://?token=secret");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no database URL or path provided"),
        "Error should mention missing database URL/path, got: {}",
        err_msg
    );
}

#[test]
fn test_parse_unknown_query_param_rejected() {
    let result = rbdc_turso::TursoConnectOptions::from_str("turso://:memory:?foo=bar");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unknown query parameter"),
        "Error should mention unknown parameter, got: {}",
        err_msg
    );
}

#[test]
fn test_parse_empty_uri_gives_in_memory() {
    let opts = rbdc_turso::TursoConnectOptions::from_str("turso://").unwrap();
    assert!(opts.is_in_memory());
}

// ──────────────────────────────────────────────────────────────────────
// Options validation tests
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_validate_in_memory_succeeds() {
    let opts = rbdc_turso::TursoConnectOptions::new();
    assert!(opts.validate().is_ok());
}

#[test]
fn test_validate_local_file_succeeds() {
    let opts = rbdc_turso::TursoConnectOptions::new().url("/tmp/test.db");
    assert!(opts.validate().is_ok());
}

#[test]
fn test_validate_remote_without_token_fails() {
    let opts = rbdc_turso::TursoConnectOptions::new().url("libsql://my-db.turso.io");
    let result = opts.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_remote_with_token_succeeds() {
    let opts = rbdc_turso::TursoConnectOptions::new()
        .url("libsql://my-db.turso.io")
        .auth_token("my-secret-token");
    assert!(opts.validate().is_ok());
}

#[test]
fn test_validate_remote_with_whitespace_token_fails() {
    let opts = rbdc_turso::TursoConnectOptions::new()
        .url("libsql://my-db.turso.io")
        .auth_token("   ");
    let result = opts.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_empty_url_fails() {
    let opts = rbdc_turso::TursoConnectOptions::new().url("");
    let result = opts.validate();
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("must not be empty"),
        "Error should mention empty URL, got: {}",
        err_msg
    );
}

#[test]
fn test_validate_https_remote_requires_token() {
    let opts = rbdc_turso::TursoConnectOptions::new().url("https://my-db.turso.io");
    assert!(opts.validate().is_err());

    let opts = rbdc_turso::TursoConnectOptions::new()
        .url("https://my-db.turso.io")
        .auth_token("tok");
    assert!(opts.validate().is_ok());
}

// ──────────────────────────────────────────────────────────────────────
// Default options tests
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_default_is_in_memory() {
    let opts = rbdc_turso::TursoConnectOptions::default();
    assert!(opts.is_in_memory());
    assert!(!opts.is_remote());
}

// ──────────────────────────────────────────────────────────────────────
// ConnectOptions trait tests
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_set_uri_updates_options() {
    let mut opts = rbdc_turso::TursoConnectOptions::new();
    opts.set_uri("turso://some/file.db").unwrap();
    assert!(!opts.is_in_memory());
    assert!(!opts.is_remote());
}

#[test]
fn test_set_uri_invalid_param_fails() {
    let mut opts = rbdc_turso::TursoConnectOptions::new();
    let result = opts.set_uri("turso://:memory:?badparam=1");
    assert!(result.is_err());
}

// ──────────────────────────────────────────────────────────────────────
// Driver interface tests (no live connection required)
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_driver_name() {
    let driver = rbdc_turso::TursoDriver {};
    assert_eq!(driver.name(), "turso");
}

#[test]
fn test_driver_default_option_type() {
    let driver = rbdc_turso::TursoDriver {};
    let opt = driver.default_option();
    // Must be downcastable to TursoConnectOptions
    assert!(opt.downcast_ref::<rbdc_turso::TursoConnectOptions>().is_some());
}

#[test]
fn test_placeholder_exchange_preserves_question_marks() {
    let driver = rbdc_turso::TursoDriver {};
    let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
    let result = driver.exchange(sql);
    assert_eq!(result, sql, "Turso uses ? placeholders like SQLite");
}

// ──────────────────────────────────────────────────────────────────────
// Startup-only activation semantics tests
// ──────────────────────────────────────────────────────────────────────

/// Verify that the API surface does not expose any runtime backend switching
/// mechanism. The TursoConnectOptions and TursoDriver types are fixed at
/// construction time and provide no methods to change the underlying backend.
///
/// This is a design-level test: we verify the absence of switching APIs
/// rather than testing runtime behavior.
#[test]
fn test_no_runtime_switch_api_on_options() {
    // TursoConnectOptions has url() and auth_token() builders but no
    // "set_backend" method. This is by design — the type itself encodes
    // that the backend is Turso.
    let opts = rbdc_turso::TursoConnectOptions::new()
        .url("libsql://db.turso.io")
        .auth_token("tok");

    // The options always identify as remote Turso
    assert!(opts.is_remote());
}

/// Verify that a connection established via TursoDriver uses Turso/libSQL
/// and that basic operations work through the backend.
#[tokio::test]
async fn test_in_memory_connection_is_functional() {
    let driver = rbdc_turso::TursoDriver {};
    let mut conn = driver.connect("turso://:memory:").await.unwrap();

    // Verify basic operations work through the Turso/libSQL backend
    conn.exec(
        "CREATE TABLE foundation_test (id INTEGER PRIMARY KEY, name TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let result = conn
        .exec(
            "INSERT INTO foundation_test (id, name) VALUES (1, 'hello')",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(result.rows_affected, 1);

    let rows = conn
        .get_rows("SELECT id, name FROM foundation_test", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    conn.close().await.unwrap();
}

/// Verify that ping works on a healthy in-memory connection.
#[tokio::test]
async fn test_ping_on_in_memory() {
    let driver = rbdc_turso::TursoDriver {};
    let mut conn = driver.connect("turso://:memory:").await.unwrap();
    conn.ping().await.unwrap();
    conn.close().await.unwrap();
}

/// Verify that connecting with incomplete remote options fails at connect time
/// (startup-only validation), not silently at query time.
#[tokio::test]
async fn test_connect_remote_without_token_fails_at_startup() {
    let opts = rbdc_turso::TursoConnectOptions::new().url("libsql://nonexistent.turso.io");

    // Should fail during connect (startup-time validation), not later
    let result = opts.connect_turso().await;
    assert!(
        result.is_err(),
        "Remote connection without token must fail at startup"
    );
}

#[tokio::test]
async fn test_connect_local_with_missing_parent_fails() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut missing_parent = std::env::temp_dir();
    missing_parent.push(format!("rbdc_turso_missing_parent_{}_{}", std::process::id(), ts));
    let _ = std::fs::remove_dir_all(&missing_parent);

    let db_path = missing_parent.join("db.sqlite");
    let opts =
        rbdc_turso::TursoConnectOptions::new().url(db_path.to_string_lossy().to_string());

    let result = opts.connect_turso().await;
    assert!(
        result.is_err(),
        "connecting to a local DB file in a missing parent directory should fail"
    );
}
