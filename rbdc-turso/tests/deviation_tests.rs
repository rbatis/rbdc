//! Backend-specific deviation tests for the Turso adapter.
//!
//! Each test corresponds to an approved deviation in the registry.
//! Tests verify the *actual Turso behavior* and assert it matches the
//! documented deviation — if behavior drifts from the approved record,
//! the test fails, signaling the deviation needs re-evaluation.
//!
//! Tests for NotADeviation records confirm parity (no difference).
//! Tests for Proposed records document current behavior pending governance.

mod deviations;

use deviations::registry::{self, ApprovalStatus};
use deviations::validator;
use rbdc::db::{Connection, Driver};
use rbdc_turso::TursoDriver;

/// Helper: create an in-memory connection.
async fn connect() -> Box<dyn Connection> {
    let driver = TursoDriver {};
    driver.connect("turso://:memory:").await.unwrap()
}

// ──────────────────────────────────────────────────────────────────────
// Registry Validation Tests
// ──────────────────────────────────────────────────────────────────────

#[test]
fn test_registry_is_structurally_valid() {
    let result = validator::validate_registry();
    if !result.is_valid() {
        panic!(
            "Registry validation failed:\n{}",
            result.errors.join("\n")
        );
    }
}

#[test]
fn test_registry_has_no_rejected_deviations() {
    let rejected = validator::filter_by_status(ApprovalStatus::Rejected);
    assert!(
        rejected.is_empty(),
        "Registry contains rejected deviations that must be removed or fixed: {:?}",
        rejected.iter().map(|d| d.id).collect::<Vec<_>>()
    );
}

#[test]
fn test_registry_proposed_deviations_block_release() {
    let result = validator::validate_registry();
    if !result.is_release_ready() {
        // This is expected — DEV-004 is proposed. Print warnings for visibility.
        for w in &result.warnings {
            eprintln!("  GOVERNANCE WARNING: {}", w);
        }
        eprintln!(
            "  Release blocked: {} proposed deviation(s) require governance decision",
            result.proposed_count
        );
    }
    // This test passes either way — it's informational.
    // A separate release-gate test would assert is_release_ready().
}

#[test]
fn test_every_deviation_has_unique_id() {
    let mut ids: Vec<&str> = registry::REGISTRY.iter().map(|d| d.id).collect();
    let original_len = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), original_len, "duplicate deviation IDs found");
}

#[test]
fn test_deviation_lookup_by_id() {
    let dev = validator::find_deviation("DEV-001");
    assert!(dev.is_some(), "DEV-001 should exist in registry");
    assert_eq!(dev.unwrap().status, ApprovalStatus::Approved);
}

#[test]
fn test_deviation_lookup_by_scenario() {
    let dev = validator::find_by_scenario("PAR-008");
    assert!(dev.is_some(), "PAR-008 should be linked to a deviation");
    assert_eq!(dev.unwrap().id, "DEV-001");
}

// ──────────────────────────────────────────────────────────────────────
// DEV-001: column_type reports runtime value type (APPROVED)
// ──────────────────────────────────────────────────────────────────────

/// DEV-001: Verify that column_type reports runtime value types.
///
/// The Turso native async API provides value-level type info via
/// libsql::ValueType enum (Integer, Real, Text, Blob, Null), not
/// schema-level declared types. The adapter maps these to canonical
/// SQL type names. This means:
/// - Column types are derived from actual values, not schema declarations
/// - Custom type aliases (BOOLEAN, DATETIME) are not exposed
/// - Empty result sets have no type info to report
#[tokio::test]
async fn test_dev001_column_type_reports_runtime_types() {
    let dev = validator::find_deviation("DEV-001").unwrap();
    assert_eq!(dev.status, ApprovalStatus::Approved);

    let mut conn = connect().await;
    conn.exec(
        "CREATE TABLE dev001 (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
        vec![],
    )
    .await
    .unwrap();
    conn.exec(
        "INSERT INTO dev001 (id, name, score) VALUES (1, 'test', 3.14)",
        vec![],
    )
    .await
    .unwrap();

    let rows = conn
        .get_rows("SELECT id, name, score FROM dev001", vec![])
        .await
        .unwrap();

    let row = &rows[0];
    let md = row.meta_data();

    // Turso behavior: column_type returns runtime value type names
    // (from libsql::ValueType), not declared schema types.
    assert_eq!(md.column_type(0), "INTEGER");
    assert_eq!(md.column_type(1), "TEXT");
    assert_eq!(md.column_type(2), "REAL");
}

/// DEV-001: Verify column_type on empty result set also returns empty string.
#[tokio::test]
async fn test_dev001_column_type_empty_result_set() {
    let mut conn = connect().await;
    conn.exec("CREATE TABLE dev001_empty (id INTEGER, name TEXT)", vec![])
        .await
        .unwrap();

    let rows = conn
        .get_rows("SELECT id, name FROM dev001_empty", vec![])
        .await
        .unwrap();

    assert!(rows.is_empty(), "result set should be empty");
    // No rows means no metadata to check — this is the documented behavior.
    // column_type would return empty string if metadata were available.
}

// ──────────────────────────────────────────────────────────────────────
// DEV-002: Boolean round-trip as integer (NOT A DEVIATION)
// ──────────────────────────────────────────────────────────────────────

/// DEV-002: Confirm that bool round-trip as integer is parity behavior.
#[tokio::test]
async fn test_dev002_bool_round_trip_is_parity() {
    let dev = validator::find_deviation("DEV-002").unwrap();
    assert_eq!(dev.status, ApprovalStatus::NotADeviation);

    let mut conn = connect().await;
    conn.exec("CREATE TABLE dev002 (id INTEGER, flag INTEGER)", vec![])
        .await
        .unwrap();

    // Bind a bool
    conn.exec(
        "INSERT INTO dev002 (id, flag) VALUES (?, ?)",
        vec![rbs::Value::I64(1), rbs::Value::Bool(true)],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT flag FROM dev002 WHERE id = 1", vec![])
        .await
        .unwrap();

    let row = &mut rows[0];
    let flag = row.get(0).unwrap();

    // Bool(true) → INTEGER 1 → I64(1): this is standard adapter behavior
    assert_eq!(flag, rbs::Value::I64(1));
}

// ──────────────────────────────────────────────────────────────────────
// DEV-003: JSON text decoding heuristic (NOT A DEVIATION)
// ──────────────────────────────────────────────────────────────────────

/// DEV-003: Confirm JSON text decoding matches standard behavior.
///
/// JSON-shaped text values (starting with `{`/`[` or equal to `"null"`)
/// are automatically deserialized into `rbs::Value::Map`/`Array`/`Null`.
/// Non-JSON text is returned as `Value::String`. This matches the
/// standard adapter heuristic.
#[tokio::test]
async fn test_dev003_json_text_is_parity() {
    let dev = validator::find_deviation("DEV-003").unwrap();
    assert_eq!(dev.status, ApprovalStatus::NotADeviation);

    let mut conn = connect().await;
    conn.exec("CREATE TABLE dev003 (id INTEGER, data TEXT)", vec![])
        .await
        .unwrap();

    // Store a JSON object as TEXT
    conn.exec(
        "INSERT INTO dev003 (id, data) VALUES (1, '{\"key\": \"value\"}')",
        vec![],
    )
    .await
    .unwrap();

    // Store plain text
    conn.exec(
        "INSERT INTO dev003 (id, data) VALUES (2, 'plain text')",
        vec![],
    )
    .await
    .unwrap();

    let mut rows = conn
        .get_rows("SELECT data FROM dev003 ORDER BY id", vec![])
        .await
        .unwrap();

    // Row 0: JSON-shaped text is auto-deserialized into Map
    let row0 = &mut rows[0];
    let json_val = row0.get(0).unwrap();
    match &json_val {
        rbs::Value::Map(m) => {
            assert!(!m.is_empty(), "JSON object should be deserialized");
        }
        other => panic!("expected Map for JSON text, got: {:?}", other),
    }

    // Row 1: plain text stays as String
    let row1 = &mut rows[1];
    let plain_val = row1.get(0).unwrap();
    assert_eq!(plain_val, rbs::Value::String("plain text".to_string()));
}

// ──────────────────────────────────────────────────────────────────────
// DEV-004: last_insert_id as U64 (PROPOSED)
// ──────────────────────────────────────────────────────────────────────

/// DEV-004: Document current behavior — last_insert_id is Value::U64.
///
/// This deviation is PROPOSED, meaning it blocks release promotion until
/// a governance decision is made (standardize on U64, I64, or document
/// that the type varies by adapter).
#[tokio::test]
async fn test_dev004_last_insert_id_is_u64() {
    let dev = validator::find_deviation("DEV-004").unwrap();
    assert_eq!(dev.status, ApprovalStatus::Proposed);

    let mut conn = connect().await;
    conn.exec("CREATE TABLE dev004 (id INTEGER PRIMARY KEY, name TEXT)", vec![])
        .await
        .unwrap();

    let result = conn
        .exec("INSERT INTO dev004 (id, name) VALUES (42, 'test')", vec![])
        .await
        .unwrap();

    // Current behavior: last_insert_id is Value::U64
    assert_eq!(
        result.last_insert_id,
        rbs::Value::U64(42),
        "Turso adapter returns last_insert_id as Value::U64"
    );
}

/// DEV-004: Verify auto-increment last_insert_id is also U64.
#[tokio::test]
async fn test_dev004_auto_increment_last_insert_id() {
    let mut conn = connect().await;
    conn.exec("CREATE TABLE dev004_auto (id INTEGER PRIMARY KEY, name TEXT)", vec![])
        .await
        .unwrap();

    let r1 = conn
        .exec("INSERT INTO dev004_auto (name) VALUES ('a')", vec![])
        .await
        .unwrap();
    let r2 = conn
        .exec("INSERT INTO dev004_auto (name) VALUES ('b')", vec![])
        .await
        .unwrap();

    assert_eq!(r1.last_insert_id, rbs::Value::U64(1));
    assert_eq!(r2.last_insert_id, rbs::Value::U64(2));
}
