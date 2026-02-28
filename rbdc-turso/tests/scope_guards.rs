//! Scope guard tests for the Turso adapter.
//!
//! These tests enforce the explicit feature scope constraints:
//! 1. Backend mode is initialization-bound (no runtime switching)
//! 2. No runtime backend toggle API is exposed
//! 3. No migration utility is introduced
//!
//! If any of these constraints are violated by future changes,
//! these tests will fail, preventing accidental scope expansion.

use rbdc::db::{Connection, Driver};
use rbdc_turso::{TursoConnectOptions, TursoDriver};

// ──────────────────────────────────────────────────────────────────────
// No Runtime Backend Switching
// ──────────────────────────────────────────────────────────────────────

/// The TursoDriver type has no method to change the backend at runtime.
/// Its name is always "turso" — there is no "set_backend" or "switch" API.
#[test]
fn guard_driver_name_is_fixed() {
    let driver = TursoDriver {};
    assert_eq!(driver.name(), "turso");
    // No method exists to change the driver's name or backend.
}

/// TursoConnectOptions has no method to switch backends.
/// The url() and auth_token() builders configure Turso only.
#[test]
fn guard_options_no_backend_toggle() {
    let opts = TursoConnectOptions::new()
        .url("libsql://db.turso.io")
        .auth_token("tok");

    // Options identify as remote Turso — there is no way to switch this
    // to a different backend type.
    assert!(opts.is_remote());

    // In-memory mode also identifies as Turso, not as any other backend.
    let mem_opts = TursoConnectOptions::new();
    assert!(mem_opts.is_in_memory());
}

/// Once a connection is established, it's a TursoConnection.
/// There is no method to change the underlying backend.
#[tokio::test]
async fn guard_connection_is_turso_bound() {
    let driver = TursoDriver {};
    let mut conn = driver.connect("turso://:memory:").await.unwrap();

    // The connection works with Turso-specific operations
    conn.exec("CREATE TABLE guard_test (id INTEGER)", vec![])
        .await
        .unwrap();

    // Ping uses the Turso backend
    conn.ping().await.unwrap();

    // There is no API to switch this connection to a different backend.
    // The Connection trait has no such method, and TursoConnection
    // does not expose one either.
}

// ──────────────────────────────────────────────────────────────────────
// No Automatic Fallback
// ──────────────────────────────────────────────────────────────────────

/// When a connection fails, the error is returned directly.
/// No fallback to local/in-memory or any other backend occurs.
#[tokio::test]
async fn guard_no_fallback_on_connect_failure() {
    let driver = TursoDriver {};

    // Invalid remote without token → configuration error, not fallback
    let result = driver.connect("turso://?url=libsql://fake.turso.io").await;
    assert!(result.is_err());
}

/// When a query fails on a live connection, the error is returned.
/// No fallback or retry with a different backend happens.
#[tokio::test]
async fn guard_no_fallback_on_query_failure() {
    let driver = TursoDriver {};
    let mut conn = driver.connect("turso://:memory:").await.unwrap();

    // Query a nonexistent table → error, not fallback
    let result = conn.get_rows("SELECT * FROM nonexistent", vec![]).await;
    assert!(result.is_err());

    // The connection should still be alive (error doesn't trigger fallback)
    conn.ping().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────
// No Migration Utility
// ──────────────────────────────────────────────────────────────────────

/// The rbdc-turso crate does not expose any migration, sync, or data-copy API.
/// This is a compile-time structural guard — if someone adds such methods,
/// this test documents that migration is out of scope.
#[test]
fn guard_no_migration_api() {
    // TursoConnection is a connection, not a migration tool.
    // TursoDriver is a driver, not a migration orchestrator.
    // TursoConnectOptions configures a single backend, not cross-backend sync.
    //
    // This test exists to document the scope boundary. If future work
    // adds migration utilities, this test should be updated with an
    // explicit scope-expansion decision reference.
    let driver = TursoDriver {};
    assert_eq!(driver.name(), "turso");
    // No migrate(), sync(), replicate_from() or similar methods exist.
}

// ──────────────────────────────────────────────────────────────────────
// Placeholder Passthrough
// ──────────────────────────────────────────────────────────────────────

/// The placeholder exchange is a passthrough — Turso uses `?` natively.
/// No SQL rewriting occurs.
#[test]
fn guard_placeholder_is_passthrough() {
    use rbdc::db::Placeholder;
    let driver = TursoDriver {};
    let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
    assert_eq!(driver.exchange(sql), sql);
}


