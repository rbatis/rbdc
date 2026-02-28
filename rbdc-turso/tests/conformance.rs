//! Conformance test target for the Turso adapter.
//!
//! Validates release-readiness via the deviation registry and release gate.
//! This test target is required by the WP05 contract:
//!   `cargo test -p rbdc-turso --test conformance`

#[path = "deviations/registry.rs"]
mod registry;
#[path = "conformance/release_gate.rs"]
mod release_gate;
#[path = "deviations/validator.rs"]
mod validator;

// ──────────────────────────────────────────────────────────────────────
// Release Gate Tests
// ──────────────────────────────────────────────────────────────────────

/// The release gate evaluates the deviation registry and reports pass/fail.
/// Currently expected to FAIL because DEV-004 is in Proposed status.
#[test]
fn test_release_gate_evaluation() {
    let result = release_gate::evaluate();
    // Print the gate result for visibility in test output
    eprintln!("\n{}", release_gate::failure_report(&result));

    // The gate currently fails because DEV-004 is proposed.
    // This is expected and intentional — the test documents the state.
    // When DEV-004 is resolved, this assertion should be updated.
    assert!(
        !result.passed,
        "Release gate should currently fail due to DEV-004 (proposed)"
    );
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("DEV-004") && f.contains("PROPOSED")),
        "Gate failure should reference DEV-004"
    );
}

/// Gate correctly identifies structural validity.
#[test]
fn test_release_gate_registry_valid() {
    let result = release_gate::evaluate();
    // No structural errors (rejected deviations, duplicate IDs, etc.)
    let structural_errors: Vec<&String> = result
        .failures
        .iter()
        .filter(|f| f.starts_with("Registry error:"))
        .collect();
    assert!(
        structural_errors.is_empty(),
        "Registry should be structurally valid: {:?}",
        structural_errors
    );
}

/// Gate reports approved deviation count correctly.
#[test]
fn test_release_gate_counts() {
    let result = release_gate::evaluate();
    assert!(
        result.summary.contains("1 approved"),
        "Should report 1 approved deviation: {}",
        result.summary
    );
    assert!(
        result.summary.contains("1 proposed"),
        "Should report 1 proposed deviation: {}",
        result.summary
    );
}

/// Gate failure report is non-empty when gate fails.
#[test]
fn test_release_gate_failure_report_not_empty() {
    let result = release_gate::evaluate();
    let report = release_gate::failure_report(&result);
    assert!(!report.is_empty(), "failure report should not be empty");
    assert!(
        report.contains("FAILED"),
        "report should indicate failure: {}",
        report
    );
}

/// If all proposed deviations were resolved, the gate would pass
/// (structural and rejection checks are clean).
#[test]
fn test_release_gate_no_rejected() {
    let rejected: Vec<_> = registry::REGISTRY
        .iter()
        .filter(|d| d.status == registry::ApprovalStatus::Rejected)
        .collect();
    assert!(
        rejected.is_empty(),
        "No rejected deviations should exist in registry"
    );
}

/// Unapproved differences (proposed status) are flagged as blocking.
#[test]
fn test_unapproved_differences_block_release() {
    let result = release_gate::evaluate();
    let proposed_failures: Vec<_> = result
        .failures
        .iter()
        .filter(|f| f.contains("PROPOSED"))
        .collect();
    assert!(
        !proposed_failures.is_empty(),
        "Proposed deviations should be flagged as release-blocking"
    );
}
