//! Release gate evaluator for the Turso adapter.
//!
//! Validates that the deviation registry meets release-readiness criteria:
//! - Registry is structurally valid (unique IDs, linked scenarios, etc.)
//! - No rejected deviations exist
//! - All required scenarios for approved deviations have passing tests
//! - Unapproved differences are flagged as release-blocking
//!
//! Gate behavior: fail closed on unknown statuses or missing required outcomes.

use super::registry::{ApprovalStatus, Deviation, REGISTRY};
use super::validator;

/// Release gate outcome.
#[derive(Debug)]
pub struct GateResult {
    pub passed: bool,
    pub failures: Vec<String>,
    pub warnings: Vec<String>,
    pub summary: String,
}

/// Evaluate the release gate.
///
/// Returns a `GateResult` indicating whether the adapter is release-ready.
/// The gate fails closed: any structural error, rejected deviation, or
/// unresolved proposed deviation causes failure.
pub fn evaluate() -> GateResult {
    let validation = validator::validate_registry();
    let mut failures = Vec::new();
    let mut warnings = Vec::new();

    // Gate 1: Registry structural integrity
    if !validation.is_valid() {
        for err in &validation.errors {
            failures.push(format!("Registry error: {}", err));
        }
    }

    // Gate 2: No rejected deviations
    let rejected: Vec<&Deviation> = REGISTRY
        .iter()
        .filter(|d| d.status == ApprovalStatus::Rejected)
        .collect();
    if !rejected.is_empty() {
        for dev in &rejected {
            failures.push(format!(
                "{}: REJECTED — adapter must be fixed to match expected behavior ({})",
                dev.id, dev.title
            ));
        }
    }

    // Gate 3: Proposed deviations block release
    let proposed: Vec<&Deviation> = REGISTRY
        .iter()
        .filter(|d| d.status == ApprovalStatus::Proposed)
        .collect();
    if !proposed.is_empty() {
        for dev in &proposed {
            failures.push(format!(
                "{}: PROPOSED — requires governance decision before release ({})",
                dev.id, dev.title
            ));
        }
    }

    // Gate 4: Every approved deviation must have linked scenarios
    let approved: Vec<&Deviation> = REGISTRY
        .iter()
        .filter(|d| d.status == ApprovalStatus::Approved)
        .collect();
    for dev in &approved {
        if dev.linked_scenarios.is_empty() {
            failures.push(format!(
                "{}: APPROVED deviation has no linked scenarios — cannot verify stability",
                dev.id
            ));
        }
    }

    // Collect informational warnings from validation
    for w in &validation.warnings {
        warnings.push(w.clone());
    }

    let passed = failures.is_empty();
    let summary = format!(
        "Release gate {}: {} approved, {} proposed, {} not-deviation, {} rejected, {} error(s)",
        if passed { "PASSED" } else { "FAILED" },
        validation.approved_count,
        validation.proposed_count,
        validation.not_deviation_count,
        validation.rejected_count,
        failures.len(),
    );

    GateResult {
        passed,
        failures,
        warnings,
        summary,
    }
}

/// Produce a concise failure summary for reviewers.
pub fn failure_report(result: &GateResult) -> String {
    if result.passed {
        return format!("{}\nNo blocking issues.", result.summary);
    }

    let mut report = format!("{}\n\nBlocking issues:\n", result.summary);
    for (i, failure) in result.failures.iter().enumerate() {
        report.push_str(&format!("  {}. {}\n", i + 1, failure));
    }

    if !result.warnings.is_empty() {
        report.push_str("\nWarnings:\n");
        for w in &result.warnings {
            report.push_str(&format!("  - {}\n", w));
        }
    }

    report
}
