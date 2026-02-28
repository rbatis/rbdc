//! Deviation registry validator.
//!
//! Validates structural integrity of the registry and enforces governance rules:
//! - Every deviation ID is unique
//! - Every deviation has linked scenarios
//! - No scenario is claimed by multiple deviations
//! - Proposed deviations are flagged as release-blocking
//! - Rejected deviations must not exist (should be removed or fixed)

use super::registry::{ApprovalStatus, Deviation, REGISTRY};
use std::collections::{HashMap, HashSet};

/// Result of registry validation.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ValidationResult {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub approved_count: usize,
    pub proposed_count: usize,
    pub not_deviation_count: usize,
    pub rejected_count: usize,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns true if the registry is release-ready: valid and no proposed
    /// deviations remaining.
    pub fn is_release_ready(&self) -> bool {
        self.is_valid() && self.proposed_count == 0
    }
}

/// Validate the deviation registry for structural integrity and governance rules.
pub fn validate_registry() -> ValidationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut seen_ids: HashSet<&str> = HashSet::new();
    let mut scenario_owners: HashMap<&str, &str> = HashMap::new();
    let mut approved_count = 0usize;
    let mut proposed_count = 0usize;
    let mut not_deviation_count = 0usize;
    let mut rejected_count = 0usize;

    for dev in REGISTRY {
        // Check unique ID
        if !seen_ids.insert(dev.id) {
            errors.push(format!("Duplicate deviation ID: {}", dev.id));
        }

        // Check ID format
        if !dev.id.starts_with("DEV-") {
            errors.push(format!(
                "{}: ID must start with 'DEV-', got '{}'",
                dev.id, dev.id
            ));
        }

        // Check linked scenarios
        if dev.linked_scenarios.is_empty() {
            errors.push(format!(
                "{}: must have at least one linked scenario",
                dev.id
            ));
        }

        // Check scenario uniqueness across deviations
        for scenario in dev.linked_scenarios {
            if let Some(owner) = scenario_owners.get(scenario) {
                errors.push(format!(
                    "Scenario {} is claimed by both {} and {}",
                    scenario, owner, dev.id
                ));
            } else {
                scenario_owners.insert(scenario, dev.id);
            }
        }

        // Check non-empty fields
        if dev.title.is_empty() {
            errors.push(format!("{}: title must not be empty", dev.id));
        }
        if dev.summary.is_empty() {
            errors.push(format!("{}: summary must not be empty", dev.id));
        }
        if dev.user_impact.is_empty() {
            errors.push(format!("{}: user_impact must not be empty", dev.id));
        }
        if dev.rationale.is_empty() {
            errors.push(format!("{}: rationale must not be empty", dev.id));
        }

        // Count by status
        match dev.status {
            ApprovalStatus::Approved => approved_count += 1,
            ApprovalStatus::Proposed => {
                proposed_count += 1;
                warnings.push(format!(
                    "{}: PROPOSED — blocks release until resolved ({})",
                    dev.id, dev.title
                ));
            }
            ApprovalStatus::NotADeviation => not_deviation_count += 1,
            ApprovalStatus::Rejected => {
                rejected_count += 1;
                errors.push(format!(
                    "{}: REJECTED deviation still in registry — must be \
                     removed or the adapter must be fixed to match expected behavior",
                    dev.id
                ));
            }
        }
    }

    ValidationResult {
        errors,
        warnings,
        approved_count,
        proposed_count,
        not_deviation_count,
        rejected_count,
    }
}

/// Look up a deviation by ID.
pub fn find_deviation(id: &str) -> Option<&'static Deviation> {
    REGISTRY.iter().find(|d| d.id == id)
}

/// Look up a deviation by linked scenario ID.
pub fn find_by_scenario(scenario: &str) -> Option<&'static Deviation> {
    REGISTRY
        .iter()
        .find(|d| d.linked_scenarios.contains(&scenario))
}

/// Return all deviations with the given approval status.
pub fn filter_by_status(status: ApprovalStatus) -> Vec<&'static Deviation> {
    REGISTRY.iter().filter(|d| d.status == status).collect()
}
