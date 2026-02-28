//! Deviation registry: canonical record of all known behavioral differences.
//!
//! Each entry is a compile-time constant, giving type-safe validation without
//! needing external file parsing.

/// Approval status for a deviation record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalStatus {
    /// Deviation is approved — the adapter intentionally behaves differently
    /// and this is acceptable per governance.
    Approved,
    /// Deviation is proposed — behavior differs but approval is pending.
    Proposed,
    /// Deviation was rejected — the adapter must match expected behavior.
    Rejected,
    /// Not actually a deviation — investigation confirmed parity.
    NotADeviation,
}

/// A deviation record describing a known behavioral difference.
#[derive(Debug, Clone)]
pub struct Deviation {
    /// Unique deviation identifier (e.g. "DEV-001").
    pub id: &'static str,
    /// Short human-readable title.
    pub title: &'static str,
    /// Linked scenario or test IDs (e.g. "PAR-008").
    pub linked_scenarios: &'static [&'static str],
    /// Current approval status.
    pub status: ApprovalStatus,
    /// Description of the difference.
    pub summary: &'static str,
    /// Impact on users of the adapter.
    pub user_impact: &'static str,
    /// Rationale for the approval decision (or why it's pending).
    pub rationale: &'static str,
}

/// The canonical deviation registry.
///
/// All known deviations must be listed here. The validator checks that:
/// 1. Every deviation has a unique ID
/// 2. Every deviation has at least one linked scenario
/// 3. No two deviations share the same linked scenario
/// 4. Proposed deviations block release gates
pub static REGISTRY: &[Deviation] = &[
    // ── DEV-001 ──────────────────────────────────────────────────────
    Deviation {
        id: "DEV-001",
        title: "column_type reports runtime value type, not declared schema type",
        linked_scenarios: &["PAR-008", "PAR-014"],
        status: ApprovalStatus::Approved,
        summary: "MetaData::column_type() returns runtime value type names \
                  (INTEGER, REAL, TEXT, BLOB, NULL) derived from libsql::ValueType, \
                  not declared schema types. Custom type aliases (BOOLEAN, DATETIME) \
                  are not exposed — only the 5 base storage classes.",
        user_impact: "Code matching specific type aliases (BOOLEAN, DATETIME) sees only \
                      base storage classes (INTEGER, TEXT, etc.). For empty result sets, \
                      type info is unavailable.",
        rationale: "The rbdc MetaData::column_type() trait contract does not specify \
                    whether declared or runtime types are returned. Turso's native async \
                    API exposes value-level types via ValueType enum, not schema-level \
                    type declarations. This is inherent to the native API design.",
    },
    // ── DEV-002 ──────────────────────────────────────────────────────
    Deviation {
        id: "DEV-002",
        title: "Boolean values round-trip as integers",
        linked_scenarios: &["PAR-007"],
        status: ApprovalStatus::NotADeviation,
        summary: "Value::Bool(true) is encoded as INTEGER 1, read back as Value::I64(1). \
                  This matches the standard adapter convention.",
        user_impact: "None. Both Turso and other adapters behave identically.",
        rationale: "Investigation confirmed parity. Documented for awareness only.",
    },
    // ── DEV-003 ──────────────────────────────────────────────────────
    Deviation {
        id: "DEV-003",
        title: "JSON text decoding heuristic",
        linked_scenarios: &["PAR-006"],
        status: ApprovalStatus::NotADeviation,
        summary: "Text values matching is_json_string() (starts with {/[ or equals 'null') \
                  are attempted as JSON parse. If parse fails, returned as String.",
        user_impact: "None. Heuristic matches standard adapter behavior.",
        rationale: "Investigation confirmed parity. Same heuristic implemented.",
    },
    // ── DEV-004 ──────────────────────────────────────────────────────
    Deviation {
        id: "DEV-004",
        title: "last_insert_id stored as Value::U64",
        linked_scenarios: &["PAR-010", "PAR-011"],
        status: ApprovalStatus::Proposed,
        summary: "ExecResult.last_insert_id is Value::U64 (cast from i64 rowid). \
                  Other adapters may use Value::I64 or Value::U64 — the ExecResult \
                  type specifies Value, not a concrete integer type.",
        user_impact: "Code that pattern-matches specifically on Value::I64 for \
                      last_insert_id will not match. Should use numeric extraction \
                      methods instead.",
        rationale: "Requires governance decision. The rowid is conceptually unsigned \
                    (always positive), but i64 is the underlying storage type. \
                    Standardizing across adapters would be ideal but is outside \
                    this crate's scope.",
    },
];
