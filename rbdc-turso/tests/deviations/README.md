# Deviation Governance

## Overview

This directory contains the deviation registry and validator for the Turso
adapter. A "deviation" is any behavior where the Turso adapter intentionally
differs from common rbdc adapter conventions.

## Registry

The deviation registry is defined in `registry.rs` as compile-time Rust
constants. Each deviation record includes:

| Field | Description |
|-------|-------------|
| `id` | Unique identifier (DEV-###) |
| `title` | Short description |
| `linked_scenarios` | Test scenario IDs this deviation affects |
| `status` | Approval status (see below) |
| `summary` | Technical description of the behavioral difference |
| `user_impact` | How this affects adapter users |
| `rationale` | Why this status was chosen |

## Approval Statuses

- **Approved** — Deviation is accepted. The adapter intentionally behaves
  differently and this is documented. A dedicated test verifies the behavior
  remains stable.
- **Proposed** — Behavior differs but no governance decision has been made.
  **Blocks release promotion.**
- **Rejected** — The adapter must be fixed to match expected behavior.
  Rejected deviations in the registry cause validation failure.
- **NotADeviation** — Investigation confirmed the behavior is actually parity.
  Documented for awareness.

## Current Registry Summary

| ID | Title | Status |
|----|-------|--------|
| DEV-001 | column_type returns empty string (no static type info) | Approved |
| DEV-002 | Boolean round-trip as integer | Not a Deviation |
| DEV-003 | JSON text decoding heuristic | Not a Deviation |
| DEV-004 | last_insert_id stored as Value::U64 | **Proposed** |

## Release Checklist

Before promoting rbdc-turso to a release:

- [ ] All deviations in the registry have status `Approved` or `NotADeviation`
- [ ] No `Proposed` deviations remain (currently: DEV-004 is proposed)
- [ ] No `Rejected` deviations exist
- [ ] All approved deviations have dedicated tests that pass
- [ ] **OPEN**: Approver role for deviation governance has not been assigned

## Deferred Governance Decision: Approver Role

> **Status: DEFERRED — no approver role has been designated.**

The deviation governance process requires someone with authority to approve
or reject proposed deviations. This role has not been assigned. Until it is:

1. Proposed deviations (currently DEV-004) remain in `Proposed` status
2. The release gate will flag these as warnings, not hard failures
3. The adapter can be used for development and testing but should not be
   promoted to production release without resolving proposed deviations

This is an explicit, intentional gap. The decision on who approves deviations
(crate maintainer, project lead, committee, etc.) is a product/governance
question outside the scope of this adapter implementation.

**Cross-reference**: See `kitty-specs/001-turso-backend-parity-rollout/spec.md`
for the full specification context.
