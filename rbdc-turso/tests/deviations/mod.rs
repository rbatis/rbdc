//! Deviation governance for the Turso adapter.
//!
//! This module defines the deviation registry and validator. A "deviation" is
//! any behavior where the Turso adapter intentionally differs from the rbdc
//! trait contract expectations or from common adapter conventions.
//!
//! Each deviation must have:
//! - A unique ID (DEV-###)
//! - A linked scenario/test ID
//! - An approval status (approved, proposed, rejected, not_a_deviation)
//! - A documented rationale and user impact assessment

pub mod registry;
pub mod validator;
