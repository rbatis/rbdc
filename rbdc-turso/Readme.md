# rbdc-turso

Turso/libSQL async database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

This crate provides an async Turso database backend for rbdc, using the [libsql](https://crates.io/crates/libsql) Rust SDK as the underlying client library. It supports both remote Turso databases (with auth tokens) and local/in-memory databases.

## Backend Selection at Startup

Backend choice is **fixed at initialization time**. The application selects Turso by wiring `TursoDriver` during startup configuration.

- **No runtime backend switching** - the active backend cannot be changed while the application is serving traffic.
- **No automatic fallback** - if Turso becomes unavailable, requests fail rather than silently falling back.

Changes to backend selection take effect only after a deploy/restart cycle.

## Usage

```rust
use rbdc_turso::{TursoDriver, TursoConnectOptions};
use rbdc::db::Driver;

// At startup: wire the Turso driver
let driver = TursoDriver {};

// In-memory (local, no network)
let mut conn = driver.connect("turso://:memory:").await?;

// Remote Turso database
let mut conn = driver.connect("turso://?url=libsql://your-db.turso.io&token=YOUR_TOKEN").await?;

// Local file database
let mut conn = driver.connect("turso://path/to/local.db").await?;
```

## Traits Implemented

This crate implements the standard rbdc driver traits:

- `rbdc::db::Driver` - via `TursoDriver`
- `rbdc::db::ConnectOptions` - via `TursoConnectOptions`
- `rbdc::db::Connection` - via `TursoConnection`
- `rbdc::db::Row` - via `TursoRow`
- `rbdc::db::MetaData` - via `TursoMetaData`
- `rbdc::db::Placeholder` - via `TursoDriver` (uses `?` placeholders)

## Feature Plan

For the full feature specification, parity requirements, and deviation governance process, see:

- `kitty-specs/001-turso-backend-parity-rollout/spec.md`
- `kitty-specs/001-turso-backend-parity-rollout/plan.md`
