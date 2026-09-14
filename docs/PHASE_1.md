# Phase 1 — Core and OAuth Foundation

## Objective

Establish a buildable, testable provider-neutral core before any real Google Drive traffic occurs.

## Implemented

- Rust toolchain pinned to 1.98.1
- Rust 2024 workspace
- provider-neutral account/change contracts
- opaque/redacted change and continuation tokens
- secret-store interface with a test-only in-memory implementation
- Google desktop OAuth authorization-request generator
- PKCE S256
- loopback callback restricted to `127.0.0.1`
- required Google identity and full-Drive scopes represented explicitly
- SQLite schema version 1
- account and provider-cursor persistence
- conservative synchronization divergence planner
- typed telemetry schema without arbitrary metadata maps
- daemon entry-point placeholder

## Intentionally not implemented

- real Google login
- token exchange
- token refresh
- Linux Secret Service integration
- Drive API requests
- file upload/download
- filesystem watchers
- destructive sync operations
- deletion semantics
- rename coalescing
- telemetry network transport
- updater
- GUI

## Exit criteria

Phase 1 closes only when all of the following pass:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `git diff --check`
- no committed real secrets or OAuth tokens
