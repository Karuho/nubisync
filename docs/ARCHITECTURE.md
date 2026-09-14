# NubiSync Architecture

## Product boundary

NubiSync is a Linux-first native cloud synchronization client.

The first provider is Google Drive, but provider-specific code must remain behind a provider abstraction so the synchronization engine does not depend directly on Google Drive concepts.

## High-level components

- `nubisync-core`: shared domain types and invariants
- `nubisync-drive`: Google Drive provider implementation
- `nubisync-storage`: SQLite metadata, journals and migrations
- `nubisync-auth`: installed-app OAuth and local credential storage
- `nubisync-sync`: synchronization planner and executor
- `nubisync-daemon`: long-running background process
- `nubisync-telemetry`: optional telemetry client and public event schemas
- `apps/desktop`: desktop UI; initially planned with Tauri
- `tests`: cross-component and integration tests

## Fundamental invariant

The GUI does not synchronize files.

The daemon and synchronization engine own synchronization behavior. CLI and GUI clients are interfaces over the same core.

## Local-first filesystem model

NubiSync does not expose Google Drive through FUSE.

The synchronized folder contains ordinary local files. Desktop applications should be able to list, stat, search, thumbnail and open files without waiting for Google Drive API calls.

## Provider boundary

The synchronization engine consumes a provider-neutral interface.

Provider implementations are responsible for:

- authentication
- remote metadata retrieval
- change journal consumption
- upload/download operations
- remote create/update/delete/move semantics
- provider-specific rate-limit and retry mapping

## Google Drive Phase 1 boundary

Initial implementation targets:

- one Google account
- My Drive
- regular files and folders
- create/update/delete/rename/move
- incremental changes using Drive change tokens
- resumable transfers

Initially excluded:

- Shared Drives
- multiple simultaneous accounts
- symbolic links
- Google Docs/Sheets/Slides native document export semantics
- other cloud providers

## Reliability

All metadata transitions must be transactional.

Remote changes and local filesystem events must be journaled before destructive application where practical.

Interrupted synchronization must resume safely after process or machine restart.

A failure in telemetry, update infrastructure or NubiSync-operated services must not stop Google Drive synchronization.

## Phase 1 implementation note

The initial provider contract intentionally exposes only account identity and incremental change-stream primitives.

Upload, download, deletion and rename APIs are deferred until their exact transactional semantics are designed and tested. This prevents the first Google Drive adapter from defining accidental semantics that would later leak into every provider.

OAuth credentials are handled behind a separate secret-store boundary. SQLite is for synchronization metadata; it is not the intended storage location for OAuth refresh tokens.

Telemetry uses typed event variants rather than arbitrary key/value metadata so filenames, local paths and provider object identifiers cannot be casually attached to diagnostic events.

## Phase 2 authentication boundary

The Google provider separates three authorization capabilities:

- metadata read-only
- content read-only
- full synchronization

Application code must request the weakest capability that satisfies the currently implemented operation.

The development CLI is an integration harness, not a second synchronization engine. It orchestrates the same auth, storage and provider crates that the desktop application will later consume.

Refresh tokens live behind `SecretStore` and the production Linux implementation uses the desktop credential service. SQLite stores account metadata and provider cursors only.
