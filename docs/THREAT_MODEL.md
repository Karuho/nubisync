# Threat Model — Initial Baseline

## Assets

High-value assets include:

- Google OAuth refresh tokens
- temporary access tokens
- local user files
- remote cloud files
- synchronization metadata
- update-signing trust material
- diagnostic identity mappings

## Initial threats

### OAuth authorization-code interception

Mitigations:

- desktop loopback callback
- bind only to loopback
- fresh PKCE S256 verifier per authorization
- fresh state per authorization
- exact state validation
- short-lived authorization session

### Credential disclosure through logs

Mitigations:

- secret values must not implement revealing `Debug`
- opaque provider cursors are redacted from `Debug`
- OAuth state and PKCE verifier are redacted
- telemetry schema accepts stable diagnostic codes instead of arbitrary error text

### Silent destructive conflict resolution

Mitigation:

- concurrent local and remote divergence is a conflict
- no overwrite-on-conflict behavior in Phase 1
- deletion semantics are deferred until journal invariants exist

### Telemetry data leakage

Mitigations:

- typed telemetry events
- no arbitrary metadata map
- no filename/path/Drive-ID fields
- telemetry remains outside the core synchronization path

### Compromised NubiSync operations infrastructure

Mitigation:

- NubiSync-operated infrastructure must not possess users' Drive refresh tokens
- core Google Drive synchronization must not depend on telemetry infrastructure

## Deferred threats

Later phases must address:

- malicious remote filenames and path traversal
- symlink attacks
- TOCTOU filesystem races
- partial file replacement
- rollback and replay of remote changes
- malicious update manifests
- revoked Google credentials
- rate-limit amplification
- SQLite corruption and recovery
- local malware reading user-session credentials
