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

## Phase 2 additions

### Loopback callback substitution

Mitigations:

- listener binds only to `127.0.0.1`
- OS selects an ephemeral port
- callback scheme, host, port and path must match exactly
- OAuth `state` must match exactly
- only HTTP GET is accepted by the development callback handler

### Overbroad OAuth authorization during early development

Mitigation:

- the first live account connection uses `drive.metadata.readonly`
- `drive.readonly` is deferred until file downloads exist
- full `drive` is deferred until remote write operations exist and are tested

### Refresh-token theft from SQLite

Mitigation:

- refresh tokens are never inserted into SQLite
- Linux stores them through Secret Service/keyring
- SQLite stores only synchronization metadata and opaque provider cursors

### Accidental file inventory during account probing

Mitigation:

- Phase 2 uses `about.get` and `changes.getStartPageToken`
- it does not call `files.list`
- field masks are explicit for the Drive account probe
