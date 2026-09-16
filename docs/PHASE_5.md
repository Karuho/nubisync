# Phase 5 — Local receive-only materialization

## Phase 5A — Read-only local reconciliation plan

Phase 5A introduces a fail-closed, metadata-only planner between the authoritative
selected-root remote catalog and the real local sync directory.

CLI surface:

`nubisync sync roots reconcile-plan --approve`

The command requires exactly one `receive_only` root, completed authoritative
snapshot + initial catch-up, a durable selected-root change cursor, and no
pending durable change window.

It reads only the durable remote catalog and local filesystem metadata. It never
requests provider/network data, reads file content, mutates SQLite, or creates,
removes, renames, truncates, or overwrites local filesystem entries.

Remote names are rejected when unsafe as local path components (`/`, `.`, `..`,
NUL). Duplicate remote IDs, duplicate sibling names, disconnected catalogs and
path collisions fail closed. The local scan rejects changed/symlinked roots,
symlink entries, non-UTF-8 names, unsupported filesystem object types, and scans
larger than 1,000,000 entries.

Only aggregate counts are printed. Local paths/names, remote names/IDs, cursors
and tokens remain private.

`READY_FOR_DIRECTORY_PHASE=yes` means no local-only entries or type conflicts
block the next non-destructive directory-creation phase. Phase 5A itself is
strictly read-only.

## Phase 5B — Supervised directory materialization

Phase 5B is the first intentional local filesystem mutation in NubiSync.

CLI surface:

`nubisync sync roots materialize-directories --approve`

It reuses the Phase 5A fail-closed preconditions and requires
`READY_FOR_DIRECTORY_PHASE=yes` semantics: no local-only entries and no type
conflicts. It performs no provider/network request and no database mutation.

Only remote folders are materialized. Files are never created, opened, read,
truncated, renamed, deleted or downloaded in this phase.

Directory targets are derived deterministically from the validated authoritative
remote catalog. Parent folders are created before children. Before every create,
the existing parent must be a real directory inside the configured canonical
sync root; symlinks and path escapes fail closed. Existing target directories are
accepted idempotently, while files/symlinks at target paths are conflicts.

Directories created by the current invocation are tracked. If creation or the
final reconciliation postcondition fails, NubiSync removes only those newly
created directories in reverse order. A rollback failure is surfaced explicitly.

The final postcondition requires every remote directory to exist locally as a
directory while the Phase 5A safety conditions remain true. Output contains only
aggregate counts; local names/paths and remote metadata remain private.

## Phase 5C1 — Explicit Drive read-only OAuth upgrade

Phase 5C1 adds the supervised command:

`nubisync auth google upgrade-readonly --approve`

The normal `auth google login` command remains metadata-only. The upgrade requests
`drive.readonly`, verifies the exact granted scope, verifies that the returned
Google subject matches the already configured account, and only then replaces
the refresh token in the OS keyring.

If scope, account identity, or refresh-token issuance is invalid, the existing
refresh token is left intact. The upgrade performs no SQLite mutation, no local
filesystem mutation, no Drive write, and no Drive file-content request.

Phase 5C1 only grants the capability required by the later supervised file
materialization phase; it does not download files itself.

## Phase 5C2 — Supervised single-file materialization

Phase 5C2 adds `nubisync sync roots materialize-file --approve`.

The command is intentionally limited to exactly one missing ordinary file. It
requires a ready receive-only selected root, no pending durable change window,
all remote directories already present locally, no local-only/type conflicts,
and no pre-existing unverified remote files.

The durable remote size must be known and at most 16 MiB. Drive content is
streamed with `alt=media` into a `create_new` temporary file in the target
folder. The byte count must exactly match durable metadata and the temporary
file is fsynced before promotion.

Promotion uses an atomic hard-link creation of the final pathname, providing
no-overwrite semantics if a local entry appears concurrently. The temp name is
removed and the parent directory is fsynced before success. Failures clean the
temp file; failures after promotion roll back only the file created by that
invocation.

No SQLite mutation or Drive write occurs. Existing local entries are never
overwritten, deleted, or renamed. Output exposes only aggregate counts/bytes,
not local names/paths, remote IDs/metadata, or OAuth token values.

Descriptor-relative/openat-style filesystem race hardening remains required
before unattended/background materialization.

## Phase 5C3 — Durable file materialization receipts

Phase 5C3 introduces SQLite schema v9 and durable SHA-256 materialization
receipts. A receipt binds a selected-root remote file to its expected relative
path, durable byte size, SHA-256 digest, and materialization timestamp.

Future `materialize-file` runs hash bytes while streaming and persist the
receipt only after no-overwrite promotion and postconditions succeed. Selected-
root catalog upserts/deletes invalidate affected receipts transactionally; the
invalidation is subtree-aware so folder moves/renames also invalidate descendant
file receipts.

For the file downloaded in Phase 5C2, `sync roots verify-file --approve` hashes
the existing local file and streams the current remote blob into a SHA-256 sink.
It requires exact byte count and digest equality before recording the receipt.
The command does not modify the filesystem and performs no Drive write.

`reconcile-plan` remains metadata-only and therefore still reports existing
files as unverified by that planner while separately exposing the durable
materialization receipt count.
