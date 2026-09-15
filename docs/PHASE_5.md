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
