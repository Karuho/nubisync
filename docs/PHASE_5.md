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
